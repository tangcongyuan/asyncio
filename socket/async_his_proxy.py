#! /usr/bin/env python3

import asyncio
import contextlib
import logging
import queue
import socket
import threading
import typing


logger = logging.getLogger(__name__)


class AHisProxy:

    def __init__(self,
                 host: str='127.0.0.1',
                 port: int=65432) -> None:
        self._logger: logging.Logger = logger
        self._host: str = host
        self._port: int = port
        self._thread: threading.Thread = threading.Thread(target=self._background_loop)
        self._loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
        self._should_stop: bool = False
        self._incoming_q: asyncio.Queue = queue.Queue() # Stores incoming prescriptions
        self._outgoing_q: asyncio.Queue = asyncio.Queue(loop=self._loop) # Stores outgoing detected medicines
        self._msg_size: int = 1024 # Should be configured by external file
        self._task_worker: asyncio.Task = None
        self._reader, self._writer = None, None
        

    def __del__(self):
        self._logger.debug('Close the loop.')
        self._loop.close()
        self._thread.join()


    @property
    def incoming_q(self):
        return list(self._incoming_q.queue)


    def send(self, data: bytes) -> None:
        if not self._thread.is_alive():
            self._logger.debug(f'Thread starting.')
            self._thread.start()

        self._outgoing_q.put_nowait(data)
        logger.debug(f'Data cached in outgoing queue: {data}')


    async def _send(self):
        """ Send data to HIS. """
        self._logger.debug(f'Calling async send.')
        item = await self._outgoing_q.get()
        self._logger.debug(f'Getting item from outgoing queue: {item}')
        if item:
            self._logger.debug(f'Writing item: {item}')
            self._writer.write(item)
            self._outgoing_q.task_done()
            self._logger.info('Send: %r' % item)


    async def _read(self) -> None:
        """ Read data from HIS and store it to incoming queue. """
        self._logger.debug(f'Calling async read.')
        data = await self._reader.read(self._msg_size)
        self._logger.info('Received: %r' % data)

        try:
            if data:
                self._incoming_q.put_nowait(data)
                self._logger.debug(f'Adding received data to incoming queue, data: {data}')
        except queue.Full:
            self._logger.error(f'self.incoming_queue is full; this should not happen!')


    def _background_loop(self) -> None:
        """ Main function in new thread. """
        self._logger.debug(f'background loop running in child thread, thread id: {threading.get_ident()}')
        # self._loop.run_until_complete(self._worker())
        # asyncio.set_event_loop(self._loop)
        asyncio.ensure_future(self._worker(), loop=self._loop)
        # loop runs forever
        self._loop.run_forever()


    async def _worker(self) -> None:
        """ Write from outgoing queue to HIS; Read from HIS to incoming queue. """
        self._logger.debug('Establishing TCP connection.')
        self._reader, self._writer = await asyncio.open_connection(self._host,
                                                                   self._port,
                                                                   loop=self._loop)
        self._logger.debug('Communicating with HIS server.')
        while True:
            await self._send()
            await self._read()


    def stop(self) -> None:
        """ Out-facing API for terminating the whole proxy. """
        self._logger.debug(f'Terminating new thread and loop from thread: {threading.get_ident()}')
        self._logger.debug(f'loop: {self._loop}')
        asyncio.run_coroutine_threadsafe(self._exit(), loop=self._loop)
        self._thread.join()
        self._logger.debug('Thread joined.')


    async def _exit(self):
        """ Stop the async loop. """
        await self._outgoing_q.join()
        self._logger.debug('Outgoing queue joined.')

        self._writer.close()
        self._logger.debug('Close the socket.')

        for task in asyncio.Task.all_tasks(loop=self._loop):
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                self._logger.debug(f'task: {task}.')
                await task

        self._logger.debug('Stop the loop.')
        self._loop.stop()


# Use this singleton instead of initiating new ones.
proxy = AHisProxy()


# For testing only
if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s [%(levelname)s]: %(message)s', level=logging.DEBUG)
    logging.addLevelName(logging.DEBUG, "\033[0;32m%s\033[0m" % logging.getLevelName(logging.DEBUG))
    logging.addLevelName(logging.ERROR, "\033[0;31m%s\033[0m" % logging.getLevelName(logging.ERROR))
    logger.debug(f'Main thread id: {threading.get_ident()}')

    from datetime import datetime
    import time
    for index in range(10):
        data = f'[{str(datetime.now())}]: Hello proxy {index}.'
        proxy.send(data.encode())
    time.sleep(3.0)
    proxy.stop()
