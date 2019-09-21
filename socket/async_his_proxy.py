#! /usr/bin/env python3

import asyncio
import contextlib
import logging
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
        self._incoming_q: asyncio.Queue = asyncio.Queue(maxsize=1, loop=self._loop) # Stores incoming prescriptions
        self._outgoing_q: asyncio.Queue = asyncio.Queue(maxsize=0, loop=self._loop) # Stores outgoing detected medicines
        self._msg_size: int = 1024 # Should be configured by external file
        self._reader, self._writer = None, None

        self._logger.debug(f'Thread starting.')
        self._thread.start()


    def __del__(self):
        self._logger.debug('Close the loop.')
        self._loop.close()
        self._thread.join()


    async def _get_incoming_q(self):
        items = list(self._incoming_q._queue)
        self._incoming_q._queue.clear()
        logger.debug(f'Incoming queue content: {items}')
        return items


    @property
    def incoming_q(self):
        future = asyncio.run_coroutine_threadsafe(
            self._get_incoming_q(),
            self._loop
        )
        return future.result()


    def send(self, data: bytes) -> None:
        future = asyncio.run_coroutine_threadsafe(
            self._outgoing_q.put(data),
            self._loop
        )
        future.result()
        self._logger.debug(f'Data cached in outgoing queue, queue size: ' \
                           f'{self._outgoing_q.qsize()}')


    async def _send(self):
        """ Send data to HIS. """
        while not self._should_stop:
            self._logger.debug(f'Calling async send.')
            item = await self._outgoing_q.get()
            self._logger.debug(f'Getting item from outgoing queue: {item}')
            if not item: break
            self._logger.debug(f'Writing item: {item}')
            self._writer.write(item)
            self._outgoing_q.task_done()


    async def _read(self) -> None:
        """ Read data from HIS and store it to incoming queue. """
        while not self._should_stop:
            self._logger.debug(f'Calling async read.')
            data = await self._reader.read(self._msg_size)
            self._logger.info('Received: %r' % data)
            if not data: break

            try:
                self._incoming_q.put_nowait(data)
                self._logger.debug(f'Adding received data to incoming queue, queue size: {self._incoming_q.qsize()}')
            except asyncio.QueueFull:
                self._logger.error(f'self.incoming_queue is full; this should not happen!')


    def _background_loop(self) -> None:
        """ Main function in new thread. """
        self._logger.debug(f'Background loop running in child thread, thread id: {threading.get_ident()}')
        con = asyncio.ensure_future(self._connect(), loop=self._loop)
        self._loop.run_until_complete(con)
        self._logger.debug('Connection established.')
        future = asyncio.gather(
            self._read(),
            self._send(),
            loop=self._loop,
            return_exceptions=True
        )
        self._loop.run_forever()

        self._logger.debug('Background loop done.')


    async def _connect(self) -> None:
        """ Write from outgoing queue to HIS; Read from HIS to incoming queue. """
        self._logger.debug('Establishing TCP connection.')
        self._reader, self._writer = await asyncio.open_connection(
            self._host,
            self._port,
            loop=self._loop
        )
        self._logger.debug('Communicating with HIS server.')


    async def _stop(self):
        tasks = asyncio.Task.all_tasks(loop=self._loop)
        for t in tasks:
            self._logger.debug(f'Canceling task: {t}.')
            t.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await t
        self._loop.stop()


    def stop(self) -> None:
        """ Out-facing API for terminating the whole proxy. """
        self._logger.debug(f'Terminating new thread and loop from thread: ' \
                           f'{threading.get_ident()}')
        asyncio.run_coroutine_threadsafe(self._stop(), loop=self._loop)

        self._logger.debug(f'All async functions done.')
        self._logger.debug(f'loop: {self._loop}')
        self._thread.join()
        self._logger.debug('Thread joined.')


# Use this singleton instead of initiating new ones.
proxy = AHisProxy()


# For testing only
if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s [%(levelname)s]: %(message)s',
        level=logging.DEBUG)
    logging.addLevelName(
        logging.DEBUG,
        "\033[0;32m%s\033[0m" % logging.getLevelName(logging.DEBUG))
    logging.addLevelName(
        logging.ERROR,
        "\033[0;31m%s\033[0m" % logging.getLevelName(logging.ERROR))
    logger.debug(f'Main thread id: {threading.get_ident()}')

    from datetime import datetime
    import time
    for index in range(10):
        data = f'[{str(datetime.now())}]: Hello proxy {index}.'
        proxy.send(data.encode())
        # Current server only echo data back; meaning client receives data after sending one.
        logger.debug(f'Incoming queue: {proxy.incoming_q}')
    time.sleep(3.0)
    proxy.stop()
