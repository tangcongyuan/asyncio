#! /usr/bin/env python3.6

import asyncio
import contextlib
import logging
import threading
import time

class Proxy:
    async def worker(self, queue: asyncio.Queue) -> None:
        """ This forever-running worker needs to be handled when exiting asyncio loop. """
        while True:
            time.sleep(0.5)
            logging.debug(f'queue size: {queue.qsize()}')
            msg = await queue.get()
            logging.info(f'msg processed: {msg}')
            queue.task_done()


    def background_loop(self, new_loop: asyncio.AbstractEventLoop,
                        queue: asyncio.Queue) -> None:
        """ Main function in new thread. """
        asyncio.set_event_loop(new_loop)
        logging.debug(f'background loop running, thread id: {threading.get_ident()}')
        asyncio.ensure_future(self.worker(queue))
        # loop runs forever
        new_loop.run_forever()

    def __init__(self, loop: asyncio.AbstractEventLoop=None,
                       host: str='127.0.0.1',
                       port: int=1688,
                       debug: bool=False) -> None:
        logging.debug(f'Creating Proxy from thread: {threading.get_ident()}')
        self._HOST_IP = host
        self._PORT = port
        self._loop = asyncio.new_event_loop() if not loop else loop
        self._queue = asyncio.Queue(loop=self._loop)
        self._thread = threading.Thread(target=self.background_loop,
                                        args=(self._loop, self._queue))


    def _send(self, queue: asyncio.Queue, data: str) -> None:
        queue.put_nowait(data)


    def send(self, data: str='Hello world') -> None:
        if not self._loop.is_running():
            logging.debug('Starting thread.')
            self._thread.start()
        logging.debug('Sending data.')
        self._loop.call_soon_threadsafe(self._send, self._queue, data)
        logging.debug('Consider data sent!')


    async def exit(self, new_loop: asyncio.AbstractEventLoop,
                         queue: asyncio.Queue) -> None:
        """ Stop the loop gracefully. """
        # Wait until all messages in queue are processed
        logging.debug(f'queue: {queue}')
        await queue.join()
        for task in asyncio.Task.all_tasks(loop=new_loop):
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                logging.debug(f'task: {task}')
                await task
        logging.debug('Stop new_loop')
        new_loop.stop()


    def stop(self) -> None:
        logging.debug(f'Terminating new thread and loop from thread: {threading.get_ident()}')
        asyncio.ensure_future(self.exit(self._loop, self._queue),
                              loop=self._loop)
        self._thread.join()
        logging.debug('All done.')

if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s [%(levelname)s]: %(message)s', level=logging.DEBUG)
    logging.addLevelName(logging.DEBUG, "\033[0;32m%s\033[0m" % logging.getLevelName(logging.DEBUG))
    logging.debug(f'Main thread id: {threading.get_ident()}')

    pro = Proxy()
    for index in range(10):
        pro.send(f'Index: {index} says hello.')
    time.sleep(3)
    pro.stop()