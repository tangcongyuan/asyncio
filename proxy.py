#! /usr/bin/env python3.6

import asyncio
import contextlib
import logging
import threading
import time

class Proxy:
    _loop: asyncio.AbstractEventLoop = None
    _queue: asyncio.Queue = None
    _thread: threading.Thread = None


    @staticmethod
    # async def worker(queue: asyncio.Queue) -> None:
    async def worker(queue: asyncio.Queue) -> None:
        """ This forever-running worker needs to be handled when exiting asyncio loop. """
        while True:
            logging.debug(f'worker running...')
            logging.debug(f'queue size: {queue.qsize()}')
            msg = await queue.get()
            logging.info(f'msg processed: {msg}')
            queue.task_done()


    @staticmethod
    def background_loop(new_loop: asyncio.AbstractEventLoop,
                        queue: asyncio.Queue) -> None:
        """ Main function in new thread. """
        asyncio.set_event_loop(new_loop)
        logging.debug(f'background loop running, thread id: {threading.get_ident()}')
        asyncio.ensure_future(Proxy.worker(queue))
        # loop runs forever
        new_loop.run_forever()


    def __init__(self, loop: asyncio.AbstractEventLoop=None,
                       host: str='127.0.0.1',
                       port: int=1688,
                       debug: bool=False) -> None:
        logging.debug(f'Creating Proxy from thread: {threading.get_ident()}')
        self._HOST_IP = host
        self._PORT = port
        Proxy._loop = asyncio.new_event_loop() if not Proxy._loop else Proxy._loop
        Proxy._queue = asyncio.Queue(loop=Proxy._loop) \
                            if not Proxy._queue else Proxy._queue
        Proxy._thread = threading.Thread(target=Proxy.background_loop,
                                        args=(Proxy._loop, Proxy._queue)) \
                                            if not Proxy._thread else Proxy._thread


    def _send(self, queue: asyncio.Queue, data: str) -> None:
        queue.put_nowait(data)


    def send(self, data: str='Hello world') -> None:
        if not Proxy._loop.is_running():
            logging.debug('Starting thread.')
            Proxy._thread.start()
        logging.debug('Sending data.')
        Proxy._loop.call_soon_threadsafe(self._send, Proxy._queue, data)
        logging.debug('Consider data sent!')


    @staticmethod
    async def exit() -> None:
        """ Stop the loop gracefully. """
        # Wait until all messages in queue are processed
        logging.debug(f'Waiting for queue to join.')
        await Proxy._queue.join()
        for task in asyncio.Task.all_tasks(loop=Proxy._loop):
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                logging.debug(f'task: {task}')
                await task
        logging.debug('Stop loop')
        Proxy._loop.stop()


    @staticmethod
    def stop() -> None:
        logging.debug(f'Terminating new thread and loop from thread: {threading.get_ident()}')
        logging.debug(f'loop: {Proxy._loop}')
        asyncio.run_coroutine_threadsafe(Proxy.exit(), loop=Proxy._loop)
        Proxy._thread.join()
        logging.debug('All done.')

if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s [%(levelname)s]: %(message)s', level=logging.DEBUG)
    logging.addLevelName(logging.DEBUG, "\033[0;32m%s\033[0m" % logging.getLevelName(logging.DEBUG))
    logging.debug(f'Main thread id: {threading.get_ident()}')

    pro1 = Proxy()
    pro2 = Proxy()
    for index in range(10):
        pro1.send(f'Proxy1 says hello with index: {index}')
        pro2.send(f'Proxy2 says hello with index: {index}')
    time.sleep(3)
    Proxy.stop()
