#! /usr/bin/env python3.6

import asyncio
import contextlib
import logging
import threading
import time


# Singleton class where class variables will only be instantiated once.
class Proxy:
    # Class variable
    _host: str
    _port: int
    _loop: asyncio.AbstractEventLoop = None
    _queue: asyncio.Queue = None
    _thread: threading.Thread = None


    @staticmethod
    async def worker() -> None:
        """ This forever-running worker needs to be handled when exiting asyncio loop. """
        while True:
            logging.debug(f'worker running...')
            logging.debug(f'queue size: {Proxy._queue.qsize()}')
            msg = await Proxy._queue.get()
            # time.sleep(0.5)
            logging.info(f'msg processed: {msg}')
            Proxy._queue.task_done()


    @staticmethod
    def background_loop() -> None:
        """ Main function in new thread. """
        asyncio.set_event_loop(Proxy._loop)
        logging.debug(f'background loop running in child thread, thread id: {threading.get_ident()}')
        asyncio.ensure_future(Proxy.worker())
        # loop runs forever
        Proxy._loop.run_forever()


    def __new__(self):
        if not hasattr(self, 'instance'):
            logging.debug(f'Creating Proxy from thread: {threading.get_ident()}')
            Proxy._loop = asyncio.new_event_loop() if not Proxy._loop else Proxy._loop
            Proxy._queue = asyncio.Queue(loop=Proxy._loop) \
                                if not Proxy._queue else Proxy._queue
            Proxy._thread = threading.Thread(target=Proxy.background_loop) \
                                                if not Proxy._thread else Proxy._thread
            self.instance = super().__new__(self)
        else:
            logging.debug(f'Proxy is already created; no action done.')
        return self.instance


    def __init__(self, host: str='127.0.0.1',
                       port: int=1688,
                       debug: bool=False) -> None:
        Proxy._host = host if not Proxy._host else Proxy._host
        Proxy._port = port if not Proxy._port else Proxy._port


    @staticmethod
    def _send(data: str) -> None:
        logging.debug(f'Sending data in child thread: {threading.get_ident()}')
        Proxy._queue.put_nowait(data)


    @staticmethod
    def send(data: str) -> None:
        if not Proxy._loop.is_running():
            logging.debug('Starting thread.')
            Proxy._thread.start()
        logging.debug('Sending data from main thread in unblocking fashion.')
        Proxy._loop.call_soon_threadsafe(Proxy._send, data)


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
    # time.sleep(3)
    Proxy.stop()
