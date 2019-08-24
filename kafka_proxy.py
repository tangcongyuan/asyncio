#! /usr/bin/env python3

import asyncio
import contextlib
import io
import logging
import threading
import time
from aiokafka import AIOKafkaProducer


logger = logging.getLogger(__name__)


class KafkaProxy:
    def __init__(self, host: str='127.0.0.1',
                       port: int=9092,
                       debug: bool=False) -> None:
        """ Every instance of this class will have its own thread and queue. """
        self._loop   = asyncio.new_event_loop()
        self._queue  = asyncio.Queue(loop=self._loop)
        self._thread = threading.Thread(target=self._background_loop)

        self._host = host
        self._port = port
        self._bootstrap_servers = f'{self._host}:{self._port}'
        self._producer = AIOKafkaProducer(
            loop=self._loop,
            bootstrap_servers=self._bootstrap_servers
        )
        self._producer_started = False


    def _background_loop(self) -> None:
        """ Main function in new thread. """
        asyncio.set_event_loop(self._loop)
        logger.debug(f'background loop running in child thread, thread id: {threading.get_ident()}')
        asyncio.ensure_future(self._worker())
        # loop runs forever
        self._loop.run_forever()


    def _enqueue(self, data: str) -> None:
        logger.debug(f'Sending data in child thread: {threading.get_ident()}')
        self._queue.put_nowait(data)


    def send(self, topic: str, data: str) -> None:
        """ Schedule data to be sent by adding it to the queue. """
        if not self._loop.is_running():
            logger.debug('Starting thread.')
            self._thread.start()
        logger.debug('Sending data from main thread in unblocking fashion.')
        self._loop.call_soon_threadsafe(self._enqueue, (topic, data))


    def stop(self) -> None:
        logger.debug(f'Terminating new thread and loop from thread: {threading.get_ident()}')
        logger.debug(f'loop: {self._loop}')
        asyncio.run_coroutine_threadsafe(self._exit(), loop=self._loop)
        self._thread.join()
        logger.debug('All done.')


    async def _send_bytes(self, topic: str, msg: bytes):
        """ Sending through AIOKafka """
        if not self._producer_started:
            await self._producer.start()
            self._producer_started = True
        logger.debug(f'AIOKafka sending bytes.')
        await self._producer.send_and_wait(topic=topic, value=msg)


    async def _worker(self) -> None:
        """ This forever-running worker needs to be handled when exiting asyncio loop. """
        while True:
            logger.debug(f'worker running...')
            logger.debug(f'queue size: {self._queue.qsize()}')
            topic, msg = await self._queue.get()
            # Hopefully msg is just a string now.
            b_msg = msg.encode('utf-8')
            logger.debug(f'After encoding: {b_msg}')
            # Based on different scenario, change this method
            await self._send_bytes(topic, b_msg)
            logger.info(f'msg sent: {b_msg}')
            self._queue.task_done()


    async def _exit(self) -> None:
        """ Stop the loop gracefully. """
        # Wait until all messages in queue are processed
        logger.debug(f'Waiting for queue to join.')
        logger.debug(f'Asycio Queue has {self._queue.qsize()} items left.')
        await self._queue.join()

        # Wait for all pending messages to be delivered or expire.
        await self._producer.stop()

        for task in asyncio.Task.all_tasks(loop=self._loop):
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                logger.debug(f'task: {task}')
                await task
        logger.debug('Stop loop')
        self._loop.stop()


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s [%(levelname)s]: %(message)s', level=logging.DEBUG)
    logging.addLevelName(logging.DEBUG, "\033[0;32m%s\033[0m" % logging.getLevelName(logging.DEBUG))
    logging.addLevelName(logging.ERROR, "\033[0;31m%s\033[0m" % logging.getLevelName(logging.ERROR))
    logger.debug(f'Main thread id: {threading.get_ident()}')

    topic = 'new_topic'
    pro1 = KafkaProxy()
    pro2 = KafkaProxy()
    for index in range(10):
        pro1.send(topic, f'KafkaProxy1 says hello with index: {index}')
        pro2.send(topic, f'KafkaProxy2 says hello with index: {index}')
    # time.sleep(3)
    pro1.stop()
    pro2.stop()
