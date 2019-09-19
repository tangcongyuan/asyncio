#! /usr/bin/env python3.7
import asyncio
import logging
import random
import threading

logger: logging.Logger = logging.getLogger(__name__)
loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
queue: asyncio.Queue = asyncio.Queue(maxsize=1, loop=loop)
should_stop: bool = False


async def producer():
    index = 0
    while not should_stop:
        logger.debug(f'should_stop: {should_stop}')
        t = random.randint(0, 3)
        logger.debug(f'Producer sleeps for {t} seconds.')
        await asyncio.sleep(t)
        logger.info('Producer put payload to queue.')
        await queue.put(f'Payload {index}')
        index += 1
    logger.debug('Producer done.')


async def consumer():
    while not should_stop:
        logger.debug(f'should_stop: {should_stop}')
        t = random.randint(0, 5)
        logger.debug(f'Consumer sleeps for {t} seconds.')
        await asyncio.sleep(t)
        logger.debug(f'Queue size: {queue.qsize()}')
        item = await queue.get()
        queue.task_done()
        logger.info(f'Consumer receives item: {item}')
    logger.debug('Consumer done.')


def pub_sub_loop():
    logger.debug('Publish-Subscribe loop running.')
    # asyncio.ensure_future(producer(), loop=loop)
    # asyncio.ensure_future(consumer(), loop=loop)

    future = asyncio.gather(
        consumer(),
        producer(),
        loop=loop
    )
    loop.run_until_complete(future)


thread: threading.Thread = threading.Thread(target=pub_sub_loop)


async def get_queue_thread_safe():
    await asyncio.sleep(1.0)
    return list(queue._queue)


async def send_stop_signal():
    global should_stop
    should_stop = True
    return should_stop


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s [%(levelname)s]: %(message)s', level=logging.DEBUG)
    logging.addLevelName(logging.DEBUG, "\033[0;32m%s\033[0m" % logging.getLevelName(logging.DEBUG))
    logging.addLevelName(logging.ERROR, "\033[0;31m%s\033[0m" % logging.getLevelName(logging.ERROR))
    thread.setDaemon(True)
    thread.start()

    while True:
        try:
            future = asyncio.run_coroutine_threadsafe(get_queue_thread_safe(), loop=loop)
            items = future.result()
            logger.debug(f'Async queue contents: {items}')
        except KeyboardInterrupt:
            logger.debug('Stop loop.')
            break
    future = asyncio.run_coroutine_threadsafe(send_stop_signal(), loop=loop)
    result = future.result()
    logger.debug(f'Send stop signal result: {result}.')
    thread.join()
