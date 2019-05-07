import asyncio
import random


async def myCoroutine(id: int) -> None:
    random_delay = random.randint(1, 5)
    await asyncio.sleep(random_delay)
    print(f'Coroutine: {id}, has successfully completed after {random_delay} seconds')


async def work_load() -> None:
    tasks = []
    for id in range(10):
        tasks.append(asyncio.ensure_future(myCoroutine(id)))

    await asyncio.gather(*tasks)


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(work_load())
    loop.close()


if __name__ == '__main__':
    main()
