import asyncio
import time
import datetime

async def df():
    print("hello")
    await asyncio.sleep(1)
    print("hi")
async def da():
    sync()
    await asyncio.sleep(2)
    print("world")
def sync():
    print("print sync")

async def main():

    back = time.time()
    await asyncio.gather(da(), df())
    print("egg")
    front = time.time()
    print(front-back)
asyncio.run(main())



