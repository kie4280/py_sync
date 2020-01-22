import asyncio

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
    await asyncio.gather(da(), df())
    print("egg")

asyncio.run(main())