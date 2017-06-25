import asyncio
import aioredis
from functools import partial
import sys, time
import simplejson as json


async def main(loop, ip, queue, message):
    # Perform connection
    pool = await aioredis.create_pool((ip, 6379), minsize=5, maxsize=20)
    for i in range(100000):
        with (await pool) as redis:
            print("Sending")
            _ = await redis.lpush(queue, message)
    print ("closing")
    pool.close()

if __name__ == "__main__":
    ip = sys.argv[1]
    queue = sys.argv[2]
    message = sys.argv[3]
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop, ip, queue, message))
