import asyncio
import aioredis
from functools import partial
import sys
import simplejson as json


async def main(loop):
    # Perform connection
    pool = await aioredis.create_pool(('172.17.0.3', 6379), minsize=5, maxsize=20)

    for i in range(100000):
        with (await pool) as redis:
            print ("sending", i)
            _ = await redis.lpush('req_queue1', '{"data": "testing"}')
    print ("closing")
    pool.close()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
