import asyncio
import aioredis
from functools import partial

async def write_response(queue, response, pool):
    with (await pool) as redis:
        _ = await redis.lpush(queue, response)
    print('RESP', queue)





async def run_redis_listener(loop, listener, host, port, queues):
    try:
        print("Connection await")
        pool = await aioredis.create_pool((host, port), minsize=5, maxsize=20)
        for queue_req, queue_res in queues:
            loop.create_task(listen_on_request(loop, pool, listener, queue_req, queue_res))
    except Exception as e:
        print("Error", str(e))


async def listen_on_request(loop, pool, listener, queue_req, queue_res):
    wr = partial(write_response, pool=pool)
    with (await pool) as redis:
        while True:
            _, event = await redis.blpop(queue_req)
            await listener.handle(event, loop, queue_req, queue_res, wr)
