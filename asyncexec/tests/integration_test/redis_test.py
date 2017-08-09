import unittest, asyncio, aiozmq
import asyncio
import aiozmq
import aiozmq.rpc
from asyncexec.channels.redis import RedisChannel
from asyncexec.core.commands import CommandRouter
import aioredis


@aiozmq.rpc.method
def _handler(data):
    print ("Got the call to handler", data)
    return 'This is a fake response'.encode('utf-8')


class TestRedisChannel(unittest.TestCase):

    def setUp(self):
        global _handler
        host = '172.17.0.3'
        port = 6379
        username =  ''
        password =  ''

        async def redis_sender(loop, host, port, queue):
            data = b'This is a test data'
            pool = await aioredis.create_pool((host, port), minsize=5, maxsize=20)
            with (await pool) as redis:
                _ = await redis.lpush(queue, data)
                print("REDIS BEGIN SENT", data)
            pool.close()

        async def redis_recv(loop, host, port, queue):
            pool = await aioredis.create_pool((host, port), minsize=5, maxsize=20)
            with (await pool) as redis:
                count = 0
                while True:
                    _, event = await redis.blpop(queue)
                    print("REDIS END RECV", event)
                    count += 1
                    if count >= 4:
                        break
            pool.close()

        async def wait_hold_on(redis):
            redis.close()

        URI = 'tcp://127.0.0.1:5555'

        self.loop = asyncio.get_event_loop()
        CommandRouter.initialize(URI, use_multiprocessing=True)
        CommandRouter.subscribe('test_handler', _handler, sync=True)
        self.loop.create_task(CommandRouter.start_server())
        self.addr = URI

        self.loop.run_until_complete(RedisChannel.initialize({
            'host': host,
            'port': port,
            'username': username,
            'password': password
        }, self.loop))

        in_q, out_q = 'redis:in_queue', 'redis:out_queue'
        self.redis = RedisChannel([self.addr], 'test_handler', in_q, out_q, sync=True)
        self.redis_recv = redis_recv
        self.redis_sender = redis_sender
        self.wait_hold_on = wait_hold_on
        self.in_q = in_q
        self.out_q = out_q
        self.host = host
        self.port = port

    def test_handler(self):
        self.loop.create_task(self.redis_recv(self.loop, self.host, self.port, self.out_q))
        self.loop.create_task(self.redis.start())
        for i in range(5):
            self.loop.run_until_complete(self.redis_sender(self.loop, self.host, self.port, self.in_q))

    def tearDown(self):
        self.loop.run_until_complete(CommandRouter.stop_server())
        self.loop.run_until_complete(self.wait_hold_on(self.redis))

if __name__ == '__main__':
    unittest.run()
