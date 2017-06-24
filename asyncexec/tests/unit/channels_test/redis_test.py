import unittest, asyncio, aiozmq
import asyncio
import aiozmq
import aiozmq.rpc
from asyncexec.channels.redis import RedisChannel
import aioredis


class TestRedisChannel(unittest.TestCase):

    def setUp(self):
        host = '172.17.0.2'
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

        RedisChannel.channel_type = DummyChannel
        handler_uris = ['DUMMY HANDLER']

        self.loop = asyncio.get_event_loop()
        self.loop.run_until_complete(RedisChannel.initialize({
            'host': host,
            'port': port,
            'username': username,
            'password': password
        }, self.loop))

        in_q, out_q = 'redis:in_queue', 'redis:out_queue'
        self.redis = RedisChannel(handler_uris, 'test_hander', in_q, out_q, sync=True)
        self.redis_recv = redis_recv
        self.redis_sender = redis_sender
        self.wait_hold_on = wait_hold_on
        self.in_q = in_q
        self.out_q = out_q
        self.host = host
        self.port = port

    def test_handler(self):
        self.loop.run_until_complete(self.redis.start())
        self.loop.create_task(self.redis_recv(self.loop, self.host, self.port, self.out_q))
        for i in range(5):
            self.loop.run_until_complete(self.redis_sender(self.loop, self.host, self.port, self.in_q))


    def tearDown(self):
        self.loop.run_until_complete(self.wait_hold_on(self.redis))

class DummyChannel:
    def __init__(self, uri, handler_key, sync=True):
        self.sync = sync

    async def initialize(self):
        pass

    async def call(self, *kargs, **kwargs):
        print("Channel handler called")
        if self.sync:
            self.write_response(b"Writing fake response")
        return b"Writing fake response"

    def set_writer(self, writer):
        self.writer = writer

    def write_response(self, result):
        if self.writer:
            self.writer.write_response(result)

    def close(self):
        pass

if __name__ == '__main__':
    unittest.run()
