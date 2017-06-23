import sys
import asyncio
import aioredis
import copy
from functools import partial
from asyncexec.core.channels import Channel
import random


class RedisChannel:

    initialized = False
    channel_type = Channel

    def __init__(self, handler_uris, handler_key, in_queue, out_queue=None, sync=True):
        if not self.__class__.initialized:
            raise Exception("Cannot directly create the objecct. Use class method 'initialize' to do so")

        self.in_queue_name = in_queue
        self.out_queue_name = out_queue
        self.handler_key = handler_key
        self.handler_uris = handler_uris
        self.sync = sync
        self.zmq_channels = []
        for handler_uri in self.handler_uris:
            self.zmq_channels.append(RedisChannel.channel_type(handler_uri, handler_key, sync=self.sync))

    async def start(self):
        cls = self.__class__

        for zmq_channel in self.zmq_channels:
            await zmq_channel.initialize()
            zmq_channel.set_writer(self)
        self.__class__.loop.create_task(self.in_message_handler())

    async def in_message_handler(self):
        redis = self.__class__.pool
        while True:
            _, message = await redis.blpop(self.in_queue_name)
            selector = random.randint(0, len(self.zmq_channels) - 1)
            await self.zmq_channels[selector].call(message)

    def write_response(self, result):
        print("Writing Response")
        self.__class__.loop.create_task(self._write_response(result))

    async def _write_response(self, result):
        redis = self.__class__.pool
        _ = await redis.lpush(self.out_queue_name, result)

    @classmethod
    async def initialize(cls, config, loop):
        cls.pool = await aioredis.create_redis((config['host'], config['port']))
        cls.initialized = True
        cls.loop = loop

    @classmethod
    async def close(cls):
        await cls.pool.close()


if __name__ == '__main__':

    host = '172.17.0.3'
    port =  6379
    username =  ''
    password =  ''

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

    async def redis_sender(loop, host, port, queue):
        data = b'This is a test data'
        pool = await aioredis.create_pool((host, port), minsize=5, maxsize=20)
        with (await pool) as redis:
            _ = await redis.lpush(queue, data)
            print("REDIS BEGIN SENT", data)

    async def redis_recv(loop, host, port, queue):
        pool = await aioredis.create_pool((host, port), minsize=5, maxsize=20)
        with (await pool) as redis:
            while True:
                _, event = await redis.blpop(queue)
                print("REDIS END RECV", event)

    async def wait_hold_on(redis):
        print('waiting beginning...')
        await asyncio.sleep(15)
        print('waiting done....')
        await redis.close()


    RedisChannel.channel_type = DummyChannel

    handler_uris = ['DUMMY HANDLER']

    loop = asyncio.get_event_loop()
    loop.run_until_complete(RedisChannel.initialize({
        'host': host,
        'port': port,
        'username': username,
        'password': password
    }, loop))
    in_q, out_q = 'redis:in_queue', 'redis:out_queue'
    redis = RedisChannel(handler_uris, 'test_hander', in_q, out_q, sync=True)
    loop.run_until_complete(redis.start())
    loop.create_task(redis_recv(loop, host, port, out_q))
    loop.run_until_complete(redis_sender(loop, host, port, in_q))
    loop.run_until_complete(redis_sender(loop, host,port, in_q))
    loop.run_until_complete(redis_sender(loop, host, port, in_q))
    loop.run_until_complete(redis_sender(loop, host, port, in_q))
    loop.run_until_complete(wait_hold_on(redis))
