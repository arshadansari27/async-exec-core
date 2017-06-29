import sys
import asyncio
from aio_pika import connect, Message, DeliveryMode, ExchangeType
import copy
from functools import partial
from asyncexec.core.channels import Channel
import random


class RabbitMQChannel:

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
            self.zmq_channels.append(
                RabbitMQChannel.channel_type(
                    handler_uri, handler_key, sync=self.sync))

    async def start(self):
        cls = self.__class__

        for zmq_channel in self.zmq_channels:
            await zmq_channel.initialize()
            zmq_channel.set_writer(self)

        in_channel = await cls.connection.channel()
        await in_channel.set_qos(prefetch_count=1)
        incoming_exchange = await in_channel.declare_exchange(self.in_queue_name, ExchangeType.DIRECT)
        in_queue = await in_channel.declare_queue(exclusive=True)
        await in_queue.bind(incoming_exchange, routing_key='async_core')

        if self.out_queue_name:
            out_channel = await cls.connection.channel()
            self.sending_exchange = await out_channel.declare_exchange(self.out_queue_name, ExchangeType.DIRECT)
        in_queue.consume(self.in_message_handler)
        print("Waiting to consume...")

    def in_message_handler(self, message):
        print("In message_handler")
        with message.process():
            selector = random.randint(0, len(self.zmq_channels) - 1)
            self.__class__.loop.create_task(self.zmq_channels[selector].call(message.body))

    def write_response(self, result):
        if not self.out_queue_name:
            return
        print("Writing Response", result)
        if type(result) == str:
            result = result.encode('utf-8')

        message = Message(result, delivery_mode=DeliveryMode.PERSISTENT)
        self.__class__.loop.create_task(self.sending_exchange.publish(message, routing_key='async_core'))

    @classmethod
    async def initialize(cls, config, loop):
        if config.get('port'):
            port = ':' + str(config['port'])
        else:
            port = ''
        con_uri = "amqp://" + config['username'] + ":" + config['password'] + "@" + config['host'] + port + "/"
        cls.connection = await connect(con_uri, loop=loop)
        cls.initialized = True
        cls.loop = loop


if __name__ == '__main__':

    host = '172.17.0.2'
    port =  5672
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

    if port:
        port = ":" + str(port)
    else:
        port = ''

    uri = "amqp://" + username + ":" + password + "@" + host + port + "/"

    async def rabbit_sender(loop, uri, queue):
        connection = await connect(uri, loop=loop)
        channel = await connection.channel()
        logs_exchange = await channel.declare_exchange(queue, ExchangeType.DIRECT)
        for i in range(1):
            message_body = '{"pi": "1049261"}'.encode('utf-8')
            message = Message(message_body, delivery_mode=DeliveryMode.PERSISTENT)
        await logs_exchange.publish(message, routing_key='async_core')
        print(" [x] Rabbit Begin SENT %r" % message_body)
        await connection.close()



    async def rabbit_recv(loop, uri, queue):
        def local_on_message(message):
            with message.process():
                print("[x] Rabbit End RECV %r" % message.body)
        connection = await connect(uri, loop=loop)
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)
        logs_exchange = await channel.declare_exchange(queue, ExchangeType.DIRECT)
        queue = await channel.declare_queue(exclusive=True)
        await queue.bind(logs_exchange, routing_key='async_core')
        queue.consume(local_on_message)

    async def wait_hold_on():
        print('waiting beginning...')
        await asyncio.sleep(1)
        print('waiting done....')

    RabbitMQChannel.channel_type = DummyChannel

    handler_uris = ['DUMMY HANDLER']

    port =  5672
    loop = asyncio.get_event_loop()
    loop.run_until_complete(RabbitMQChannel.initialize({
        'host': host,
        'port': port,
        'username': username,
        'password': password
    }, loop))
    in_q, out_q = 'rabbit:in_queue', 'rabbit:out_queue'
    rabbitmq = RabbitMQChannel(handler_uris, 'test_hander', in_q, out_q, sync=True)
    loop.run_until_complete(rabbitmq.start())
    loop.create_task(rabbit_recv(loop, uri, out_q))
    loop.run_until_complete(rabbit_sender(loop, uri, in_q))
    loop.run_until_complete(rabbit_sender(loop, uri, in_q))
    loop.run_until_complete(rabbit_sender(loop, uri, in_q))
    loop.run_until_complete(rabbit_sender(loop, uri, in_q))
    loop.run_until_complete(wait_hold_on())
