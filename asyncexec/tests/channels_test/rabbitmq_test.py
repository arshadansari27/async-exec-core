import unittest, asyncio, aiozmq
import asyncio
import aiozmq
import aiozmq.rpc
from asyncexec.channels.rabbitmq import RabbitMQChannel
from aio_pika import connect, Message, DeliveryMode, ExchangeType


class TestRabbitMQChannel(unittest.TestCase):

    def setUp(self):
        host = '172.17.0.3'
        port =  5672
        username =  ''
        password =  ''

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

        self.loop = asyncio.get_event_loop()
        self.loop.run_until_complete(RabbitMQChannel.initialize({
            'host': host,
            'port': port,
            'username': username,
            'password': password
        }, self.loop))

        in_q, out_q = 'rabbit:in_queue', 'rabbit:out_queue'
        self.rabbitmq = RabbitMQChannel(handler_uris, 'test_hander', in_q, out_q, sync=True)
        self.rabbit_recv = rabbit_recv
        self.rabbit_sender = rabbit_sender
        self.wait_hold_on = wait_hold_on
        self.uri = uri
        self.in_q = in_q
        self.out_q = out_q

    def test_handler(self):
        self.loop.run_until_complete(self.rabbitmq.start())
        self.loop.create_task(self.rabbit_recv(self.loop, self.uri, self.out_q))
        self.loop.run_until_complete(self.rabbit_sender(self.loop, self.uri, self.in_q))
        self.loop.run_until_complete(self.rabbit_sender(self.loop, self.uri, self.in_q))
        self.loop.run_until_complete(self.rabbit_sender(self.loop, self.uri, self.in_q))
        self.loop.run_until_complete(self.rabbit_sender(self.loop, self.uri, self.in_q))


    def tearDown(self):
        self.loop.run_until_complete(self.wait_hold_on())

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
