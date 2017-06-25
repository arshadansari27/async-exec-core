import unittest, asyncio, aiozmq
import asyncio
import aiozmq
import aiozmq.rpc
from asyncexec.channels.rabbitmq import RabbitMQChannel
from asyncexec.core.commands import CommandRouter
from aio_pika import connect, Message, DeliveryMode, ExchangeType


@aiozmq.rpc.method
def _handler(data):
    print ("Got the call to handler", data)
    return 'This is a fake response'.encode('utf-8')


class TestRabbitMQChannel(unittest.TestCase):

    def setUp(self):
        global _handler
        host = '172.17.0.3'
        port = 5672
        username =  ''
        password =  ''

        URI = 'tcp://127.0.0.1:5555'

        if port:
            port = ":" + str(port)
        else:
            port = ''
    
        self.uri = "amqp://" + username + ":" + password + "@" + host + port + "/"

        async def rabbitmq_sender(loop, uri, queue):
            connection = await connect(uri, loop=loop)
            channel = await connection.channel()
            logs_exchange = await channel.declare_exchange(queue, ExchangeType.DIRECT)
            message_body = 'This is a test message'.encode('utf-8')
            message = Message(message_body, delivery_mode=DeliveryMode.PERSISTENT)
            await logs_exchange.publish(message, routing_key='async_core')
            print(" [x] Rabbit Begin SENT %r" % message_body)
            await connection.close()

        async def rabbitmq_recv(loop, uri, queue):
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


        self.loop = asyncio.get_event_loop()
        CommandRouter.initialize(URI, use_multiprocessing=True)
        CommandRouter.subscribe('test_handler', _handler, sync=True)
        self.loop.create_task(CommandRouter.start_server())
        self.addr = URI

        port =  5672
        self.loop.run_until_complete(RabbitMQChannel.initialize({
            'host': host,
            'port': port,
            'username': username,
            'password': password
        }, self.loop))

        in_q, out_q = 'rabbitmq:in_queue', 'rabbitmq:out_queue'
        self.rabbitmq = RabbitMQChannel([self.addr], 'test_handler', in_q, out_q, sync=True)
        self.rabbitmq_recv = rabbitmq_recv
        self.rabbitmq_sender = rabbitmq_sender
        self.wait_hold_on = wait_hold_on
        self.in_q = in_q
        self.out_q = out_q
        self.host = host
        self.port = port

    def test_handler(self):
        self.loop.create_task(self.rabbitmq_recv(self.loop, self.uri, self.out_q))
        self.loop.run_until_complete(self.rabbitmq.start())
        for i in range(5):
            self.loop.create_task(self.rabbitmq_sender(self.loop, self.uri, self.in_q))

    def tearDown(self):
        self.loop.run_until_complete(self.wait_hold_on())
        self.loop.run_until_complete(CommandRouter.stop_server())

if __name__ == '__main__':
    unittest.run()
