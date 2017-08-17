import asyncio
from aio_pika import connect, Message, DeliveryMode, ExchangeType
from asyncexec.channels import Listener, Publisher
import random


class RabbitMQListener(Listener):

    def __init__(self, loop, configurations, queue_name, consumer):
        super(RabbitMQListener, self).__init__(loop, configurations, queue_name, consumer)

    async def in_message_handler(self, message):
        with message.process():
            await self.consumer.consume(message.body)

    async def start(self):
        try:
            con_uri = "amqp://" + self.username + ":" + self.password + "@" + self.host + ":" + str(self.port) + "/"
            connection = await connect(con_uri, loop=self.loop)
            in_channel = await connection.channel()
            # await in_channel.set_qos(prefetch_count=1)
            in_queue = await in_channel.declare_queue(self.queue_name, durable=True)
            in_queue.consume(self.in_message_handler)
        except:
            self.event.data = 'TERMINATE'
            self.event.set()
            self.loop.stop()
            raise
        finally:
            if connection:
                connection.close()


class RabbitMQPublisher(Publisher):

    def __init__(self, loop, configurations, queue_name, publisher):
        super(RabbitMQPublisher, self).__init__(loop, configurations, queue_name, publisher)

    async def start(self):
        if not self.username or not self.password:
            raise Exception("RabbitMQ Connection requires credentials")
        connection = None
        try:
            con_uri = "amqp://" + self.username + ":" + self.password + "@" + self.host + ":" + str(self.port) + "/"
            connection = await connect(con_uri, loop=self.loop)
            out_channel = await connection.channel()
            while True:
                message = await self.publisher.publish()
                message_body = Message(str(message).encode('utf-8'), delivery_mode=DeliveryMode.PERSISTENT)
                await out_channel.default_exchange.publish(message_body, routing_key=self.queue_name)
        except:
            self.event.data = 'TERMINATE'
            self.event.set()
            self.loop.stop()
            raise
        finally:
            if connection:
                connection.close()
