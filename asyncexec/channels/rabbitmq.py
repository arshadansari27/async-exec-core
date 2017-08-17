import asyncio
from aio_pika import connect, Message, DeliveryMode, ExchangeType
from asyncexec.channels import Listener, Publisher
import random


class RabbitMQListener(Listener):

    def __init__(self, loop, configurations, queue_name, consumer, start_event, terminate_event):
        super(RabbitMQListener, self).__init__(loop, configurations, queue_name, consumer, start_event, terminate_event)

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
            print("Rabbit Listener: awaiting to start")
            await self.start_event.wait()
            print("Rabbit Listener: started..")
            if self.terminate_event.is_set():
                raise Exception("Rabbit Listener: Termination event occurred before starting...")
            in_queue.consume(self.in_message_handler)
        except Exception as e:
            self.error_handler(e)
            raise
        finally:
            if connection:
                connection.close()


class RabbitMQPublisher(Publisher):

    def __init__(self, loop, configurations, queue_name, publisher, ready_event, terminate_event):
        super(RabbitMQPublisher, self).__init__(loop, configurations, queue_name, publisher, ready_event, terminate_event)

    async def start(self):
        if not self.username or not self.password:
            raise Exception("RabbitMQ Connection requires credentials")
        connection = None
        try:
            con_uri = "amqp://" + self.username + ":" + self.password + "@" + self.host + ":" + str(self.port) + "/"
            connection = await connect(con_uri, loop=self.loop)
            out_channel = await connection.channel()
            self.ready_event.data = 'RabbitMQPublisher'
            self.ready_event.set()
            print("Rabbit Publisher: started..")
            while True:
                if self.publisher.empty() and self.terminate_event.is_set():
                    print("rabbitmq publisher queue empty and terminated")
                    break
                message = await self.publisher.publish()
                message_body = Message(str(message).encode('utf-8'), delivery_mode=DeliveryMode.PERSISTENT)
                await out_channel.default_exchange.publish(message_body, routing_key=self.queue_name)
        except Exception as e:
            self.error_handler(e)
            raise
        finally:
            if connection:
                connection.close()
