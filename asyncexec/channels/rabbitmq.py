import asyncio
from aio_pika import connect, Message, DeliveryMode, ExchangeType
from asyncexec.channels import Listener, Publisher
import random
import traceback
from concurrent.futures import as_completed


class RabbitMQListener(Listener):

    def __init__(self, loop, configurations, queue_name, consumer, start_event, terminate_event, flow_id=None):
        super(RabbitMQListener, self).__init__(loop, configurations, queue_name, consumer, start_event, terminate_event, flow_id=flow_id)

    async def in_message_handler(self, message):
        with message.process():
            msg = message.body
            # print('[RabbitMQ: {}](Listener) {}'.format(self.flow_id, msg))
            await self.consumer.consume(msg)

    async def start(self):
        try:
            con_uri = "amqp://" + self.username + ":" + self.password + "@" + self.host + ":" + str(self.port) + "/"
            connection = await connect(con_uri, loop=self.loop)
            in_channel = await connection.channel()
            # await in_channel.set_qos(prefetch_count=1)
            in_queue = await in_channel.declare_queue(self.queue_name, durable=False)
            print('[RabbitMQ: {}](Listener) awaiting to start..'.format(self.flow_id))
            await self.start_event.wait()
            print('[RabbitMQ: {}](Listener) started..'.format(self.flow_id))
            if self.terminate_event.is_set():
                raise Exception("Rabbit Listener: Termination event occurred before starting...")
            in_queue.consume(self.in_message_handler)
        except Exception as e:
            traceback.print_exc()
            exit(1)
        finally:
            if connection:
                connection.close()


class RabbitMQPublisher(Publisher):

    def __init__(self, loop, configurations, queue_name, publisher, ready_event, terminate_event, flow_id=None):
        super(RabbitMQPublisher, self).__init__(loop, configurations, queue_name, publisher, ready_event, terminate_event, flow_id=flow_id)

    async def start(self):
        if not self.username or not self.password:
            raise Exception("RabbitMQ Connection requires credentials")
        connection = None
        count = 0
        try:
            con_uri = "amqp://" + self.username + ":" + self.password + "@" + self.host + ":" + str(self.port) + "/"
            connection = await connect(con_uri, loop=self.loop)
            out_channel = await connection.channel()
            self.ready_event.data = 'RabbitMQPublisher'
            self.ready_event.set()
            print('[RabbitMQ: {}](Publisher) started...'.format(self.flow_id))
            while True:
                if self.publisher.empty() and self.terminate_event.is_set():
                    print('[RabbitMQ: {}](Publisher) done...'.format(self.flow_id))
                    break
                mms = await self.publisher.publish()
                futures = []
                for message in mms:
                    count += 1
                    message_body = Message(str(message).encode('utf-8'), delivery_mode=DeliveryMode.PERSISTENT)
                    futures.append(out_channel.default_exchange.publish(message_body, routing_key=self.queue_name))
                    if count % 10 is 0:
                        print('[*] C', count)
                        await asyncio.gather(*futures)
                        futures = []
                await asyncio.gather(*futures)

        except Exception as e:
            traceback.print_exc()
            exit(1)
        finally:
            if connection:
                connection.close()
