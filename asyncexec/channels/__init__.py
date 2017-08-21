from uuid import uuid4
import asyncio


class External(object):
    def __init__(self, loop, configurations, queue_name, flow_id=None):
        self.loop = loop
        self.host = configurations['host']
        self.port = configurations['port']
        self.username = configurations.get('username', None)
        self.password = configurations.get('password', None)
        self.queue_name = queue_name
        self.flow_id = flow_id if flow_id else str(uuid4())


class Listener(External):

    def __init__(self, loop, configurations, queue_name, consumer, start_event, terminate_event, flow_id=None):
        super(Listener, self).__init__(loop, configurations, queue_name, flow_id=flow_id)
        self.consumer = consumer
        self.start_event = start_event
        self.terminate_event = terminate_event

    def error_handler(self, error):
        self.terminate_event.data = str(error)
        self.terminate_event.set()


class Publisher(External):

    def __init__(self, loop, configurations, queue_name, publisher, ready_event, terminate_event, flow_id=None):
        super(Publisher, self).__init__(loop, configurations, queue_name, flow_id=flow_id)
        self.publisher = publisher
        self.ready_event = ready_event
        self.terminate_event = terminate_event

    def error_handler(self, error):
        if not self.ready_event.is_set():
            self.ready_event.data = 'ERROR: ' + str(error)
            self.ready_event.set()
        if not self.terminate_event.is_set():
            self.terminate_event.data = str(error)
            self.terminate_event.set()


class CompositePublisher(object):

    def __init__(self, loop, publisher, consumers, ready_event, terminate_event, flow_id=None):
        assert len(consumers) > 0
        self.loop = loop
        self.publisher = publisher
        self.consumers = consumers
        self.ready_event = ready_event
        self.terminate_event = terminate_event
        self.flow_id = flow_id

    def error_handler(self, error):
        if not self.ready_event.is_set():
            self.ready_event.data = 'ERROR: ' + str(error)
            self.ready_event.set()
        if not self.terminate_event.is_set():
            self.terminate_event.data = str(error)
            self.terminate_event.set()

    async def start(self):
        try:
            self.ready_event.data = 'CompositePublisher'
            self.ready_event.set()
            print('[CompositePublisher: {}](Publisher) started...'.format(self.flow_id))
            while True:
                while True:
                    if self.publisher.empty() and self.terminate_event.is_set():
                        print('[CompositePublisher: {}](Publisher) ... done'.format(self.flow_id))
                        break
                    message = await self.publisher.publish_nowait()
                    if not message:
                        await asyncio.sleep(.1)
                    else:
                        break

                for consumer in self.consumers:
                    await consumer.consume(message)
                if self.publisher.empty() and self.terminate_event.is_set():
                    print('[CompositePublisher: {}](Publisher) ... done'.format(self.flow_id))
                    break
        except Exception as e:
            self.error_handler(e)
            print("Error occured", e)
            raise


class ListenerFactory(object):

    @staticmethod
    def instantiate(name, loop, configurations, queue_name, consumer, start_event, terminate_event, flow_id=None):
        from asyncexec.channels.rabbitmq import RabbitMQListener
        from asyncexec.channels.redis import RedisListener
        if name == 'rabbitmq':
            return RabbitMQListener(loop, configurations, queue_name, consumer, start_event, terminate_event, flow_id=flow_id)
        elif name == 'redis':
            return RedisListener(loop, configurations, queue_name, consumer, start_event, terminate_event, flow_id=flow_id)
        else:
            raise Exception('Not implemented')


class PublisherFactory(object):

    @staticmethod
    def instantiate(name, loop, configurations, queue_name, publisher, ready_event, terminate_event, flow_id=None):
        from asyncexec.channels.rabbitmq import RabbitMQPublisher
        from asyncexec.channels.redis import RedisPublisher
        if name == 'rabbitmq':
            return RabbitMQPublisher(loop, configurations, queue_name, publisher, ready_event, terminate_event, flow_id=flow_id)
        elif name == 'redis':
            return RedisPublisher(loop, configurations, queue_name, publisher, ready_event, terminate_event, flow_id=flow_id)
        else:
            raise Exception('Not implemented')

