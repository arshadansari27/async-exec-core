class External(object):
    def __init__(self, loop, configurations, queue_name):
        self.loop = loop
        self.host = configurations['host']
        self.port = configurations['port']
        self.username = configurations.get('username', None)
        self.password = configurations.get('password', None)
        self.queue_name = queue_name


class Listener(External):

    def __init__(self, loop, configurations, queue_name, consumer, start_event, terminate_event):
        super(Listener, self).__init__(loop, configurations, queue_name)
        self.consumer = consumer
        self.start_event = start_event
        self.terminate_event = terminate_event

    def error_handler(self, error):
        self.terminate_event.data = str(error)
        self.terminate_event.set()


class Publisher(External):

    def __init__(self, loop, configurations, queue_name, publisher, ready_event, terminate_event):
        super(Publisher, self).__init__(loop, configurations, queue_name)
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


class ListenerFactory(object):

    @staticmethod
    def instantiate(name, loop, configurations, queue_name, consumer, start_event, terminate_event):
        from asyncexec.channels.rabbitmq import RabbitMQListener
        from asyncexec.channels.redis import RedisListener
        if name == 'rabbitmq':
            return RabbitMQListener(loop, configurations, queue_name, consumer, start_event, terminate_event)
        elif name == 'redis':
            return RedisListener(loop, configurations, queue_name, consumer, start_event, terminate_event)
        else:
            raise Exception('Not implemented')


class PublisherFactory(object):

    @staticmethod
    def instantiate(name, loop, configurations, queue_name, publisher, ready_event, terminate_event):
        from asyncexec.channels.rabbitmq import RabbitMQPublisher
        from asyncexec.channels.redis import RedisPublisher
        if name == 'rabbitmq':
            return RabbitMQPublisher(loop, configurations, queue_name, publisher, ready_event, terminate_event)
        elif name == 'redis':
            return RedisPublisher(loop, configurations, queue_name, publisher, ready_event, terminate_event)
        else:
            raise Exception('Not implemented')

