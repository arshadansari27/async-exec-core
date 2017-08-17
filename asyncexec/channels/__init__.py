class External(object):
    def __init__(self, loop, configurations, queue_name):
        self.loop = loop
        self.host = configurations['host']
        self.port = configurations['port']
        self.username = configurations.get('username', None)
        self.password = configurations.get('password', None)
        self.queue_name = queue_name
        self.event = None

    def check_if_ok(self):
        if self.event and self.event.is_set() and self.event.data == 'TERMINATE':
            return False
        return True


class Listener(External):

    def __init__(self, loop, configurations, queue_name, consumer):
        super(Listener, self).__init__(loop, configurations, queue_name)
        self.consumer = consumer

    def set_termination_event(self, event):
        self.event = event


class Publisher(External):

    def __init__(self, loop, configurations, queue_name, publisher):
        super(Publisher, self).__init__(loop, configurations, queue_name)
        self.publisher = publisher

    def set_termination_event(self, event):
        self.event = event


class ListenerFactory(object):

    @staticmethod
    def instantiate(name, loop, configurations, queue_name, consumer):
        from asyncexec.channels.rabbitmq import RabbitMQListener
        from asyncexec.channels.redis import RedisListener
        if name == 'rabbitmq':
            return RabbitMQListener(loop, configurations, queue_name, consumer)
        elif name == 'redis':
            return RedisListener(loop, configurations, queue_name, consumer)
        else:
            raise Exception('Not implemented')


class PublisherFactory(object):

    @staticmethod
    def instantiate(name, loop, configurations, queue_name, publisher):
        from asyncexec.channels.rabbitmq import RabbitMQPublisher
        from asyncexec.channels.redis import RedisPublisher
        if name == 'rabbitmq':
            return RabbitMQPublisher(loop, configurations, queue_name, publisher)
        elif name == 'redis':
            return RedisPublisher(loop, configurations, queue_name, publisher)
        else:
            raise Exception('Not implemented')

