import asyncio
import uvloop
from collections import defaultdict
from asyncexec.workers.flow_builder import Flow


class AsyncExecutor(object):

    channels  = defaultdict(list)

    def __init__(self, configurations):
        self.channel_configurations = {}
        self.flows = []

        if 'rabbitmq' in configurations:
            rabbitmq = configurations['rabbitmq']
            host = rabbitmq['host']
            port = rabbitmq['port']
            username = rabbitmq.get('username', 'guest')
            password = rabbitmq.get('password', 'guest')
            self.channel_configurations['rabbitmq'] = (host, port, username, password)

        if 'redis' in configurations:
            redis = configurations['redis']
            host = redis['host']
            port = redis['port']
            username = redis.get('user', 'guest')
            password = redis.get('password', 'guest')
            self.channel_configurations['redis'] = (host, port, username, password)

        if 'http' in configurations:
            http = configurations['http']
            port = http['port']
            self.channel_configurations['http'] = (None, port, None, None)

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        self.loop = uvloop.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def start(self):
        for flow in self.flows:
            flow.start(external_loop_start=True)
        self.loop.run_forever()

    def handler(self, channel, queue_request, queue_response, max_workers=4):
        def decorator(func):
            print ("Registering handler {} for channel: {} on queues ({}, {})".format(
                func.__name__, channel, queue_request, queue_response
            ))
            assert channel in self.channel_configurations
            host, port, username, password = self.channel_configurations[channel]
            config = {
                'max_workers': max_workers,
                'middlewares': {
                    channel: {
                        'host': host,
                        'port': port,
                        'username': username,
                        'password': password
                    }
                }
            }
            flow = Flow(config, loop=self.loop)\
                .add_listener(channel, queue_request)\
                .add_worker(func)\
                .add_publisher(channel, queue_response)
            self.flows.append(flow)

            return func
        return decorator
