import asyncio
import asyncio_redis
import simplejson as json
import uvloop
from .events import Listener, Handler
from .channels.redis_b import run_redis_listener
from .channels.rabbitmq import run_rabbitmq_listener
from .channels.http_handler import  create_http_listener

async def main(loop, listener, configurations, channel_wise_queues):

    coro_listeners = []
    for k, v in configurations.items():
        print ("Checking listeners for ", k, "...", v)
        host, port, username, password = v
        if k == 'redis':
            if 'redis' not in channel_wise_queues:
                continue
            print ("Starting ", k, "...")
            coro_listeners.append(run_redis_listener(loop, listener, host, port, channel_wise_queues['redis']))
        elif k == 'rabbitmq':
            if 'rabbitmq' not in channel_wise_queues:
                continue
            coro_listeners.append(run_rabbitmq_listener(loop, listener, host, port, username, password, channel_wise_queues['rabbitmq']))
        elif k == 'http':
            coro_listeners.append(create_http_listener(loop, listener, port))
        else:
            raise Exception("Channel not supported")
    await asyncio.gather(*coro_listeners)

class AsyncExecutor(object):

    def __init__(self, configurations):
        workers = 4
        if 'multiprocess' in configurations:
            if 'workers' in configurations['multiprocess']:
                workers = configurations['multiprocess']['workers']
        self.listener = Listener(workers=workers)
        self.channel_configurations = {}
        self.channel_wise_queues = {}

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

    def start(self):
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        loop = uvloop.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.create_task(main(loop, self.listener, self.channel_configurations, self.channel_wise_queues))
        loop.run_forever()

    def handler(self, channel, queue_request, queue_response, multiprocess=True):
        def decorator(func):
            print ("Registering handler {} for channel: {} on queues ({}, {})".format(
                func.__name__, channel, queue_request, queue_response
            ))
            if channel not in self.channel_wise_queues:
                self.channel_wise_queues[channel] = []
            self.channel_wise_queues[channel].append((queue_request, queue_response, multiprocess))
            self.listener.register_handler(Handler(queue_request, func))
            return func
        return decorator

