import asyncio
import aiozmq
import asyncio_redis
import simplejson as json
import uvloop
from .channels.redis import RedisChannel
from .channels.rabbitmq import RabbitMQChannel
# from .channels.http_handler import  create_http_listener
from .core.commands import CommandRouter
from collections import defaultdict

async def main(loop, configurations, channel_wise_queues):

    loop.create_task(CommandRouter.start_server())
    coro_listeners = []
    for channel_name, handler_details in channel_wise_queues.items():
        for in_q, out_q, func in handler_details:
            if channel_name == 'redis':
                channel_class = RedisChannel
            elif channel_name == 'rabbitmq':
                channel_class = RabbitMQChannel
            elif channel_name == 'http':
                raise Exception("Channel http not supported yet, but is in progress")
            else:
                raise Exception("Channel not supported")

            host, port, username, password = configurations[channel_name]

            await loop.create_task(channel_class.initialize({
                'host': host,
                'port': port,
                'username': username,
                'password': password
            }, loop))
            if out_q:
                sync = True
            else:
                sync = False

            channel_obj = channel_class([CommandRouter.command_addr], func.__name__, in_q, out_q, sync=sync)
            AsyncExecutor.channels[channel_name].append(channel_obj)
    await asyncio.gather(*coro_listeners)
    for channel_name, channel_objs in AsyncExecutor.channels.items():
        print("Staring", channel_name, '...')
        for idx, channel_obj in enumerate(channel_objs):
            print('\t', idx)
            await channel_obj.start()

class AsyncExecutor(object):

    channels  = defaultdict(list)

    def __init__(self, configurations):
        workers = 4
        self.URI = None # 'tcp://0.0.0.0:5555'
        use_multiprocessing = True
        if 'process_info' in configurations:
            if 'workers' in configurations['process_info']:
                workers = configurations['process_info']['workers']
            if 'pool' in configurations['process_info']:
                if configurations['process_info']['pool'] != 'process':
                    use_multiprocessing = False

        CommandRouter.initialize(self.URI,
                                 max_workers=workers,
                                 use_multiprocessing=use_multiprocessing)
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
        loop.create_task(main(loop, self.channel_configurations, self.channel_wise_queues))
        loop.run_forever()

    def handler(self, channel, queue_request, queue_response, multiprocess=True):
        def decorator(func):
            print ("Registering handler {} for channel: {} on queues ({}, {})".format(
                func.__name__, channel, queue_request, queue_response
            ))
            if channel not in self.channel_wise_queues:
                self.channel_wise_queues[channel] = []
            self.channel_wise_queues[channel].append((queue_request, queue_response, func))
            if queue_response:
                sync = True
            else:
                sync = False
            func = aiozmq.rpc.method(func)
            CommandRouter.subscribe(func.__name__, func, sync)
            return func
        return decorator
