import asyncio
import logging

import uvloop

from asyncexec.workers.flow_builder import Flow

logger = logging.getLogger(__name__)


class AsyncExecutor(object):

    def __init__(self, configurations):
        self.channel_configurations = {}
        self.flows = []

        if 'rabbitmq' in configurations:
            rabbitmq = configurations['rabbitmq']
            host = rabbitmq['host']
            port = rabbitmq['port']
            username = rabbitmq.get('username', 'guest')
            password = rabbitmq.get('password', 'guest')
            prefetch = rabbitmq.get('prefetch', 1)
            self.channel_configurations['rabbitmq'] = (host, port, username,
                                                       password, prefetch)

        if 'redis' in configurations:
            redis = configurations['redis']
            host = redis['host']
            port = redis['port']
            username = redis.get('user', 'guest')
            password = redis.get('password', 'guest')
            self.channel_configurations['redis'] = (host, port, username,
                                                    password, None)

        if 'kafka' in configurations:
            kafka = configurations['kafka']
            host = kafka['host']
            port = kafka['port']
            username = None
            password = None
            self.channel_configurations['kafka'] = (host, port, username,
                                                    password, None)

        if 'http' in configurations:
            http = configurations['http']
            port = http['port']
            self.channel_configurations['http'] = (None, port, None, None, None)

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        self.loop = uvloop.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def start(self):
        futures = []
        for ix, flow in enumerate(self.flows):
            logger.info('Async Exec: ' + str(ix))
            future = self.loop.run_until_complete(flow.start())
            futures.append(future)
        for future in asyncio.gather(*futures):
            f = self.loop.run_until_complete(future)
            if hasattr(f, 'exception') and f.exception():
                logger.error(f.exception())
                exit(1)

    def publisher(self, out_channel, out_queue):
        def decorator(func):
            logger.info(
                    "Registering handler {} for channel: {} on queues ({"
                    "})".format(
                            func.__name__, out_channel, out_queue))
            assert out_channel in self.channel_configurations
            config = self._get_config(out_channel, None)
            flow = Flow(config, loop=self.loop) \
                .add_generator(func) \
                .add_publisher(out_channel, out_queue)
            self.flows.append(flow)
            logger.info(
                'flow [publisher: {}] {} {} {}'.format(flow.id, out_channel,
                                                       out_queue, 'ready'))

            return func

        return decorator

    def listener(self, in_channel, in_queue, max_workers=4):
        def decorator(func):
            logger.info(
                    "Registering handler {} for channel: {} on queues ({"
                    "})".format(
                            func.__name__, in_channel, in_queue))
            assert in_channel in self.channel_configurations
            config = self._get_config(in_channel, max_workers)
            flow = Flow(config, loop=self.loop) \
                .add_listener(in_channel, in_queue) \
                .add_sink(func)
            self.flows.append(flow)
            logger.info(
                'flow [subscriber: {}] {} {} {}'.format(flow.id, in_channel,
                                                        in_queue, 'ready'))

            return func

        return decorator

    def handler(self, channel, queue_request, queue_response, max_workers=4):
        def decorator(func):
            logger.info(
                    "Registering handler {} for channel: {} on queues ({}, "
                    "{})".format(
                            func.__name__, channel, queue_request,
                            queue_response
                    ))
            assert channel in self.channel_configurations
            config = self._get_config(channel, max_workers)
            flow = Flow(config, loop=self.loop)
            flow.add_listener(channel, queue_request)
            if queue_response is None:
                flow.add_sink(func)
            else:
                flow.add_worker(func).add_publisher(channel, queue_response)
            self.flows.append(flow)
            logger.info(
                'flow [handler: {}] {} {} {} {}'.format(flow.id, channel,
                                                        queue_request,
                                                        queue_response,
                                                        'ready'))

            return func

        return decorator

    def handle_and_collect(self, in_channel, in_queue, reducer=None,
                           callback=None, count=None, max_workers=4):
        def decorator(func):
            logger.info(
                    "Registering collector {} for channel: {} on queue ({}) "
                    "with callback {}".format(
                            func.__name__, in_channel, in_queue, callback
                    ))
            assert in_channel in self.channel_configurations
            config = self._get_config(in_channel, max_workers)
            flow = Flow(config, loop=self.loop)
            flow.add_listener(in_channel, in_queue)
            flow.add_worker(func)
            flow.add_sink(reducer, callback=callback, count=count)
            self.flows.append(flow)
            logger.info('flow [handle and collect: {}] {} {} {}'.format(flow.id,
                                                                        in_channel,
                                                                        in_channel,
                                                                        'ready'))

            return func

        return decorator

    def handle_and_publish_many(self, in_channel, in_queue, out_channel,
                                out_queue, max_workers=4):
        def decorator(func):
            logger.info(
                    "Registering handler and broadcast '{}' for channel: {} "
                    "and {} and on queue ({}, {}) ".format(
                            func.__name__, in_channel, out_channel, in_queue,
                            out_queue
                    ))
            assert in_channel in self.channel_configurations
            config = self._get_config(in_channel, max_workers)
            flow = Flow(config, loop=self.loop)
            flow.add_listener(in_channel, in_queue)
            flow.add_worker_many(func)
            flow.add_publisher(out_channel, out_queue)
            self.flows.append(flow)
            logger.info(
                'flow [handler and publish many: {}] {} {} {} {} {}'.format(
                    flow.id, in_channel, in_queue, out_channel, out_queue,
                    'ready'))

            return func

        return decorator

    def handle_and_broadcast(self, in_channel, in_queue, out_channel,
                             *out_queues, max_workers=4):
        def decorator(func):
            logger.info(
                    "Registering handler and broadcast '{}' for channel: {} "
                    "and {} and on queue ({}, {}) ".format(
                            func.__name__, in_channel, out_channel, in_queue,
                            ','.join(out_queues)
                    ))
            assert in_channel in self.channel_configurations
            config = self._get_config(in_channel, max_workers)
            flow = Flow(config, loop=self.loop)
            flow.add_listener(in_channel, in_queue)
            flow.add_worker(func)
            flow.add_broadcast_publisher(out_channel, *out_queues)
            self.flows.append(flow)
            logger.info(
                'flow [handler and broadcast: {}] {} {} {} {} {}'.format(
                    flow.id, in_channel, in_queue, out_channel,
                    ','.join(out_queues), 'ready'))

            return func

        return decorator

    def collector(self, in_channel, in_queue, callback=None, count=None,
                  max_workers=4):
        def decorator(func):
            logger.info(
                    "Registering collector {} for channel: {} on queue ({}) with callback {}".format(
                            func.__name__, in_channel, in_queue, callback
                    ))
            assert in_channel in self.channel_configurations
            config = self._get_config(in_channel, max_workers)
            flow = Flow(config, loop=self.loop)
            flow.add_listener(in_channel, in_queue)
            flow.add_sink(func, callback=callback, count=count)
            self.flows.append(flow)
            logger.info(
                'flow [collector: {}] {} {} {}'.format(flow.id, in_channel,
                                                       in_channel, 'ready'))

            return func

        return decorator

    def _get_config(self, channel, max_workers):
        host, port, username, password, prefetch = \
            self.channel_configurations[channel]
        config = {
            'max_workers': max_workers,
            'middlewares': {
                channel: {
                    'host': host,
                    'port': port,
                    'username': username,
                    'password': password,
                    'prefetch': prefetch
                }
            }
        }
        return config
