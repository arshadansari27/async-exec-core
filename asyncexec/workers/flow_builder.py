import asyncio

from asyncexec.workers.generator_worker import GeneratorWorker
from asyncexec.workers.inout_worker import InOutWorker
from asyncexec.workers.sink_worker import SinkWorker
from asyncexec.workers import Communicator
from asyncexec.channels import PublisherFactory, ListenerFactory
from concurrent.futures import ProcessPoolExecutor
import signal


def worker_factory(type='inout'):
    if type == 'generator':
        return GeneratorWorker
    elif type == 'sink':
        return SinkWorker
    else:
        return InOutWorker


def shutdown():
    """Performs a clean shutdown"""
    for task in asyncio.Task.all_tasks():
        task.cancel()

def shutdown_handler(pool):
    def on_shutdown():
        pool.shutdown()
        shutdown()
    return on_shutdown


class Flow(object):

    def __init__(self, config, loop=None):
        self.config = config
        max_workers = config.get('max_workers', 4)
        self.pool = ProcessPoolExecutor(max_workers=max_workers)
        if loop is None:
            self.loop = asyncio.get_event_loop()
        else:
            self.loop = loop
        self.loop.add_signal_handler(signal.SIGHUP, shutdown_handler(self.pool))
        self.loop.add_signal_handler(signal.SIGTERM, shutdown_handler(self.pool))
        self.loop.add_signal_handler(signal.SIG_IGN, shutdown_handler(self.pool))
        self.loop.set_exception_handler(self.exception_handler)
        self.middleware_config = config.get('middlewares', {})
        self.coroutines = []
        self.previous_communicator = None
        self.terminate_event = asyncio.Event()

    def exception_handler(self, loop, context):
        print("[*] Handling exception here", context)
        loop.close()
        self.pool.shutdown()

    def add_http_listener(self, endpoint):
        from asyncexec.channels.http_c import HTTPListener
        assert 'http' in self.middleware_config
        communicator = Communicator()
        http_listener = HTTPListener(self.loop, self.middleware_config['http'], endpoint, communicator)
        self.coroutines.append(http_listener)
        self.previous_communicator = communicator
        return self

    def add_listener(self, tech, queue):
        assert self.middleware_config.get(tech) is not None
        communicator = Communicator()
        listener = ListenerFactory.instantiate(
            tech, self.loop, self.middleware_config.get(tech),
            queue,
            communicator
        )
        listener.set_termination_event(self.terminate_event)
        self.coroutines.append(listener)
        self.previous_communicator = communicator
        return self

    def add_publisher(self, tech, queue):
        assert self.middleware_config.get(tech) is not None
        if self.previous_communicator is None:
            raise Exception("Cannot add a publisher to middleware without anything to publish from")
        publisher = PublisherFactory.instantiate(
            tech, self.loop, self.middleware_config.get(tech),
            queue, self.previous_communicator
        )
        publisher.set_termination_event(self.terminate_event)
        self.coroutines.append(publisher)
        self.previous_communicator = None
        return self

    def add_generator(self, func):
        self.previous_communicator = Communicator()
        generator_worker = GeneratorWorker(
            self.loop, self.pool, func, self.previous_communicator, event=self.terminate_event
        )
        self.coroutines.append(generator_worker)
        return self

    def add_sink(self, func):
        if self.previous_communicator is None or len(self.coroutines) is 0:
            raise Exception("Cannot add a sink worker without anything to listen from")
        sink_worker = SinkWorker(
            self.loop, self.pool, func, self.previous_communicator
        )
        sink_worker.set_termination_event(self.terminate_event)
        self.coroutines.append(sink_worker)
        return self

    def add_worker(self, func):
        if self.previous_communicator is None or len(self.coroutines) is 0:
            raise Exception("Cannot add a worker without anything to listen from")
        next_communicator = Communicator()
        in_out_worker = InOutWorker(
            self.loop, self.pool, func, self.previous_communicator, next_communicator
        )
        in_out_worker.set_termination_event(self.terminate_event)
        self.previous_communicator = next_communicator
        self.coroutines.append(in_out_worker)
        return self

    def start(self, timeout=None, external_loop_start=False):

        async def stopper():
            await asyncio.sleep(timeout)
            shutdown()

        async def check_event_to_stop():
            while True:
                await asyncio.sleep(3)
                if self.terminate_event.is_set and self.terminate_event.data == 'TERMINATE':
                    shutdown()

        self.futures = []
        for coroutine in self.coroutines:
            self.futures.append(self.loop.create_task(coroutine.start()))
        if not external_loop_start:
            if not timeout:
                self.loop.run_forever()
            else:
                self.loop.run_until_complete(stopper())
        else:
            self.loop.create_task(check_event_to_stop())


    def stop(self):
        self.pool.shutdown()
        shutdown()


if __name__ == '__main__':
    print("Starting")
    from datetime import datetime
    start  = datetime.now()
    def fun(x):
        x = int(x)
        return x

    def gen():
        for i in range(100000):
            yield i

    def con(data):
        print('con', data)
        diff = datetime.now() - start
        print(diff.seconds + (diff.microseconds / 1000000))


    import signal
    loop = asyncio.get_event_loop()
    config = {
        'middlewares': {
            'redis': {
                'host': '172.17.0.2',
                'port': 6379
            },
            'rabbitmq': {
                'host': '172.17.0.3',
                'port': 5672,
                'username': 'guest',
                'password': 'guest'
            },
            'http': {
                'host': '0.0.0.0',
                'port': 5555,
                'username': 'guest',
                'password': 'guest'
            }
        },
        'max_workers': 4
    }

    '''
    Flow(config, loop=loop)\
        .add_generator(gen)\
        .add_worker(fun)\
        .add_publisher('rabbitmq', 'testing')\
        .add_listener('rabbitmq', 'testing')\
        .add_sink(con).start()
    '''
    Flow(config, loop=loop).add_http_listener('/testing').add_sink(con).add_http_listener('/testing2').add_sink(con).start()
