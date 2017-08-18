import asyncio

from asyncexec.workers.generator_worker import GeneratorWorker
from asyncexec.workers.inout_worker import InOutWorker
from asyncexec.workers.sink_worker import SinkWorker
from asyncexec.workers import Communicator
from asyncexec.channels import PublisherFactory, ListenerFactory
from concurrent.futures import ProcessPoolExecutor
from uuid import uuid4
from collections import defaultdict
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
    while True:
        import time
        for task in asyncio.Task.all_tasks():
            if task.done():
                continue
            print('Task Pending', task)
        time.sleep(1)


def shutdown_handler(pool):
    def on_shutdown():
        pool.shutdown()
        shutdown()
    return on_shutdown


class Flow(object):

    def __init__(self, config, loop=None):
        self.id = str(uuid4())
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
        self.terminate_event = None # asyncio.Event()
        self.terminate_events = []
        self.start_event = None # asyncio.Event()
        self.start_event_name = None
        self.start_events = {}
        self.ready_events = defaultdict(list)

    def exception_handler(self, loop, context):
        print("[*] Handling exception here", context)
        loop.close()
        self.pool.shutdown()

    def add_http_listener(self, endpoint):
        self.start_event = asyncio.Event()
        self.terminate_event = asyncio.Event()
        self.start_event_name = str(uuid4())
        self.start_events[self.start_event_name] = self.start_event
        self.terminate_events.append(self.terminate_event)
        from asyncexec.channels.http_c import HTTPListener
        assert 'http' in self.middleware_config
        communicator = Communicator()
        http_listener = HTTPListener(self.loop, self.middleware_config['http'], endpoint, communicator)
        self.coroutines.append(http_listener)
        self.previous_communicator = communicator
        return self

    def add_listener(self, tech, queue):
        assert self.middleware_config.get(tech) is not None
        self.start_event = asyncio.Event()
        self.terminate_event = asyncio.Event()
        self.start_event_name = str(uuid4())
        self.start_events[self.start_event_name] = self.start_event
        self.terminate_events.append(self.terminate_event)
        communicator = Communicator()
        listener = ListenerFactory.instantiate(
            tech, self.loop, self.middleware_config.get(tech),
            queue,
            communicator,
            self.start_event,
            self.terminate_event
        )
        self.coroutines.append(listener)
        self.previous_communicator = communicator
        return self

    def add_publisher(self, tech, queue):
        assert self.middleware_config.get(tech) is not None
        if self.previous_communicator is None:
            raise Exception("Cannot add a publisher to middleware without anything to publish from")
        ready_event = asyncio.Event()

        publisher = PublisherFactory.instantiate(
            tech, self.loop, self.middleware_config.get(tech),
            queue, self.previous_communicator, ready_event, self.terminate_event
        )
        self.ready_events[self.start_event_name].append(ready_event)
        self.coroutines.append(publisher)
        self.previous_communicator = None
        return self

    def add_generator(self, func):
        self.start_event = asyncio.Event()
        self.start_event_name = str(uuid4())
        self.start_events[self.start_event_name] = self.start_event
        self.terminate_event = asyncio.Event()
        self.terminate_events.append(self.terminate_event)

        self.previous_communicator = Communicator()
        generator_worker = GeneratorWorker(
            self.loop, self.pool, func, self.previous_communicator,
            self.start_event, self.terminate_event
        )
        self.coroutines.append(generator_worker)
        return self

    def add_sink(self, func):
        if self.previous_communicator is None or len(self.coroutines) is 0:
            raise Exception("Cannot add a sink worker without anything to listen from")
        ready_event = asyncio.Event()
        sink_worker = SinkWorker(
            self.loop, self.pool, func, self.previous_communicator, ready_event, self.terminate_event
        )
        self.ready_events[self.start_event_name].append(ready_event)
        self.coroutines.append(sink_worker)
        return self

    def add_worker(self, func):
        if self.previous_communicator is None or len(self.coroutines) is 0:
            raise Exception("Cannot add a worker without anything to listen from")
        next_communicator = Communicator()
        ready_event = asyncio.Event()
        in_out_worker = InOutWorker(
            self.loop, self.pool, func, self.previous_communicator, next_communicator,
            ready_event, self.terminate_event
        )
        self.ready_events[self.start_event_name].append(ready_event)
        self.previous_communicator = next_communicator
        self.coroutines.append(in_out_worker)
        return self

    async def start(self):
        print("Starting", self.__class__)
        self.futures = []
        for coroutine in self.coroutines:
            self.futures.append(self.loop.create_task(coroutine.start()))
            # self.loop.run_until_complete(coroutine.start())
            print('....')
        return self.loop.create_task(self.start_on_all_ready())

    async def start_on_all_ready(self):
        event_data = [(n, ev) for n, events in self.ready_events.items() for ev in events]
        done = set([])
        while len(event_data) > 0:
            indexes = []
            for i in range(len(event_data)):
                start_event_name, event = event_data[i]
                if event.is_set() and 'ERROR:' in event.data:
                    raise Exception(event.data)
                elif not event.is_set():
                    await asyncio.sleep(1)
                else:
                    done.add(start_event_name)
                    indexes.append(i)
            event_data = [u for i, u in enumerate(event_data) if i not in indexes]
            print(event_data)
        for ev_name in done:
            self.start_events[ev_name].set()
        while True:
            if all(terminate_event.is_set() for terminate_event in self.terminate_events):
                break
            await asyncio.sleep(3)
        tasks = [task for task in asyncio.Task.all_tasks() if task.done() != True]
        while len(tasks) > 1:
            await asyncio.sleep(2)
            tasks = [task for task in asyncio.Task.all_tasks() if task.done() != True]
        self.pool.shutdown()
        return self.id

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
