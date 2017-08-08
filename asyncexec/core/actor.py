import aiozmq
import aiozmq.rpc
import asyncio
import multiprocessing as mp
import aioprocessing

class Actor(aiozmq.rpc.AttrHandler):

    def __init__(self, pool, func, is_generator=True):
        self.pool = pool
        self.loop = loop
        self.func = func
        self.is_generator = is_generator
        self.addr = None

    async def start(self):
        server = await aiozmq.rpc.serve_rpc(self, bind='ipc://*:*')
        self.addr = list(server.transport.bindings())[0]
        self.client = await aiozmq.rpc.connect_rpc(connect=self.addr)
        print('ready')
        return self.client

    @aiozmq.rpc.method
    def handler(self, *args, **kwargs):
        if not self.func:
            raise Exception("Actor does not have the method configured")
        future = self.pool.submit(self.func, *args, **kwargs)
        if self.is_generator:
            return future.result()
        else:
            return None

class SinkWorker(Actor):

    def __init__(self, loop, pool, func, publisher):
        self.loop = loop
        self.actor = Actor(pool, func, is_generator=False)
        self.client = loop.run_until_complete(self.actor.start())
        self.publisher = publisher

    async def start(self):
        while True:
            data = await self.publisher.publish()
            await self.client.call.handler(data)


class InOutWorker(Actor):

    def __init__(self, loop, pool, func, publisher, consumer):
        self.loop = loop
        self.actor = Actor(pool, func, is_generator=True)
        self.client = loop.run_until_complete(self.actor.start())
        self.publisher = publisher
        self.consumer = consumer

    async def start(self):
        while True:
            data  = await self.publisher.publish()
            response = await self.client.call.handler(data)
            await self.consumer.consume(response)


class GeneratorWorker(object):

    TERMINATOR = '999999999999999999999999999999999999999999999999999999999999999'

    def func_run(self, queue, lock, func):
        with lock:
            for data in func():
                queue.put(data)
        queue.put(GeneratorWorker.TERMINATOR)
        queue.close()

    def __init__(self, pool, func, consumer, loop):
        self.pool = pool
        self.func = func
        self.queue = aioprocessing.AioQueue()
        self.lock = aioprocessing.AioLock()
        # self.event = aioprocessing.AioEvent()
        self.consumer = consumer
        self.loop = loop
        self.process = aioprocessing.AioProcess(target=self.func_run, args=(
            self.queue,
            self.lock,
            self.func))

    async def start(self):
        self.process.start()
        while True:
            data = await self.queue.coro_get()
            if not data:
                continue
            if data == GeneratorWorker.TERMINATOR:
                break
            await self.consumer.consume(data)
        await self.process.coro_join()


class Communicator(object):

    def __init__(self):
        self.queue  = asyncio.Queue()

    async def publish(self):
        return await self.queue.get()

    async def consume(self, data):
        await self.queue.put(data)


if __name__ == '__main__':
    print("Starting")
    from concurrent.futures import ProcessPoolExecutor
    pool = ProcessPoolExecutor(max_workers=4)

    def fun(x):
        print("fun", x)
        return x*x

    def gen():
        for i in range(10):
            print('gen', i)
            yield i

    def con(data):
        print('con', data)

    loop = asyncio.get_event_loop()
    communicator1 = Communicator()
    communicator2 = Communicator()
    generator_worker = GeneratorWorker(pool, gen, communicator1, loop)
    in_out_worker = InOutWorker(loop, pool, fun, communicator1, communicator2)
    sink_worker = SinkWorker(loop, pool, con, communicator2)
    loop.create_task(generator_worker.start())
    loop.create_task(in_out_worker.start())
    loop.create_task(sink_worker.start())
    loop.run_forever()
