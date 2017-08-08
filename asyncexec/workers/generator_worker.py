import aioprocessing
from . import Actor


class GeneratorWorker(object):

    TERMINATOR = '999999999999999999999999999999999999999999999999999999999999999'

    def func_run(self, queue, lock, func):
        with lock:
            for data in func():
                queue.put(data)
        queue.put(GeneratorWorker.TERMINATOR)
        queue.close()

    def __init__(self, loop, pool, func, consumer):
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
