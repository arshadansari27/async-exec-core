import asyncio
import aioprocessing
from uuid import uuid4

TERMINATOR = 'TERM:' + str(uuid4())


class GeneratorWorker(object):

    @staticmethod
    def func_run(loop, queue, lock, event, func):

        with lock:
            for data in func():
                if not loop.is_running():
                    break
                print("Putting", data)
                queue.put(data)
            print("Setting event to terminate")
            event.set()
        # queue.put(TERMINATOR)

    def __init__(self, loop, pool, func, consumer, start_event, terminate_event):
        self.start_event = start_event
        self.terminate_event = terminate_event
        self.pool = pool
        self.func = func
        self.queue = aioprocessing.AioQueue()
        self.lock = aioprocessing.AioLock()
        self._event = aioprocessing.AioEvent()
        self.consumer = consumer
        self.loop = loop
        self.process = aioprocessing.AioProcess(target=GeneratorWorker.func_run, args=(
            self.loop,
            self.queue,
            self.lock,
            self._event,
            self.func))

    async def start(self):
        try:
            print("Generator: Waiting to begin...")
            await self.start_event.wait()
            print("Generator: Begun.")
            self.process.start()
            while True:
                if self.queue.empty() and (self.terminate_event.is_set() or self._event.is_set()):
                    break
                if not self.queue.empty():
                    data = await self.queue.coro_get()
                    await self.consumer.consume(data)
                else:
                    await asyncio.sleep(1)
            await self.process.coro_join()
            self.terminate_event.data = 'DONE'
            self.terminate_event.set()
        except Exception as e:
            self.terminate_event.data = str(e)
            self.terminate_event.set()
            raise
        print("Generator: Done...")
