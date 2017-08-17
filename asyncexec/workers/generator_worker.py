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

    def __init__(self, loop, pool, func, consumer, event=None):
        self.event = event if event else asyncio.Event()
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
            self.process.start()
            retries = 0
            while True:
                try:
                    data = await self.queue.coro_get(timeout=1)
                    print("Taking", data)
                except Exception as e:
                    retries += 1
                    print('Managing error', e, 'retry', retries)
                    await asyncio.sleep(retries)
                    if retries < 3:
                        continue

                if retries >= 3 and self._event and self._event.is_set():
                    self.event.data = 'TERMINATE'
                    self.event.set()
                    self.queue.close()
                    break
                await self.consumer.consume(data)
                retries = 0
            await self.process.coro_join()
        except Exception as e:
            print('[*] Error in generator', e)
            raise
