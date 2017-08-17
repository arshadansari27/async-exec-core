import unittest
import asyncio
from asyncexec.workers.generator_worker import GeneratorWorker
from asyncexec.workers import Communicator
from asyncexec.tests.utils import AsyncMock


def gen_func():
    for i in range(10):
        yield i


class TestGeneratorWorker(unittest.TestCase):

    def setUp(self):
        global gen_func
        self.loop = asyncio.get_event_loop()
        self.gen_func = gen_func
        self.consumer = Communicator()
        self.consumer.consume = AsyncMock(return_value=None)

    def test_generation(self):
        start_event = asyncio.Event()
        async def start():
            await asyncio.sleep(1)
            start_event.set()
        generator_worker = GeneratorWorker(
            self.loop, None, self.gen_func, self.consumer, start_event, asyncio.Event())
        self.loop.create_task(start())
        self.loop.run_until_complete(generator_worker.start())
        self.consumer.consume.assert_called_with(9)

    def tearDown(self):
        for task in asyncio.Task.all_tasks():
            task.cancel()
