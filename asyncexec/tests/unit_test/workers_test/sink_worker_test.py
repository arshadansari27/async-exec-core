import unittest
import asyncio
from concurrent.futures import ProcessPoolExecutor
from unittest.mock import MagicMock

from asyncexec.workers.sink_worker import SinkWorker
from asyncexec.workers import Communicator
from asyncexec.tests.utils import AsyncMock


def executable(data): print('[*]', data)


class TestSinkWorker(unittest.TestCase):

    def setUp(self):
        global executable
        self.loop = asyncio.get_event_loop()
        self.pool = ProcessPoolExecutor(max_workers=4)
        self.publisher = Communicator()
        self.sink_func = executable
        self.pool.submit = MagicMock(return_value=None)

    def test_sinking(self):
        sink_worker = SinkWorker(
            self.loop, self.pool, self.sink_func, self.publisher, asyncio.Event(), asyncio.Event())
        self.loop.create_task(self.publisher.consume(1))
        self.loop.create_task(sink_worker.start())
        self.loop.run_until_complete(asyncio.sleep(.1))
        self.pool.submit.assert_called_once_with(self.sink_func, 1)

    def tearDown(self):
        for task in asyncio.Task.all_tasks():
            task.cancel()
