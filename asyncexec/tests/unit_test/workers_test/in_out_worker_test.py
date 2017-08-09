import unittest
import asyncio
from concurrent.futures import ProcessPoolExecutor
from unittest.mock import MagicMock

from asyncexec.workers.inout_worker import InOutWorker
from asyncexec.workers import Communicator
from asyncexec.tests.utils import AsyncMock


def executable(data):
    print('[*]', data)
    return data


class TestInOutWorker(unittest.TestCase):

    def setUp(self):
        global executable
        self.loop = asyncio.get_event_loop()
        self.pool = ProcessPoolExecutor(max_workers=4)
        self.publisher = Communicator()
        self.consumer = Communicator()
        self.in_out_func = executable
        self.consumer.consume = AsyncMock(return_value=None)

    def test_sinking(self):
        inout_worker = InOutWorker(
            self.loop, self.pool, self.in_out_func, self.publisher, self.consumer)
        self.loop.create_task(self.publisher.consume(1))
        self.loop.create_task(inout_worker.start())
        self.loop.run_until_complete(asyncio.sleep(.1))
        self.consumer.consume.assert_called_once_with(1)

    def tearDown(self):
        for task in asyncio.Task.all_tasks():
            task.cancel()
