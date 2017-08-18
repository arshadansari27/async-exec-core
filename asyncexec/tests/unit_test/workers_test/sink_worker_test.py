import unittest
import asyncio
from concurrent.futures import ProcessPoolExecutor
from unittest.mock import MagicMock

from asyncexec.workers.sink_worker import SinkWorker
from asyncexec.workers import Communicator
from asyncexec.tests.utils import AsyncMock




def executable(result, data):
    print('[S]', data)
    return int(data) + 1


def callback(result):
    print('[R]', result)


async def fake_sleep(sec):
    await asyncio.sleep(sec)


class _Future():
    def __init__(self, val):
        self._result = val

    def result(self):
        return self._result


class TestSinkWorker(unittest.TestCase):

    def setUp(self):
        global executable
        self.loop = asyncio.get_event_loop()
        ex_h = self.loop.get_exception_handler()

        def err_handler(context, error):
            ex_h(context, error)
            raise Exception(str(error))

        self.loop.set_exception_handler(err_handler)
        self.pool = ProcessPoolExecutor(max_workers=4)
        self.publisher = Communicator()
        self.sink_func = executable
        self.pool.submit = MagicMock(return_value=_Future(1))

    def test_sinking(self):
        global callback
        terminate_event = asyncio.Event()
        sink_worker = SinkWorker(
            self.loop, self.pool, self.sink_func, self.publisher, asyncio.Event(), terminate_event, callback=callback)
        future = self.loop.create_task(sink_worker.start())
        self.loop.run_until_complete(fake_sleep(1))
        terminate_event.set()
        self.loop.create_task(self.publisher.consume(1))
        self.loop.run_until_complete(fake_sleep(1))
        self.pool.submit.assert_called_once_with(self.sink_func, None, 1)
        assert future.exception() is None
