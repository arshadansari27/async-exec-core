import unittest, asyncio, aiozmq
import asyncio
import aiozmq
import aiozmq.rpc
from asyncexec.exec import AsyncExecutor
import threading


import time
def test_method(data):
    time.sleep(.1)
    return 'result'


class TestExecutor(unittest.TestCase):

    def setUp(self):
        global test_method
        self.async_executor = AsyncExecutor({
            'process_info': {
                'workers': 10,
                'pool': 'process'
            },
            "rabbitmq": {
                "host": "172.17.0.2",
                "port": 5672,
                "username": "guest",
                "password": "guest"
            }
        })
        
        self.async_executor.handler('rabbitmq', 'rabbit:in_q', 'rabbit:out_q')(test_method)
        #threading.Thread(target=self.async_executor.start).start()
        async_executor.start()

    def unit_test(self):
        print("Running unittest")


