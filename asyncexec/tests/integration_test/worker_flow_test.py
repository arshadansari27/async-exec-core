import pytest
import math
import os
import unittest
import sys
from asyncexec.workers.flow_builder import Flow
import asyncio
from threading import Thread


def fun(x):
    x = int(x)
    return x

def gen():
    for i in range(10):
        yield i


def con(data):
    print('[*]', data)


class TestFlow(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.get_event_loop()
        self.config = {
            'middlewares': {
                'redis': {
                    'host': '172.17.0.3',
                    'port': 6379
                },
                'rabbitmq': {
                    'host': '172.17.0.2',
                    'port': 5672,
                    'username': 'guest',
                    'password': 'guest'
                }
            },
            'max_workers': 4
        }



    def test_flow(self):
        flow = Flow(self.config, loop=self.loop)\
            .add_generator(gen)\
            .add_worker(fun)\
            .add_publisher('rabbitmq', 'testing')
            #.add_listener('rabbitmq', 'testing')\
            #.add_sink(con)
        future = self.loop.run_until_complete(flow.start())
        print(self.loop.run_until_complete(future))

    def tearDown(self):
        pass