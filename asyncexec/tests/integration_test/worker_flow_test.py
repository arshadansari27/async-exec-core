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


def con1(R, data):
    print('[*](1)', data)
    return R


def con2(R, data):
    print('[*](2)', data)
    return R


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
            .add_broadcast_publisher('rabbitmq', 'testing1', 'testing2')
            #.add_listener('rabbitmq', 'testing1')\
            #.add_sink(con1)\
            #.add_listener('rabbitmq', 'testing2') \
            #.add_sink(con2)
        future = self.loop.run_until_complete(flow.start())
        print(self.loop.run_until_complete(future))

    def tearDown(self):
        pass