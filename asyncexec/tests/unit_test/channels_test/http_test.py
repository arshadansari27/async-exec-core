import unittest, asyncio, aiozmq
import asyncio
import aiozmq
import aiozmq.rpc
from asyncexec.channels.http_c import HTTPChannel
from concurrent.futures import ProcessPoolExecutor
import requests
import traceback, sys

def http_sender(host, port, queue):
    print("Calling http api")
    try:
        response = requests.post('http://%s:%d/api/execute/%s' % (host, port, queue), data={'data': 10})
        print("Called http api")
        print(response.text)
    except Exception as e:
        print(e)
        raise


class TestHTTPChannel(unittest.TestCase):

    def setUp(self):
        host = '0.0.0.0'
        port =  8888
        username =  ''
        password =  ''
        self.pool = ProcessPoolExecutor(max_workers=10)

        async def wait_hold_on():
            print('waiting beginning...')
            await asyncio.sleep(1)
            print('waiting done....')

        HTTPChannel.channel_type = DummyChannel
        handler_uris = ['DUMMY HANDLER']

        self.loop = asyncio.get_event_loop()
        self.loop.run_until_complete(HTTPChannel.initialize({
            'host': host,
            'port': port,
            'username': username,
            'password': password
        }, self.loop))

        self.handler_key = 'test_handler'
        self.http_channel = HTTPChannel(handler_uris, self.handler_key)
        self.wait_hold_on = wait_hold_on
        self.http_sender = http_sender
        self.host, self.port = host, port

    def test_handler(self):
        global http_sender
        self.loop.create_task(self.http_channel.start())
        print("Running tests")
        try:
            response = self.loop.run_in_executor(self.pool, http_sender, self.host, self.port, self.handler_key)
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
            raise

    def tearDown(self):
        self.loop.run_until_complete(self.wait_hold_on())


class DummyChannel:
    def __init__(self, uri, handler_key, sync=True):
        self.sync = sync

    async def initialize(self):
        pass

    async def call(self, *kargs, **kwargs):
        print("Channel handler called")
        if self.sync:
            self.write_response(b"Writing fake response")
        return b"Writing fake response"

    def set_writer(self, writer):
        self.writer = writer

    def write_response(self, result):
        if self.writer:
            self.writer.write_response(result)

    def close(self):
        pass


if __name__ == '__main__':
    unittest.run()
