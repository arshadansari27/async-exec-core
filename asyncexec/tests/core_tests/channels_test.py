import unittest, asyncio, aiozmq
from asyncexec.core.channels import Channel 
import asyncio
import aiozmq
import aiozmq.rpc


class TestChannel(unittest.TestCase):

    def setUp(self):
        uri = 'tcp://*:*'

        class FakeWriter:

            def write_response(self, message):
                assert message is not None

        @aiozmq.rpc.method
        def _handler(x, y):
            return x + y

        self.loop = asyncio.get_event_loop()
        handlers = {
            'test_handler': _handler
        }
        async def start_server(handlers, uri):
            command = await aiozmq.rpc.serve_rpc(handlers, bind=uri)
            command_addr = list(command.transport.bindings())[0]
            return command_addr, command

        async def stop_server(command):
            command.close()

        self.stop_server = stop_server

        addr, cmd = self.loop.run_until_complete(start_server(handlers, uri))
        self.addr = addr
        self.cmd = cmd
        self.channel = None
        self.fake_writer = FakeWriter()

    def test_handler(self):
        print('{*}', self.addr)
        self.channel = Channel(self.addr, 'test_handler', sync=True)
        self.channel.set_writer(self.fake_writer)
        self.loop.run_until_complete(self.channel.initialize())
        response = self.loop.run_until_complete(self.channel.call(10, 20))
        assert response == 30
        self.channel.close()

    def tearDown(self):
        self.loop.run_until_complete(self.stop_server(self.cmd))

if __name__ == '__main__':
    unittest.run()
