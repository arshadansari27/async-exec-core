import unittest, asyncio, aiozmq
from asyncexec.core.commands import CommandRouter


URI = None
def _handler(x, y): return x + y


class TestCommandRouter(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.get_event_loop()
        CommandRouter.initialize(URI, use_multiprocessing=True)
        CommandRouter.subscribe('test_handler1', _handler)
        CommandRouter.subscribe('test_handler2', _handler, sync=False)
        self.loop.run_until_complete(CommandRouter.start_server())
        self.client = self.loop.run_until_complete(aiozmq.rpc.connect_rpc(connect=CommandRouter.command_addr))

    def test_handler1(self):
        print("Running 1")
        handler_func = getattr(self.client.call, 'test_handler1')
        ret = self.loop.run_until_complete(handler_func(10, 20))
        print('[*]', ret)
        assert ret == 30
        print("Done 1")

    def test_handler2(self):
        print("Running 2")
        handler_func = getattr(self.client.call, 'test_handler2')
        ret = self.loop.run_until_complete(handler_func(10, 20))
        print('[*]', ret)
        assert ret == None
        print("Done 2")

    def tearDown(self):
        self.loop.run_until_complete(CommandRouter.stop_server())

if __name__ == '__main__':
    unittest.run()
