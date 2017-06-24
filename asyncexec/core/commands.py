import asyncio
import aiozmq
import aiozmq.rpc
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor


class CommandHandler(aiozmq.rpc.AttrHandler):

    def __init__(self, func, sync):
        self.sync = sync
        self.func = func

    @aiozmq.rpc.method
    def handler(self, *args, **kwargs):
        if not self.func:
            raise Exception("Please set the handler before running command handler")
        print("Got the call...")
        future = CommandRouter.pool.submit(self.func, *args, **kwargs)
        if self.sync:
            print("responding...")
            return future.result()
        else:
            return None


class CommandRouter:

    @classmethod
    def initialize(cls, uri='tcp://127.0.0.1:5555', max_workers=4, use_multiprocessing=False):
        if use_multiprocessing:
            cls.pool = ProcessPoolExecutor(max_workers=max_workers)
        else:
            cls.pool = ThreadPoolExecutor(max_workers=max_workers)
        cls.handlers = {}
        cls.uri = uri

    @classmethod
    def subscribe(cls, key, func, sync=True):
        handler_wrapper = CommandHandler(func, sync)
        cls.handlers[key] = handler_wrapper.handler

    @classmethod
    def unsubscribe(cls, key):
        del cls.handlers[key]

    @classmethod
    async def start_server(cls):
        if not cls.uri:
            cls.uri = 'tcp://*:*'
        command = await aiozmq.rpc.serve_rpc(cls.handlers, bind=cls.uri)
        cls.command_addr = list(command.transport.bindings())[0]
        cls.command = command

    @classmethod
    async def stop_server(cls):
        cls.command.close()


if __name__ == '__main__':

    def test_handler(x, y):
        return x + y

    uri = 'tcp://127.0.0.1:5555'
    CommandRouter.initialize(uri, use_multiprocessing=True)
    CommandRouter.subscribe('test_handler1', test_handler)
    CommandRouter.subscribe('test_handler2', test_handler, sync=False)

    async def run_client(uri):
        client = await aiozmq.rpc.connect_rpc(connect=uri)
        ret = await client.call.test_handler1(10, 20)
        assert ret == 30
        ret = await client.call.test_handler2(10, 20)
        assert ret == None

    loop = asyncio.get_event_loop()
    loop.create_task(CommandRouter.start_server())
    loop.run_until_complete(run_client(uri))
    loop.run_until_complete(CommandRouter.stop_server())
