import aiozmq
import aiozmq.rpc
import asyncio
import traceback, sys
import zmq


class Actor(aiozmq.rpc.AttrHandler):

    def __init__(self, pool, func, is_generator=True):
        self.pool = pool
        self.func = func
        self.is_generator = is_generator
        self.client = None

    async def start(self):
        server = await aiozmq.rpc.serve_rpc(self, bind='ipc://*:*')
        self.client = await aiozmq.rpc.connect_rpc(
            connect=list(server.transport.bindings())[0])
        return self.client

    @aiozmq.rpc.method
    def handler(self, *args, **kwargs):
        if not self.func:
            raise Exception("Actor does not have the method configured")
        try:
            future = self.pool.submit(self.func, *args, **kwargs)
            if self.is_generator:
                res = future.result()
                return res
            else:
                return None
        except Exception as e:
            print('Error Occurred in Actor', e)
            traceback.print_exc(file=sys.stdout)
            exit(1)


class Communicator(object):

    def __init__(self):
        self.queue  = asyncio.Queue()
        self.router = yield from aiozmq.create_zmq_stream(zmq.ROUTER, bind='ipc://*:*')
        addr = list(router.transport.bindings())[0]
        self.dealer = yield from aiozmq.create_zmq_stream(zmq.DEALER, connect=addr)

    async def publish(self):
        data = await self.router.read()
        print('router:', data)
        return data

    async def publish_nowait(self):
        raise Exception("Not implemented")
        if self.queue.empty():
            return None
        return self.queue.get_nowait()

    def empty(self):
        raise Exception("Not implemented")

    async def consume(self, data):
        print('dealer:', data)
        await self.dealer.write((data.encode('utf-8'),))
