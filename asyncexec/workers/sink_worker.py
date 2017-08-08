from . import Actor


class SinkWorker(object):

    def __init__(self, loop, pool, func, publisher):
        self.loop = loop
        self.actor = Actor(pool, func, is_generator=False)
        self.client = loop.run_until_complete(self.actor.start())
        self.publisher = publisher

    async def start(self):
        while True:
            data = await self.publisher.publish()
            await self.client.call.handler(data)
