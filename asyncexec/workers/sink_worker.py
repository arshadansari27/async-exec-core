from . import Actor


class SinkWorker(object):

    def __init__(self, loop, pool, func, publisher):
        self.loop = loop
        self.actor = Actor(pool, func, is_generator=False)
        self.client = loop.run_until_complete(self.actor.start())
        self.publisher = publisher

    async def start(self):
        count = 0
        while True:
            try:
                count += 1
                data = await self.publisher.publish()
                await self.client.call.handler(data)
            except Exception as e:
                print('Error occured', e)
                raise
