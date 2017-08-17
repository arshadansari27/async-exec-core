from . import Actor


class SinkWorker(object):

    def __init__(self, loop, pool, func, publisher):
        self.loop = loop
        self.actor = Actor(pool, func, is_generator=False)
        self.client = loop.run_until_complete(self.actor.start())
        self.publisher = publisher
        self.event = None

    async def start(self):
        count = 0
        while True:
            try:
                if self.event and self.event.is_set() and self.event.data == 'TERMINATE':
                    break
                count += 1
                data = await self.publisher.publish()
                await self.client.call.handler(data)
            except Exception as e:
                print('Error occured', e)
                raise

    def set_termination_event(self, event):
        self.event = event
