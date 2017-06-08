import asyncio
import asyncio_redis
import simplejson as json
from .events import Listener, Handler
from .channels.redis import run_redis_listener
from .channels.rabbitmq import run_rabbitmq_listener

async def main(loop, coro_listeners):
    coro_listeners = [l[0](loop, l[1], l[2], l[3]) for l in coro_listeners]
    await asyncio.gather(*coro_listeners)

class AsyncExecutor(object):

    def __init__(self, configurations):
        self.listener = Listener()
        self.coroutinues = []

        if 'rabbitmq' in configurations:
            for queues in configurations['rabbitmq']:
                self.coroutinues.append(
					(run_rabbitmq_listener,
					self.listener,
					queues[0],
					queues[1])
				)


        if 'redis' in configurations:
            for queues in configurations['redis']:
                self.coroutinues.append(
					(run_redis_listener,
					self.listener,
					queues[0],
					queues[1])
				)

    def start(self):
        loop = asyncio.get_event_loop()
        loop.create_task(main(loop, self.coroutinues))
        loop.run_forever()

    def handler(self, func):
        print(func.__name__)
        self.listener.register_handler(Handler(func.__name__, func))
        return func

