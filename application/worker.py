import asyncio
import asyncio_redis
import simplejson as json
from events import Listener, Handler
from events.redis_listener import run_redis_listener

'''
async def main(loop, listener, queue1, queue2):
	await asyncio.gather(
		run_redis_listener(listener, queue1),
		run_redis_listener(listener, queue2)
	)
 '''
queue_req = 'pyasync_core_request'
queue_res = 'pyasync_core_result'

listener = Listener()
handler = Handler('X', lambda u: u*2)
listener.register_handler(handler)
loop = asyncio.get_event_loop()
# Blocking call which returns when the hello_world() coroutine is done
# loop.run_until_complete(main(loop, listener, queue1, queue2))
loop.run_until_complete(run_redis_listener(loop, listener, queue_req, queue_res))
loop.close()
