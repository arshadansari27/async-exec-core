import asyncio
import asyncio_redis
import simplejson as json
from uuid import uuid4

async def write_response(connection, queue, response, event_id):
	response = await response
	print('RESP', event_id)
	result = json.dumps(dict(id=str(event_id), response=response))
	await connection.publish(queue, result)


async def run_redis_listener(loop, listener, queue_req, queue_res):
	retries = 0
	while True:
		listen_connection = await asyncio_redis.Connection.create(host='127.0.0.1', port=6379)
		send_connection = await asyncio_redis.Connection.create(host='127.0.0.1', port=6379)
		try:
			subscriber = await listen_connection.start_subscribe()

			await subscriber.subscribe([ queue_req ])

			while True:
				reply = await subscriber.next_published()
				event = json.loads(reply.value)
				response = listener.handle(event)
				if not event.get('id'):
					event['id'] = uuid4()
				event_id = event['id']
				print('REQ', event_id)
				await write_response(send_connection, queue_res, response, event_id)
		except:
			retries += 1
			if retries > 3:
				print("Enough Retries, breaking out of listener, restart the listener...")
				break
		finally:
			listen_connection.close()
			send_connection.close()

