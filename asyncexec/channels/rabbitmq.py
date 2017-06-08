import sys
import asyncio
from aio_pika import connect, Message, DeliveryMode, ExchangeType
import simplejson as json
from uuid import uuid4
import copy

async def write_response(exchange, queue, response, event_id):
	response = await response
	print('RESP', event_id)
	result = json.dumps(dict(id=str(event_id), response=response)).encode('utf-8')
	message = Message(result, delivery_mode=DeliveryMode.PERSISTENT)
	await exchange.publish(message, routing_key='async_core')


def on_message(loop, exchange, queue, listener):
	print ("setting up handler", queue)
	def message_handler(message):
		with message.process() as mb:
			event = json.loads(message.body)
			if not event.get('id'):
				event['id'] = uuid4()
			event_id = event['id']
			response = listener.handle(copy.copy(event))
			print('REQ', event_id)
			loop.create_task(write_response(exchange, queue, response, event_id))
	return message_handler


async def run_rabbitmq_listener(loop, listener, queue_req, queue_res):
	connection = None
	connection = await connect("amqp://guest:guest@localhost/", loop=loop)
	channel1 = await connection.channel()
	await channel1.set_qos(prefetch_count=1)
	incoming_exchange = await channel1.declare_exchange(queue_req, ExchangeType.DIRECT)
	queue = await channel1.declare_queue(exclusive=True)
	await queue.bind(incoming_exchange, routing_key='async_core')

	channel2 = await connection.channel()
	sending_exchange = await channel2.declare_exchange(queue_res, ExchangeType.DIRECT)
	queue.consume(on_message(loop, sending_exchange, queue_res, listener))
