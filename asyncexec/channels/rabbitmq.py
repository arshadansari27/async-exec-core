import sys
import asyncio
from aio_pika import connect, Message, DeliveryMode, ExchangeType
import copy
from functools import partial

async def write_response(queue, response, exchange):
	print('RESP', response)
	message = Message(response.encode('utf-8'), delivery_mode=DeliveryMode.PERSISTENT)
	await exchange.publish(message, routing_key='async_core')


def on_message(loop, exchange, queue_req, queue_res, listener):
    if exchange:
        wr = partial(write_response, exchange=exchange)
    else:
        wr = None
    def message_handler(message):
        with message.process() as mb:
            loop.create_task(listener.handle(message.body, loop, queue_req, queue_res, wr))
    return message_handler


async def run_rabbitmq_listener(loop, listener, host, port, username, password, queues):
    connection = await connect("amqp://" + username + ":" + password + "@" + host + "/", loop=loop)
    for queue_req, queue_res in queues:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)
        incoming_exchange = await channel.declare_exchange(queue_req, ExchangeType.DIRECT)
        queue = await channel.declare_queue(exclusive=True)
        await queue.bind(incoming_exchange, routing_key='async_core')

        if queue_res:
            channel = await connection.channel()
            sending_exchange = await channel.declare_exchange(queue_res, ExchangeType.DIRECT)
        else:
            sending_exchange = None
        queue.consume(on_message(loop, sending_exchange, queue_req, queue_res, listener))
