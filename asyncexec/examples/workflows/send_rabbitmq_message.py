import sys
import asyncio
from aio_pika import connect, Message, DeliveryMode, ExchangeType
import simplejson as json


async def main(loop):
    # Perform connection
    connection = await connect("amqp://guest:guest@localhost/", loop=loop)

    # Creating a channel
    channel = await connection.channel()

    logs_exchange = await channel.declare_exchange('pyasync_core_request', ExchangeType.DIRECT)

    message_body = json.dumps({
		'event': 'test_method',
		'data': 10
	}).encode('utf-8')

    message = Message(
        message_body,
        delivery_mode=DeliveryMode.PERSISTENT
    )

    # Sending the message
    await logs_exchange.publish(message, routing_key='async_core')

    # print(" [x] Sent %r" % message)

    await connection.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
