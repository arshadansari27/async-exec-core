import sys
import asyncio
from aio_pika import connect, Message, DeliveryMode, ExchangeType
import simplejson as json


async def main(loop):
    # Perform connection
    connection = await connect("amqp://user:password@172.17.0.2/", loop=loop)

    # Creating a channel
    channel = await connection.channel()

    logs_exchange = await channel.declare_exchange('req_queue1', ExchangeType.DIRECT)

    for i in range(1):

        message_body = json.dumps({'pi': '1049261'}).encode('utf-8')

        message = Message(
            message_body,
            delivery_mode=DeliveryMode.PERSISTENT
        )

        await logs_exchange.publish(message, routing_key='async_core')

        print(" [x] Sent %r" % message_body)

    await connection.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
