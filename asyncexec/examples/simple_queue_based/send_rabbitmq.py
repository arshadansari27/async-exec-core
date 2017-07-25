import sys
import asyncio
from aio_pika import connect, Message, DeliveryMode, ExchangeType
import simplejson as json


async def main(loop, ip, queue, message):
    # Perform connection
    connection = await connect("amqp://guest:guest@%s/" % ip, loop=loop)

    # Creating a channel
    channel = await connection.channel()

    # logs_exchange = await channel.declare_exchange(queue, ExchangeType.TOPIC)

    for i in range(1):

        message_body = json.dumps({'pi': '1049261'}).encode('utf-8')

        message = Message(
            message_body,
            delivery_mode=DeliveryMode.PERSISTENT
        )

        await channel.default_exchange.publish(message, routing_key='in_q2')

        print(" [x] Sent %r" % message_body)

    await connection.close()


if __name__ == "__main__":
    ip = sys.argv[1]
    queue = sys.argv[2]
    message = sys.argv[3]
    print ("senging to queue", queue, 'message:', message)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop, ip, queue, message))
