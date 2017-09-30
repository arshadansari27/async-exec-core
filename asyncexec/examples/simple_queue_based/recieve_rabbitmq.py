import asyncio
from aio_pika import connect, IncomingMessage, ExchangeType
from datetime import datetime
import sys

loop = asyncio.get_event_loop()

first_message = False
arrival_begin_time_stamp = None
message_count = 0


def on_message(message: IncomingMessage):
    global first_message, message_count, arrival_begin_time_stamp
    with message.process():
        '''
        if not first_message:
            first_message = True
            arrival_begin_time_stamp = datetime.now()
        d = datetime.now() - arrival_begin_time_stamp
        dd = d.days
        ds = d.seconds
        diff = (3600 * 24) * dd + ds 
        '''
        message_count += 1
        print("[x] %s %d" % (message.body, message_count))


async def main(ip, queue):
    # Perform connection
    connection = await connect("amqp://guest:guest@%s/" % ip, loop=loop)

    # Creating a channel
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)

    # Declaring queue
    queue = await channel.declare_queue(queue, durable=False)

    # Binding the queue to the exchange

    # Start listening the queue with name 'task_queue'
    queue.consume(on_message)


if __name__ == "__main__":
    ip = sys.argv[1]
    queue = sys.argv[2]
    loop = asyncio.get_event_loop()
    loop.create_task(main(ip, queue))
    print(' [*] Waiting for logs. To exit press CTRL+C. Listening on', ip, queue)
    loop.run_forever()
