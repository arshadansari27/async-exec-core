import asyncio
from aio_pika import connect, IncomingMessage, ExchangeType
from datetime import datetime

loop = asyncio.get_event_loop()

first_message = False
arrival_begin_time_stamp = None
message_count = 0


def on_message(message: IncomingMessage):
    global first_message, message_count, arrival_begin_time_stamp
    with message.process():
        if not first_message:
            first_message = True
            arrival_begin_time_stamp = datetime.now()
        d = datetime.now() - arrival_begin_time_stamp
        dd = d.days
        ds = d.seconds
        diff = (3600 * 24) * dd + ds 
        message_count += 1.

        
        print("[x] %r" % message.body, message_count / diff if diff > 0 else message_count)


async def main():
    # Perform connection
    connection = await connect("amqp://guest:guest@172.17.0.3/", loop=loop)

    # Creating a channel
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)

    logs_exchange = await channel.declare_exchange(
        'rabbit:out_q',
        ExchangeType.DIRECT
    )

    # Declaring queue
    queue = await channel.declare_queue(exclusive=True)

    # Binding the queue to the exchange
    await queue.bind(logs_exchange, routing_key='async_core')

    # Start listening the queue with name 'task_queue'
    queue.consume(on_message)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(main())

    # we enter a never-ending loop that waits for data and runs callbacks whenever necessary.
    print(' [*] Waiting for logs. To exit press CTRL+C')
    loop.run_forever()
