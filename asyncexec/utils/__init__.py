import aiozmq
import aiozmq.rpc


async def monitor_stream(stream):
    try:
        while True:
            event = await stream.read_event()
            print(event)
    except aiozmq.ZmqStreamClosed:
        pass

