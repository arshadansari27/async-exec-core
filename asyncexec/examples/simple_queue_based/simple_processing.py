import sys
sys.path.insert(0, '../../../')

from asyncexec.exec import  AsyncExecutor
'''
    "redis": {
        "host": "172.17.0.3",
        "port": 6379
    }
    "rabbitmq": {
        "host": "172.17.0.2",
        "port": 5672,
        "username": "guest",
        "password": "guest"
    }
'''

async_executor = AsyncExecutor({
    "rabbitmq": {
        "host": "172.17.0.2",
        "port": 5672,
        "username": "guest",
        "password": "guest"
    },
    "redis": {
        "host": "172.17.0.3",
        "port": 6379
    },
    "kafka": {
        "host": "localhost",
        "port": 9092
    }
})


def callback(result):
    print("FINAL RESULT", result)

def _collector(result, data):
    result = int(result) if result else 0
    result = result + int(data)
    return result

@async_executor.publisher('kafka', 'test_queue1')
def _generator():
    for i in range(10):
        yield i

@async_executor.handle_and_collect('kafka', 'test_queue1', reducer=_collector, callback=callback, count=2)
def _handler(data):
    print ("Called:", data)
    return int(data) * 2


'''
@async_executor.publisher('redis', 'tseing')
def test_method3():
    for i in range(10):
        yield i

@async_executor.listener('redis', 'tseing')
def test_method4(data):
    print('I received', data)

'''

if __name__ == '__main__':
    async_executor.start()
