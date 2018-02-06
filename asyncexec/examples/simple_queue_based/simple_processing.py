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
        "host": "localhost",
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



@async_executor.handle_and_publish_many('redis', 'test_queue1', 'redis', 'test_queue2')
def _handler(data):
    dd = []
    for i in range(1, 10):
        print ("Called:", i)
        dd.append(int(data) * i)
    return dd


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
