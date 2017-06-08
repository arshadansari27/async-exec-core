import sys
sys.path.insert(0, '../../../')

from asyncexec.exec import  AsyncExecutor


async_executor = AsyncExecutor({
    "rabbitmq": [
        ['pyasync_core_request', 'pyasync_core_result']
    ]
})
''',
    "redis": [
        ['pyasync_core_request', 'pyasync_core_result']
    ]
'''


@async_executor.handler
def test_method(data):
    print("Hello", data)
    return str(data) + ': response'


async_executor.start()
