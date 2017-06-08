from asyncexec.exec import  AsyncExecutor

'''
    "redis": [
        ['pyasync_core_request', 'pyasync_core_result']
    ]
'''

async_executor = AsyncExecutor({
    "rabbitmq": [
        ['pyasync_core_request', 'pyasync_core_result']
    ]
})


@async_executor.handler
def test_method(data):
    print("Hello", data)


async_executor.start()
