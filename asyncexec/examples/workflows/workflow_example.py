import sys
sys.path.insert(0, '../../../')


from asyncexec.exec import  AsyncExecutor
from asyncexec.commands import Command
from asyncexec.commands.unit import UnitCommand
from asyncexec.commands.composite import SequentialCommandGroup, ParallelCommandGroup


async_executor = AsyncExecutor({
    "rabbitmq": [
        ['pyasync_core_request', 'pyasync_core_result']
    ]
})


def method_to_run(x):
    return x * 2


def method_to_callback(rs):
    return sum(rs)


@async_executor.handler
def test_method(data):
    commands = []
    for i in range(1, 4):
        command = method_to_run
        commands.append(command)
    callback = method_to_callback
    command1 = ParallelCommandGroup(1, commands, callback)
    command2 = ParallelCommandGroup(2, commands, callback)
    command = SequentialCommandGroup(0, [command1, command2])

    result = command.execute(int(data))
    return result


async_executor.start()
