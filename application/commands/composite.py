from . import Command


class SequentialCommandGroup(Command):

    def __init__(self, id, parameters, commands):
        self.id = id
        self.parameters = parameters
        if any([not isinstance(command, Command) for command in commands]):
            raise Exception('Invalid Command in the given command list')
        self.commands = commands

    def _execute(self, context, sync=True):
        for command in self.commands:
            context = command.execute(context)
        return context


class ParallelCommandGroup(Command):
    def __init__(self, id, parameters, commands, callback=None):
        self.id = id
        self.parameters = parameters
        if callback and not isinstance(callback, Command):
            raise Exception('Invalid Command in as callback')
        if any([not isinstance(command, Command) for command in commands]):
            raise Exception('Invalid Command in the given command list')
        self.commands = commands
        self.callback = callback

    def _execute(self, context, sync=True):
        results = []
        for command in self.commands:
            result = command.execute(context)
            results.append(result)
            if self.callback:
                results = self.callback(results)
            return results
