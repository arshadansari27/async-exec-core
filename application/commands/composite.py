from . import Command
from .unit import UnitCommand
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

class CommandGroup(Command):

    def __init__(self, id, commands, callback=None):
        self.id = id
        if any([(not callable(command) and not isinstance(command, Command)) for command in commands]):
            raise Exception('Invalid Command in the given command list')
        self.commands = [UnitCommand(str(id) + '-' + str(idx), u)
                         if not isinstance(u, Command) else u for idx, u in enumerate(commands)]
        if callback:
            if not isinstance(callback, Command):
                self.callback = UnitCommand(str(id) + 'xcall', callback)
            else:
                self.callback = callback
        else:
            self.callback = None

    def execute(self, context):
        for command in self.commands:
            command.execute(context)
        if callback:
            return self.callback.execute(context)
        return None


class SequentialCommandGroup(CommandGroup):

    def __init__(self, id, commands):
        super(self.__class__, self).__init__(id, commands, callback=None)
    def execute(self, context):
        for command in self.commands:
            context = command.execute(context)
        return context


class ParallelCommandGroup(CommandGroup):

    def __init__(self, id, commands, callback=None):
        super(self.__class__, self).__init__(id, commands, callback)

    def execute(self, context, sync=True, multiprocessing=False):
        results = []
        if sync:
            for command in self.commands:
                result = command.execute(context)
                results.append(result)
            if self.callback:
                return self.callback.execute(results)
            return results
        else:
            if multiprocessing:
                Pool = ProcessPoolExecutor
            else:
                Pool = ThreadPoolExecutor
            with Pool(max_workers=len(self.commands)) as executor:
                futures = [executor.submit(cmd.execute, context) for cmd in self.commands]
                for future in as_completed(futures):
                    results.append(future.result())
                if self.callback:
                    return executor.submit(self.callback.execute, results).result()
                return results


