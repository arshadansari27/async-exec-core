from . import Command


class UnitCommand(Command):

    def __init__(self, id, callable):
        super(self.__class__, self).__init__(id, callable)
