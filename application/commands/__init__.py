class Command(object):

    def __init__(self, id, callable, **kwargs):
        self.id = id
        assert callable is not None
        self.callable = callable

    def execute(self, context=None):
        if not context:
            return self.callable()
        else:
            return self.callable(context)

    def undo(self):
        pass

