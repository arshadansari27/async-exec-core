import abc


class Command(object):

       def __init__(self, id, **kwargs):
              self.id = id
              for k, v in kwargs.items():
                     setattr(self, k, v)

       def execute(self, context):
              return self._execute(context)

       def undo(self):
              pass

       @abc.abstractmethod
       def _execute(self, params, context):
              pass
