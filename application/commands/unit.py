from . import Command


class UnitCommand(Command):

       def __init__(self, id, module_registry, template, params={}):
              self.id = id
              self.module_registry = module_registry
              self.template = template
              self.params = params

       def _execute(self, context):
              params = self.params
              return self.module_registry.get(
                  self.template).execute(context, params)
