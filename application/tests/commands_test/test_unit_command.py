import pytest
import math
import os
import unittest
import sys
sys.path.insert(0, os.getcwd())
import models_test
from commands.unit import UnitCommand


class TestTemplate(object):

       def __init__(self):
              pass

       def execute(self, context, params):
              return math.sqrt(context['x'])


class TestUnitCommand(unittest.TestCase):

       def setUp(self):
              self.template = 'math'
              self.module_registry = {}
              self.module_registry[self.template] = TestTemplate()

       def test_execute(self):
              command = UnitCommand(1, self.module_registry, self.template, {})
              self.assertEquals(2., command.execute({'x': 4}))

       def tearDown(self):
              pass
