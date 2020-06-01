################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
from pyflink.java_gateway import get_gateway
from pyflink.table.function_definition import FunctionDefinition

__all__ = ['Module', 'HiveModule']


class Module(object):
    """
    Modules define a set of metadata, including functions, user defined types, operators, rules,
    etc.
    Metadata from modules are regarded as built-in or system metadata that users can take advantages
    of.
    """

    def __init__(self, j_module):
        self._j_module = j_module

    @staticmethod
    def _get(j_module):
        if j_module.getClass().getName() == 'org.apache.flink.table.module.hive.HiveModule':
            return HiveModule(j_hive_module=j_module)
        else:
            return Module(j_module)

    def list_functions(self):
        """
        List names of all functions in this module.

        :return: A set of function names.
        :rtype: set
        """
        return self._j_module.listFunctions()

    def get_function_definition(self, name):
        """
        Get an optional of :class:`~pyflink.table.FunctionDefinition` by a given name.

        :param name: Name of the :class:`~pyflink.table.FunctionDefinition`.
        :type name: str
        :return: An optional function definition.
        :rtype: pyflink.table.FunctionDefinition
        """
        function_definition = self._j_module.getFunctionDefinition(name)
        if function_definition.isPresent():
            return FunctionDefinition(function_definition.get())
        else:
            return None


class HiveModule(Module):
    """
    Module to provide Hive built-in metadata.
    """

    def __init__(self, j_hive_module=None):
        gateway = get_gateway()

        if j_hive_module is None:
            j_hive_module = gateway.jvm.org.apache.flink.table.module.hive.HiveModule()
        super(HiveModule, self).__init__(j_hive_module)
