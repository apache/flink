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
__all__ = ['FunctionDefinition']


class FunctionDefinition(object):
    """
    Definition of a function. Instances of this class provide all details necessary to validate a
    function call and perform planning.

    A pure function definition must not contain a runtime implementation. This can be provided by
    the planner at later stages.
    """

    def __init__(self, j_function_definition):
        self._j_function_definition = j_function_definition

    def is_deterministic(self):
        """
        Returns information about the determinism of the function's results.

        It returns true if and only if a call to this function is guaranteed to
        always return the same result given the same parameters. true is
        assumed by default. If the function is not pure functional like random(), date(), now(), ...
        this method must return false.

        :return: The determinism of the function's results.
        :rtype: bool
        """
        return self._j_function_definition.isDeterministic()
