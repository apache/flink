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

__all__ = ['InputDependencyConstraint']


class InputDependencyConstraint(object):
    """
    This constraint indicates when a task should be scheduled considering its inputs status.

    :data:`ANY`:

    Schedule the task if any input is consumable.

    :data:`ALL`:

    Schedule the task if all the inputs are consumable.
    """

    ANY = 0
    ALL = 1

    @staticmethod
    def _from_j_input_dependency_constraint(j_input_dependency_constraint):
        gateway = get_gateway()
        JInputDependencyConstraint = gateway.jvm.org.apache.flink.api.common \
            .InputDependencyConstraint
        if j_input_dependency_constraint == JInputDependencyConstraint.ANY:
            return InputDependencyConstraint.ANY
        elif j_input_dependency_constraint == JInputDependencyConstraint.ALL:
            return InputDependencyConstraint.ALL
        else:
            raise Exception("Unsupported java input dependency constraint: %s"
                            % j_input_dependency_constraint)

    @staticmethod
    def _to_j_input_dependency_constraint(input_dependency_constraint):
        gateway = get_gateway()
        JInputDependencyConstraint = gateway.jvm.org.apache.flink.api.common \
            .InputDependencyConstraint
        if input_dependency_constraint == InputDependencyConstraint.ANY:
            return JInputDependencyConstraint.ANY
        elif input_dependency_constraint == InputDependencyConstraint.ALL:
            return JInputDependencyConstraint.ALL
        else:
            raise TypeError("Unsupported input dependency constraint: %s, supported input "
                            "dependency constraints are: InputDependencyConstraint.ANY and "
                            "InputDependencyConstraint.ALL." % input_dependency_constraint)
