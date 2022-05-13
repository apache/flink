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
from enum import Enum

from pyflink.java_gateway import get_gateway

__all__ = ['InputDependencyConstraint']


class InputDependencyConstraint(Enum):
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
    def _from_j_input_dependency_constraint(j_input_dependency_constraint) \
            -> 'InputDependencyConstraint':
        return InputDependencyConstraint[j_input_dependency_constraint.name()]

    def _to_j_input_dependency_constraint(self):
        gateway = get_gateway()
        JInputDependencyConstraint = gateway.jvm.org.apache.flink.api.common \
            .InputDependencyConstraint
        return getattr(JInputDependencyConstraint, self.name)
