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

__all__ = ['ExecutionMode']


class ExecutionMode(Enum):
    """
    The execution mode specifies how a batch program is executed in terms
    of data exchange: pipelining or batched.

    :data:`PIPELINED`:

    Executes the program in a pipelined fashion (including shuffles and broadcasts),
    except for data exchanges that are susceptible to deadlocks when pipelining.
    These data exchanges are performed in a batch manner.

    An example of situations that are susceptible to deadlocks (when executed in a
    pipelined manner) are data flows that branch (one data set consumed by multiple
    operations) and re-join later.


    :data:`PIPELINED_FORCED`:

    Executes the program in a pipelined fashion (including shuffles and broadcasts),
    **including** data exchanges that are susceptible to deadlocks when
    executed via pipelining.

    Usually, PIPELINED is the preferable option, which pipelines most
    data exchanges and only uses batch data exchanges in situations that are
    susceptible to deadlocks.

    This option should only be used with care and only in situations where the
    programmer is sure that the program is safe for full pipelining and that
    Flink was too conservative when choosing the batch exchange at a certain
    point.

    :data:`BATCH`:

    This mode executes all shuffles and broadcasts in a batch fashion, while
    pipelining data between operations that exchange data only locally
    between one producer and one consumer.

    :data:`BATCH_FORCED`:

    This mode executes the program in a strict batch way, including all points
    where data is forwarded locally from one producer to one consumer. This mode
    is typically more expensive to execute than the BATCH mode. It does
    guarantee that no successive operations are ever executed concurrently.
    """

    PIPELINED = 0
    PIPELINED_FORCED = 1
    BATCH = 2
    BATCH_FORCED = 3

    @staticmethod
    def _from_j_execution_mode(j_execution_mode) -> 'ExecutionMode':
        return ExecutionMode[j_execution_mode.name()]

    def _to_j_execution_mode(self):
        gateway = get_gateway()
        JExecutionMode = gateway.jvm.org.apache.flink.api.common.ExecutionMode
        return getattr(JExecutionMode, self.name)
