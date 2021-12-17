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

__all__ = ['RuntimeExecutionMode']


class RuntimeExecutionMode(Enum):
    """
    Runtime execution mode of DataStream programs. Among other things, this controls task
    scheduling, network shuffle behavior, and time semantics. Some operations will also change
    their record emission behaviour based on the configured execution mode.

    :data:`STREAMING`:

    The Pipeline will be executed with Streaming Semantics. All tasks will be deployed before
    execution starts, checkpoints will be enabled, and both processing and event time will be
    fully supported.

    :data:`BATCH`:

    The Pipeline will be executed with Batch Semantics. Tasks will be scheduled gradually based
    on the scheduling region they belong, shuffles between regions will be blocking, watermarks
    are assumed to be "perfect" i.e. no late data, and processing time is assumed to not advance
    during execution.

    :data:`AUTOMATIC`:

    Flink will set the execution mode to BATCH if all sources are bounded, or STREAMING if there
    is at least one source which is unbounded.
    """
    STREAMING = 0
    BATCH = 1
    AUTOMATIC = 2

    @staticmethod
    def _from_j_execution_mode(j_execution_mode) -> 'RuntimeExecutionMode':
        return RuntimeExecutionMode[j_execution_mode.name()]

    def _to_j_execution_mode(self):
        gateway = get_gateway()
        JRuntimeExecutionMode = \
            gateway.jvm.org.apache.flink.api.common.RuntimeExecutionMode
        return getattr(JRuntimeExecutionMode, self.name)
