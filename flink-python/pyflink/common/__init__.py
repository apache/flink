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

"""
Important classes used by both Flink Streaming and Batch API:

    - :class:`ExecutionConfig`:
      A config to define the behavior of the program execution.
"""
from pyflink.common.completable_future import CompletableFuture
from pyflink.common.configuration import Configuration
from pyflink.common.execution_config import ExecutionConfig
from pyflink.common.execution_mode import ExecutionMode
from pyflink.common.input_dependency_constraint import InputDependencyConstraint
from pyflink.common.job_client import JobClient
from pyflink.common.job_execution_result import JobExecutionResult
from pyflink.common.job_status import JobStatus
from pyflink.common.restart_strategy import RestartStrategies, RestartStrategyConfiguration

__all__ = [
    'CompletableFuture',
    'Configuration',
    'ExecutionConfig',
    'ExecutionMode',
    'InputDependencyConstraint',
    'JobClient',
    'JobExecutionResult',
    'JobStatus',
    'RestartStrategies',
    'RestartStrategyConfiguration',
]
