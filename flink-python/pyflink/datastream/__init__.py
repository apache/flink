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
Important classes of Flink Streaming API:

    - :class:`StreamExecutionEnvironment`:
      The context in which a streaming program is executed.
    - :class:`CheckpointConfig`:
      Configuration that captures all checkpointing related settings.
    - :class:`StateBackend`:
      Defines how the state of a streaming application is stored and checkpointed.
"""
from pyflink.datastream.checkpoint_config import CheckpointConfig, ExternalizedCheckpointCleanup
from pyflink.datastream.checkpointing_mode import CheckpointingMode
from pyflink.datastream.state_backend import (StateBackend, MemoryStateBackend, FsStateBackend,
                                              RocksDBStateBackend, CustomStateBackend,
                                              PredefinedOptions)
from pyflink.datastream.stream_execution_environment import StreamExecutionEnvironment
from pyflink.datastream.time_characteristic import TimeCharacteristic

__all__ = [
    'StreamExecutionEnvironment',
    'CheckpointConfig',
    'CheckpointingMode',
    'StateBackend',
    'MemoryStateBackend',
    'FsStateBackend',
    'RocksDBStateBackend',
    'CustomStateBackend',
    'PredefinedOptions',
    'ExternalizedCheckpointCleanup',
    'TimeCharacteristic'
]
