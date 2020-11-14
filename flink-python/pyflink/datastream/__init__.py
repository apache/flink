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
    - :class:`CheckpointingMode`:
      Defines what consistency guarantees the system gives in the presence of failures.
    - :class:`CoMapFunction`:
      Implements a map transformation over two connected streams.
    - :class:`CoFlatMapFunction`:
      Implements a flat-map transformation over two connected streams.
    - :class:`DataStream`:
      Represents a stream of elements of the same type. A DataStream can be transformed
      into another DataStream by applying a transformation
    - :class:`FlatMapFunction`:
      FlatMap functions take elements and transform them, into zero, one, or more elements.
    - :class:`FilterFunction`:
      A filter function is a predicate applied individually to each record.
    - :class:`KeySelector`:
      The extractor takes an object and returns the deterministic key for that object.
    - :class:`Partitioner`:
      Function to implement a custom partition assignment for keys.
    - :class:`ReduceFunction`:
      Reduce functions combine groups of elements to a single value.
    - :class:`SinkFunction`:
      Interface for implementing user defined sink functionality.
    - :class:`SourceFunction`:
      Interface for implementing user defined source functionality.
    - :class:`StateBackend`:
      Defines how the state of a streaming application is stored and checkpointed.
    - :class:`MapFunction`:
      Map functions take elements and transform them, element wise.
    - :class:`MemoryStateBackend`:
      This state backend holds the working state in the memory (JVM heap) of the TaskManagers.
    - :class:`FsStateBackend`:
      The state backend checkpoints state as files to a file system.
    - :class:`RocksDBStateBackend`:
      A State Backend that stores its state in `RocksDB`.
    - :class:`CustomStateBackend`:
      A wrapper of customized java state backend created from the provided `StateBackendFactory`.
    - :class:`PredefinedOptions`:
      Configuration settings for the `RocksDBStateBackend`.
    - :class:`ExternalizedCheckpointCleanup`:
      Cleanup behaviour for externalized checkpoints when the job is cancelled.
    - :class:`TimeCharacteristic`:
      The time characteristic defines how the system determines time for time-dependent
      order and operations that depend on time (such as time windows).
"""
from pyflink.datastream.checkpoint_config import CheckpointConfig, ExternalizedCheckpointCleanup
from pyflink.datastream.checkpointing_mode import CheckpointingMode
from pyflink.datastream.data_stream import DataStream
from pyflink.datastream.functions import (MapFunction, CoMapFunction, FlatMapFunction,
                                          CoFlatMapFunction, ReduceFunction, RuntimeContext,
                                          KeySelector, FilterFunction, Partitioner, SourceFunction,
                                          SinkFunction)
from pyflink.datastream.state_backend import (StateBackend, MemoryStateBackend, FsStateBackend,
                                              RocksDBStateBackend, CustomStateBackend,
                                              PredefinedOptions)
from pyflink.datastream.stream_execution_environment import StreamExecutionEnvironment
from pyflink.datastream.time_characteristic import TimeCharacteristic
from pyflink.datastream.time_domain import TimeDomain
from pyflink.datastream.functions import ProcessFunction, Collector, TimerService

__all__ = [
    'StreamExecutionEnvironment',
    'CheckpointConfig',
    'CheckpointingMode',
    'CoMapFunction',
    'CoFlatMapFunction',
    'DataStream',
    'FlatMapFunction',
    'FilterFunction',
    'KeySelector',
    'Partitioner',
    'ReduceFunction',
    'RuntimeContext',
    'SinkFunction',
    'SourceFunction',
    'StateBackend',
    'MapFunction',
    'MemoryStateBackend',
    'FsStateBackend',
    'RocksDBStateBackend',
    'CustomStateBackend',
    'PredefinedOptions',
    'ExternalizedCheckpointCleanup',
    'TimeCharacteristic',
    'TimeDomain',
    'ProcessFunction',
    'Collector',
    'TimerService'
]
