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
Entry point classes of Flink DataStream API:

    - :class:`StreamExecutionEnvironment`:
      The context in which a streaming program is executed.
    - :class:`DataStream`:
      Represents a stream of elements of the same type. A DataStream can be transformed
      into another DataStream by applying a transformation.
    - :class:`KeyedStream`:
      Represents a :class:`DataStream` where elements are partitioned by key using a
      provided KeySelector.
    - :class:`WindowedStream`:
      Represents a data stream where elements are grouped by key, and for each
      key, the stream of elements is split into windows based on a WindowAssigner. Window emission
      is triggered based on a Trigger.
    - :class:`ConnectedStreams`:
      Represent two connected streams of (possibly) different data types. Connected
      streams are useful for cases where operations on one stream directly affect the operations on
      the other stream, usually via shared state between the streams.

Functions used to transform a :class:`DataStream` into another :class:`DataStream`:

    - :class:`MapFunction`:
      Performs a map transformation of a :class:`DataStream` at element wise.
    - :class:`CoMapFunction`:
      Performs a map transformation over two connected streams.
    - :class:`FlatMapFunction`:
      Performs a flatmap transformation of a :class:`DataStream` which produces zero, one, or more
      elements for each input element.
    - :class:`CoFlatMapFunction`:
      Performs a flatmap transformation over two connected streams.
    - :class:`FilterFunction`:
      A filter function is a predicate applied individually to each record.
    - :class:`ReduceFunction`:
      Combines groups of elements to a single value.
    - :class:`ProcessFunction`:
      Similar to :class:`FlatMapFunction`, except that it could access the current timestamp and
      watermark in :class:`ProcessFunction`.
    - :class:`KeyedProcessFunction`:
      Similar to :class:`ProcessFunction`, except that it was applied to a :class:`KeyedStream` and
      could register event-time and processing-time timers.
    - :class:`KeyedCoProcessFunction`:
      Similar to :class:`CoFlatMapFunction`, except that it was applied to a keyed
      :class:`ConnectedStreams` and could register event-time and processing-time timers.
    - :class:`WindowFunction`:
      Base interface for functions that are evaluated over keyed (grouped) windows.
    - :class:`ProcessWindowFunction`:
      Similar to :class:`WindowFunction`, except that it could access a context for retrieving extra
      information such as the current timestamp, the watermark, etc.
    - :class:`AggregateFunction`:
      Base class for a user-defined aggregate function.
    - :class:`RuntimeContext`:
      Contains information about the context in which functions are executed. Each
      parallel instance of the function will have a context through which it can access static
      contextual information (such as the current parallelism), etc.

Classes to define window:

    - :class:`Window`:
      A grouping of elements into finite buckets.
    - :class:`TimeWindow`:
      A grouping of elements according to a time interval from start (inclusive) to end (exclusive).
    - :class:`CountWindow`:
      A grouping of elements according to element count from start (inclusive) to end (exclusive).
    - :class:`WindowAssigner`:
      Assigns zero or more :class:`Window` to an element.
    - :class:`MergingWindowAssigner`:
      A :class:`WindowAssigner` that can merge windows.
    - :class:`TriggerResult`:
      Result type for trigger methods. This determines what happens with the window, for example
      whether the window function should be called, or the window should be discarded.
    - :class:`Trigger`:
      Determines when a pane of a window should be evaluated to emit the results for that
      part of the window.

Classes to define the behavior of checkpoint and state backend:

    - :class:`CheckpointingMode`:
      Defines what consistency guarantees the system gives in the presence of failures.
    - :class:`CheckpointConfig`:
      Configuration that captures all checkpointing related settings.
    - :class:`StateBackend`:
      Base class of the state backends which define how the state of a streaming application is
      stored locally within the cluster. Different state backends store their state in different
      fashions, and use different data structures to hold the state of a running application.
    - :class:`MemoryStateBackend`:
      holds the working state in the memory (JVM heap) of the TaskManagers.
      The state backend checkpoints state directly to the JobManager's memory (hence the backend's
      name), but the checkpoints will be persisted to a file system for high-availability setups and
      savepoints. The MemoryStateBackend is consequently a FileSystem-based backend that can work
      without a file system dependency in simple setups.
    - :class:`RocksDBStateBackend`:
      Stores its state in ``RocksDB``. This state backend can store very large state that
      exceeds memory and spills to disk.
    - :class:`FsStateBackend`:
      Holds the working state in the memory (JVM heap) of the TaskManagers.
      The state backend checkpoints state as files to a file system (hence the backend's name).
    - :class:`CustomStateBackend`:
      A wrapper of customized java state backend.

Classes for state operations:

    - :class:`state.ValueState`:
      Interface for partitioned single-value state. The value can be retrieved or updated.
    - :class:`state.ListState`:
      Interface for partitioned list state in Operations. The state is accessed and modified by
      user functions, and checkpointed consistently by the system as part of the distributed
      snapshots.
    - :class:`state.MapState`:
      Interface for partitioned key-value state. The key-value pair can be added, updated and
      retrieved.
    - :class:`state.ReducingState`:
      Interface for reducing state. Elements can be added to the state, they will be combined using
      a :class:`ReduceFunction`. The current state can be inspected.
    - :class:`state.AggregatingState`:
      Interface for aggregating state, based on an :class:`AggregateFunction`. Elements that are
      added to this type of state will be eagerly pre-aggregated using a given AggregateFunction.

Classes to define source & sink:

    - :class:`connectors.FlinkKafkaConsumer`:
      A streaming data source that pulls a parallel data stream from Apache Kafka.
    - :class:`connectors.FlinkKafkaProducer`:
      A streaming data sink to produce data into a Kafka topic.
    - :class:`connectors.FileSource`:
      A unified data source that reads files - both in batch and in streaming mode.
      This source supports all (distributed) file systems and object stores that can be accessed via
      the Flink's FileSystem class.
    - :class:`connectors.FileSink`:
      A unified sink that emits its input elements to FileSystem files within buckets. This
      sink achieves exactly-once semantics for both BATCH and STREAMING.
    - :class:`connectors.NumberSequenceSource`:
      A data source that produces a sequence of numbers (longs). This source is useful for testing
      and for cases that just need a stream of N events of any kind.
    - :class:`connectors.JdbcSink`:
      A data sink to produce data into an external storage using JDBC.
    - :class:`connectors.StreamingFileSink`:
      Sink that emits its input elements to files within buckets. This is integrated with the
      checkpointing mechanism to provide exactly once semantics.

Other important classes:

    - :class:`TimeCharacteristic`:
      Defines how the system determines time for time-dependent order and operations that depend
      on time (such as time windows).
    - :class:`TimeDomain`:
      Specifies whether a firing timer is based on event time or processing time.
    - :class:`KeySelector`:
      The extractor takes an object and returns the deterministic key for that object.
    - :class:`Partitioner`:
      Function to implement a custom partition assignment for keys.
    - :class:`SinkFunction`:
      Interface for implementing user defined sink functionality.
    - :class:`SourceFunction`:
      Interface for implementing user defined source functionality.
"""
from pyflink.datastream.checkpoint_config import CheckpointConfig, ExternalizedCheckpointCleanup
from pyflink.datastream.checkpointing_mode import CheckpointingMode
from pyflink.datastream.data_stream import DataStream, KeyedStream, WindowedStream, \
    ConnectedStreams, DataStreamSink
from pyflink.datastream.execution_mode import RuntimeExecutionMode
from pyflink.datastream.functions import (MapFunction, CoMapFunction, FlatMapFunction,
                                          CoFlatMapFunction, ReduceFunction, RuntimeContext,
                                          KeySelector, FilterFunction, Partitioner, SourceFunction,
                                          SinkFunction, KeyedProcessFunction,
                                          KeyedCoProcessFunction, AggregateFunction, WindowFunction,
                                          ProcessWindowFunction)
from pyflink.datastream.state_backend import (StateBackend, MemoryStateBackend, FsStateBackend,
                                              RocksDBStateBackend, CustomStateBackend,
                                              PredefinedOptions)
from pyflink.datastream.stream_execution_environment import StreamExecutionEnvironment
from pyflink.datastream.time_characteristic import TimeCharacteristic
from pyflink.datastream.time_domain import TimeDomain
from pyflink.datastream.functions import ProcessFunction
from pyflink.datastream.timerservice import TimerService
from pyflink.datastream.window import Window, TimeWindow, CountWindow, WindowAssigner, \
    MergingWindowAssigner, TriggerResult, Trigger

__all__ = [
    'StreamExecutionEnvironment',
    'DataStream',
    'KeyedStream',
    'WindowedStream',
    'ConnectedStreams',
    'DataStreamSink',
    'MapFunction',
    'CoMapFunction',
    'FlatMapFunction',
    'CoFlatMapFunction',
    'ReduceFunction',
    'FilterFunction',
    'ProcessFunction',
    'KeyedProcessFunction',
    'KeyedCoProcessFunction',
    'WindowFunction',
    'ProcessWindowFunction',
    'AggregateFunction',
    'RuntimeContext',
    'TimerService',
    'CheckpointingMode',
    'CheckpointConfig',
    'ExternalizedCheckpointCleanup',
    'StateBackend',
    'CustomStateBackend',
    'MemoryStateBackend',
    'RocksDBStateBackend',
    'FsStateBackend',
    'PredefinedOptions',
    'RuntimeExecutionMode',
    'Window',
    'TimeWindow',
    'CountWindow',
    'WindowAssigner',
    'MergingWindowAssigner',
    'TriggerResult',
    'Trigger',
    'TimeCharacteristic',
    'TimeDomain',
    'KeySelector',
    'Partitioner',
    'SourceFunction',
    'SinkFunction'
]
