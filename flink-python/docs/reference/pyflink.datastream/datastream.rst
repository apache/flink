.. ################################################################################
     Licensed to the Apache Software Foundation (ASF) under one
     or more contributor license agreements.  See the NOTICE file
     distributed with this work for additional information
     regarding copyright ownership.  The ASF licenses this file
     to you under the Apache License, Version 2.0 (the
     "License"); you may not use this file except in compliance
     with the License.  You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

     Unless required by applicable law or agreed to in writing, software
     distributed under the License is distributed on an "AS IS" BASIS,
     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     See the License for the specific language governing permissions and
    limitations under the License.
   ################################################################################

==========
DataStream
==========

DataStream
----------

A DataStream represents a stream of elements of the same type.

.. currentmodule:: pyflink.datastream.data_stream

.. autosummary::
    :toctree: api/

    DataStream.get_name
    DataStream.name
    DataStream.uid
    DataStream.set_uid_hash
    DataStream.set_parallelism
    DataStream.set_max_parallelism
    DataStream.get_type
    DataStream.get_execution_environment
    DataStream.force_non_parallel
    DataStream.set_buffer_timeout
    DataStream.start_new_chain
    DataStream.disable_chaining
    DataStream.slot_sharing_group
    DataStream.set_description
    DataStream.map
    DataStream.flat_map
    DataStream.key_by
    DataStream.filter
    DataStream.union
    DataStream.connect
    DataStream.shuffle
    DataStream.project
    DataStream.rescale
    DataStream.rebalance
    DataStream.forward
    DataStream.broadcast
    DataStream.process
    DataStream.assign_timestamps_and_watermarks
    DataStream.partition_custom
    DataStream.add_sink
    DataStream.sink_to
    DataStream.execute_and_collect
    DataStream.print


DataStreamSink
--------------

A Stream Sink. This is used for emitting elements from a streaming topology.

.. currentmodule:: pyflink.datastream.data_stream

.. autosummary::
    :toctree: api/

    DataStreamSink.name
    DataStreamSink.uid
    DataStreamSink.set_uid_hash
    DataStreamSink.set_parallelism
    DataStreamSink.set_description
    DataStreamSink.disable_chaining
    DataStreamSink.slot_sharing_group


KeyedStream
-----------

A Stream Sink. This is used for emitting elements from a streaming topology.

.. currentmodule:: pyflink.datastream.data_stream

.. autosummary::
    :toctree: api/

    KeyedStream.map
    KeyedStream.flat_map
    KeyedStream.reduce
    KeyedStream.filter
    KeyedStream.add_sink
    KeyedStream.key_by
    KeyedStream.process
    KeyedStream.window
    KeyedStream.union
    KeyedStream.connect
    KeyedStream.partition_custom
    KeyedStream.print


WindowedStream
--------------

A WindowedStream represents a data stream where elements are grouped by key, and for each
key, the stream of elements is split into windows based on a WindowAssigner. Window emission
is triggered based on a Trigger.

The windows are conceptually evaluated for each key individually, meaning windows can trigger
at different points for each key.

Note that the WindowedStream is purely an API construct, during runtime the WindowedStream will
be collapsed together with the KeyedStream and the operation over the window into one single
operation.

.. currentmodule:: pyflink.datastream.data_stream

.. autosummary::
    :toctree: api/

    WindowedStream.get_execution_environment
    WindowedStream.get_input_type
    WindowedStream.trigger
    WindowedStream.allowed_lateness
    WindowedStream.apply
    WindowedStream.process


ConnectedStreams
----------------

ConnectedStreams represent two connected streams of (possibly) different data types.
Connected streams are useful for cases where operations on one stream directly
affect the operations on the other stream, usually via shared state between the streams.

An example for the use of connected streams would be to apply rules that change over time
onto another stream. One of the connected streams has the rules, the other stream the
elements to apply the rules to. The operation on the connected stream maintains the
current set of rules in the state. It may receive either a rule update and update the state
or a data element and apply the rules in the state to the element.

The connected stream can be conceptually viewed as a union stream of an Either type, that
holds either the first stream's type or the second stream's type.

.. currentmodule:: pyflink.datastream.data_stream

.. autosummary::
    :toctree: api/

    ConnectedStreams.key_by
    ConnectedStreams.map
    ConnectedStreams.flat_map
    ConnectedStreams.process

