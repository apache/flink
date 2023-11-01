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


=====
State
=====

OperatorStateStore
------------------

.. currentmodule:: pyflink.datastream.state

.. autosummary::
    :toctree: api/

    OperatorStateStore.get_broadcast_state


State
-----

.. currentmodule:: pyflink.datastream.state

.. autosummary::
    :toctree: api/

    ValueState
    AppendingState
    MergingState
    ReducingState
    AggregatingState
    ListState
    MapState
    ReadOnlyBroadcastState
    BroadcastState


StateDescriptor
---------------

.. currentmodule:: pyflink.datastream.state

.. autosummary::
    :toctree: api/

    ValueStateDescriptor
    ListStateDescriptor
    MapStateDescriptor
    ReducingStateDescriptor
    AggregatingStateDescriptor


StateTtlConfig
--------------

.. currentmodule:: pyflink.datastream.state

.. autoclass:: pyflink.datastream.state::StateTtlConfig.UpdateType
    :members:

.. autoclass:: pyflink.datastream.state::StateTtlConfig.StateVisibility
    :members:

.. autoclass:: pyflink.datastream.state::StateTtlConfig.TtlTimeCharacteristic
    :members:

.. autoclass:: pyflink.datastream.state::StateTtlConfig.CleanupStrategies
    :members:

.. currentmodule:: pyflink.datastream.state

.. autosummary::
    :toctree: api/

    StateTtlConfig.new_builder
    StateTtlConfig.get_update_type
    StateTtlConfig.get_state_visibility
    StateTtlConfig.get_ttl
    StateTtlConfig.get_ttl_time_characteristic
    StateTtlConfig.is_enabled
    StateTtlConfig.get_cleanup_strategies
    StateTtlConfig.Builder.set_update_type
    StateTtlConfig.Builder.update_ttl_on_create_and_write
    StateTtlConfig.Builder.update_ttl_on_read_and_write
    StateTtlConfig.Builder.set_state_visibility
    StateTtlConfig.Builder.return_expired_if_not_cleaned_up
    StateTtlConfig.Builder.never_return_expired
    StateTtlConfig.Builder.set_ttl_time_characteristic
    StateTtlConfig.Builder.use_processing_time
    StateTtlConfig.Builder.cleanup_full_snapshot
    StateTtlConfig.Builder.cleanup_incrementally
    StateTtlConfig.Builder.cleanup_in_rocksdb_compact_filter
    StateTtlConfig.Builder.disable_cleanup_in_background
    StateTtlConfig.Builder.set_ttl
    StateTtlConfig.Builder.build


StateBackend
------------

A **State Backend** defines how the state of a streaming application is stored locally within
the cluster. Different state backends store their state in different fashions, and use different
data structures to hold the state of running applications.

For example, the :class:`HashMapStateBackend` keeps working state in the memory of the
TaskManager. The backend is lightweight and without additional dependencies.

The :class:`EmbeddedRocksDBStateBackend` keeps working state in the memory of the TaskManager
and stores state checkpoints in a filesystem(typically a replicated highly-available filesystem,
like `HDFS <https://hadoop.apache.org/>`_, `Ceph <https://ceph.com/>`_,
`S3 <https://aws.amazon.com/documentation/s3/>`_, `GCS <https://cloud.google.com/storage/>`_,
etc).

The :class:`EmbeddedRocksDBStateBackend` stores working state in an embedded
`RocksDB <http://rocksdb.org/>`_, instance and is able to scale working state to many
terrabytes in size, only limited by available disk space across all task managers.

**Raw Bytes Storage and Backends**

The :class:`StateBackend` creates services for *raw bytes storage* and for *keyed state*
and *operator state*.

The `org.apache.flink.runtime.state.AbstractKeyedStateBackend and
`org.apache.flink.runtime.state.OperatorStateBackend` created by this state backend define how
to hold the working state for keys and operators. They also define how to checkpoint that
state, frequently using the raw bytes storage (via the
`org.apache.flink.runtime.state.CheckpointStreamFactory`). However, it is also possible that
for example a keyed state backend simply implements the bridge to a key/value store, and that
it does not need to store anything in the raw byte storage upon a checkpoint.

**Serializability**

State Backends need to be serializable(`java.io.Serializable`), because they distributed
across parallel processes (for distributed execution) together with the streaming application
code.

Because of that, :class:`StateBackend` implementations are meant to be like *factories* that
create the proper states stores that provide access to the persistent storage and hold the
keyed- and operator state data structures. That way, the State Backend can be very lightweight
(contain only configurations) which makes it easier to be serializable.

**Thread Safety**

State backend implementations have to be thread-safe. Multiple threads may be creating
streams and keyed-/operator state backends concurrently.

.. currentmodule:: pyflink.datastream.state_backend

.. autosummary::
    :toctree: api/

    HashMapStateBackend
    EmbeddedRocksDBStateBackend
    MemoryStateBackend
    FsStateBackend
    RocksDBStateBackend
    CustomStateBackend
    PredefinedOptions
