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
Checkpoint
==========

CheckpointConfig
----------------

Configuration that captures all checkpointing related settings.

:data:`DEFAULT_MODE`:

The default checkpoint mode: exactly once.

:data:`DEFAULT_TIMEOUT`:

The default timeout of a checkpoint attempt: 10 minutes.

:data:`DEFAULT_MIN_PAUSE_BETWEEN_CHECKPOINTS`:

The default minimum pause to be made between checkpoints: none.

:data:`DEFAULT_MAX_CONCURRENT_CHECKPOINTS`:

The default limit of concurrently happening checkpoints: one.

.. currentmodule:: pyflink.datastream.checkpoint_config

.. autosummary::
    :toctree: api/

    CheckpointConfig.DEFAULT_MODE
    CheckpointConfig.DEFAULT_TIMEOUT
    CheckpointConfig.DEFAULT_MIN_PAUSE_BETWEEN_CHECKPOINTS
    CheckpointConfig.DEFAULT_MIN_PAUSE_BETWEEN_CHECKPOINTS
    CheckpointConfig.DEFAULT_MAX_CONCURRENT_CHECKPOINTS
    CheckpointConfig.is_checkpointing_enabled
    CheckpointConfig.get_checkpointing_mode
    CheckpointConfig.set_checkpointing_mode
    CheckpointConfig.get_checkpoint_interval
    CheckpointConfig.set_checkpoint_interval
    CheckpointConfig.get_checkpoint_timeout
    CheckpointConfig.set_checkpoint_timeout
    CheckpointConfig.get_min_pause_between_checkpoints
    CheckpointConfig.set_min_pause_between_checkpoints
    CheckpointConfig.get_max_concurrent_checkpoints
    CheckpointConfig.set_max_concurrent_checkpoints
    CheckpointConfig.is_fail_on_checkpointing_errors
    CheckpointConfig.set_fail_on_checkpointing_errors
    CheckpointConfig.get_tolerable_checkpoint_failure_number
    CheckpointConfig.set_tolerable_checkpoint_failure_number
    CheckpointConfig.enable_externalized_checkpoints
    CheckpointConfig.set_externalized_checkpoint_cleanup
    CheckpointConfig.is_externalized_checkpoints_enabled
    CheckpointConfig.get_externalized_checkpoint_cleanup
    CheckpointConfig.is_unaligned_checkpoints_enabled
    CheckpointConfig.enable_unaligned_checkpoints
    CheckpointConfig.disable_unaligned_checkpoints
    CheckpointConfig.set_alignment_timeout
    CheckpointConfig.get_alignment_timeout
    CheckpointConfig.set_force_unaligned_checkpoints
    CheckpointConfig.is_force_unaligned_checkpoints
    CheckpointConfig.set_checkpoint_storage
    CheckpointConfig.set_checkpoint_storage_dir
    CheckpointConfig.get_checkpoint_storage
    ExternalizedCheckpointCleanup


CheckpointStorage
-----------------

Checkpoint storage defines how :class:`StateBackend`'s store their state for fault-tolerance
in streaming applications. Various implementations store their checkpoints in different fashions
and have different requirements and availability guarantees.

For example, :class:`JobManagerCheckpointStorage` stores checkpoints in the memory of the
`JobManager`. It is lightweight and without additional dependencies but is not scalable
and only supports small state sizes. This checkpoints storage policy is convenient for local
testing and development.

:class:`FileSystemCheckpointStorage` stores checkpoints in a filesystem. For systems like HDFS
NFS drives, S3, and GCS, this storage policy supports large state size, in the magnitude of many
terabytes while providing a highly available foundation for streaming applications. This
checkpoint storage policy is recommended for most production deployments.

**Raw Bytes Storage**

The `CheckpointStorage` creates services for raw bytes storage.

The raw bytes storage (through the CheckpointStreamFactory) is the fundamental service that
simply stores bytes in a fault tolerant fashion. This service is used by the JobManager to
store checkpoint and recovery metadata and is typically also used by the keyed- and operator-
state backends to store checkpoint state.

**Serializability**

Implementations need to be serializable(`java.io.Serializable`), because they are distributed
across parallel processes (for distributed execution) together with the streaming application
code.

Because of that `CheckpointStorage` implementations are meant to be like _factories_ that create
the proper state stores that provide access to the persistent layer. That way, the storage
policy can be very lightweight (contain only configurations) which makes it easier to be
serializable.

**Thread Safety**

Checkpoint storage implementations have to be thread-safe. Multiple threads may be creating
streams concurrently.

.. currentmodule:: pyflink.datastream.checkpoint_storage

.. autosummary::
    :toctree: api/

    JobManagerCheckpointStorage
    FileSystemCheckpointStorage
    CustomCheckpointStorage
