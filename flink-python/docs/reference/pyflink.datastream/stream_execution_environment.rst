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

==========================
StreamExecutionEnvironment
==========================

StreamExecutionEnvironment
--------------------------

The StreamExecutionEnvironment is the context in which a streaming program is executed. A
*LocalStreamEnvironment* will cause execution in the attached JVM, a
*RemoteStreamEnvironment* will cause execution on a remote setup.

The environment provides methods to control the job execution (such as setting the parallelism
or the fault tolerance/checkpointing parameters) and to interact with the outside world (data
access).

.. currentmodule:: pyflink.datastream.stream_execution_environment

.. autosummary::
    :toctree: api/

    StreamExecutionEnvironment.get_config
    StreamExecutionEnvironment.set_parallelism
    StreamExecutionEnvironment.set_max_parallelism
    StreamExecutionEnvironment.register_slot_sharing_group
    StreamExecutionEnvironment.get_parallelism
    StreamExecutionEnvironment.get_max_parallelism
    StreamExecutionEnvironment.set_runtime_mode
    StreamExecutionEnvironment.set_buffer_timeout
    StreamExecutionEnvironment.get_buffer_timeout
    StreamExecutionEnvironment.disable_operator_chaining
    StreamExecutionEnvironment.is_chaining_enabled
    StreamExecutionEnvironment.get_checkpoint_config
    StreamExecutionEnvironment.enable_checkpointing
    StreamExecutionEnvironment.get_checkpoint_interval
    StreamExecutionEnvironment.get_checkpointing_mode
    StreamExecutionEnvironment.get_state_backend
    StreamExecutionEnvironment.set_state_backend
    StreamExecutionEnvironment.enable_changelog_state_backend
    StreamExecutionEnvironment.is_changelog_state_backend_enabled
    StreamExecutionEnvironment.set_default_savepoint_directory
    StreamExecutionEnvironment.get_default_savepoint_directory
    StreamExecutionEnvironment.set_restart_strategy
    StreamExecutionEnvironment.get_restart_strategy
    StreamExecutionEnvironment.add_default_kryo_serializer
    StreamExecutionEnvironment.register_type_with_kryo_serializer
    StreamExecutionEnvironment.register_type
    StreamExecutionEnvironment.set_stream_time_characteristic
    StreamExecutionEnvironment.get_stream_time_characteristic
    StreamExecutionEnvironment.configure
    StreamExecutionEnvironment.add_python_file
    StreamExecutionEnvironment.set_python_requirements
    StreamExecutionEnvironment.add_python_archive
    StreamExecutionEnvironment.set_python_executable
    StreamExecutionEnvironment.add_jars
    StreamExecutionEnvironment.add_classpaths
    StreamExecutionEnvironment.get_default_local_parallelism
    StreamExecutionEnvironment.set_default_local_parallelism
    StreamExecutionEnvironment.execute
    StreamExecutionEnvironment.execute_async
    StreamExecutionEnvironment.get_execution_plan
    StreamExecutionEnvironment.register_cached_file
    StreamExecutionEnvironment.get_execution_environment
    StreamExecutionEnvironment.create_input
    StreamExecutionEnvironment.add_source
    StreamExecutionEnvironment.from_source
    StreamExecutionEnvironment.read_text_file
    StreamExecutionEnvironment.from_collection
    StreamExecutionEnvironment.is_unaligned_checkpoints_enabled
    StreamExecutionEnvironment.is_force_unaligned_checkpoints
    StreamExecutionEnvironment.close


RuntimeExecutionMode
--------------------

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

.. currentmodule:: pyflink.datastream.execution_mode

.. autosummary::
    :toctree: api/

    RuntimeExecutionMode.STREAMING
    RuntimeExecutionMode.BATCH
    RuntimeExecutionMode.AUTOMATIC


SlotSharingGroup
----------------

.. currentmodule:: pyflink.datastream.slot_sharing_group

.. autosummary::
    :toctree: api/

    SlotSharingGroup
    MemorySize