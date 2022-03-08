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
import os
import tempfile

from typing import List, Any, Optional

from py4j.java_gateway import JavaObject

from pyflink.common import Configuration, WatermarkStrategy
from pyflink.common.execution_config import ExecutionConfig
from pyflink.common.job_client import JobClient
from pyflink.common.job_execution_result import JobExecutionResult
from pyflink.common.restart_strategy import RestartStrategies, RestartStrategyConfiguration
from pyflink.common.typeinfo import TypeInformation, Types
from pyflink.datastream import SlotSharingGroup
from pyflink.datastream.checkpoint_config import CheckpointConfig
from pyflink.datastream.checkpointing_mode import CheckpointingMode
from pyflink.datastream.connectors import Source
from pyflink.datastream.data_stream import DataStream
from pyflink.datastream.execution_mode import RuntimeExecutionMode
from pyflink.datastream.functions import SourceFunction
from pyflink.datastream.state_backend import _from_j_state_backend, StateBackend
from pyflink.datastream.time_characteristic import TimeCharacteristic
from pyflink.java_gateway import get_gateway
from pyflink.serializers import PickleSerializer
from pyflink.util.java_utils import load_java_class, add_jars_to_context_class_loader, \
    invoke_method, get_field_value, is_local_deployment, get_j_env_configuration

__all__ = ['StreamExecutionEnvironment']


class StreamExecutionEnvironment(object):
    """
    The StreamExecutionEnvironment is the context in which a streaming program is executed. A
    *LocalStreamEnvironment* will cause execution in the attached JVM, a
    *RemoteStreamEnvironment* will cause execution on a remote setup.

    The environment provides methods to control the job execution (such as setting the parallelism
    or the fault tolerance/checkpointing parameters) and to interact with the outside world (data
    access).
    """

    def __init__(self, j_stream_execution_environment, serializer=PickleSerializer()):
        self._j_stream_execution_environment = j_stream_execution_environment
        self.serializer = serializer
        self._open()

    def get_config(self) -> ExecutionConfig:
        """
        Gets the config object.

        :return: The :class:`~pyflink.common.ExecutionConfig` object.
        """
        return ExecutionConfig(self._j_stream_execution_environment.getConfig())

    def set_parallelism(self, parallelism: int) -> 'StreamExecutionEnvironment':
        """
        Sets the parallelism for operations executed through this environment.
        Setting a parallelism of x here will cause all operators (such as map,
        batchReduce) to run with x parallel instances. This method overrides the
        default parallelism for this environment. The
        *LocalStreamEnvironment* uses by default a value equal to the
        number of hardware contexts (CPU cores / threads). When executing the
        program via the command line client from a JAR file, the default degree
        of parallelism is the one configured for that setup.

        :param parallelism: The parallelism.
        :return: This object.
        """
        self._j_stream_execution_environment = \
            self._j_stream_execution_environment.setParallelism(parallelism)
        return self

    def set_max_parallelism(self, max_parallelism: int) -> 'StreamExecutionEnvironment':
        """
        Sets the maximum degree of parallelism defined for the program. The upper limit (inclusive)
        is 32767.

        The maximum degree of parallelism specifies the upper limit for dynamic scaling. It also
        defines the number of key groups used for partitioned state.

        :param max_parallelism: Maximum degree of parallelism to be used for the program,
                                with 0 < maxParallelism <= 2^15 - 1.
        :return: This object.
        """
        self._j_stream_execution_environment = \
            self._j_stream_execution_environment.setMaxParallelism(max_parallelism)
        return self

    def register_slot_sharing_group(self, slot_sharing_group: SlotSharingGroup) \
            -> 'StreamExecutionEnvironment':
        """
        Register a slot sharing group with its resource spec.

        Note that a slot sharing group hints the scheduler that the grouped operators CAN be
        deployed into a shared slot. There's no guarantee that the scheduler always deploy the
        grouped operators together. In cases grouped operators are deployed into separate slots, the
        slot resources will be derived from the specified group requirements.

        :param slot_sharing_group: Which contains name and its resource spec.
        :return: This object.
        """
        self._j_stream_execution_environment = \
            self._j_stream_execution_environment.registerSlotSharingGroup(
                slot_sharing_group.get_java_slot_sharing_group())
        return self

    def get_parallelism(self) -> int:
        """
        Gets the parallelism with which operation are executed by default.
        Operations can individually override this value to use a specific
        parallelism.

        :return: The parallelism used by operations, unless they override that value.
        """
        return self._j_stream_execution_environment.getParallelism()

    def get_max_parallelism(self) -> int:
        """
        Gets the maximum degree of parallelism defined for the program.

        The maximum degree of parallelism specifies the upper limit for dynamic scaling. It also
        defines the number of key groups used for partitioned state.

        :return: Maximum degree of parallelism.
        """
        return self._j_stream_execution_environment.getMaxParallelism()

    def set_runtime_mode(self, execution_mode: RuntimeExecutionMode):
        """
        Sets the runtime execution mode for the application
        :class:`~pyflink.datastream.execution_mode.RuntimeExecutionMode`. This
        is equivalent to setting the `execution.runtime-mode` in your application's
        configuration file.

        We recommend users to NOT use this method but set the `execution.runtime-mode` using
        the command-line when submitting the application. Keeping the application code
        configuration-free allows for more flexibility as the same application will be able to be
        executed in any execution mode.

        :param execution_mode: The desired execution mode.
        :return: The execution environment of your application.

        .. versionadded:: 1.13.0
        """
        return self._j_stream_execution_environment.setRuntimeMode(
            execution_mode._to_j_execution_mode())

    def set_buffer_timeout(self, timeout_millis: int) -> 'StreamExecutionEnvironment':
        """
        Sets the maximum time frequency (milliseconds) for the flushing of the
        output buffers. By default the output buffers flush frequently to provide
        low latency and to aid smooth developer experience. Setting the parameter
        can result in three logical modes:

        - A positive integer triggers flushing periodically by that integer
        - 0 triggers flushing after every record thus minimizing latency
        - -1 triggers flushing only when the output buffer is full thus maximizing throughput

        :param timeout_millis: The maximum time between two output flushes.
        :return: This object.
        """
        self._j_stream_execution_environment = \
            self._j_stream_execution_environment.setBufferTimeout(timeout_millis)
        return self

    def get_buffer_timeout(self) -> int:
        """
        Gets the maximum time frequency (milliseconds) for the flushing of the
        output buffers. For clarification on the extremal values see
        :func:`set_buffer_timeout`.

        :return: The timeout of the buffer.
        """
        return self._j_stream_execution_environment.getBufferTimeout()

    def disable_operator_chaining(self) -> 'StreamExecutionEnvironment':
        """
        Disables operator chaining for streaming operators. Operator chaining
        allows non-shuffle operations to be co-located in the same thread fully
        avoiding serialization and de-serialization.

        :return: This object.
        """
        self._j_stream_execution_environment = \
            self._j_stream_execution_environment.disableOperatorChaining()
        return self

    def is_chaining_enabled(self) -> bool:
        """
        Returns whether operator chaining is enabled.

        :return: True if chaining is enabled, false otherwise.
        """
        return self._j_stream_execution_environment.isChainingEnabled()

    def get_checkpoint_config(self) -> CheckpointConfig:
        """
        Gets the checkpoint config, which defines values like checkpoint interval, delay between
        checkpoints, etc.

        :return: The :class:`~pyflink.datastream.CheckpointConfig`.
        """
        j_checkpoint_config = self._j_stream_execution_environment.getCheckpointConfig()
        return CheckpointConfig(j_checkpoint_config)

    def enable_checkpointing(self, interval: int, mode: CheckpointingMode = None) \
            -> 'StreamExecutionEnvironment':
        """
        Enables checkpointing for the streaming job. The distributed state of the streaming
        dataflow will be periodically snapshotted. In case of a failure, the streaming
        dataflow will be restarted from the latest completed checkpoint.

        The job draws checkpoints periodically, in the given interval. The system uses the
        given :class:`~pyflink.datastream.CheckpointingMode` for the checkpointing ("exactly once"
        vs "at least once"). The state will be stored in the configured state backend.

        .. note::
            Checkpointing iterative streaming dataflows in not properly supported at
            the moment. For that reason, iterative jobs will not be started if used
            with enabled checkpointing.

        Example:
        ::

            >>> env.enable_checkpointing(300000, CheckpointingMode.AT_LEAST_ONCE)

        :param interval: Time interval between state checkpoints in milliseconds.
        :param mode: The checkpointing mode, selecting between "exactly once" and "at least once"
                     guaranteed.
        :return: This object.
        """
        if mode is None:
            self._j_stream_execution_environment = \
                self._j_stream_execution_environment.enableCheckpointing(interval)
        else:
            j_checkpointing_mode = CheckpointingMode._to_j_checkpointing_mode(mode)
            self._j_stream_execution_environment.enableCheckpointing(
                interval,
                j_checkpointing_mode)
        return self

    def get_checkpoint_interval(self) -> int:
        """
        Returns the checkpointing interval or -1 if checkpointing is disabled.

        Shorthand for get_checkpoint_config().get_checkpoint_interval().

        :return: The checkpointing interval or -1.
        """
        return self._j_stream_execution_environment.getCheckpointInterval()

    def get_checkpointing_mode(self) -> CheckpointingMode:
        """
        Returns the checkpointing mode (exactly-once vs. at-least-once).

        Shorthand for get_checkpoint_config().get_checkpointing_mode().

        :return: The :class:`~pyflink.datastream.CheckpointingMode`.
        """
        j_checkpointing_mode = self._j_stream_execution_environment.getCheckpointingMode()
        return CheckpointingMode._from_j_checkpointing_mode(j_checkpointing_mode)

    def get_state_backend(self) -> StateBackend:
        """
        Gets the state backend that defines how to store and checkpoint state.

        .. seealso:: :func:`set_state_backend`

        :return: The :class:`StateBackend`.
        """
        j_state_backend = self._j_stream_execution_environment.getStateBackend()
        return _from_j_state_backend(j_state_backend)

    def set_state_backend(self, state_backend: StateBackend) -> 'StreamExecutionEnvironment':
        """
        Sets the state backend that describes how to store and checkpoint operator state. It
        defines both which data structures hold state during execution (for example hash tables,
        RockDB, or other data stores) as well as where checkpointed data will be persisted.

        The :class:`~pyflink.datastream.MemoryStateBackend` for example maintains the state in heap
        memory, as objects. It is lightweight without extra dependencies, but can checkpoint only
        small states(some counters).

        In contrast, the :class:`~pyflink.datastream.FsStateBackend` stores checkpoints of the state
        (also maintained as heap objects) in files. When using a replicated file system (like HDFS,
        S3, Alluxio, etc) this will guarantee that state is not lost upon failures of
        individual nodes and that streaming program can be executed highly available and strongly
        consistent(assuming that Flink is run in high-availability mode).

        The build-in state backend includes:
            :class:`~pyflink.datastream.MemoryStateBackend`,
            :class:`~pyflink.datastream.FsStateBackend`
            and :class:`~pyflink.datastream.RocksDBStateBackend`.

        .. seealso:: :func:`get_state_backend`

        Example:
        ::

            >>> env.set_state_backend(EmbeddedRocksDBStateBackend())

        :param state_backend: The :class:`StateBackend`.
        :return: This object.
        """
        self._j_stream_execution_environment = \
            self._j_stream_execution_environment.setStateBackend(state_backend._j_state_backend)
        return self

    def enable_changelog_state_backend(self, enabled: bool) -> 'StreamExecutionEnvironment':
        """
        Enable the change log for current state backend. This change log allows operators to persist
        state changes in a very fine-grained manner. Currently, the change log only applies to keyed
        state, so non-keyed operator state and channel state are persisted as usual. The 'state'
        here refers to 'keyed state'. Details are as follows:

        * Stateful operators write the state changes to that log (logging the state), in addition \
        to applying them to the state tables in RocksDB or the in-mem Hashtable.
        * An operator can acknowledge a checkpoint as soon as the changes in the log have reached \
        the durable checkpoint storage.
        * The state tables are persisted periodically, independent of the checkpoints. We call \
        this the materialization of the state on the checkpoint storage.
        * Once the state is materialized on checkpoint storage, the state changelog can be \
        truncated to the corresponding point.

        It establish a way to drastically reduce the checkpoint interval for streaming
        applications across state backends. For more details please check the FLIP-158.

        If this method is not called explicitly, it means no preference for enabling the change
        log. Configs for change log enabling will override in different config levels
        (job/local/cluster).

        .. seealso:: :func:`is_changelog_state_backend_enabled`


        :param enabled: True if enable the change log for state backend explicitly, otherwise
                        disable the change log.
        :return: This object.

        .. versionadded:: 1.14.0
        """
        self._j_stream_execution_environment = \
            self._j_stream_execution_environment.enableChangelogStateBackend(enabled)
        return self

    def is_changelog_state_backend_enabled(self) -> Optional[bool]:
        """
        Gets the enable status of change log for state backend.

        .. seealso:: :func:`enable_changelog_state_backend`

        :return: An :class:`Optional[bool]` for the enable status of change log for state backend.
                 Could be None if user never specify this by calling
                 :func:`enable_changelog_state_backend`.

        .. versionadded:: 1.14.0
        """
        j_ternary_boolean = self._j_stream_execution_environment.isChangelogStateBackendEnabled()
        return j_ternary_boolean.getAsBoolean()

    def set_default_savepoint_directory(self, directory: str) -> 'StreamExecutionEnvironment':
        """
        Sets the default savepoint directory, where savepoints will be written to if none
        is explicitly provided when triggered.

        Example:
        ::

            >>> env.set_default_savepoint_directory("hdfs://savepoints")

        :param directory The savepoint directory
        :return: This object.
        """
        self._j_stream_execution_environment.setDefaultSavepointDirectory(directory)
        return self

    def get_default_savepoint_directory(self) -> Optional[str]:
        """
        Gets the default savepoint directory for this Job.
        """
        j_path = self._j_stream_execution_environment.getDefaultSavepointDirectory()
        if j_path is None:
            return None
        else:
            return j_path.toString()

    def set_restart_strategy(self, restart_strategy_configuration: RestartStrategyConfiguration):
        """
        Sets the restart strategy configuration. The configuration specifies which restart strategy
        will be used for the execution graph in case of a restart.

        Example:
        ::

            >>> env.set_restart_strategy(RestartStrategies.no_restart())

        :param restart_strategy_configuration: Restart strategy configuration to be set.
        :return:
        """
        self._j_stream_execution_environment.setRestartStrategy(
            restart_strategy_configuration._j_restart_strategy_configuration)

    def get_restart_strategy(self) -> RestartStrategyConfiguration:
        """
        Returns the specified restart strategy configuration.

        :return: The restart strategy configuration to be used.
        """
        return RestartStrategies._from_j_restart_strategy(
            self._j_stream_execution_environment.getRestartStrategy())

    def add_default_kryo_serializer(self, type_class_name: str, serializer_class_name: str):
        """
        Adds a new Kryo default serializer to the Runtime.

        Example:
        ::

            >>> env.add_default_kryo_serializer("com.aaa.bbb.TypeClass", "com.aaa.bbb.Serializer")

        :param type_class_name: The full-qualified java class name of the types serialized with the
                                given serializer.
        :param serializer_class_name: The full-qualified java class name of the serializer to use.
        """
        type_clz = load_java_class(type_class_name)
        j_serializer_clz = load_java_class(serializer_class_name)
        self._j_stream_execution_environment.addDefaultKryoSerializer(type_clz, j_serializer_clz)

    def register_type_with_kryo_serializer(self, type_class_name: str, serializer_class_name: str):
        """
        Registers the given Serializer via its class as a serializer for the given type at the
        KryoSerializer.

        Example:
        ::

            >>> env.register_type_with_kryo_serializer("com.aaa.bbb.TypeClass",
            ...                                        "com.aaa.bbb.Serializer")

        :param type_class_name: The full-qualified java class name of the types serialized with
                                the given serializer.
        :param serializer_class_name: The full-qualified java class name of the serializer to use.
        """
        type_clz = load_java_class(type_class_name)
        j_serializer_clz = load_java_class(serializer_class_name)
        self._j_stream_execution_environment.registerTypeWithKryoSerializer(
            type_clz, j_serializer_clz)

    def register_type(self, type_class_name: str):
        """
        Registers the given type with the serialization stack. If the type is eventually
        serialized as a POJO, then the type is registered with the POJO serializer. If the
        type ends up being serialized with Kryo, then it will be registered at Kryo to make
        sure that only tags are written.

        Example:
        ::

            >>> env.register_type("com.aaa.bbb.TypeClass")

        :param type_class_name: The full-qualified java class name of the type to register.
        """
        type_clz = load_java_class(type_class_name)
        self._j_stream_execution_environment.registerType(type_clz)

    def set_stream_time_characteristic(self, characteristic: TimeCharacteristic):
        """
        Sets the time characteristic for all streams create from this environment, e.g., processing
        time, event time, or ingestion time.

        If you set the characteristic to IngestionTime of EventTime this will set a default
        watermark update interval of 200 ms. If this is not applicable for your application
        you should change it using
        :func:`pyflink.common.ExecutionConfig.set_auto_watermark_interval`.

        Example:
        ::

            >>> env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

        :param characteristic: The time characteristic, which could be
                               :data:`TimeCharacteristic.ProcessingTime`,
                               :data:`TimeCharacteristic.IngestionTime`,
                               :data:`TimeCharacteristic.EventTime`.
        """
        j_characteristic = TimeCharacteristic._to_j_time_characteristic(characteristic)
        self._j_stream_execution_environment.setStreamTimeCharacteristic(j_characteristic)

    def get_stream_time_characteristic(self) -> 'TimeCharacteristic':
        """
        Gets the time characteristic.

        .. seealso:: :func:`set_stream_time_characteristic`

        :return: The :class:`TimeCharacteristic`.
        """
        j_characteristic = self._j_stream_execution_environment.getStreamTimeCharacteristic()
        return TimeCharacteristic._from_j_time_characteristic(j_characteristic)

    def configure(self, configuration: Configuration):
        """
        Sets all relevant options contained in the :class:`~pyflink.common.Configuration`. such as
        e.g. `pipeline.time-characteristic`. It will reconfigure
        :class:`~pyflink.datastream.StreamExecutionEnvironment`,
        :class:`~pyflink.common.ExecutionConfig` and :class:`~pyflink.datastream.CheckpointConfig`.

        It will change the value of a setting only if a corresponding option was set in the
        `configuration`. If a key is not present, the current value of a field will remain
        untouched.

        :param configuration: a configuration to read the values from.

        .. versionadded:: 1.15.0
        """
        self._j_stream_execution_environment.configure(configuration._j_configuration,
                                                       get_gateway().jvm.Thread.currentThread()
                                                       .getContextClassLoader())

    def add_python_file(self, file_path: str):
        """
        Adds a python dependency which could be python files, python packages or
        local directories. They will be added to the PYTHONPATH of the python UDF worker.
        Please make sure that these dependencies can be imported.

        :param file_path: The path of the python dependency.
        """
        jvm = get_gateway().jvm
        env_config = jvm.org.apache.flink.python.util.PythonConfigUtil\
            .getEnvironmentConfig(self._j_stream_execution_environment)
        python_files = env_config.getString(jvm.PythonOptions.PYTHON_FILES.key(), None)
        if python_files is not None:
            python_files = jvm.PythonDependencyUtils.FILE_DELIMITER.join([file_path, python_files])
        else:
            python_files = file_path
        env_config.setString(jvm.PythonOptions.PYTHON_FILES.key(), python_files)

    def set_python_requirements(self, requirements_file_path: str,
                                requirements_cache_dir: str = None):
        """
        Specifies a requirements.txt file which defines the third-party dependencies.
        These dependencies will be installed to a temporary directory and added to the
        PYTHONPATH of the python UDF worker.

        For the dependencies which could not be accessed in the cluster, a directory which contains
        the installation packages of these dependencies could be specified using the parameter
        "requirements_cached_dir". It will be uploaded to the cluster to support offline
        installation.

        Example:
        ::

            # commands executed in shell
            $ echo numpy==1.16.5 > requirements.txt
            $ pip download -d cached_dir -r requirements.txt --no-binary :all:

            # python code
            >>> stream_env.set_python_requirements("requirements.txt", "cached_dir")

        .. note::

            Please make sure the installation packages matches the platform of the cluster
            and the python version used. These packages will be installed using pip,
            so also make sure the version of Pip (version >= 7.1.0) and the version of
            SetupTools (version >= 37.0.0).

        :param requirements_file_path: The path of "requirements.txt" file.
        :param requirements_cache_dir: The path of the local directory which contains the
                                       installation packages.
        """
        jvm = get_gateway().jvm
        python_requirements = requirements_file_path
        if requirements_cache_dir is not None:
            python_requirements = jvm.PythonDependencyUtils.PARAM_DELIMITER.join(
                [python_requirements, requirements_cache_dir])
        env_config = jvm.org.apache.flink.python.util.PythonConfigUtil \
            .getEnvironmentConfig(self._j_stream_execution_environment)
        env_config.setString(jvm.PythonOptions.PYTHON_REQUIREMENTS.key(), python_requirements)

    def add_python_archive(self, archive_path: str, target_dir: str = None):
        """
        Adds a python archive file. The file will be extracted to the working directory of
        python UDF worker.

        If the parameter "target_dir" is specified, the archive file will be extracted to a
        directory named ${target_dir}. Otherwise, the archive file will be extracted to a
        directory with the same name of the archive file.

        If python UDF depends on a specific python version which does not exist in the cluster,
        this method can be used to upload the virtual environment.
        Note that the path of the python interpreter contained in the uploaded environment
        should be specified via the method :func:`pyflink.table.TableConfig.set_python_executable`.

        The files uploaded via this method are also accessible in UDFs via relative path.

        Example:
        ::

            # command executed in shell
            # assert the relative path of python interpreter is py_env/bin/python
            $ zip -r py_env.zip py_env

            # python code
            >>> stream_env.add_python_archive("py_env.zip")
            >>> stream_env.set_python_executable("py_env.zip/py_env/bin/python")

            # or
            >>> stream_env.add_python_archive("py_env.zip", "myenv")
            >>> stream_env.set_python_executable("myenv/py_env/bin/python")

            # the files contained in the archive file can be accessed in UDF
            >>> def my_udf():
            ...     with open("myenv/py_env/data/data.txt") as f:
            ...         ...

        .. note::

            Please make sure the uploaded python environment matches the platform that the cluster
            is running on and that the python version must be 3.6 or higher.

        .. note::

            Currently only zip-format is supported. i.e. zip, jar, whl, egg, etc.
            The other archive formats such as tar, tar.gz, 7z, rar, etc are not supported.

        :param archive_path: The archive file path.
        :param target_dir: Optional, the target dir name that the archive file extracted to.
        """
        jvm = get_gateway().jvm
        if target_dir is not None:
            archive_path = jvm.PythonDependencyUtils.PARAM_DELIMITER.join(
                [archive_path, target_dir])
        env_config = jvm.org.apache.flink.python.util.PythonConfigUtil \
            .getEnvironmentConfig(self._j_stream_execution_environment)
        python_archives = env_config.getString(jvm.PythonOptions.PYTHON_ARCHIVES.key(), None)
        if python_archives is not None:
            python_files = jvm.PythonDependencyUtils.FILE_DELIMITER.join(
                [python_archives, archive_path])
        else:
            python_files = archive_path
        env_config.setString(jvm.PythonOptions.PYTHON_ARCHIVES.key(), python_files)

    def set_python_executable(self, python_exec: str):
        """
        Sets the path of the python interpreter which is used to execute the python udf workers.

        e.g. "/usr/local/bin/python3".

        If python UDF depends on a specific python version which does not exist in the cluster,
        the method :func:`pyflink.datastream.StreamExecutionEnvironment.add_python_archive` can be
        used to upload a virtual environment. The path of the python interpreter contained in the
        uploaded environment can be specified via this method.

        Example:
        ::

            # command executed in shell
            # assume that the relative path of python interpreter is py_env/bin/python
            $ zip -r py_env.zip py_env

            # python code
            >>> stream_env.add_python_archive("py_env.zip")
            >>> stream_env.set_python_executable("py_env.zip/py_env/bin/python")

        .. note::

            Please make sure the uploaded python environment matches the platform that the cluster
            is running on and that the python version must be 3.6 or higher.

        .. note::

            The python udf worker depends on Apache Beam (version == 2.27.0).
            Please ensure that the specified environment meets the above requirements.

        :param python_exec: The path of python interpreter.
        """
        jvm = get_gateway().jvm
        env_config = jvm.org.apache.flink.python.util.PythonConfigUtil \
            .getEnvironmentConfig(self._j_stream_execution_environment)
        env_config.setString(jvm.PythonOptions.PYTHON_EXECUTABLE.key(), python_exec)

    def add_jars(self, *jars_path: str):
        """
        Adds a list of jar files that will be uploaded to the cluster and referenced by the job.

        :param jars_path: Path of jars.
        """
        add_jars_to_context_class_loader(jars_path)
        jvm = get_gateway().jvm
        jars_key = jvm.org.apache.flink.configuration.PipelineOptions.JARS.key()
        env_config = jvm.org.apache.flink.python.util.PythonConfigUtil \
            .getEnvironmentConfig(self._j_stream_execution_environment)
        old_jar_paths = env_config.getString(jars_key, None)
        joined_jars_path = ';'.join(jars_path)
        if old_jar_paths and old_jar_paths.strip():
            joined_jars_path = ';'.join([old_jar_paths, joined_jars_path])
        env_config.setString(jars_key, joined_jars_path)

    def add_classpaths(self, *classpaths: str):
        """
        Adds a list of URLs that are added to the classpath of each user code classloader of the
        program. Paths must specify a protocol (e.g. file://) and be accessible on all nodes

        :param classpaths: Classpaths that will be added.
        """
        add_jars_to_context_class_loader(classpaths)
        jvm = get_gateway().jvm
        classpaths_key = jvm.org.apache.flink.configuration.PipelineOptions.CLASSPATHS.key()
        env_config = jvm.org.apache.flink.python.util.PythonConfigUtil \
            .getEnvironmentConfig(self._j_stream_execution_environment)
        old_classpaths = env_config.getString(classpaths_key, None)
        joined_classpaths = ';'.join(list(classpaths))
        if old_classpaths and old_classpaths.strip():
            joined_classpaths = ';'.join([old_classpaths, joined_classpaths])
        env_config.setString(classpaths_key, joined_classpaths)

    def get_default_local_parallelism(self) -> int:
        """
        Gets the default parallelism that will be used for the local execution environment.

        :return: The default local parallelism.
        """
        return self._j_stream_execution_environment.getDefaultLocalParallelism()

    def set_default_local_parallelism(self, parallelism: int):
        """
        Sets the default parallelism that will be used for the local execution environment.

        :param parallelism: The parallelism to use as the default local parallelism.
        """
        self._j_stream_execution_environment.setDefaultLocalParallelism(parallelism)

    def execute(self, job_name: str = None) -> JobExecutionResult:
        """
        Triggers the program execution. The environment will execute all parts of
        the program that have resulted in a "sink" operation. Sink operations are
        for example printing results or forwarding them to a message queue.

        The program execution will be logged and displayed with the provided name

        :param job_name: Desired name of the job, optional.
        :return: The result of the job execution, containing elapsed time and accumulators.
        """

        j_stream_graph = self._generate_stream_graph(clear_transformations=True, job_name=job_name)
        return JobExecutionResult(self._j_stream_execution_environment.execute(j_stream_graph))

    def execute_async(self, job_name: str = 'Flink Streaming Job') -> JobClient:
        """
        Triggers the program asynchronously. The environment will execute all parts of the program
        that have resulted in a "sink" operation. Sink operations are for example printing results
        or forwarding them to a message queue.
        The program execution will be logged and displayed with a generated default name.

        :param job_name: Desired name of the job.
        :return: A JobClient that can be used to communicate with the submitted job, completed on
                 submission succeeded.
        """
        j_stream_graph = self._generate_stream_graph(clear_transformations=True, job_name=job_name)
        j_job_client = self._j_stream_execution_environment.executeAsync(j_stream_graph)
        return JobClient(j_job_client=j_job_client)

    def get_execution_plan(self) -> str:
        """
        Creates the plan with which the system will execute the program, and returns it as
        a String using a JSON representation of the execution data flow graph.
        Note that this needs to be called, before the plan is executed.

        If the compiler could not be instantiated, or the master could not
        be contacted to retrieve information relevant to the execution planning,
        an exception will be thrown.

        :return: The execution plan of the program, as a JSON String.
        """
        j_stream_graph = self._generate_stream_graph(False)
        return j_stream_graph.getStreamingPlanAsJSON()

    def register_cached_file(self, file_path: str, name: str, executable: bool = False):
        """
        Registers a file at the distributed cache under the given name. The file will be accessible
        from any user-defined function in the (distributed) runtime under a local path. Files may be
        local files (which will be distributed via BlobServer), or files in a distributed file
        system. The runtime will copy the files temporarily to a local cache, if needed.

        :param file_path: The path of the file, as a URI (e.g. "file:///some/path" or
                         hdfs://host:port/and/path").
        :param name: The name under which the file is registered.
        :param executable: Flag indicating whether the file should be executable.

        .. versionadded:: 1.16.0
        """
        self._j_stream_execution_environment.registerCachedFile(file_path, name, executable)

    @staticmethod
    def get_execution_environment() -> 'StreamExecutionEnvironment':
        """
        Creates an execution environment that represents the context in which the
        program is currently executed. If the program is invoked standalone, this
        method returns a local execution environment.

        :return: The execution environment of the context in which the program is executed.
        """
        gateway = get_gateway()
        j_stream_exection_environment = gateway.jvm.org.apache.flink.streaming.api.environment\
            .StreamExecutionEnvironment.getExecutionEnvironment()
        return StreamExecutionEnvironment(j_stream_exection_environment)

    def add_source(self, source_func: SourceFunction, source_name: str = 'Custom Source',
                   type_info: TypeInformation = None) -> 'DataStream':
        """
        Adds a data source to the streaming topology.

        :param source_func: the user defined function.
        :param source_name: name of the data source. Optional.
        :param type_info: type of the returned stream. Optional.
        :return: the data stream constructed.
        """
        if type_info:
            j_type_info = type_info.get_java_type_info()
        else:
            j_type_info = None
        j_data_stream = self._j_stream_execution_environment.addSource(source_func
                                                                       .get_java_function(),
                                                                       source_name,
                                                                       j_type_info)
        return DataStream(j_data_stream=j_data_stream)

    def from_source(self,
                    source: Source,
                    watermark_strategy: WatermarkStrategy,
                    source_name: str,
                    type_info: TypeInformation = None) -> 'DataStream':
        """
        Adds a data :class:`~pyflink.datastream.connectors.Source` to the environment to get a
        :class:`~pyflink.datastream.DataStream`.

        The result will be either a bounded data stream (that can be processed in a batch way) or
        an unbounded data stream (that must be processed in a streaming way), based on the
        boundedness property of the source.

        This method takes an explicit type information for the produced data stream, so that
        callers can define directly what type/serializer will be used for the produced stream. For
        sources that describe their produced type, the parameter type_info should not be specified
        to avoid specifying the produced type redundantly.

        .. versionadded:: 1.13.0
        """
        if type_info:
            j_type_info = type_info.get_java_type_info()
        else:
            j_type_info = None
        j_data_stream = self._j_stream_execution_environment.fromSource(
            source.get_java_function(),
            watermark_strategy._j_watermark_strategy,
            source_name,
            j_type_info)
        return DataStream(j_data_stream=j_data_stream)

    def read_text_file(self, file_path: str, charset_name: str = "UTF-8") -> DataStream:
        """
        Reads the given file line-by-line and creates a DataStream that contains a string with the
        contents of each such line. The charset with the given name will be used to read the files.

        Note that this interface is not fault tolerant that is supposed to be used for test purpose.

        :param file_path: The path of the file, as a URI (e.g., "file:///some/local/file" or
                          "hdfs://host:port/file/path")
        :param charset_name: The name of the character set used to read the file.
        :return: The DataStream that represents the data read from the given file as text lines.
        """
        return DataStream(self._j_stream_execution_environment
                          .readTextFile(file_path, charset_name))

    def from_collection(self, collection: List[Any],
                        type_info: TypeInformation = None) -> DataStream:
        """
        Creates a data stream from the given non-empty collection. The type of the data stream is
        that of the elements in the collection.

        Note that this operation will result in a non-parallel data stream source, i.e. a data
        stream source with parallelism one.

        :param collection: The collection of elements to create the data stream from.
        :param type_info: The TypeInformation for the produced data stream
        :return: the data stream representing the given collection.
        """
        if type_info is not None:
            collection = [type_info.to_internal_type(element) for element in collection]
        return self._from_collection(collection, type_info)

    def _from_collection(self, elements: List[Any],
                         type_info: TypeInformation = None) -> DataStream:
        temp_file = tempfile.NamedTemporaryFile(delete=False, dir=tempfile.mkdtemp())
        serializer = self.serializer
        try:
            with temp_file:
                # dumps elements to a temporary file by pickle serializer.
                serializer.serialize(elements, temp_file)
            gateway = get_gateway()
            # if user does not defined the element data types, read the pickled data as a byte array
            # list.
            if type_info is None:
                j_objs = gateway.jvm.PythonBridgeUtils.readPickledBytes(temp_file.name)
                out_put_type_info = Types.PICKLED_BYTE_ARRAY()  # type: TypeInformation
            else:
                j_objs = gateway.jvm.PythonBridgeUtils.readPythonObjects(temp_file.name)
                out_put_type_info = type_info
            # Since flink python module depends on table module, we can make use of utils of it when
            # implementing python DataStream API.
            PythonTableUtils = gateway.jvm\
                .org.apache.flink.table.utils.python.PythonTableUtils
            execution_config = self._j_stream_execution_environment.getConfig()
            j_input_format = PythonTableUtils.getCollectionInputFormat(
                j_objs,
                out_put_type_info.get_java_type_info(),
                execution_config
            )

            JInputFormatSourceFunction = gateway.jvm.org.apache.flink.streaming.api.functions.\
                source.InputFormatSourceFunction
            JBoundedness = gateway.jvm.org.apache.flink.api.connector.source.Boundedness

            j_data_stream_source = invoke_method(
                self._j_stream_execution_environment,
                "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment",
                "addSource",
                [JInputFormatSourceFunction(j_input_format, out_put_type_info.get_java_type_info()),
                 "Collection Source",
                 out_put_type_info.get_java_type_info(),
                 JBoundedness.BOUNDED],
                ["org.apache.flink.streaming.api.functions.source.SourceFunction",
                 "java.lang.String",
                 "org.apache.flink.api.common.typeinfo.TypeInformation",
                 "org.apache.flink.api.connector.source.Boundedness"])
            j_data_stream_source.forceNonParallel()
            return DataStream(j_data_stream=j_data_stream_source)
        finally:
            os.unlink(temp_file.name)

    def _generate_stream_graph(self, clear_transformations: bool = False, job_name: str = None) \
            -> JavaObject:
        gateway = get_gateway()
        JPythonConfigUtil = gateway.jvm.org.apache.flink.python.util.PythonConfigUtil

        JPythonConfigUtil.configPythonOperator(self._j_stream_execution_environment)

        gateway.jvm.org.apache.flink.python.chain.PythonOperatorChainingOptimizer.apply(
            self._j_stream_execution_environment)

        JPythonConfigUtil.setPartitionCustomOperatorNumPartitions(
            get_field_value(self._j_stream_execution_environment, "transformations"))

        j_stream_graph = self._j_stream_execution_environment.getStreamGraph(clear_transformations)
        if job_name is not None:
            j_stream_graph.setJobName(job_name)
        return j_stream_graph

    def _open(self):
        # start BeamFnLoopbackWorkerPoolServicer when executed in MiniCluster
        j_configuration = get_j_env_configuration(self._j_stream_execution_environment)

        def startup_loopback_server():
            from pyflink.common import Configuration
            from pyflink.fn_execution.beam.beam_worker_pool_service import \
                BeamFnLoopbackWorkerPoolServicer
            config = Configuration(j_configuration=j_configuration)
            config.set_string(
                "PYFLINK_LOOPBACK_SERVER_ADDRESS", BeamFnLoopbackWorkerPoolServicer().start())

        python_worker_execution_mode = os.environ.get('_python_worker_execution_mode')

        if python_worker_execution_mode is None:
            if is_local_deployment(j_configuration):
                startup_loopback_server()
        elif python_worker_execution_mode == 'loopback':
            if is_local_deployment(j_configuration):
                startup_loopback_server()
            else:
                raise ValueError("Loopback mode is enabled, however the job wasn't configured to "
                                 "run in local deployment mode")
        elif python_worker_execution_mode != 'process':
            raise ValueError(
                "It only supports to execute the Python worker in 'loopback' mode and 'process' "
                "mode, unknown mode '%s' is configured" % python_worker_execution_mode)

    def is_unaligned_checkpoints_enabled(self):
        """
        Returns whether Unaligned Checkpoints are enabled.
        """
        return self._j_stream_execution_environment.isUnalignedCheckpointsEnabled()

    def is_force_unaligned_checkpoints(self):
        """
        Returns whether Unaligned Checkpoints are force-enabled.
        """
        return self._j_stream_execution_environment.isForceUnalignedCheckpoints()
