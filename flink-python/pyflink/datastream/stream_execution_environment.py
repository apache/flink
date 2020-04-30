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
from pyflink.common.execution_config import ExecutionConfig
from pyflink.common.job_execution_result import JobExecutionResult
from pyflink.common.restart_strategy import RestartStrategies
from pyflink.datastream.checkpoint_config import CheckpointConfig
from pyflink.datastream.checkpointing_mode import CheckpointingMode
from pyflink.datastream.state_backend import _from_j_state_backend
from pyflink.datastream.time_characteristic import TimeCharacteristic
from pyflink.java_gateway import get_gateway
from pyflink.util.utils import load_java_class

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

    def __init__(self, j_stream_execution_environment):
        self._j_stream_execution_environment = j_stream_execution_environment

    def get_config(self):
        """
        Gets the config object.

        :return: The :class:`~pyflink.common.ExecutionConfig` object.
        """
        return ExecutionConfig(self._j_stream_execution_environment.getConfig())

    def set_parallelism(self, parallelism):
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

    def set_max_parallelism(self, max_parallelism):
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

    def get_parallelism(self):
        """
        Gets the parallelism with which operation are executed by default.
        Operations can individually override this value to use a specific
        parallelism.

        :return: The parallelism used by operations, unless they override that value.
        """
        return self._j_stream_execution_environment.getParallelism()

    def get_max_parallelism(self):
        """
        Gets the maximum degree of parallelism defined for the program.

        The maximum degree of parallelism specifies the upper limit for dynamic scaling. It also
        defines the number of key groups used for partitioned state.

        :return: Maximum degree of parallelism.
        """
        return self._j_stream_execution_environment.getMaxParallelism()

    def set_buffer_timeout(self, timeout_millis):
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

    def get_buffer_timeout(self):
        """
        Gets the maximum time frequency (milliseconds) for the flushing of the
        output buffers. For clarification on the extremal values see
        :func:`set_buffer_timeout`.

        :return: The timeout of the buffer.
        """
        return self._j_stream_execution_environment.getBufferTimeout()

    def disable_operator_chaining(self):
        """
        Disables operator chaining for streaming operators. Operator chaining
        allows non-shuffle operations to be co-located in the same thread fully
        avoiding serialization and de-serialization.

        :return: This object.
        """
        self._j_stream_execution_environment = \
            self._j_stream_execution_environment.disableOperatorChaining()
        return self

    def is_chaining_enabled(self):
        """
        Returns whether operator chaining is enabled.

        :return: True if chaining is enabled, false otherwise.
        """
        return self._j_stream_execution_environment.isChainingEnabled()

    def get_checkpoint_config(self):
        """
        Gets the checkpoint config, which defines values like checkpoint interval, delay between
        checkpoints, etc.

        :return: The :class:`~pyflink.datastream.CheckpointConfig`.
        """
        j_checkpoint_config = self._j_stream_execution_environment.getCheckpointConfig()
        return CheckpointConfig(j_checkpoint_config)

    def enable_checkpointing(self, interval, mode=None):
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

    def get_checkpoint_interval(self):
        """
        Returns the checkpointing interval or -1 if checkpointing is disabled.

        Shorthand for get_checkpoint_config().get_checkpoint_interval().

        :return: The checkpointing interval or -1.
        """
        return self._j_stream_execution_environment.getCheckpointInterval()

    def get_checkpointing_mode(self):
        """
        Returns the checkpointing mode (exactly-once vs. at-least-once).

        Shorthand for get_checkpoint_config().get_checkpointing_mode().

        :return: The :class:`~pyflink.datastream.CheckpointingMode`.
        """
        j_checkpointing_mode = self._j_stream_execution_environment.getCheckpointingMode()
        return CheckpointingMode._from_j_checkpointing_mode(j_checkpointing_mode)

    def get_state_backend(self):
        """
        Gets the state backend that defines how to store and checkpoint state.

        .. seealso:: :func:`set_state_backend`

        :return: The :class:`StateBackend`.
        """
        j_state_backend = self._j_stream_execution_environment.getStateBackend()
        return _from_j_state_backend(j_state_backend)

    def set_state_backend(self, state_backend):
        """
        Sets the state backend that describes how to store and checkpoint operator state. It
        defines both which data structures hold state during execution (for example hash tables,
        RockDB, or other data stores) as well as where checkpointed data will be persisted.

        The :class:`~pyflink.datastream.MemoryStateBackend` for example maintains the state in heap
        memory, as objects. It is lightweight without extra dependencies, but can checkpoint only
        small states(some counters).

        In contrast, the :class:`~pyflink.datastream.FsStateBackend` stores checkpoints of the state
        (also maintained as heap objects) in files. When using a replicated file system (like HDFS,
        S3, MapR FS, Alluxio, etc) this will guarantee that state is not lost upon failures of
        individual nodes and that streaming program can be executed highly available and strongly
        consistent(assuming that Flink is run in high-availability mode).

        The build-in state backend includes:
            :class:`~pyflink.datastream.MemoryStateBackend`,
            :class:`~pyflink.datastream.FsStateBackend`
            and :class:`~pyflink.datastream.RocksDBStateBackend`.

        .. seealso:: :func:`get_state_backend`

        Example:
        ::

            >>> env.set_state_backend(RocksDBStateBackend("file://var/checkpoints/"))

        :param state_backend: The :class:`StateBackend`.
        :return: This object.
        """
        self._j_stream_execution_environment = \
            self._j_stream_execution_environment.setStateBackend(state_backend._j_state_backend)
        return self

    def set_restart_strategy(self, restart_strategy_configuration):
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

    def get_restart_strategy(self):
        """
        Returns the specified restart strategy configuration.

        :return: The restart strategy configuration to be used.
        """
        return RestartStrategies._from_j_restart_strategy(
            self._j_stream_execution_environment.getRestartStrategy())

    def add_default_kryo_serializer(self, type_class_name, serializer_class_name):
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

    def register_type_with_kryo_serializer(self, type_class_name, serializer_class_name):
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

    def register_type(self, type_class_name):
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

    def set_stream_time_characteristic(self, characteristic):
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

    def get_stream_time_characteristic(self):
        """
        Gets the time characteristic.

        .. seealso:: :func:`set_stream_time_characteristic`

        :return: The :class:`TimeCharacteristic`.
        """
        j_characteristic = self._j_stream_execution_environment.getStreamTimeCharacteristic()
        return TimeCharacteristic._from_j_time_characteristic(j_characteristic)

    def get_default_local_parallelism(self):
        """
        Gets the default parallelism that will be used for the local execution environment.

        :return: The default local parallelism.
        """
        return self._j_stream_execution_environment.getDefaultLocalParallelism()

    def set_default_local_parallelism(self, parallelism):
        """
        Sets the default parallelism that will be used for the local execution environment.

        :param parallelism: The parallelism to use as the default local parallelism.
        """
        self._j_stream_execution_environment.setDefaultLocalParallelism(parallelism)

    def execute(self, job_name=None):
        """
        Triggers the program execution. The environment will execute all parts of
        the program that have resulted in a "sink" operation. Sink operations are
        for example printing results or forwarding them to a message queue.

        The program execution will be logged and displayed with the provided name

        :param job_name: Desired name of the job, optional.
        :return: The result of the job execution, containing elapsed time and accumulators.
        """
        if job_name is None:
            return JobExecutionResult(self._j_stream_execution_environment.execute())
        else:
            return JobExecutionResult(self._j_stream_execution_environment.execute(job_name))

    def get_execution_plan(self):
        """
        Creates the plan with which the system will execute the program, and returns it as
        a String using a JSON representation of the execution data flow graph.
        Note that this needs to be called, before the plan is executed.

        If the compiler could not be instantiated, or the master could not
        be contacted to retrieve information relevant to the execution planning,
        an exception will be thrown.

        :return: The execution plan of the program, as a JSON String.
        """
        return self._j_stream_execution_environment.getExecutionPlan()

    @staticmethod
    def get_execution_environment():
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
