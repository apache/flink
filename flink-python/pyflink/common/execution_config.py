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

from pyflink.common.execution_mode import ExecutionMode
from pyflink.common.input_dependency_constraint import InputDependencyConstraint
from pyflink.common.restart_strategy import RestartStrategies
from pyflink.java_gateway import get_gateway
from pyflink.util.utils import load_java_class

__all__ = ['ExecutionConfig']


class ExecutionConfig(object):
    """
    A config to define the behavior of the program execution. It allows to define (among other
    options) the following settings:

    - The default parallelism of the program, i.e., how many parallel tasks to use for
      all functions that do not define a specific value directly.

    - The number of retries in the case of failed executions.

    - The delay between execution retries.

    - The :class:`ExecutionMode` of the program: Batch or Pipelined.
      The default execution mode is :data:`ExecutionMode.PIPELINED`

    - Enabling or disabling the "closure cleaner". The closure cleaner pre-processes
      the implementations of functions. In case they are (anonymous) inner classes,
      it removes unused references to the enclosing class to fix certain serialization-related
      problems and to reduce the size of the closure.

    - The config allows to register types and serializers to increase the efficiency of
      handling *generic types* and *POJOs*. This is usually only needed
      when the functions return not only the types declared in their signature, but
      also subclasses of those types.

    :data:`PARALLELISM_DEFAULT`:

    The flag value indicating use of the default parallelism. This value can
    be used to reset the parallelism back to the default state.

    :data:`PARALLELISM_UNKNOWN`:

    The flag value indicating an unknown or unset parallelism. This value is
    not a valid parallelism and indicates that the parallelism should remain
    unchanged.
    """

    PARALLELISM_DEFAULT = -1

    PARALLELISM_UNKNOWN = -2

    def __init__(self, j_execution_config):
        self._j_execution_config = j_execution_config

    def enable_closure_cleaner(self):
        """
        Enables the ClosureCleaner. This analyzes user code functions and sets fields to null
        that are not used. This will in most cases make closures or anonymous inner classes
        serializable that where not serializable due to some Scala or Java implementation artifact.
        User code must be serializable because it needs to be sent to worker nodes.

        :return: This object.
        """
        self._j_execution_config = self._j_execution_config.enableClosureCleaner()
        return self

    def disable_closure_cleaner(self):
        """
        Disables the ClosureCleaner.

        .. seealso:: :func:`enable_closure_cleaner`

        :return: This object.
        """
        self._j_execution_config = self._j_execution_config.disableClosureCleaner()
        return self

    def is_closure_cleaner_enabled(self):
        """
        Returns whether the ClosureCleaner is enabled.

        .. seealso:: :func:`enable_closure_cleaner`

        :return: ``True`` means enable and ``False`` means disable.
        """
        return self._j_execution_config.isClosureCleanerEnabled()

    def set_auto_watermark_interval(self, interval):
        """
        Sets the interval of the automatic watermark emission. Watermarks are used throughout
        the streaming system to keep track of the progress of time. They are used, for example,
        for time based windowing.

        :param interval: The integer value interval between watermarks in milliseconds.
        :return: This object.
        """
        self._j_execution_config = self._j_execution_config.setAutoWatermarkInterval(interval)
        return self

    def get_auto_watermark_interval(self):
        """
        Returns the interval of the automatic watermark emission.

        .. seealso:: :func:`set_auto_watermark_interval`

        :return: The integer value interval in milliseconds of the automatic watermark emission.
        """
        return self._j_execution_config.getAutoWatermarkInterval()

    def set_latency_tracking_interval(self, interval):
        """
        Interval for sending latency tracking marks from the sources to the sinks.

        Flink will send latency tracking marks from the sources at the specified interval.
        Setting a tracking interval <= 0 disables the latency tracking.

        :param interval: Integer value interval in milliseconds.
        :return: This object.
        """
        self._j_execution_config = self._j_execution_config.setLatencyTrackingInterval(interval)
        return self

    def get_latency_tracking_interval(self):
        """
        Returns the latency tracking interval.

        :return: The latency tracking interval in milliseconds.
        """
        return self._j_execution_config.getLatencyTrackingInterval()

    def get_parallelism(self):
        """
        Gets the parallelism with which operation are executed by default. Operations can
        individually override this value to use a specific parallelism.

        Other operations may need to run with a different parallelism - for example calling
        a reduce operation over the entire data set will involve an operation that runs
        with a parallelism of one (the final reduce to the single result value).

        :return: The parallelism used by operations, unless they override that value. This method
                 returns :data:`ExecutionConfig.PARALLELISM_DEFAULT` if the environment's default
                 parallelism should be used.
        """
        return self._j_execution_config.getParallelism()

    def set_parallelism(self, parallelism):
        """
        Sets the parallelism for operations executed through this environment.
        Setting a parallelism of x here will cause all operators (such as join, map, reduce) to run
        with x parallel instances.

        This method overrides the default parallelism for this environment.
        The local execution environment uses by default a value equal to the number of hardware
        contexts (CPU cores / threads). When executing the program via the command line client
        from a JAR/Python file, the default parallelism is the one configured for that setup.

        :param parallelism: The parallelism to use.
        :return: This object.
        """
        self._j_execution_config = self._j_execution_config.setParallelism(parallelism)
        return self

    def get_max_parallelism(self):
        """
        Gets the maximum degree of parallelism defined for the program.

        The maximum degree of parallelism specifies the upper limit for dynamic scaling. It also
        defines the number of key groups used for partitioned state.

        :return: Maximum degree of parallelism.
        """
        return self._j_execution_config.getMaxParallelism()

    def set_max_parallelism(self, max_parallelism):
        """
        Sets the maximum degree of parallelism defined for the program.

        The maximum degree of parallelism specifies the upper limit for dynamic scaling. It also
        defines the number of key groups used for partitioned state.

        :param max_parallelism: Maximum degree of parallelism to be used for the program.
        """
        self._j_execution_config.setMaxParallelism(max_parallelism)

    def get_task_cancellation_interval(self):
        """
        Gets the interval (in milliseconds) between consecutive attempts to cancel a running task.

        :return: The integer value interval in milliseconds.
        """
        return self._j_execution_config.getTaskCancellationInterval()

    def set_task_cancellation_interval(self, interval):
        """
        Sets the configuration parameter specifying the interval (in milliseconds)
        between consecutive attempts to cancel a running task.

        :param interval: The integer value interval in milliseconds.
        :return: This object.
        """
        self._j_execution_config = self._j_execution_config.setTaskCancellationInterval(interval)
        return self

    def get_task_cancellation_timeout(self):
        """
        Returns the timeout (in milliseconds) after which an ongoing task
        cancellation leads to a fatal TaskManager error.

        The value ``0`` means that the timeout is disabled. In
        this case a stuck cancellation will not lead to a fatal error.

        :return: The timeout in milliseconds.
        """
        return self._j_execution_config.getTaskCancellationTimeout()

    def set_task_cancellation_timeout(self, timeout):
        """
        Sets the timeout (in milliseconds) after which an ongoing task cancellation
        is considered failed, leading to a fatal TaskManager error.

        The cluster default is configured via ``TaskManagerOptions#TASK_CANCELLATION_TIMEOUT``.

        The value ``0`` disables the timeout. In this case a stuck
        cancellation will not lead to a fatal error.

        :param timeout: The task cancellation timeout (in milliseconds).
        :return: This object.
        """
        self._j_execution_config = self._j_execution_config.setTaskCancellationTimeout(timeout)
        return self

    def set_restart_strategy(self, restart_strategy_configuration):
        """
        Sets the restart strategy to be used for recovery.
        ::

            >>> config = env.get_config()
            >>> config.set_restart_strategy(RestartStrategies.fixed_delay_restart(10, 1000))

        The restart strategy configurations are all created from :class:`RestartStrategies`.

        :param restart_strategy_configuration: Configuration defining the restart strategy to use.
        """
        self._j_execution_config.setRestartStrategy(
            restart_strategy_configuration._j_restart_strategy_configuration)

    def get_restart_strategy(self):
        """
        Returns the restart strategy which has been set for the current job.

        .. seealso:: :func:`set_restart_strategy`

        :return: The specified restart configuration.
        """
        return RestartStrategies._from_j_restart_strategy(
            self._j_execution_config.getRestartStrategy())

    def set_execution_mode(self, execution_mode):
        """
        Sets the execution mode to execute the program. The execution mode defines whether
        data exchanges are performed in a batch or on a pipelined manner.

        The default execution mode is :data:`ExecutionMode.PIPELINED`.

        Example:
        ::

            >>> config.set_execution_mode(ExecutionMode.BATCH)

        :param execution_mode: The execution mode to use. The execution mode could be
                               :data:`ExecutionMode.PIPELINED`,
                               :data:`ExecutionMode.PIPELINED_FORCED`,
                               :data:`ExecutionMode.BATCH` or
                               :data:`ExecutionMode.BATCH_FORCED`.
        """
        self._j_execution_config.setExecutionMode(
            ExecutionMode._to_j_execution_mode(execution_mode))

    def get_execution_mode(self):
        """
        Gets the execution mode used to execute the program. The execution mode defines whether
        data exchanges are performed in a batch or on a pipelined manner.

        The default execution mode is :data:`ExecutionMode.PIPELINED`.

        .. seealso:: :func:`set_execution_mode`

        :return: The execution mode for the program.
        """
        j_execution_mode = self._j_execution_config.getExecutionMode()
        return ExecutionMode._from_j_execution_mode(j_execution_mode)

    def set_default_input_dependency_constraint(self, input_dependency_constraint):
        """
        Sets the default input dependency constraint for vertex scheduling. It indicates when a
        task should be scheduled considering its inputs status.

        The default constraint is :data:`InputDependencyConstraint.ANY`.

        Example:
        ::

            >>> config.set_default_input_dependency_constraint(InputDependencyConstraint.ALL)

        :param input_dependency_constraint: The input dependency constraint. The constraints could
                                            be :data:`InputDependencyConstraint.ANY` or
                                            :data:`InputDependencyConstraint.ALL`.
        """
        self._j_execution_config.setDefaultInputDependencyConstraint(
            InputDependencyConstraint._to_j_input_dependency_constraint(
                input_dependency_constraint))

    def get_default_input_dependency_constraint(self):
        """
        Gets the default input dependency constraint for vertex scheduling. It indicates when a
        task should be scheduled considering its inputs status.

        The default constraint is :data:`InputDependencyConstraint.ANY`.

        .. seealso:: :func:`set_default_input_dependency_constraint`

        :return: The input dependency constraint of this job. The possible constraints are
                 :data:`InputDependencyConstraint.ANY` and :data:`InputDependencyConstraint.ALL`.
        """
        j_input_dependency_constraint = self._j_execution_config\
            .getDefaultInputDependencyConstraint()
        return InputDependencyConstraint._from_j_input_dependency_constraint(
            j_input_dependency_constraint)

    def enable_force_kryo(self):
        """
        Force TypeExtractor to use Kryo serializer for POJOS even though we could analyze as POJO.
        In some cases this might be preferable. For example, when using interfaces
        with subclasses that cannot be analyzed as POJO.
        """
        self._j_execution_config.enableForceKryo()

    def disable_force_kryo(self):
        """
        Disable use of Kryo serializer for all POJOs.
        """
        self._j_execution_config.disableForceKryo()

    def is_force_kryo_enabled(self):
        """
        :return: Boolean value that represent whether the usage of Kryo serializer for all POJOs
                 is enabled.
        """
        return self._j_execution_config.isForceKryoEnabled()

    def enable_generic_types(self):
        """
        Enables the use generic types which are serialized via Kryo.

        Generic types are enabled by default.

        .. seealso:: :func:`disable_generic_types`
        """
        self._j_execution_config.enableGenericTypes()

    def disable_generic_types(self):
        """
        Disables the use of generic types (types that would be serialized via Kryo). If this option
        is used, Flink will throw an ``UnsupportedOperationException`` whenever it encounters
        a data type that would go through Kryo for serialization.

        Disabling generic types can be helpful to eagerly find and eliminate the use of types
        that would go through Kryo serialization during runtime. Rather than checking types
        individually, using this option will throw exceptions eagerly in the places where generic
        types are used.

        **Important:** We recommend to use this option only during development and pre-production
        phases, not during actual production use. The application program and/or the input data may
        be such that new, previously unseen, types occur at some point. In that case, setting this
        option would cause the program to fail.

        .. seealso:: :func:`enable_generic_types`
        """
        self._j_execution_config.disableGenericTypes()

    def has_generic_types_disabled(self):
        """
        Checks whether generic types are supported. Generic types are types that go through Kryo
        during serialization.

        Generic types are enabled by default.

        .. seealso:: :func:`enable_generic_types`

        .. seealso:: :func:`disable_generic_types`

        :return: Boolean value that represent whether the generic types are supported.
        """
        return self._j_execution_config.hasGenericTypesDisabled()

    def enable_auto_generated_uids(self):
        """
        Enables the Flink runtime to auto-generate UID's for operators.

        .. seealso:: :func:`disable_auto_generated_uids`
        """
        self._j_execution_config.enableAutoGeneratedUIDs()

    def disable_auto_generated_uids(self):
        """
        Disables auto-generated UIDs. Forces users to manually specify UIDs
        on DataStream applications.

        It is highly recommended that users specify UIDs before deploying to
        production since they are used to match state in savepoints to operators
        in a job. Because auto-generated ID's are likely to change when modifying
        a job, specifying custom IDs allow an application to evolve overtime
        without discarding state.
        """
        self._j_execution_config.disableAutoGeneratedUIDs()

    def has_auto_generated_uids_enabled(self):
        """
        Checks whether auto generated UIDs are supported.

        Auto generated UIDs are enabled by default.

        .. seealso:: :func:`enable_auto_generated_uids`

        .. seealso:: :func:`disable_auto_generated_uids`

        :return: Boolean value that represent whether auto generated UIDs are supported.
        """
        return self._j_execution_config.hasAutoGeneratedUIDsEnabled()

    def enable_force_avro(self):
        """
        Forces Flink to use the Apache Avro serializer for POJOs.

        **Important:** Make sure to include the *flink-avro* module.
        """
        self._j_execution_config.enableForceAvro()

    def disable_force_avro(self):
        """
        Disables the Apache Avro serializer as the forced serializer for POJOs.
        """
        self._j_execution_config.disableForceAvro()

    def is_force_avro_enabled(self):
        """
        Returns whether the Apache Avro is the default serializer for POJOs.

        :return: Boolean value that represent whether the Apache Avro is the default serializer
                 for POJOs.
        """
        return self._j_execution_config.isForceAvroEnabled()

    def enable_object_reuse(self):
        """
        Enables reusing objects that Flink internally uses for deserialization and passing
        data to user-code functions. Keep in mind that this can lead to bugs when the
        user-code function of an operation is not aware of this behaviour.

        :return: This object.
        """
        self._j_execution_config = self._j_execution_config.enableObjectReuse()
        return self

    def disable_object_reuse(self):
        """
        Disables reusing objects that Flink internally uses for deserialization and passing
        data to user-code functions.

        .. seealso:: :func:`enable_object_reuse`

        :return: This object.
        """
        self._j_execution_config = self._j_execution_config.disableObjectReuse()
        return self

    def is_object_reuse_enabled(self):
        """
        Returns whether object reuse has been enabled or disabled.

        .. seealso:: :func:`enable_object_reuse`

        :return: Boolean value that represent whether object reuse has been enabled or disabled.
        """
        return self._j_execution_config.isObjectReuseEnabled()

    def enable_sysout_logging(self):
        """
        Enables the printing of progress update messages to stdout.

        :return: This object.
        """
        self._j_execution_config = self._j_execution_config.enableSysoutLogging()
        return self

    def disable_sysout_logging(self):
        """
        Disables the printing of progress update messages to stdout.

        :return: This object.
        """
        self._j_execution_config = self._j_execution_config.disableSysoutLogging()
        return self

    def is_sysout_logging_enabled(self):
        """
        Gets whether progress update messages should be printed to stdout.

        :return: True, if progress update messages should be printed, false otherwise.
        """
        return self._j_execution_config.isSysoutLoggingEnabled()

    def get_global_job_parameters(self):
        """
        Gets current configuration dict.

        :return: The configuration dict.
        """
        return dict(self._j_execution_config.getGlobalJobParameters().toMap())

    def set_global_job_parameters(self, global_job_parameters_dict):
        """
        Register a custom, serializable user configuration dict.

        Example:
        ::

            >>> config.set_global_job_parameters({"environment.checkpoint_interval": "1000"})

        :param global_job_parameters_dict: Custom user configuration dict.
        """
        gateway = get_gateway()
        Configuration = gateway.jvm.org.apache.flink.configuration.Configuration
        j_global_job_parameters = Configuration()
        for key in global_job_parameters_dict:
            if not isinstance(global_job_parameters_dict[key], str):
                value = str(global_job_parameters_dict[key])
            else:
                value = global_job_parameters_dict[key]
            j_global_job_parameters.setString(key, value)
        self._j_execution_config.setGlobalJobParameters(j_global_job_parameters)

    def add_default_kryo_serializer(self, type_class_name, serializer_class_name):
        """
        Adds a new Kryo default serializer to the Runtime.

        Example:
        ::

            >>> config.add_default_kryo_serializer("com.aaa.bbb.PojoClass",
            ...                                    "com.aaa.bbb.Serializer")

        :param type_class_name: The full-qualified java class name of the types serialized with the
                                given serializer.
        :param serializer_class_name: The full-qualified java class name of the serializer to use.
        """
        type_clz = load_java_class(type_class_name)
        j_serializer_clz = load_java_class(serializer_class_name)
        self._j_execution_config.addDefaultKryoSerializer(type_clz, j_serializer_clz)

    def register_type_with_kryo_serializer(self, type_class_name, serializer_class_name):
        """
        Registers the given Serializer via its class as a serializer for the given type at the
        KryoSerializer.

        Example:
        ::

            >>> config.register_type_with_kryo_serializer("com.aaa.bbb.PojoClass",
            ...                                           "com.aaa.bbb.Serializer")

        :param type_class_name: The full-qualified java class name of the types serialized with
                                the given serializer.
        :param serializer_class_name: The full-qualified java class name of the serializer to use.
        """
        type_clz = load_java_class(type_class_name)
        j_serializer_clz = load_java_class(serializer_class_name)
        self._j_execution_config.registerTypeWithKryoSerializer(type_clz, j_serializer_clz)

    def register_pojo_type(self, type_class_name):
        """
        Registers the given type with the serialization stack. If the type is eventually
        serialized as a POJO, then the type is registered with the POJO serializer. If the
        type ends up being serialized with Kryo, then it will be registered at Kryo to make
        sure that only tags are written.

        Example:
        ::

            >>> config.register_pojo_type("com.aaa.bbb.PojoClass")

        :param type_class_name: The full-qualified java class name of the type to register.
        """
        type_clz = load_java_class(type_class_name)
        self._j_execution_config.registerPojoType(type_clz)

    def register_kryo_type(self, type_class_name):
        """
        Registers the given type with the serialization stack. If the type is eventually
        serialized as a POJO, then the type is registered with the POJO serializer. If the
        type ends up being serialized with Kryo, then it will be registered at Kryo to make
        sure that only tags are written.

        Example:
        ::

            >>> config.register_kryo_type("com.aaa.bbb.KryoClass")

        :param type_class_name: The full-qualified java class name of the type to register.
        """
        type_clz = load_java_class(type_class_name)
        self._j_execution_config.registerKryoType(type_clz)

    def get_registered_types_with_kryo_serializer_classes(self):
        """
        Returns the registered types with their Kryo Serializer classes.

        :return: The dict which the keys are full-qualified java class names of the registered
                 types and the values are full-qualified java class names of the Kryo Serializer
                 classes.
        """
        j_clz_map = self._j_execution_config.getRegisteredTypesWithKryoSerializerClasses()
        registered_serializers = {}
        for key in j_clz_map:
            registered_serializers[key.getName()] = j_clz_map[key].getName()
        return registered_serializers

    def get_default_kryo_serializer_classes(self):
        """
        Returns the registered default Kryo Serializer classes.

        :return: The dict which the keys are full-qualified java class names of the registered
                 types and the values are full-qualified java class names of the Kryo default
                 Serializer classes.
        """
        j_clz_map = self._j_execution_config.getDefaultKryoSerializerClasses()
        default_kryo_serializers = {}
        for key in j_clz_map:
            default_kryo_serializers[key.getName()] = j_clz_map[key].getName()
        return default_kryo_serializers

    def get_registered_kryo_types(self):
        """
        Returns the registered Kryo types.

        :return: The list of full-qualified java class names of the registered Kryo types.
        """
        j_clz_set = self._j_execution_config.getRegisteredKryoTypes()
        return [value.getName() for value in j_clz_set]

    def get_registered_pojo_types(self):
        """
        Returns the registered POJO types.

        :return: The list of full-qualified java class names of the registered POJO types.
        """
        j_clz_set = self._j_execution_config.getRegisteredPojoTypes()
        return [value.getName() for value in j_clz_set]

    def is_auto_type_registration_disabled(self):
        """
        Returns whether Flink is automatically registering all types in the user programs with
        Kryo.

        :return: ``True`` means auto type registration is disabled and ``False`` means enabled.
        """
        return self._j_execution_config.isAutoTypeRegistrationDisabled()

    def disable_auto_type_registration(self):
        """
        Control whether Flink is automatically registering all types in the user programs with
        Kryo.
        """
        self._j_execution_config.disableAutoTypeRegistration()

    def is_use_snapshot_compression(self):
        """
        Returns whether he compression (snappy) for keyed state in full checkpoints and savepoints
        is enabled.

        :return: ``True`` means enabled and ``False`` means disabled.
        """
        return self._j_execution_config.isUseSnapshotCompression()

    def set_use_snapshot_compression(self, use_snapshot_compression):
        """
        Control whether the compression (snappy) for keyed state in full checkpoints and savepoints
        is enabled.

        :param use_snapshot_compression: ``True`` means enabled and ``False`` means disabled.
        """
        self._j_execution_config.setUseSnapshotCompression(use_snapshot_compression)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and \
            self._j_execution_config == other._j_execution_config

    def __hash__(self):
        return self._j_execution_config.hashCode()
