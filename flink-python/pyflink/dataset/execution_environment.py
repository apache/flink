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
from pyflink.common.restart_strategy import RestartStrategies, RestartStrategyConfiguration
from pyflink.java_gateway import get_gateway
from pyflink.util.java_utils import load_java_class


class ExecutionEnvironment(object):
    """
    The ExecutionEnvironment is the context in which a program is executed.

    The environment provides methods to control the job execution (such as setting the parallelism)
    and to interact with the outside world (data access).
    """

    def __init__(self, j_execution_environment):
        self._j_execution_environment = j_execution_environment

    def get_parallelism(self) -> int:
        """
        Gets the parallelism with which operation are executed by default.

        :return: The parallelism.
        """
        return self._j_execution_environment.getParallelism()

    def set_parallelism(self, parallelism: int):
        """
        Sets the parallelism for operations executed through this environment.
        Setting a parallelism of x here will cause all operators to run with
        x parallel instances.

        :param parallelism: The parallelism.
        """
        self._j_execution_environment.setParallelism(parallelism)

    def get_default_local_parallelism(self) -> int:
        """
        Gets the default parallelism that will be used for the local execution environment.

        :return: The parallelism.
        """
        return self._j_execution_environment.getDefaultLocalParallelism()

    def set_default_local_parallelism(self, parallelism: int):
        """
        Sets the default parallelism that will be used for the local execution environment.

        :param parallelism: The parallelism.
        """
        self._j_execution_environment.setDefaultLocalParallelism(parallelism)

    def get_config(self) -> ExecutionConfig:
        """
        Gets the config object that defines execution parameters.

        :return: An :class:`ExecutionConfig` object, the environment's execution configuration.
        """
        return ExecutionConfig(self._j_execution_environment.getConfig())

    def set_restart_strategy(self, restart_strategy_configuration: RestartStrategyConfiguration):
        """
        Sets the restart strategy configuration. The configuration specifies which restart strategy
        will be used for the execution graph in case of a restart.

        Example:
        ::

            >>> env.set_restart_strategy(RestartStrategies.no_restart())

        :param restart_strategy_configuration: Restart strategy configuration to be set.
        """
        self._j_execution_environment.setRestartStrategy(
            restart_strategy_configuration._j_restart_strategy_configuration)

    def get_restart_strategy(self) -> RestartStrategyConfiguration:
        """
        Returns the specified restart strategy configuration.

        :return: The restart strategy configuration to be used.
        """
        return RestartStrategies._from_j_restart_strategy(
            self._j_execution_environment.getRestartStrategy())

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
        self._j_execution_environment.addDefaultKryoSerializer(type_clz, j_serializer_clz)

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
        self._j_execution_environment.registerTypeWithKryoSerializer(type_clz, j_serializer_clz)

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
        self._j_execution_environment.registerType(type_clz)

    def execute(self, job_name: str = None) -> JobExecutionResult:
        """
        Triggers the program execution. The environment will execute all parts of the program that
        have resulted in a "sink" operation.

        The program execution will be logged and displayed with the given job name.

        :param job_name: Desired name of the job, optional.
        :return: The result of the job execution, containing elapsed time and accumulators.
        """
        if job_name is None:
            return JobExecutionResult(self._j_execution_environment.execute())
        else:
            return JobExecutionResult(self._j_execution_environment.execute(job_name))

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
        return self._j_execution_environment.getExecutionPlan()

    @staticmethod
    def get_execution_environment() -> 'ExecutionEnvironment':
        """
        Creates an execution environment that represents the context in which the program is
        currently executed. If the program is invoked standalone, this method returns a local
        execution environment. If the program is invoked from within the command line client to be
        submitted to a cluster, this method returns the execution environment of this cluster.

        :return: The :class:`ExecutionEnvironment` of the context in which the program is executed.
        """
        gateway = get_gateway()
        j_execution_environment = gateway.jvm.org.apache.flink.api.java.ExecutionEnvironment\
            .getExecutionEnvironment()
        return ExecutionEnvironment(j_execution_environment)
