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
from pyflink.java_gateway import get_gateway
from pyflink.util.utils import load_java_class


class ExecutionEnvironment(object):
    """
    The ExecutionEnvironment is the context in which a program is executed.

    The environment provides methods to control the job execution (such as setting the parallelism)
    and to interact with the outside world (data access).
    """

    def __init__(self, j_execution_environment):
        self._j_execution_environment = j_execution_environment

    def get_parallelism(self):
        """
        Gets the parallelism with which operation are executed by default.

        :return: The parallelism.
        """
        return self._j_execution_environment.getParallelism()

    def set_parallelism(self, parallelism):
        """
        Sets the parallelism for operations executed through this environment.
        Setting a parallelism of x here will cause all operators to run with
        x parallel instances.

        :param parallelism: The parallelism.
        """
        self._j_execution_environment.setParallelism(parallelism)

    def set_session_timeout(self, timeout):
        """
        Sets the session timeout to hold the intermediate results of a job. This only
        applies the updated timeout in future executions.

        :param timeout: The timeout value, in seconds.
        """
        self._j_execution_environment.setSessionTimeout(timeout)

    def get_session_timeout(self):
        """
        Gets the session timeout for this environment. The session timeout defines for how long
        after an execution, the job and its intermediate results will be kept for future
        interactions.

        :return: The session timeout value, in seconds.
        """
        return self._j_execution_environment.getSessionTimeout()

    def get_default_local_parallelism(self):
        """
        Gets the default parallelism that will be used for the local execution environment.

        :return: The parallelism.
        """
        return self._j_execution_environment.getDefaultLocalParallelism()

    def set_default_local_parallelism(self, parallelism):
        """
        Sets the default parallelism that will be used for the local execution environment.

        :param parallelism: The parallelism.
        """
        self._j_execution_environment.setDefaultLocalParallelism(parallelism)

    def add_default_kryo_serializer(self, type_class_name, serializer_class_name):
        """
        Adds a new Kryo default serializer to the Runtime.

        :param type_class_name: The full-qualified java class name of the types serialized with the
                                given serializer.
        :param serializer_class_name: The full-qualified java class name of the serializer to use.
        """
        type_clz = load_java_class(type_class_name)
        j_serializer_clz = load_java_class(serializer_class_name)
        self._j_execution_environment.addDefaultKryoSerializer(type_clz, j_serializer_clz)

    def register_type_with_kryo_serializer(self, type_class_name, serializer_class_name):
        """
        Registers the given Serializer via its class as a serializer for the given type at the
        KryoSerializer.

        :param type_class_name: The full-qualified java class name of the types serialized with
                                the given serializer.
        :param serializer_class_name: The full-qualified java class name of the serializer to use.
        """
        type_clz = load_java_class(type_class_name)
        j_serializer_clz = load_java_class(serializer_class_name)
        self._j_execution_environment.registerTypeWithKryoSerializer(type_clz, j_serializer_clz)

    def register_type(self, type_class_name):
        """
        Registers the given type with the serialization stack. If the type is eventually
        serialized as a POJO, then the type is registered with the POJO serializer. If the
        type ends up being serialized with Kryo, then it will be registered at Kryo to make
        sure that only tags are written.

        :param type_class_name: The full-qualified java class name of the type to register.
        """
        type_clz = load_java_class(type_class_name)
        self._j_execution_environment.registerType(type_clz)

    def execute(self, job_name=None):
        """
        Triggers the program execution. The environment will execute all parts of the program that
        have resulted in a "sink" operation.

        The program execution will be logged and displayed with the given job name.

        :param job_name: Desired name of the job, optional.
        """
        if job_name is None:
            self._j_execution_environment.execute()
        else:
            self._j_execution_environment.execute(job_name)

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
        return self._j_execution_environment.getExecutionPlan()

    @classmethod
    def get_execution_environment(cls):
        """
        Creates an execution environment that represents the context in which the program is
        currently executed. If the program is invoked standalone, this method returns a local
        execution environment. If the program is invoked from within the command line client to be
        submitted to a cluster, this method returns the execution environment of this cluster.

        :return: The execution environment of the context in which the program is executed.
        """
        gateway = get_gateway()
        j_execution_environment = gateway.jvm.org.apache.flink.api.java.ExecutionEnvironment\
            .getExecutionEnvironment()
        return ExecutionEnvironment(j_execution_environment)
