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

from typing import Callable, Union

from pyflink.common import typeinfo, ExecutionConfig
from pyflink.common.typeinfo import RowTypeInfo, PickledBytesTypeInfo
from pyflink.common.typeinfo import TypeInformation
from pyflink.datastream.functions import _get_python_env, FlatMapFunctionWrapper, FlatMapFunction, \
    MapFunction, MapFunctionWrapper, Function, FunctionWrapper
from pyflink.java_gateway import get_gateway


class DataStream(object):
    """
    A DataStream represents a stream of elements of the same type. A DataStream can be transformed
    into another DataStream by applying a transformation as for example:
    ::
        >>> DataStream.map(MapFunctionImpl())
        >>> DataStream.filter(FilterFunctionImpl())
    """

    def __init__(self, j_data_stream):
        self._j_data_stream = j_data_stream

    def get_name(self) -> str:
        """
        Gets the name of the current data stream. This name is used by the visualization and logging
        during runtime.

        :return: Name of the stream.
        """
        return self._j_data_stream.getName()

    def name(self, name: str):
        """
        Sets the name of the current data stream. This name is used by the visualization and logging
        during runting.

        :param name: Name of the stream.
        :return: The named operator.
        """
        self._j_data_stream.name(name)
        return self

    def uid(self, uid: str):
        """
        Sets an ID for this operator. The specified ID is used to assign the same operator ID across
        job submissions (for example when starting a job from a savepoint).

        Important: this ID needs to be unique per transformation and job. Otherwise, job submission
        will fail.

        :param uid: The unique user-specified ID of this transformation.
        :return: The operator with the specified ID.
        """
        self._j_data_stream.uid(uid)
        return self

    def set_uid_hash(self, uid_hash: str):
        """
        Sets an user provided hash for this operator. This will be used AS IS the create the
        JobVertexID. The user provided hash is an alternative to the generated hashed, that is
        considered when identifying an operator through the default hash mechanics fails (e.g.
        because of changes between Flink versions).

        Important: this should be used as a workaround or for trouble shooting. The provided hash
        needs to be unique per transformation and job. Otherwise, job submission will fail.
        Furthermore, you cannot assign user-specified hash to intermediate nodes in an operator
        chain and tryint so will let your job fail.

        A use case for this is in migration between Flink versions or changing the jobs in a way
        that changes the automatically generated hashes. In this case, providing the previous hashes
        directly through this method (e.g. obtained from old logs) can help to reestablish a lost
        mapping from states to their target operator.

        :param uid_hash: The user provided hash for this operator. This will become the jobVertexID,
                         which is shown in the logs and web ui.
        :return: The operator with the user provided hash.
        """
        self._j_data_stream.setUidHash(uid_hash)
        return self

    def set_parallelism(self, parallelism: int):
        """
        Sets the parallelism for this operator.

        :param parallelism: THe parallelism for this operator.
        :return: The operator with set parallelism.
        """
        self._j_data_stream.setParallelism(parallelism)
        return self

    def set_max_parallelism(self, max_parallelism: int):
        """
        Sets the maximum parallelism of this operator.

        The maximum parallelism specifies the upper bound for dynamic scaling. It also defines the
        number of key groups used for partitioned state.

        :param max_parallelism: Maximum parallelism.
        :return: The operator with set maximum parallelism.
        """
        self._j_data_stream.setMaxParallelism(max_parallelism)
        return self

    def get_type(self) -> TypeInformation:
        """
        Gets the type of the stream.

        :return: The type of the DataStream.
        """
        return typeinfo._from_java_type(self._j_data_stream.getType())

    def get_execution_environment(self):
        """
        Returns the StreamExecutionEnvironment that was used to create this DataStream.

        :return: The Execution Environment.
        """
        from pyflink.datastream import StreamExecutionEnvironment
        return StreamExecutionEnvironment(
            j_stream_execution_environment=self._j_data_stream.getExecutionEnvironment())

    def get_execution_config(self) -> ExecutionConfig:
        return ExecutionConfig(j_execution_config=self._j_data_stream.getExecutionConfig())

    def force_non_parallel(self):
        """
        Sets the parallelism and maximum parallelism of this operator to one. And mark this operator
        cannot set a non-1 degree of parallelism.

        :return: The operator with only one parallelism.
        """
        self._j_data_stream.forceNonParallel()
        return self

    def set_buffer_timeout(self, timeout_millis: int):
        """
        Sets the buffering timeout for data produced by this operation. The timeout defines how long
        data may linger ina partially full buffer before being sent over the network.

        Lower timeouts lead to lower tail latencies, but may affect throughput. Timeouts of 1 ms
        still sustain high throughput, even for jobs with high parallelism.

        A value of '-1' means that the default buffer timeout should be used. A value of '0'
        indicates that no buffering should happen, and all records/events should be immediately sent
        through the network, without additional buffering.

        :param timeout_millis: The maximum time between two output flushes.
        :return: The operator with buffer timeout set.
        """
        self._j_data_stream.setBufferTimeout(timeout_millis)
        return self

    def map(self, func: Union[Callable, MapFunction], type_info: TypeInformation = None) \
            -> 'DataStream':
        """
        Applies a Map transformation on a DataStream. The transformation calls a MapFunction for
        each element of the DataStream. Each MapFunction call returns exactly one element. The user
        can also extend RichMapFunction to gain access to other features provided by the
        RichFunction interface.

        Note that If user does not specify the output data type, the output data will be serialized
        as pickle primitive byte array.

        :param func: The MapFunction that is called for each element of the DataStream.
        :param type_info: The type information of the MapFunction output data.
        :return: The transformed DataStream.
        """
        if not isinstance(func, MapFunction):
            if callable(func):
                func = MapFunctionWrapper(func)
            else:
                raise TypeError("The input must be a MapFunction or a callable function")
        func_name = str(func)
        from pyflink.fn_execution import flink_fn_execution_pb2
        j_python_data_stream_scalar_function_operator, output_type_info = \
            self._get_java_python_function_operator(func,
                                                    type_info,
                                                    func_name,
                                                    flink_fn_execution_pb2
                                                    .UserDefinedDataStreamFunction.MAP)
        return DataStream(self._j_data_stream.transform(
            "Map",
            output_type_info.get_java_type_info(),
            j_python_data_stream_scalar_function_operator
        ))

    def flat_map(self, func: Union[Callable, FlatMapFunction], type_info: TypeInformation = None) \
            -> 'DataStream':
        """
        Applies a FlatMap transformation on a DataStream. The transformation calls a FlatMapFunction
        for each element of the DataStream. Each FlatMapFunction call can return any number of
        elements including none. The user can also extend RichFlatMapFunction to gain access to
        other features provided by the RichFUnction.

        :param func: The FlatMapFunction that is called for each element of the DataStream.
        :param type_info: The type information of output data.
        :return: The transformed DataStream.
        """
        if not isinstance(func, FlatMapFunction):
            if callable(func):
                func = FlatMapFunctionWrapper(func)
            else:
                raise TypeError("The input must be a FlatMapFunction or a callable function")
        func_name = str(func)
        from pyflink.fn_execution import flink_fn_execution_pb2
        j_python_data_stream_scalar_function_operator, output_type_info = \
            self._get_java_python_function_operator(func,
                                                    type_info,
                                                    func_name,
                                                    flink_fn_execution_pb2
                                                    .UserDefinedDataStreamFunction.FLAT_MAP)
        return DataStream(self._j_data_stream.transform(
            "FLAT_MAP",
            output_type_info.get_java_type_info(),
            j_python_data_stream_scalar_function_operator
        ))

    def _get_java_python_function_operator(self, func: Union[Function, FunctionWrapper],
                                           type_info: TypeInformation, func_name: str,
                                           func_type: int):
        """
        Create a flink operator according to user provided function object, data types,
        function name and function type.

        :param func: a function object that implements the Function interface.
        :param type_info: the data type of the function output data.
        :param func_name: function name.
        :param func_type: function type, supports MAP, FLAT_MAP, etc.
        :return: A flink java operator which is responsible for execution user defined python
                 function.
        """

        gateway = get_gateway()
        import cloudpickle
        serialized_func = cloudpickle.dumps(func)

        j_input_types = self._j_data_stream.getTransformation().getOutputType()

        if type_info is None:
            output_type_info = PickledBytesTypeInfo.PICKLED_BYTE_ARRAY_TYPE_INFO()
        else:
            if isinstance(type_info, list):
                output_type_info = RowTypeInfo(type_info)
            else:
                output_type_info = type_info

        DataStreamPythonFunction = gateway.jvm.org.apache.flink.datastream.runtime.functions \
            .python.DataStreamPythonFunction
        j_python_data_stream_scalar_function = DataStreamPythonFunction(
            func_name,
            bytearray(serialized_func),
            _get_python_env())

        DataStreamPythonFunctionInfo = gateway.jvm. \
            org.apache.flink.datastream.runtime.functions.python \
            .DataStreamPythonFunctionInfo

        j_python_data_stream_function_info = DataStreamPythonFunctionInfo(
            j_python_data_stream_scalar_function,
            func_type)

        j_env = self._j_data_stream.getExecutionEnvironment()
        PythonConfigUtil = gateway.jvm.org.apache.flink.python.util.PythonConfigUtil
        j_conf = PythonConfigUtil.getMergedConfig(j_env)

        DataStreamPythonFunctionOperator = gateway.jvm.org.apache.flink.datastream.runtime \
            .operators.python.DataStreamPythonStatelessFunctionOperator

        j_python_data_stream_scalar_function_operator = DataStreamPythonFunctionOperator(
            j_conf,
            j_input_types,
            output_type_info.get_java_type_info(),
            j_python_data_stream_function_info)
        return j_python_data_stream_scalar_function_operator, output_type_info
