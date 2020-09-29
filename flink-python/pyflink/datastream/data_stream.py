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
from typing import Callable, Union

from pyflink.common import typeinfo, ExecutionConfig, Row
from pyflink.common.typeinfo import RowTypeInfo, PickledBytesTypeInfo, Types
from pyflink.common.typeinfo import TypeInformation
from pyflink.datastream.functions import _get_python_env, FlatMapFunctionWrapper, FlatMapFunction, \
    MapFunction, MapFunctionWrapper, Function, FunctionWrapper, SinkFunction, FilterFunction, \
    FilterFunctionWrapper, KeySelectorFunctionWrapper, KeySelector, ReduceFunction, \
    ReduceFunctionWrapper, CoMapFunction, CoFlatMapFunction, Partitioner, \
    PartitionerFunctionWrapper
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
        chain and trying so will let your job fail.

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

    def start_new_chain(self) -> 'DataStream':
        """
        Starts a new task chain beginning at this operator. This operator will be chained (thread
        co-located for increased performance) to any previous tasks even if possible.

        :return: The operator with chaining set.
        """
        self._j_data_stream.startNewChain()
        return self

    def disable_chaining(self) -> 'DataStream':
        """
        Turns off chaining for this operator so thread co-location will not be used as an
        optimization.
        Chaining can be turned off for the whole job by
        StreamExecutionEnvironment.disableOperatorChaining() however it is not advised for
        performance consideration.

        :return: The operator with chaining disabled.
        """
        self._j_data_stream.disableChaining()
        return self

    def slot_sharing_group(self, slot_sharing_group: str) -> 'DataStream':
        """
        Sets the slot sharing group of this operation. Parallel instances of operations that are in
        the same slot sharing group will be co-located in the same TaskManager slot, if possible.

        Operations inherit the slot sharing group of input operations if all input operations are in
        the same slot sharing group and no slot sharing group was explicitly specified.

        Initially an operation is in the default slot sharing group. An operation can be put into
        the default group explicitly by setting the slot sharing group to 'default'.

        :param slot_sharing_group: The slot sharing group name.
        :return: This operator.
        """
        self._j_data_stream.slotSharingGroup(slot_sharing_group)
        return self

    def map(self, func: Union[Callable, MapFunction], output_type: TypeInformation = None) \
            -> 'DataStream':
        """
        Applies a Map transformation on a DataStream. The transformation calls a MapFunction for
        each element of the DataStream. Each MapFunction call returns exactly one element. The user
        can also extend RichMapFunction to gain access to other features provided by the
        RichFunction interface.

        Note that If user does not specify the output data type, the output data will be serialized
        as pickle primitive byte array.

        :param func: The MapFunction that is called for each element of the DataStream.
        :param output_type: The type information of the MapFunction output data.
        :return: The transformed DataStream.
        """
        if not isinstance(func, MapFunction):
            if callable(func):
                func = MapFunctionWrapper(func)
            else:
                raise TypeError("The input must be a MapFunction or a callable function")
        from pyflink.fn_execution import flink_fn_execution_pb2
        j_operator, j_output_type_info = self._get_java_python_function_operator(
            func,
            output_type,
            flink_fn_execution_pb2.UserDefinedDataStreamFunction.MAP)
        return DataStream(self._j_data_stream.transform(
            "Map",
            j_output_type_info,
            j_operator
        ))

    def flat_map(self, func: Union[Callable, FlatMapFunction],
                 result_type: TypeInformation = None) -> 'DataStream':
        """
        Applies a FlatMap transformation on a DataStream. The transformation calls a FlatMapFunction
        for each element of the DataStream. Each FlatMapFunction call can return any number of
        elements including none. The user can also extend RichFlatMapFunction to gain access to
        other features provided by the RichFUnction.

        :param func: The FlatMapFunction that is called for each element of the DataStream.
        :param result_type: The type information of output data.
        :return: The transformed DataStream.
        """
        if not isinstance(func, FlatMapFunction):
            if callable(func):
                func = FlatMapFunctionWrapper(func)
            else:
                raise TypeError("The input must be a FlatMapFunction or a callable function")
        from pyflink.fn_execution import flink_fn_execution_pb2
        j_operator, j_output_type_info = self._get_java_python_function_operator(
            func,
            result_type,
            flink_fn_execution_pb2.UserDefinedDataStreamFunction.FLAT_MAP)
        return DataStream(self._j_data_stream.transform(
            "FLAT_MAP",
            j_output_type_info,
            j_operator
        ))

    def key_by(self, key_selector: Union[Callable, KeySelector],
               key_type_info: TypeInformation = None) -> 'KeyedStream':
        """
        Creates a new KeyedStream that uses the provided key for partitioning its operator states.

        :param key_selector: The KeySelector to be used for extracting the key for partitioning.
        :param key_type_info: The type information describing the key type.
        :return: The DataStream with partitioned state(i.e. KeyedStream).
        """
        if callable(key_selector):
            key_selector = KeySelectorFunctionWrapper(key_selector)
        if not isinstance(key_selector, (KeySelector, KeySelectorFunctionWrapper)):
            raise TypeError("Parameter key_selector should be a type of KeySelector.")

        gateway = get_gateway()
        PickledKeySelector = gateway.jvm.PickledKeySelector
        output_type_info = typeinfo._from_java_type(
            self._j_data_stream.getTransformation().getOutputType())
        is_key_pickled_byte_array = False
        if key_type_info is None:
            key_type_info = Types.PICKLED_BYTE_ARRAY()
            is_key_pickled_byte_array = True

        intermediate_map_stream = self.map(lambda x: Row(key_selector.get_key(x), x),
                                           output_type=Types.ROW([key_type_info, output_type_info]))
        intermediate_map_stream.name(gateway.jvm.org.apache.flink.python.util.PythonConfigUtil
                                     .STREAM_KEY_BY_MAP_OPERATOR_NAME)
        key_stream = KeyedStream(
            intermediate_map_stream._j_data_stream.keyBy(
                PickledKeySelector(is_key_pickled_byte_array),
                key_type_info.get_java_type_info()),
            self)
        key_stream._original_data_type_info = output_type_info
        return key_stream

    def filter(self, func: Union[Callable, FilterFunction]) -> 'DataStream':
        """
        Applies a Filter transformation on a DataStream. The transformation calls a FilterFunction
        for each element of the DataStream and retains only those element for which the function
        returns true. Elements for which the function returns false are filtered. The user can also
        extend RichFilterFunction to gain access to other features provided by the RichFunction
        interface.

        :param func: The FilterFunction that is called for each element of the DataStream.
        :return: The filtered DataStream.
        """
        class FilterFlatMap(FlatMapFunction):
            def __init__(self, filter_func):
                self._func = filter_func

            def flat_map(self, value):
                if self._func.filter(value):
                    yield value

        if isinstance(func, Callable):
            func = FilterFunctionWrapper(func)
        elif not isinstance(func, FilterFunction):
            raise TypeError("func must be a Callable or instance of FilterFunction.")

        type_info = typeinfo._from_java_type(
            self._j_data_stream.getTransformation().getOutputType())
        j_data_stream = self.flat_map(FilterFlatMap(func), result_type=type_info)._j_data_stream
        filtered_stream = DataStream(j_data_stream)
        filtered_stream.name("Filter")
        return filtered_stream

    def union(self, *streams) -> 'DataStream':
        """
        Creates a new DataStream by merging DataStream outputs of the same type with each other. The
        DataStreams merged using this operator will be transformed simultaneously.

        :param streams: The DataStream to union outputwith.
        :return: The DataStream.
        """
        j_data_streams = []
        for data_stream in streams:
            j_data_streams.append(data_stream._j_data_stream)
        gateway = get_gateway()
        j_data_stream_class = gateway.jvm.org.apache.flink.streaming.api.datastream.DataStream
        j_data_stream_arr = get_gateway().new_array(j_data_stream_class, len(j_data_streams))
        for i in range(len(j_data_streams)):
            j_data_stream_arr[i] = j_data_streams[i]
        j_united_stream = self._j_data_stream.union(j_data_stream_arr)
        return DataStream(j_data_stream=j_united_stream)

    def connect(self, ds: 'DataStream') -> 'ConnectedStreams':
        """
        Creates a new 'ConnectedStreams' by connecting 'DataStream' outputs of (possible)
        different types with each other. The DataStreams connected using this operator can
        be used with CoFunctions to apply joint transformations.

        :param ds: The DataStream with which this stream will be connected.
        :return: The `ConnectedStreams`.
        """
        return ConnectedStreams(self, ds)

    def shuffle(self) -> 'DataStream':
        """
        Sets the partitioning of the DataStream so that the output elements are shuffled uniformly
        randomly to the next operation.

        :return: The DataStream with shuffle partitioning set.
        """
        return DataStream(self._j_data_stream.shuffle())

    def project(self, *field_indexes) -> 'DataStream':
        """
        Initiates a Project transformation on a Tuple DataStream.

        Note that only Tuple DataStreams can be projected.

        :param field_indexes: The field indexes of the input tuples that are retained. The order of
                              fields in the output tuple corresponds to the order of field indexes.
        :return: The projected DataStream.
        """
        if not isinstance(self.get_type(), typeinfo.TupleTypeInfo):
            raise Exception('Only Tuple DataStreams can be projected.')

        gateway = get_gateway()
        j_index_arr = gateway.new_array(gateway.jvm.int, len(field_indexes))
        for i in range(len(field_indexes)):
            j_index_arr[i] = field_indexes[i]
        return DataStream(self._j_data_stream.project(j_index_arr))

    def rescale(self) -> 'DataStream':
        """
        Sets the partitioning of the DataStream so that the output elements are distributed evenly
        to a subset of instances of the next operation in a round-robin fashion.

        The subset of downstream operations to which the upstream operation sends elements depends
        on the degree of parallelism of both the upstream and downstream operation. For example, if
        the upstream operation has parallelism 2 and the downstream operation has parallelism 4,
        then one upstream operation would distribute elements to two downstream operations. If, on
        the other hand, the downstream operation has parallelism 4 then two upstream operations will
        distribute to one downstream operation while the other two upstream operations will
        distribute to the other downstream operations.

        In cases where the different parallelisms are not multiples of each one or several
        downstream operations will have a differing number of inputs from upstream operations.

        :return: The DataStream with rescale partitioning set.
        """
        return DataStream(self._j_data_stream.rescale())

    def rebalance(self) -> 'DataStream':
        """
        Sets the partitioning of the DataStream so that the output elements are distributed evenly
        to instances of the next operation in a round-robin fashion.

        :return: The DataStream with rebalance partition set.
        """
        return DataStream(self._j_data_stream.rebalance())

    def forward(self) -> 'DataStream':
        """
        Sets the partitioning of the DataStream so that the output elements are forwarded to the
        local sub-task of the next operation.

        :return: The DataStream with forward partitioning set.
        """
        return DataStream(self._j_data_stream.forward())

    def broadcast(self) -> 'DataStream':
        """
        Sets the partitioning of the DataStream so that the output elements are broadcasted to every
        parallel instance of the next operation.

        :return: The DataStream with broadcast partitioning set.
        """
        return DataStream(self._j_data_stream.broadcast())

    def partition_custom(self, partitioner: Union[Callable, Partitioner],
                         key_selector: Union[Callable, KeySelector]) -> 'DataStream':
        """
        Partitions a DataStream on the key returned by the selector, using a custom partitioner.
        This method takes the key selector to get the key to partition on, and a partitioner that
        accepts the key type.

        Note that this method works only on single field keys, i.e. the selector cannet return
        tuples of fields.

        :param partitioner: The partitioner to assign partitions to keys.
        :param key_selector: The KeySelector with which the DataStream is partitioned.
        :return: The partitioned DataStream.
        """
        if callable(key_selector):
            key_selector = KeySelectorFunctionWrapper(key_selector)
        if not isinstance(key_selector, (KeySelector, KeySelectorFunctionWrapper)):
            raise TypeError("Parameter key_selector should be a type of KeySelector.")

        if callable(partitioner):
            partitioner = PartitionerFunctionWrapper(partitioner)
        if not isinstance(partitioner, (Partitioner, PartitionerFunctionWrapper)):
            raise TypeError("Parameter partitioner should be a type of Partitioner.")

        gateway = get_gateway()
        data_stream_num_partitions_env_key = \
            gateway.jvm.PythonPartitionCustomOperator.DATA_STREAM_NUM_PARTITIONS

        class PartitionCustomMapFunction(MapFunction):
            """
            A wrapper class for partition_custom map function. It indicates that it is a partition
            custom operation that we need to apply DataStreamPythonPartitionCustomFunctionOperator
            to run the map function.
            """

            def __init__(self):
                self.num_partitions = None

            def map(self, value):
                return self.partition_custom_map(value)

            def partition_custom_map(self, value):
                if self.num_partitions is None:
                    self.num_partitions = int(os.environ[data_stream_num_partitions_env_key])
                partition = partitioner.partition(key_selector.get_key(value), self.num_partitions)
                return Row(partition, value)

            def __repr__(self) -> str:
                return '_Flink_PartitionCustomMapFunction'

        original_type_info = self.get_type()
        intermediate_map_stream = self.map(PartitionCustomMapFunction(),
                                           output_type=Types.ROW([Types.INT(), original_type_info]))
        intermediate_map_stream.name(
            gateway.jvm.org.apache.flink.python.util.PythonConfigUtil
            .STREAM_PARTITION_CUSTOM_MAP_OPERATOR_NAME)

        JPartitionCustomKeySelector = gateway.jvm.PartitionCustomKeySelector
        JIdParitioner = gateway.jvm.org.apache.flink.api.java.functions.IdPartitioner
        intermediate_map_stream = DataStream(intermediate_map_stream._j_data_stream
                                             .partitionCustom(JIdParitioner(),
                                                              JPartitionCustomKeySelector()))

        values_map_stream = intermediate_map_stream.map(lambda x: x[1], original_type_info)
        values_map_stream.name(gateway.jvm.org.apache.flink.python.util.PythonConfigUtil
                               .KEYED_STREAM_VALUE_OPERATOR_NAME)
        return DataStream(values_map_stream._j_data_stream)

    def _get_java_python_function_operator(self,
                                           func: Union[Function, FunctionWrapper],
                                           type_info: TypeInformation,
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

        JDataStreamPythonFunction = gateway.jvm.DataStreamPythonFunction
        j_data_stream_python_function = JDataStreamPythonFunction(
            bytearray(serialized_func),
            _get_python_env())

        JDataStreamPythonFunctionInfo = gateway.jvm.DataStreamPythonFunctionInfo
        j_data_stream_python_function_info = JDataStreamPythonFunctionInfo(
            j_data_stream_python_function,
            func_type)

        j_conf = gateway.jvm.org.apache.flink.configuration.Configuration()

        # set max bundle size to 1 to force synchronize process for reduce function.
        from pyflink.fn_execution.flink_fn_execution_pb2 import UserDefinedDataStreamFunction
        if func_type == UserDefinedDataStreamFunction.REDUCE:
            j_conf.setInteger(gateway.jvm.org.apache.flink.python.PythonOptions.MAX_BUNDLE_SIZE, 1)

            j_output_type_info = j_input_types.getTypeAt(1)
            j_python_reduce_operator = gateway.jvm.PythonReduceOperator(
                j_conf,
                j_input_types,
                j_output_type_info,
                j_data_stream_python_function_info)
            return j_python_reduce_operator, j_output_type_info
        else:
            if str(func) == '_Flink_PartitionCustomMapFunction':
                JDataStreamPythonFunctionOperator = gateway.jvm.PythonPartitionCustomOperator
            else:
                JDataStreamPythonFunctionOperator = \
                    gateway.jvm.StatelessOneInputPythonFunctionOperator

            j_data_stream_python_function_operator = JDataStreamPythonFunctionOperator(
                j_conf,
                j_input_types,
                output_type_info.get_java_type_info(),
                j_data_stream_python_function_info)

            return j_data_stream_python_function_operator, output_type_info.get_java_type_info()

    def add_sink(self, sink_func: SinkFunction) -> 'DataStreamSink':
        """
        Adds the given sink to this DataStream. Only streams with sinks added will be executed once
        the StreamExecutionEnvironment.execute() method is called.

        :param sink_func: The SinkFunction object.
        :return: The closed DataStream.
        """
        return DataStreamSink(self._j_data_stream.addSink(sink_func.get_java_function()))

    def print(self, sink_identifier: str = None) -> 'DataStreamSink':
        """
        Writes a DataStream to the standard output stream (stdout).
        For each element of the DataStream the object string is writen.

        NOTE: This will print to stdout on the machine where the code is executed, i.e. the Flink
        worker, and is not fault tolerant.

        :param sink_identifier: The string to prefix the output with.
        :return: The closed DataStream.
        """
        if sink_identifier is not None:
            j_data_stream_sink = self._align_output_type()._j_data_stream.print(sink_identifier)
        else:
            j_data_stream_sink = self._align_output_type()._j_data_stream.print()
        return DataStreamSink(j_data_stream_sink)

    def _align_output_type(self) -> 'DataStream':
        """
        Transform the pickled python object into String if the output type is PickledByteArrayInfo.
        """
        output_type_info_class = self._j_data_stream.getTransformation().getOutputType().getClass()
        if output_type_info_class.isAssignableFrom(
                PickledBytesTypeInfo.PICKLED_BYTE_ARRAY_TYPE_INFO().get_java_type_info()
                .getClass()):
            def python_obj_to_str_map_func(value):
                if not isinstance(value, (str, bytes)):
                    value = str(value)
                return value

            transformed_data_stream = DataStream(
                self.map(python_obj_to_str_map_func,
                         output_type=Types.STRING())._j_data_stream)
            return transformed_data_stream
        else:
            return self


class DataStreamSink(object):
    """
    A Stream Sink. This is used for emitting elements from a streaming topology.
    """

    def __init__(self, j_data_stream_sink):
        """
        The constructor of DataStreamSink.

        :param j_data_stream_sink: A DataStreamSink java object.
        """
        self._j_data_stream_sink = j_data_stream_sink

    def name(self, name: str) -> 'DataStreamSink':
        """
        Sets the name of this sink. THis name is used by the visualization and logging during
        runtime.

        :param name: The name of this sink.
        :return: The named sink.
        """
        self._j_data_stream_sink.name(name)
        return self

    def uid(self, uid: str) -> 'DataStreamSink':
        """
        Sets an ID for this operator. The specified ID is used to assign the same operator ID across
        job submissions (for example when starting a job from a savepoint).

        Important: this ID needs to be unique per transformation and job. Otherwise, job submission
        will fail.

        :param uid: The unique user-specified ID of this transformation.
        :return: The operator with the specified ID.
        """
        self._j_data_stream_sink.uid(uid)
        return self

    def set_uid_hash(self, uid_hash: str) -> 'DataStreamSink':
        """
        Sets an user provided hash for this operator. This will be used AS IS the create the
        JobVertexID. The user provided hash is an alternative to the generated hashed, that is
        considered when identifying an operator through the default hash mechanics fails (e.g.
        because of changes between Flink versions).

        Important: this should be used as a workaround or for trouble shooting. The provided hash
        needs to be unique per transformation and job. Otherwise, job submission will fail.
        Furthermore, you cannot assign user-specified hash to intermediate nodes in an operator
        chain and trying so will let your job fail.

        A use case for this is in migration between Flink versions or changing the jobs in a way
        that changes the automatically generated hashes. In this case, providing the previous hashes
        directly through this method (e.g. obtained from old logs) can help to reestablish a lost
        mapping from states to their target operator.

        :param uid_hash: The user provided hash for this operator. This will become the jobVertexID,
                         which is shown in the logs and web ui.
        :return: The operator with the user provided hash.
        """
        self._j_data_stream_sink.setUidHash(uid_hash)
        return self

    def set_parallelism(self, parallelism: int) -> 'DataStreamSink':
        """
        Sets the parallelism for this operator.

        :param parallelism: THe parallelism for this operator.
        :return: The operator with set parallelism.
        """
        self._j_data_stream_sink.setParallelism(parallelism)
        return self

    def disable_chaining(self) -> 'DataStreamSink':
        """
        Turns off chaining for this operator so thread co-location will not be used as an
        optimization.
        Chaining can be turned off for the whole job by
        StreamExecutionEnvironment.disableOperatorChaining() however it is not advised for
        performance consideration.

        :return: The operator with chaining disabled.
        """
        self._j_data_stream_sink.disableChaining()
        return self

    def slot_sharing_group(self, slot_sharing_group: str) -> 'DataStreamSink':
        """
        Sets the slot sharing group of this operation. Parallel instances of operations that are in
        the same slot sharing group will be co-located in the same TaskManager slot, if possible.

        Operations inherit the slot sharing group of input operations if all input operations are in
        the same slot sharing group and no slot sharing group was explicitly specified.

        Initially an operation is in the default slot sharing group. An operation can be put into
        the default group explicitly by setting the slot sharing group to 'default'.

        :param slot_sharing_group: The slot sharing group name.
        :return: This operator.
        """
        self._j_data_stream_sink.slotSharingGroup(slot_sharing_group)
        return self


class KeyedStream(DataStream):
    """
    A KeyedStream represents a DataStream on which operator state is partitioned by key using a
    provided KeySelector. Typical operations supported by a DataStream are also possible on a
    KeyedStream, with the exception of partitioning methods such as shuffle, forward and keyBy.

    Reduce-style operations, such as reduce and sum work on elements that have the same key.
    """

    def __init__(self, j_keyed_stream, origin_stream: DataStream):
        """
        Constructor of KeyedStream.

        :param j_keyed_stream: A java KeyedStream object.
        :param origin_stream: The DataStream before key by.
        """
        super(KeyedStream, self).__init__(j_data_stream=j_keyed_stream)
        self._original_data_type_info = None
        self._origin_stream = origin_stream

    def map(self, func: Union[Callable, MapFunction], output_type: TypeInformation = None) \
            -> 'DataStream':
        return self._values().map(func, output_type)

    def flat_map(self, func: Union[Callable, FlatMapFunction], result_type: TypeInformation = None)\
            -> 'DataStream':
        return self._values().flat_map(func, result_type)

    def reduce(self, func: Union[Callable, ReduceFunction]) -> 'DataStream':
        """
        Applies a reduce transformation on the grouped data stream grouped on by the given
        key position. The `ReduceFunction` will receive input values based on the key value.
        Only input values with the same key will go to the same reducer.

        Example:
        ::
            >>> ds = env.from_collection([(1, 'a'), (2, 'a'), (3, 'a'), (4, 'b'])
            >>> ds.key_by(lambda x: x[1]).reduce(lambda a, b: a[0] + b[0], b[1])

        :param func: The ReduceFunction that is called for each element of the DataStream.
        :return: The transformed DataStream.
        """

        if not isinstance(func, ReduceFunction):
            if callable(func):
                func = ReduceFunctionWrapper(func)
            else:
                raise TypeError("The input must be a ReduceFunction or a callable function!")

        from pyflink.fn_execution.flink_fn_execution_pb2 import UserDefinedDataStreamFunction
        j_operator, j_output_type_info = \
            self._get_java_python_function_operator(func,
                                                    None,
                                                    UserDefinedDataStreamFunction.REDUCE)
        return DataStream(self._j_data_stream.transform(
            "Keyed Reduce",
            j_output_type_info,
            j_operator
        ))

    def filter(self, func: Union[Callable, FilterFunction]) -> 'DataStream':
        return self._values().filter(func)

    def add_sink(self, sink_func: SinkFunction) -> 'DataStreamSink':
        return self._values().add_sink(sink_func)

    def key_by(self, key_selector: Union[Callable, KeySelector],
               key_type_info: TypeInformation = None) -> 'KeyedStream':
        return self._origin_stream.key_by(key_selector, key_type_info)

    def union(self, *streams) -> 'DataStream':
        return self._values().union(*streams)

    def shuffle(self) -> 'DataStream':
        raise Exception('Cannot override partitioning for KeyedStream.')

    def project(self, *field_indexes) -> 'DataStream':
        return self._values().project(*field_indexes)

    def rescale(self) -> 'DataStream':
        raise Exception('Cannot override partitioning for KeyedStream.')

    def rebalance(self) -> 'DataStream':
        raise Exception('Cannot override partitioning for KeyedStream.')

    def forward(self) -> 'DataStream':
        raise Exception('Cannot override partitioning for KeyedStream.')

    def broadcast(self) -> 'DataStream':
        raise Exception('Cannot override partitioning for KeyedStream.')

    def partition_custom(self, partitioner: Union[Callable, Partitioner],
                         key_selector: Union[Callable, KeySelector]) -> 'DataStream':
        raise Exception('Cannot override partitioning for KeyedStream.')

    def print(self, sink_identifier=None):
        return self._values().print()

    def _values(self) -> 'DataStream':
        """
        Since python KeyedStream is in the format of Row(key_value, original_data), it is used for
        getting the original_data.
        """
        transformed_stream = super().map(lambda x: x[1], output_type=self._original_data_type_info)
        transformed_stream.name(get_gateway().jvm.org.apache.flink.python.util.PythonConfigUtil
                                .KEYED_STREAM_VALUE_OPERATOR_NAME)
        return DataStream(transformed_stream._j_data_stream)

    def set_parallelism(self, parallelism: int):
        raise Exception("Set parallelism for KeyedStream is not supported.")

    def name(self, name: str):
        raise Exception("Set name for KeyedStream is not supported.")

    def get_name(self) -> str:
        raise Exception("Get name of KeyedStream is not supported.")

    def uid(self, uid: str):
        raise Exception("Set uid for KeyedStream is not supported.")

    def set_uid_hash(self, uid_hash: str):
        raise Exception("Set uid hash for KeyedStream is not supported.")

    def set_max_parallelism(self, max_parallelism: int):
        raise Exception("Set max parallelism for KeyedStream is not supported.")

    def force_non_parallel(self):
        raise Exception("Set force non-parallel for KeyedStream is not supported.")

    def set_buffer_timeout(self, timeout_millis: int):
        raise Exception("Set buffer timeout for KeyedStream is not supported.")

    def start_new_chain(self) -> 'DataStream':
        raise Exception("Start new chain for KeyedStream is not supported.")

    def disable_chaining(self) -> 'DataStream':
        raise Exception("Disable chaining for KeyedStream is not supported.")

    def slot_sharing_group(self, slot_sharing_group: str) -> 'DataStream':
        raise Exception("Setting slot sharing group for KeyedStream is not supported.")


class ConnectedStreams(object):
    """
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
    """

    def __init__(self, stream1: DataStream, stream2: DataStream):
        self.stream1 = stream1
        self.stream2 = stream2

    def key_by(self, key_selector1: Union[Callable, KeySelector],
               key_selector2: Union[Callable, KeySelector],
               key_type_info: TypeInformation = None) -> 'ConnectedStreams':
        """
        KeyBy operation for connected data stream. Assigns keys to the elements of
        input1 and input2 using keySelector1 and keySelector2 with explicit type information
        for the common key type.

        :param key_selector1: The `KeySelector` used for grouping the first input.
        :param key_selector2: The `KeySelector` used for grouping the second input.
        :param key_type_info: The type information of the common key type.
        :return: The partitioned `ConnectedStreams`
        """

        ds1 = self.stream1
        ds2 = self.stream2
        if isinstance(self.stream1, KeyedStream):
            ds1 = self.stream1._origin_stream
        if isinstance(self.stream2, KeyedStream):
            ds2 = self.stream2._origin_stream
        return ConnectedStreams(
            ds1.key_by(key_selector1, key_type_info),
            ds2.key_by(key_selector2, key_type_info))

    def map(self, func: CoMapFunction, output_type: TypeInformation = None) \
            -> 'DataStream':
        """
        Applies a CoMap transformation on a `ConnectedStreams` and maps the output to a common
        type. The transformation calls a `CoMapFunction.map1` for each element of the first
        input and `CoMapFunction.map2` for each element of the second input. Each CoMapFunction
        call returns exactly one element.

        :param func: The CoMapFunction used to jointly transform the two input DataStreams
        :param output_type: `TypeInformation` for the result type of the function.
        :return: The transformed `DataStream`
        """
        if not isinstance(func, CoMapFunction):
            raise TypeError("The input function must be a CoMapFunction!")

        # get connected stream
        j_connected_stream = self.stream1._j_data_stream.connect(self.stream2._j_data_stream)
        from pyflink.fn_execution import flink_fn_execution_pb2
        j_operator, j_output_type = self._get_connected_stream_operator(
            func,
            output_type,
            flink_fn_execution_pb2.UserDefinedDataStreamFunction.CO_MAP)
        return DataStream(j_connected_stream.transform("Co-Map", j_output_type, j_operator))

    def flat_map(self, func: CoFlatMapFunction, output_type: TypeInformation = None) \
            -> 'DataStream':
        """
        Applies a CoFlatMap transformation on a `ConnectedStreams` and maps the output to a
        common type. The transformation calls a `CoFlatMapFunction.flatMap1` for each element
        of the first input and `CoFlatMapFunction.flatMap2` for each element of the second
        input. Each CoFlatMapFunction call returns any number of elements including none.

        :param func: The CoFlatMapFunction used to jointly transform the two input DataStreams
        :param output_type: `TypeInformation` for the result type of the function.
        :return: The transformed `DataStream`
        """

        if not isinstance(func, CoFlatMapFunction):
            raise TypeError("The input must be a CoFlatMapFunction!")

        # get connected stream
        j_connected_stream = self.stream1._j_data_stream.connect(self.stream2._j_data_stream)
        from pyflink.fn_execution import flink_fn_execution_pb2
        j_operator, j_output_type = self._get_connected_stream_operator(
            func,
            output_type,
            flink_fn_execution_pb2.UserDefinedDataStreamFunction.CO_FLAT_MAP)
        return DataStream(j_connected_stream.transform("Co-Flat Map", j_output_type, j_operator))

    def _get_connected_stream_operator(self,
                                       func: Union[Function, FunctionWrapper],
                                       type_info: TypeInformation,
                                       func_type: int):
        gateway = get_gateway()
        import cloudpickle
        serialized_func = cloudpickle.dumps(func)

        j_input_types1 = self.stream1._j_data_stream.getTransformation().getOutputType()
        j_input_types2 = self.stream2._j_data_stream.getTransformation().getOutputType()

        if type_info is None:
            output_type_info = PickledBytesTypeInfo.PICKLED_BYTE_ARRAY_TYPE_INFO()
        else:
            if isinstance(type_info, list):
                output_type_info = RowTypeInfo(type_info)
            else:
                output_type_info = type_info

        DataStreamPythonFunction = gateway.jvm.DataStreamPythonFunction
        j_data_stream_python_function = DataStreamPythonFunction(
            bytearray(serialized_func),
            _get_python_env())

        DataStreamPythonFunctionInfo = gateway.jvm.DataStreamPythonFunctionInfo

        j_data_stream_python_function_info = DataStreamPythonFunctionInfo(
            j_data_stream_python_function,
            func_type)

        j_conf = gateway.jvm.org.apache.flink.configuration.Configuration()
        StatelessTwoInputPythonFunctionOperator = \
            gateway.jvm.StatelessTwoInputPythonFunctionOperator
        j_python_data_stream_function_operator = StatelessTwoInputPythonFunctionOperator(
            j_conf,
            j_input_types1,
            j_input_types2,
            output_type_info.get_java_type_info(),
            j_data_stream_python_function_info,
            self._is_keyed_stream())

        return j_python_data_stream_function_operator, output_type_info.get_java_type_info()

    def _is_keyed_stream(self):
        return isinstance(self.stream1, KeyedStream) and isinstance(self.stream2, KeyedStream)
