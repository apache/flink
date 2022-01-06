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
import typing
import uuid
from typing import Callable, Union, List, cast

from pyflink.common import typeinfo, ExecutionConfig, Row
from pyflink.datastream.slot_sharing_group import SlotSharingGroup
from pyflink.datastream.window import (TimeWindowSerializer, CountWindowSerializer, WindowAssigner,
                                       Trigger, WindowOperationDescriptor)
from pyflink.common.typeinfo import RowTypeInfo, Types, TypeInformation, _from_java_type
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream.connectors import Sink
from pyflink.datastream.functions import (_get_python_env, FlatMapFunction, MapFunction, Function,
                                          FunctionWrapper, SinkFunction, FilterFunction,
                                          KeySelector, ReduceFunction, CoMapFunction,
                                          CoFlatMapFunction, Partitioner, RuntimeContext,
                                          ProcessFunction, KeyedProcessFunction,
                                          KeyedCoProcessFunction, WindowFunction,
                                          ProcessWindowFunction, InternalWindowFunction,
                                          InternalIterableWindowFunction,
                                          InternalIterableProcessWindowFunction, CoProcessFunction)
from pyflink.datastream.state import ValueStateDescriptor, ValueState, ListStateDescriptor
from pyflink.datastream.utils import convert_to_python_obj
from pyflink.java_gateway import get_gateway


__all__ = ['CloseableIterator', 'DataStream', 'KeyedStream', 'ConnectedStreams', 'WindowedStream',
           'DataStreamSink', 'CloseableIterator']


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

    def name(self, name: str) -> 'DataStream':
        """
        Sets the name of the current data stream. This name is used by the visualization and logging
        during runtime.

        :param name: Name of the stream.
        :return: The named operator.
        """
        self._j_data_stream.name(name)
        return self

    def uid(self, uid: str) -> 'DataStream':
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

    def set_uid_hash(self, uid_hash: str) -> 'DataStream':
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

    def set_parallelism(self, parallelism: int) -> 'DataStream':
        """
        Sets the parallelism for this operator.

        :param parallelism: THe parallelism for this operator.
        :return: The operator with set parallelism.
        """
        self._j_data_stream.setParallelism(parallelism)
        return self

    def set_max_parallelism(self, max_parallelism: int) -> 'DataStream':
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

    def force_non_parallel(self) -> 'DataStream':
        """
        Sets the parallelism and maximum parallelism of this operator to one. And mark this operator
        cannot set a non-1 degree of parallelism.

        :return: The operator with only one parallelism.
        """
        self._j_data_stream.forceNonParallel()
        return self

    def set_buffer_timeout(self, timeout_millis: int) -> 'DataStream':
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

    def slot_sharing_group(self, slot_sharing_group: Union[str, SlotSharingGroup]) -> 'DataStream':
        """
        Sets the slot sharing group of this operation. Parallel instances of operations that are in
        the same slot sharing group will be co-located in the same TaskManager slot, if possible.

        Operations inherit the slot sharing group of input operations if all input operations are in
        the same slot sharing group and no slot sharing group was explicitly specified.

        Initially an operation is in the default slot sharing group. An operation can be put into
        the default group explicitly by setting the slot sharing group to 'default'.

        :param slot_sharing_group: The slot sharing group name or which contains name and its
                        resource spec.
        :return: This operator.
        """
        if isinstance(slot_sharing_group, SlotSharingGroup):
            self._j_data_stream.slotSharingGroup(slot_sharing_group.get_java_slot_sharing_group())
        else:
            self._j_data_stream.slotSharingGroup(slot_sharing_group)
        return self

    def map(self, func: Union[Callable, MapFunction], output_type: TypeInformation = None) \
            -> 'DataStream':
        """
        Applies a Map transformation on a DataStream. The transformation calls a MapFunction for
        each element of the DataStream. Each MapFunction call returns exactly one element.

        Note that If user does not specify the output data type, the output data will be serialized
        as pickle primitive byte array.

        :param func: The MapFunction that is called for each element of the DataStream.
        :param output_type: The type information of the MapFunction output data.
        :return: The transformed DataStream.
        """
        if not isinstance(func, MapFunction) and not callable(func):
            raise TypeError("The input must be a MapFunction or a callable function")

        class MapProcessFunctionAdapter(ProcessFunction):

            def __init__(self, map_func):
                if isinstance(map_func, MapFunction):
                    self._open_func = map_func.open
                    self._close_func = map_func.close
                    self._map_func = map_func.map
                else:
                    self._open_func = None
                    self._close_func = None
                    self._map_func = map_func

            def open(self, runtime_context: RuntimeContext):
                if self._open_func:
                    self._open_func(runtime_context)

            def close(self):
                if self._close_func:
                    self._close_func()

            def process_element(self, value, ctx: 'ProcessFunction.Context'):
                yield self._map_func(value)

        return self.process(MapProcessFunctionAdapter(func), output_type) \
            .name("Map")

    def flat_map(self,
                 func: Union[Callable, FlatMapFunction],
                 output_type: TypeInformation = None) -> 'DataStream':
        """
        Applies a FlatMap transformation on a DataStream. The transformation calls a FlatMapFunction
        for each element of the DataStream. Each FlatMapFunction call can return any number of
        elements including none.

        :param func: The FlatMapFunction that is called for each element of the DataStream.
        :param output_type: The type information of output data.
        :return: The transformed DataStream.
        """

        if not isinstance(func, FlatMapFunction) and not callable(func):
            raise TypeError("The input must be a FlatMapFunction or a callable function")

        class FlatMapProcessFunctionAdapter(ProcessFunction):

            def __init__(self, flat_map_func):
                if isinstance(flat_map_func, FlatMapFunction):
                    self._open_func = flat_map_func.open
                    self._close_func = flat_map_func.close
                    self._flat_map_func = flat_map_func.flat_map
                else:
                    self._open_func = None
                    self._close_func = None
                    self._flat_map_func = flat_map_func

            def open(self, runtime_context: RuntimeContext):
                if self._open_func:
                    self._open_func(runtime_context)

            def close(self):
                if self._close_func:
                    self._close_func()

            def process_element(self, value, ctx: 'ProcessFunction.Context'):
                yield from self._flat_map_func(value)

        return self.process(FlatMapProcessFunctionAdapter(func), output_type) \
            .name("FlatMap")

    def key_by(self,
               key_selector: Union[Callable, KeySelector],
               key_type: TypeInformation = None) -> 'KeyedStream':
        """
        Creates a new KeyedStream that uses the provided key for partitioning its operator states.

        :param key_selector: The KeySelector to be used for extracting the key for partitioning.
        :param key_type: The type information describing the key type.
        :return: The DataStream with partitioned state(i.e. KeyedStream).
        """

        if not isinstance(key_selector, KeySelector) and not callable(key_selector):
            raise TypeError("Parameter key_selector should be type of KeySelector or a callable "
                            "function.")

        class AddKey(ProcessFunction):

            def __init__(self, key_selector):
                if isinstance(key_selector, KeySelector):
                    self._key_selector_open_func = key_selector.open
                    self._key_selector_close_func = key_selector.close
                    self._get_key_func = key_selector.get_key
                else:
                    self._key_selector_open_func = None
                    self._key_selector_close_func = None
                    self._get_key_func = key_selector

            def open(self, runtime_context: RuntimeContext):
                if self._key_selector_open_func:
                    self._key_selector_open_func(runtime_context)

            def close(self):
                if self._key_selector_close_func:
                    self._key_selector_close_func()

            def process_element(self, value, ctx: 'ProcessFunction.Context'):
                yield Row(self._get_key_func(value), value)

        output_type_info = typeinfo._from_java_type(
            self._j_data_stream.getTransformation().getOutputType())
        if key_type is None:
            key_type = Types.PICKLED_BYTE_ARRAY()

        gateway = get_gateway()
        stream_with_key_info = self.process(
            AddKey(key_selector),
            output_type=Types.ROW([key_type, output_type_info]))
        stream_with_key_info.name(gateway.jvm.org.apache.flink.python.util.PythonConfigUtil
                                  .STREAM_KEY_BY_MAP_OPERATOR_NAME)

        JKeyByKeySelector = gateway.jvm.KeyByKeySelector
        key_stream = KeyedStream(
            stream_with_key_info._j_data_stream.keyBy(
                JKeyByKeySelector(),
                Types.ROW([key_type]).get_java_type_info()), output_type_info,
            self)
        return key_stream

    def filter(self, func: Union[Callable, FilterFunction]) -> 'DataStream':
        """
        Applies a Filter transformation on a DataStream. The transformation calls a FilterFunction
        for each element of the DataStream and retains only those element for which the function
        returns true. Elements for which the function returns false are filtered.

        :param func: The FilterFunction that is called for each element of the DataStream.
        :return: The filtered DataStream.
        """

        if not isinstance(func, FilterFunction) and not callable(func):
            raise TypeError("The input must be a FilterFunction or a callable function")

        class FilterProcessFunctionAdapter(ProcessFunction):

            def __init__(self, filter_func):
                if isinstance(filter_func, FilterFunction):
                    self._open_func = filter_func.open
                    self._close_func = filter_func.close
                    self._filter_func = filter_func.filter
                else:
                    self._open_func = None
                    self._close_func = None
                    self._filter_func = filter_func

            def open(self, runtime_context: RuntimeContext):
                if self._open_func:
                    self._open_func(runtime_context)

            def close(self):
                if self._close_func:
                    self._close_func()

            def process_element(self, value, ctx: 'ProcessFunction.Context'):
                if self._filter_func(value):
                    yield value

        output_type = typeinfo._from_java_type(
            self._j_data_stream.getTransformation().getOutputType())
        return self.process(FilterProcessFunctionAdapter(func), output_type=output_type) \
            .name("Filter")

    def union(self, *streams: 'DataStream') -> 'DataStream':
        """
        Creates a new DataStream by merging DataStream outputs of the same type with each other. The
        DataStreams merged using this operator will be transformed simultaneously.

        :param streams: The DataStream to union outputwith.
        :return: The DataStream.
        """
        j_data_streams = []
        for data_stream in streams:
            if isinstance(data_stream, KeyedStream):
                j_data_streams.append(data_stream._values()._j_data_stream)
            else:
                j_data_streams.append(data_stream._j_data_stream)
        gateway = get_gateway()
        JDataStream = gateway.jvm.org.apache.flink.streaming.api.datastream.DataStream
        j_data_stream_arr = get_gateway().new_array(JDataStream, len(j_data_streams))
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

    def project(self, *field_indexes: int) -> 'DataStream':
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

    def process(self, func: ProcessFunction, output_type: TypeInformation = None) -> 'DataStream':
        """
        Applies the given ProcessFunction on the input stream, thereby creating a transformed output
        stream.

        The function will be called for every element in the input streams and can produce zero or
        more output elements.

        :param func: The ProcessFunction that is called for each element in the stream.
        :param output_type: TypeInformation for the result type of the function.
        :return: The transformed DataStream.
        """

        from pyflink.fn_execution import flink_fn_execution_pb2
        j_python_data_stream_function_operator, j_output_type_info = \
            _get_one_input_stream_operator(
                self,
                func,
                flink_fn_execution_pb2.UserDefinedDataStreamFunction.PROCESS,  # type: ignore
                output_type)
        return DataStream(self._j_data_stream.transform(
            "PROCESS",
            j_output_type_info,
            j_python_data_stream_function_operator))

    def assign_timestamps_and_watermarks(self, watermark_strategy: WatermarkStrategy) -> \
            'DataStream':
        """
        Assigns timestamps to the elements in the data stream and generates watermarks to signal
        event time progress. The given {@link WatermarkStrategy} is used to create a
        TimestampAssigner and WatermarkGenerator.

        :param watermark_strategy: The strategy to generate watermarks based on event timestamps.
        :return: The stream after the transformation, with assigned timestamps and watermarks.
        """
        if watermark_strategy._timestamp_assigner is not None:
            # in case users have specified custom TimestampAssigner, we need to extract and
            # generate watermark according to the specified TimestampAssigner.

            class TimestampAssignerProcessFunctionAdapter(ProcessFunction):

                def __init__(self, timestamp_assigner: TimestampAssigner):
                    self._extract_timestamp_func = timestamp_assigner.extract_timestamp

                def process_element(self, value, ctx: 'ProcessFunction.Context'):
                    yield value, self._extract_timestamp_func(value, ctx.timestamp())

            # step 1: extract the timestamp according to the specified TimestampAssigner
            timestamped_data_stream = self.process(
                TimestampAssignerProcessFunctionAdapter(watermark_strategy._timestamp_assigner),
                Types.TUPLE([self.get_type(), Types.LONG()]))
            timestamped_data_stream.name("Extract-Timestamp")

            # step 2: assign timestamp and watermark
            gateway = get_gateway()
            JCustomTimestampAssigner = gateway.jvm.org.apache.flink.streaming.api.functions.python \
                .eventtime.CustomTimestampAssigner
            j_watermarked_data_stream = (
                timestamped_data_stream._j_data_stream.assignTimestampsAndWatermarks(
                    watermark_strategy._j_watermark_strategy.withTimestampAssigner(
                        JCustomTimestampAssigner())))

            # step 3: remove the timestamp field which is added in step 1
            JRemoveTimestampMapFunction = gateway.jvm.org.apache.flink.streaming.api.functions \
                .python.eventtime.RemoveTimestampMapFunction
            result = DataStream(j_watermarked_data_stream.map(
                JRemoveTimestampMapFunction(), self._j_data_stream.getType()))
            result.name("Remove-Timestamp")
            return result
        else:
            # if user not specify a TimestampAssigner, then return directly assign the Java
            # watermark strategy.
            return DataStream(self._j_data_stream.assignTimestampsAndWatermarks(
                watermark_strategy._j_watermark_strategy))

    def partition_custom(self, partitioner: Union[Callable, Partitioner],
                         key_selector: Union[Callable, KeySelector]) -> 'DataStream':
        """
        Partitions a DataStream on the key returned by the selector, using a custom partitioner.
        This method takes the key selector to get the key to partition on, and a partitioner that
        accepts the key type.

        Note that this method works only on single field keys, i.e. the selector cannot return
        tuples of fields.

        :param partitioner: The partitioner to assign partitions to keys.
        :param key_selector: The KeySelector with which the DataStream is partitioned.
        :return: The partitioned DataStream.
        """

        if not isinstance(partitioner, Partitioner) and not callable(partitioner):
            raise TypeError("Parameter partitioner should be type of Partitioner or a callable "
                            "function.")

        if not isinstance(key_selector, KeySelector) and not callable(key_selector):
            raise TypeError("Parameter key_selector should be type of KeySelector or a callable "
                            "function.")

        gateway = get_gateway()

        class CustomPartitioner(ProcessFunction):
            """
            A wrapper class for partition_custom map function. It indicates that it is a partition
            custom operation that we need to apply PythonPartitionCustomOperator
            to run the map function.
            """

            def __init__(self, partitioner, key_selector):
                if isinstance(partitioner, Partitioner):
                    self._partitioner_open_func = partitioner.open
                    self._partitioner_close_func = partitioner.close
                    self._partition_func = partitioner.partition
                else:
                    self._partitioner_open_func = None
                    self._partitioner_close_func = None
                    self._partition_func = partitioner

                if isinstance(key_selector, KeySelector):
                    self._key_selector_open_func = key_selector.open
                    self._key_selector_close_func = key_selector.close
                    self._get_key_func = key_selector.get_key
                else:
                    self._key_selector_open_func = None
                    self._key_selector_close_func = None
                    self._get_key_func = key_selector

            def open(self, runtime_context: RuntimeContext):
                if self._partitioner_open_func:
                    self._partitioner_open_func(runtime_context)
                if self._key_selector_open_func:
                    self._key_selector_open_func(runtime_context)

                self.num_partitions = int(runtime_context.get_job_parameter(
                    "NUM_PARTITIONS", "-1"))
                if self.num_partitions <= 0:
                    raise ValueError(
                        "The partition number should be a positive value, got %s"
                        % self.num_partitions)

            def close(self):
                if self._partitioner_close_func:
                    self._partitioner_close_func()
                if self._key_selector_close_func:
                    self._key_selector_close_func()

            def process_element(self, value, ctx: 'ProcessFunction.Context'):
                partition = self._partition_func(self._get_key_func(value), self.num_partitions)
                yield Row(partition, value)

        original_type_info = self.get_type()
        stream_with_partition_info = self.process(
            CustomPartitioner(partitioner, key_selector),
            output_type=Types.ROW([Types.INT(), original_type_info]))
        stream_with_partition_info._j_data_stream.getTransformation().getOperatorFactory() \
            .getOperator().setContainsPartitionCustom(True)

        stream_with_partition_info.name(
            gateway.jvm.org.apache.flink.python.util.PythonConfigUtil
            .STREAM_PARTITION_CUSTOM_MAP_OPERATOR_NAME)

        JPartitionCustomKeySelector = gateway.jvm.PartitionCustomKeySelector
        JIdParitioner = gateway.jvm.org.apache.flink.api.java.functions.IdPartitioner
        partitioned_stream_with_partition_info = DataStream(
            stream_with_partition_info._j_data_stream.partitionCustom(
                JIdParitioner(), JPartitionCustomKeySelector()))

        partitioned_stream = partitioned_stream_with_partition_info.map(
            lambda x: x[1], original_type_info)
        partitioned_stream.name(gateway.jvm.org.apache.flink.python.util.PythonConfigUtil
                                .KEYED_STREAM_VALUE_OPERATOR_NAME)
        return DataStream(partitioned_stream._j_data_stream)

    def add_sink(self, sink_func: SinkFunction) -> 'DataStreamSink':
        """
        Adds the given sink to this DataStream. Only streams with sinks added will be executed once
        the StreamExecutionEnvironment.execute() method is called.

        :param sink_func: The SinkFunction object.
        :return: The closed DataStream.
        """
        return DataStreamSink(self._j_data_stream.addSink(sink_func.get_java_function()))

    def sink_to(self, sink: Sink) -> 'DataStreamSink':
        """
        Adds the given sink to this DataStream. Only streams with sinks added will be
        executed once the
        :func:`~pyflink.datastream.stream_execution_environment.StreamExecutionEnvironment.execute`
        method is called.

        :param sink: The user defined sink.
        :return: The closed DataStream.
        """
        return DataStreamSink(self._j_data_stream.sinkTo(sink.get_java_function()))

    def execute_and_collect(self, job_execution_name: str = None, limit: int = None) \
            -> Union['CloseableIterator', list]:
        """
        Triggers the distributed execution of the streaming dataflow and returns an iterator over
        the elements of the given DataStream.

        The DataStream application is executed in the regular distributed manner on the target
        environment, and the events from the stream are polled back to this application process and
        thread through Flink's REST API.

        The returned iterator must be closed to free all cluster resources.

        :param job_execution_name: The name of the job execution.
        :param limit: The limit for the collected elements.
        """
        JPythonConfigUtil = get_gateway().jvm.org.apache.flink.python.util.PythonConfigUtil
        JPythonConfigUtil.configPythonOperator(self._j_data_stream.getExecutionEnvironment())
        self._apply_chaining_optimization()
        if job_execution_name is None and limit is None:
            return CloseableIterator(self._j_data_stream.executeAndCollect(), self.get_type())
        elif job_execution_name is not None and limit is None:
            return CloseableIterator(self._j_data_stream.executeAndCollect(job_execution_name),
                                     self.get_type())
        if job_execution_name is None and limit is not None:
            return list(map(lambda data: convert_to_python_obj(data, self.get_type()),
                            self._j_data_stream.executeAndCollect(limit)))
        else:
            return list(map(lambda data: convert_to_python_obj(data, self.get_type()),
                            self._j_data_stream.executeAndCollect(job_execution_name, limit)))

    def print(self, sink_identifier: str = None) -> 'DataStreamSink':
        """
        Writes a DataStream to the standard output stream (stdout).
        For each element of the DataStream the object string is written.

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

    def _apply_chaining_optimization(self):
        """
        Chain the Python operators if possible.
        """
        gateway = get_gateway()
        JPythonOperatorChainingOptimizer = gateway.jvm.org.apache.flink.python.chain. \
            PythonOperatorChainingOptimizer
        j_transformation = JPythonOperatorChainingOptimizer.apply(
            self._j_data_stream.getExecutionEnvironment(),
            self._j_data_stream.getTransformation())
        self._j_data_stream = gateway.jvm.org.apache.flink.streaming.api.datastream.DataStream(
            self._j_data_stream.getExecutionEnvironment(), j_transformation)

    def _align_output_type(self) -> 'DataStream':
        """
        Transform the pickled python object into String if the output type is PickledByteArrayInfo.
        """
        from py4j.java_gateway import get_java_class

        gateway = get_gateway()
        ExternalTypeInfo_CLASS = get_java_class(
            gateway.jvm.org.apache.flink.table.runtime.typeutils.ExternalTypeInfo)
        RowTypeInfo_CLASS = get_java_class(
            gateway.jvm.org.apache.flink.api.java.typeutils.RowTypeInfo)
        output_type_info_class = self._j_data_stream.getTransformation().getOutputType().getClass()
        if output_type_info_class.isAssignableFrom(
                Types.PICKLED_BYTE_ARRAY().get_java_type_info()
                .getClass()):
            def python_obj_to_str_map_func(value):
                if not isinstance(value, (str, bytes)):
                    value = str(value)
                return value

            transformed_data_stream = DataStream(
                self.map(python_obj_to_str_map_func,
                         output_type=Types.STRING())._j_data_stream)
            return transformed_data_stream
        elif (output_type_info_class.isAssignableFrom(ExternalTypeInfo_CLASS) or
              output_type_info_class.isAssignableFrom(RowTypeInfo_CLASS)):
            def python_obj_to_str_map_func(value):
                assert isinstance(value, Row)
                return '{}[{}]'.format(value.get_row_kind(),
                                       ','.join([str(item) for item in value._values]))
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

    def slot_sharing_group(self, slot_sharing_group: Union[str, SlotSharingGroup]) \
            -> 'DataStreamSink':
        """
        Sets the slot sharing group of this operation. Parallel instances of operations that are in
        the same slot sharing group will be co-located in the same TaskManager slot, if possible.

        Operations inherit the slot sharing group of input operations if all input operations are in
        the same slot sharing group and no slot sharing group was explicitly specified.

        Initially an operation is in the default slot sharing group. An operation can be put into
        the default group explicitly by setting the slot sharing group to 'default'.

        :param slot_sharing_group: The slot sharing group name or which contains name and its
                        resource spec.
        :return: This operator.
        """
        if isinstance(slot_sharing_group, SlotSharingGroup):
            self._j_data_stream_sink.slotSharingGroup(
                slot_sharing_group.get_java_slot_sharing_group())
        else:
            self._j_data_stream_sink.slotSharingGroup(slot_sharing_group)
        return self


class KeyedStream(DataStream):
    """
    A KeyedStream represents a DataStream on which operator state is partitioned by key using a
    provided KeySelector. Typical operations supported by a DataStream are also possible on a
    KeyedStream, with the exception of partitioning methods such as shuffle, forward and keyBy.

    Reduce-style operations, such as reduce and sum work on elements that have the same key.
    """

    def __init__(self, j_keyed_stream, original_data_type_info, origin_stream: DataStream):
        """
        Constructor of KeyedStream.

        :param j_keyed_stream: A java KeyedStream object.
        :param original_data_type_info: Original data typeinfo.
        :param origin_stream: The DataStream before key by.
        """
        super(KeyedStream, self).__init__(j_data_stream=j_keyed_stream)
        self._original_data_type_info = original_data_type_info
        self._origin_stream = origin_stream

    def map(self, func: Union[Callable, MapFunction], output_type: TypeInformation = None) \
            -> 'DataStream':
        """
        Applies a Map transformation on a KeyedStream. The transformation calls a MapFunction for
        each element of the DataStream. Each MapFunction call returns exactly one element.

        Note that If user does not specify the output data type, the output data will be serialized
        as pickle primitive byte array.

        :param func: The MapFunction that is called for each element of the DataStream.
        :param output_type: The type information of the MapFunction output data.
        :return: The transformed DataStream.
        """
        if not isinstance(func, MapFunction) and not callable(func):
            raise TypeError("The input must be a MapFunction or a callable function")

        class MapKeyedProcessFunctionAdapter(KeyedProcessFunction):

            def __init__(self, map_func):
                if isinstance(map_func, MapFunction):
                    self._open_func = map_func.open
                    self._close_func = map_func.close
                    self._map_func = map_func.map
                else:
                    self._open_func = None
                    self._close_func = None
                    self._map_func = map_func

            def open(self, runtime_context: RuntimeContext):
                if self._open_func:
                    self._open_func(runtime_context)

            def close(self):
                if self._close_func:
                    self._close_func()

            def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
                yield self._map_func(value)

        return self.process(MapKeyedProcessFunctionAdapter(func), output_type) \
            .name("Map")  # type: ignore

    def flat_map(self,
                 func: Union[Callable, FlatMapFunction],
                 output_type: TypeInformation = None) -> 'DataStream':
        """
        Applies a FlatMap transformation on a KeyedStream. The transformation calls a
        FlatMapFunction for each element of the DataStream. Each FlatMapFunction call can return
        any number of elements including none.

        :param func: The FlatMapFunction that is called for each element of the DataStream.
        :param output_type: The type information of output data.
        :return: The transformed DataStream.
        """

        if not isinstance(func, FlatMapFunction) and not callable(func):
            raise TypeError("The input must be a FlatMapFunction or a callable function")

        class FlatMapKeyedProcessFunctionAdapter(KeyedProcessFunction):

            def __init__(self, flat_map_func):
                if isinstance(flat_map_func, FlatMapFunction):
                    self._open_func = flat_map_func.open
                    self._close_func = flat_map_func.close
                    self._flat_map_func = flat_map_func.flat_map
                else:
                    self._open_func = None
                    self._close_func = None
                    self._flat_map_func = flat_map_func

            def open(self, runtime_context: RuntimeContext):
                if self._open_func:
                    self._open_func(runtime_context)

            def close(self):
                if self._close_func:
                    self._close_func()

            def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
                yield from self._flat_map_func(value)

        return self.process(FlatMapKeyedProcessFunctionAdapter(func), output_type) \
            .name("FlatMap")

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

        if not isinstance(func, ReduceFunction) and not callable(func):
            raise TypeError("The input must be a ReduceFunction or a callable function")

        output_type = _from_java_type(self._original_data_type_info.get_java_type_info())

        class ReduceProcessKeyedProcessFunctionAdapter(KeyedProcessFunction):

            def __init__(self, reduce_function):
                if isinstance(reduce_function, ReduceFunction):
                    self._open_func = reduce_function.open
                    self._close_func = reduce_function.close
                    self._reduce_function = reduce_function.reduce
                else:
                    self._open_func = None
                    self._close_func = None
                    self._reduce_function = reduce_function
                self._reduce_value_state = None  # type: ValueState

            def open(self, runtime_context: RuntimeContext):
                if self._open_func:
                    self._open_func(runtime_context)

                self._reduce_value_state = runtime_context.get_state(
                    ValueStateDescriptor("_reduce_state" + str(uuid.uuid4()), output_type))
                from pyflink.fn_execution.datastream.runtime_context import StreamingRuntimeContext
                self._in_batch_execution_mode = \
                    cast(StreamingRuntimeContext, runtime_context)._in_batch_execution_mode

            def close(self):
                if self._close_func:
                    self._close_func()

            def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
                reduce_value = self._reduce_value_state.value()
                if reduce_value is not None:
                    reduce_value = self._reduce_function(reduce_value, value)
                else:
                    # register a timer for emitting the result at the end when this is the
                    # first input for this key
                    if self._in_batch_execution_mode:
                        ctx.timer_service().register_event_time_timer(0x7fffffffffffffff)
                    reduce_value = value
                self._reduce_value_state.update(reduce_value)
                if not self._in_batch_execution_mode:
                    # only emitting the result when all the data for a key is received
                    yield reduce_value

            def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
                current_value = self._reduce_value_state.value()
                if current_value is not None:
                    yield current_value

        return self.process(ReduceProcessKeyedProcessFunctionAdapter(func), output_type) \
            .name("Reduce")

    def filter(self, func: Union[Callable, FilterFunction]) -> 'DataStream':
        if not isinstance(func, FilterFunction) and not callable(func):
            raise TypeError("The input must be a FilterFunction or a callable function")

        class FilterKeyedProcessFunctionAdapter(KeyedProcessFunction):

            def __init__(self, filter_func):
                if isinstance(filter_func, FilterFunction):
                    self._open_func = filter_func.open
                    self._close_func = filter_func.close
                    self._filter_func = filter_func.filter
                else:
                    self._open_func = None
                    self._close_func = None
                    self._filter_func = filter_func

            def open(self, runtime_context: RuntimeContext):
                if self._open_func:
                    self._open_func(runtime_context)

            def close(self):
                if self._close_func:
                    self._close_func()

            def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
                if self._filter_func(value):
                    yield value

        return self.process(FilterKeyedProcessFunctionAdapter(func), self._original_data_type_info)\
            .name("Filter")

    def add_sink(self, sink_func: SinkFunction) -> 'DataStreamSink':
        return self._values().add_sink(sink_func)

    def key_by(self, key_selector: Union[Callable, KeySelector],
               key_type: TypeInformation = None) -> 'KeyedStream':
        return self._origin_stream.key_by(key_selector, key_type)

    def process(self, func: KeyedProcessFunction,  # type: ignore
                output_type: TypeInformation = None) -> 'DataStream':
        """
        Applies the given ProcessFunction on the input stream, thereby creating a transformed output
        stream.

        The function will be called for every element in the input streams and can produce zero or
        more output elements.

        :param func: The KeyedProcessFunction that is called for each element in the stream.
        :param output_type: TypeInformation for the result type of the function.
        :return: The transformed DataStream.
        """
        if not isinstance(func, KeyedProcessFunction):
            raise TypeError("KeyedProcessFunction is required for KeyedStream.")

        from pyflink.fn_execution import flink_fn_execution_pb2
        j_python_data_stream_function_operator, j_output_type_info = \
            _get_one_input_stream_operator(
                self,
                func,
                flink_fn_execution_pb2.UserDefinedDataStreamFunction.KEYED_PROCESS,  # type: ignore
                output_type)
        return DataStream(self._j_data_stream.transform(
            "KEYED PROCESS",
            j_output_type_info,
            j_python_data_stream_function_operator))

    def window(self, window_assigner: WindowAssigner) -> 'WindowedStream':
        """
        Windows this data stream to a WindowedStream, which evaluates windows over a key
        grouped stream. Elements are put into windows by a WindowAssigner. The grouping of
        elements is done both by key and by window.

        A Trigger can be defined to specify when windows are evaluated. However, WindowAssigners
        have a default Trigger that is used if a Trigger is not specified.

        :param window_assigner: The WindowAssigner that assigns elements to windows.
        :return: The trigger windows data stream.
        """
        return WindowedStream(self, window_assigner)

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
        transformed_stream = self.map(lambda x: x, output_type=self._original_data_type_info)
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

    def slot_sharing_group(self, slot_sharing_group: Union[str, SlotSharingGroup]) -> 'DataStream':
        raise Exception("Setting slot sharing group for KeyedStream is not supported.")


class WindowedStream(object):
    """
    A WindowedStream represents a data stream where elements are grouped by key, and for each
    key, the stream of elements is split into windows based on a WindowAssigner. Window emission
    is triggered based on a Trigger.

    The windows are conceptually evaluated for each key individually, meaning windows can trigger
    at different points for each key.

    Note that the WindowedStream is purely an API construct, during runtime the WindowedStream will
    be collapsed together with the KeyedStream and the operation over the window into one single
    operation.
    """

    def __init__(self, keyed_stream: KeyedStream, window_assigner: WindowAssigner):
        self._keyed_stream = keyed_stream
        self._window_assigner = window_assigner
        self._allowed_lateness = 0
        self._window_trigger = None  # type: Trigger

    def get_execution_environment(self):
        return self._keyed_stream.get_execution_environment()

    def get_input_type(self):
        return _from_java_type(self._keyed_stream._original_data_type_info.get_java_type_info())

    def trigger(self, trigger: Trigger):
        """
        Sets the Trigger that should be used to trigger window emission.
        """
        self._window_trigger = trigger
        return self

    def allowed_lateness(self, time_ms: int):
        """
        Sets the time by which elements are allowed to be late. Elements that arrive behind the
        watermark by more than the specified time will be dropped. By default, the allowed lateness
        is 0.

        Setting an allowed lateness is only valid for event-time windows.
        """
        self._allowed_lateness = time_ms
        return self

    def apply(self,
              window_function: WindowFunction, result_type: TypeInformation = None) -> DataStream:
        """
        Applies the given window function to each window. The window function is called for each
        evaluation of the window for each key individually. The output of the window function is
        interpreted as a regular non-windowed stream.

        Note that this function requires that all data in the windows is buffered until the window
        is evaluated, as the function provides no means of incremental aggregation.

        :param window_function: The window function.
        :param result_type: Type information for the result type of the window function.
        :return: The data stream that is the result of applying the window function to the window.
        """
        internal_window_function = InternalIterableWindowFunction(
            window_function)  # type: InternalWindowFunction
        return self._get_result_data_stream(internal_window_function, result_type)

    def process(self,
                process_window_function: ProcessWindowFunction,
                result_type: TypeInformation = None):
        """
        Applies the given window function to each window. The window function is called for each
        evaluation of the window for each key individually. The output of the window function is
        interpreted as a regular non-windowed stream.

        Note that this function requires that all data in the windows is buffered until the window
        is evaluated, as the function provides no means of incremental aggregation.

        :param process_window_function: The window function.
        :param result_type: Type information for the result type of the window function.
        :return: The data stream that is the result of applying the window function to the window.
        """
        internal_window_function = InternalIterableProcessWindowFunction(
            process_window_function)  # type: InternalWindowFunction
        return self._get_result_data_stream(internal_window_function, result_type)

    def _get_result_data_stream(
            self, internal_window_function: InternalWindowFunction, result_type):
        if self._window_trigger is None:
            self._window_trigger = self._window_assigner.get_default_trigger(
                self.get_execution_environment())
        window_serializer = self._window_assigner.get_window_serializer()
        window_state_descriptor = ListStateDescriptor(
            "window-contents", self.get_input_type())
        window_operation_descriptor = WindowOperationDescriptor(
            self._window_assigner,
            self._window_trigger,
            self._allowed_lateness,
            window_state_descriptor,
            window_serializer,
            internal_window_function)

        from pyflink.fn_execution import flink_fn_execution_pb2
        j_python_data_stream_function_operator, j_output_type_info = \
            _get_one_input_stream_operator(
                self._keyed_stream,
                window_operation_descriptor,
                flink_fn_execution_pb2.UserDefinedDataStreamFunction.WINDOW,  # type: ignore
                result_type)

        return DataStream(self._keyed_stream._j_data_stream.transform(
            "WINDOW",
            j_output_type_info,
            j_python_data_stream_function_operator))


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
               key_type: TypeInformation = None) -> 'ConnectedStreams':
        """
        KeyBy operation for connected data stream. Assigns keys to the elements of
        input1 and input2 using keySelector1 and keySelector2 with explicit type information
        for the common key type.

        :param key_selector1: The `KeySelector` used for grouping the first input.
        :param key_selector2: The `KeySelector` used for grouping the second input.
        :param key_type: The type information of the common key type
        :return: The partitioned `ConnectedStreams`
        """

        ds1 = self.stream1
        ds2 = self.stream2
        if isinstance(self.stream1, KeyedStream):
            ds1 = self.stream1._origin_stream
        if isinstance(self.stream2, KeyedStream):
            ds2 = self.stream2._origin_stream
        return ConnectedStreams(
            ds1.key_by(key_selector1, key_type),
            ds2.key_by(key_selector2, key_type))

    def map(self, func: CoMapFunction, output_type: TypeInformation = None) -> 'DataStream':
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

        if self._is_keyed_stream():
            class CoMapKeyedCoProcessFunctionAdapter(KeyedCoProcessFunction):
                def __init__(self, co_map_func: CoMapFunction):
                    self._open_func = co_map_func.open
                    self._close_func = co_map_func.close
                    self._map1_func = co_map_func.map1
                    self._map2_func = co_map_func.map2

                def open(self, runtime_context: RuntimeContext):
                    self._open_func(runtime_context)

                def close(self):
                    self._close_func()

                def process_element1(self, value, ctx: 'KeyedCoProcessFunction.Context'):
                    result = self._map1_func(value)
                    if result is not None:
                        yield result

                def process_element2(self, value, ctx: 'KeyedCoProcessFunction.Context'):
                    result = self._map2_func(value)
                    if result is not None:
                        yield result

            return self.process(CoMapKeyedCoProcessFunctionAdapter(func), output_type) \
                .name("Co-Map")
        else:
            class CoMapCoProcessFunctionAdapter(CoProcessFunction):
                def __init__(self, co_map_func: CoMapFunction):
                    self._open_func = co_map_func.open
                    self._close_func = co_map_func.close
                    self._map1_func = co_map_func.map1
                    self._map2_func = co_map_func.map2

                def open(self, runtime_context: RuntimeContext):
                    self._open_func(runtime_context)

                def close(self):
                    self._close_func()

                def process_element1(self, value, ctx: 'CoProcessFunction.Context'):
                    result = self._map1_func(value)
                    if result is not None:
                        yield result

                def process_element2(self, value, ctx: 'CoProcessFunction.Context'):
                    result = self._map2_func(value)
                    if result is not None:
                        yield result

            return self.process(CoMapCoProcessFunctionAdapter(func), output_type) \
                .name("Co-Map")

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

        if self._is_keyed_stream():
            class FlatMapKeyedCoProcessFunctionAdapter(KeyedCoProcessFunction):

                def __init__(self, co_flat_map_func: CoFlatMapFunction):
                    self._open_func = co_flat_map_func.open
                    self._close_func = co_flat_map_func.close
                    self._flat_map1_func = co_flat_map_func.flat_map1
                    self._flat_map2_func = co_flat_map_func.flat_map2

                def open(self, runtime_context: RuntimeContext):
                    self._open_func(runtime_context)

                def close(self):
                    self._close_func()

                def process_element1(self, value, ctx: 'KeyedCoProcessFunction.Context'):
                    result = self._flat_map1_func(value)
                    if result:
                        yield from result

                def process_element2(self, value, ctx: 'KeyedCoProcessFunction.Context'):
                    result = self._flat_map2_func(value)
                    if result:
                        yield from result

            return self.process(FlatMapKeyedCoProcessFunctionAdapter(func), output_type) \
                .name("Co-Flat Map")
        else:
            class FlatMapCoProcessFunctionAdapter(CoProcessFunction):

                def __init__(self, co_flat_map_func: CoFlatMapFunction):
                    self._open_func = co_flat_map_func.open
                    self._close_func = co_flat_map_func.close
                    self._flat_map1_func = co_flat_map_func.flat_map1
                    self._flat_map2_func = co_flat_map_func.flat_map2

                def open(self, runtime_context: RuntimeContext):
                    self._open_func(runtime_context)

                def close(self):
                    self._close_func()

                def process_element1(self, value, ctx: 'CoProcessFunction.Context'):
                    result = self._flat_map1_func(value)
                    if result:
                        yield from result

                def process_element2(self, value, ctx: 'CoProcessFunction.Context'):
                    result = self._flat_map2_func(value)
                    if result:
                        yield from result

            return self.process(FlatMapCoProcessFunctionAdapter(func), output_type) \
                .name("Co-Flat Map")

    def process(self,
                func: Union[CoProcessFunction, KeyedCoProcessFunction],
                output_type: TypeInformation = None) -> 'DataStream':
        if not isinstance(func, CoProcessFunction) and not isinstance(func, KeyedCoProcessFunction):
            raise TypeError("The input must be a CoProcessFunction or KeyedCoProcessFunction!")

        from pyflink.fn_execution.flink_fn_execution_pb2 import UserDefinedDataStreamFunction
        if self._is_keyed_stream():
            func_type = UserDefinedDataStreamFunction.KEYED_CO_PROCESS  # type: ignore
            func_name = "Keyed Co-Process"
        else:
            func_type = UserDefinedDataStreamFunction.CO_PROCESS  # type: ignore
            func_name = "Co-Process"

        j_connected_stream = self.stream1._j_data_stream.connect(self.stream2._j_data_stream)
        j_operator, j_output_type = _get_two_input_stream_operator(
            self,
            func,
            func_type,
            output_type)
        return DataStream(j_connected_stream.transform(func_name, j_output_type, j_operator))

    def _is_keyed_stream(self):
        return isinstance(self.stream1, KeyedStream) and isinstance(self.stream2, KeyedStream)


def _get_one_input_stream_operator(data_stream: DataStream,
                                   func: Union[Function,
                                               FunctionWrapper,
                                               WindowOperationDescriptor],
                                   func_type: int,
                                   output_type: Union[TypeInformation, List] = None):
    """
    Create a Java one input stream operator.

    :param func: a function object that implements the Function interface.
    :param func_type: function type, supports MAP, FLAT_MAP, etc.
    :param output_type: the data type of the function output data.
    :return: A Java operator which is responsible for execution user defined python function.
    """

    gateway = get_gateway()
    import cloudpickle
    serialized_func = cloudpickle.dumps(func)
    j_input_types = data_stream._j_data_stream.getTransformation().getOutputType()
    if output_type is None:
        output_type_info = Types.PICKLED_BYTE_ARRAY()  # type: TypeInformation
    elif isinstance(output_type, list):
        output_type_info = RowTypeInfo(output_type)
    else:
        output_type_info = output_type

    j_output_type_info = output_type_info.get_java_type_info()

    j_data_stream_python_function = gateway.jvm.DataStreamPythonFunction(
        bytearray(serialized_func),
        _get_python_env())
    j_data_stream_python_function_info = gateway.jvm.DataStreamPythonFunctionInfo(
        j_data_stream_python_function,
        func_type)

    j_conf = gateway.jvm.org.apache.flink.configuration.Configuration()

    from pyflink.fn_execution.flink_fn_execution_pb2 import UserDefinedDataStreamFunction
    if func_type == UserDefinedDataStreamFunction.PROCESS:  # type: ignore
        JDataStreamPythonFunctionOperator = gateway.jvm.PythonProcessOperator
    elif func_type == UserDefinedDataStreamFunction.KEYED_PROCESS:  # type: ignore
        JDataStreamPythonFunctionOperator = gateway.jvm.PythonKeyedProcessOperator
    elif func_type == UserDefinedDataStreamFunction.WINDOW:  # type: ignore
        window_serializer = typing.cast(WindowOperationDescriptor, func).window_serializer
        if isinstance(window_serializer, TimeWindowSerializer):
            j_namespace_serializer = \
                gateway.jvm.org.apache.flink.table.runtime.operators.window.TimeWindow.Serializer()
        elif isinstance(window_serializer, CountWindowSerializer):
            j_namespace_serializer = \
                gateway.jvm.org.apache.flink.table.runtime.operators.window.CountWindow.Serializer()
        else:
            j_namespace_serializer = \
                gateway.jvm.org.apache.flink.streaming.api.utils.ByteArrayWrapperSerializer()
        j_python_function_operator = gateway.jvm.PythonKeyedProcessOperator(
            j_conf,
            j_data_stream_python_function_info,
            j_input_types,
            j_output_type_info,
            j_namespace_serializer)
        return j_python_function_operator, j_output_type_info
    else:
        raise TypeError("Unsupported function type: %s" % func_type)

    j_python_function_operator = JDataStreamPythonFunctionOperator(
        j_conf,
        j_data_stream_python_function_info,
        j_input_types,
        j_output_type_info)

    return j_python_function_operator, j_output_type_info


def _get_two_input_stream_operator(connected_streams: ConnectedStreams,
                                   func: Union[Function, FunctionWrapper],
                                   func_type: int,
                                   type_info: TypeInformation):
    """
    Create a Java two input stream operator.

    :param func: a function object that implements the Function interface.
    :param func_type: function type, supports MAP, FLAT_MAP, etc.
    :param type_info: the data type of the function output data.
    :return: A Java operator which is responsible for execution user defined python function.
    """

    gateway = get_gateway()
    import cloudpickle
    serialized_func = cloudpickle.dumps(func)

    j_input_types1 = connected_streams.stream1._j_data_stream.getTransformation().getOutputType()
    j_input_types2 = connected_streams.stream2._j_data_stream.getTransformation().getOutputType()

    if type_info is None:
        output_type_info = Types.PICKLED_BYTE_ARRAY()  # type: TypeInformation
    elif isinstance(type_info, list):
        output_type_info = RowTypeInfo(type_info)
    else:
        output_type_info = type_info

    j_output_type_info = output_type_info.get_java_type_info()

    j_data_stream_python_function = gateway.jvm.DataStreamPythonFunction(
        bytearray(serialized_func),
        _get_python_env())
    j_data_stream_python_function_info = gateway.jvm.DataStreamPythonFunctionInfo(
        j_data_stream_python_function,
        func_type)

    from pyflink.fn_execution.flink_fn_execution_pb2 import UserDefinedDataStreamFunction
    if func_type == UserDefinedDataStreamFunction.CO_PROCESS:  # type: ignore
        JTwoInputPythonFunctionOperator = gateway.jvm.PythonCoProcessOperator
    elif func_type == UserDefinedDataStreamFunction.KEYED_CO_PROCESS:  # type: ignore
        JTwoInputPythonFunctionOperator = gateway.jvm.PythonKeyedCoProcessOperator
    else:
        raise TypeError("Unsupported function type: %s" % func_type)

    j_conf = gateway.jvm.org.apache.flink.configuration.Configuration()

    j_python_data_stream_function_operator = JTwoInputPythonFunctionOperator(
        j_conf,
        j_data_stream_python_function_info,
        j_input_types1,
        j_input_types2,
        j_output_type_info)

    return j_python_data_stream_function_operator, j_output_type_info


class CloseableIterator(object):
    """
    Representing an Iterator that is also auto closeable.
    """

    def __init__(self, j_closeable_iterator, type_info: TypeInformation = None):
        self._j_closeable_iterator = j_closeable_iterator
        self._type_info = type_info

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def next(self):
        if not self._j_closeable_iterator.hasNext():
            raise StopIteration('No more data.')
        return convert_to_python_obj(self._j_closeable_iterator.next(), self._type_info)

    def close(self):
        self._j_closeable_iterator.close()
