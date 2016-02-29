/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.datastream;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.SocketClientSink;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamFilter;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.streaming.api.transformations.UnionTransformation;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.ExtractTimestampsOperator;
import org.apache.flink.streaming.runtime.operators.TimestampsAndPeriodicWatermarksOperator;
import org.apache.flink.streaming.runtime.operators.TimestampsAndPunctuatedWatermarksOperator;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.runtime.partitioner.CustomPartitionerWrapper;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.GlobalPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ShufflePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.util.keys.KeySelectorUtil;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

import com.google.common.base.Preconditions;

/**
 * A DataStream represents a stream of elements of the same type. A DataStream
 * can be transformed into another DataStream by applying a transformation as
 * for example:
 * <ul>
 * <li>{@link DataStream#map},
 * <li>{@link DataStream#filter}, or
 * </ul>
 *
 * @param <T> The type of the elements in this Stream
 */
@Public
public class DataStream<T> {

	protected final StreamExecutionEnvironment environment;

	protected final StreamTransformation<T> transformation;

	/**
	 * Create a new {@link DataStream} in the given execution environment with
	 * partitioning set to forward by default.
	 *
	 * @param environment The StreamExecutionEnvironment
	 */
	public DataStream(StreamExecutionEnvironment environment, StreamTransformation<T> transformation) {
		this.environment = Preconditions.checkNotNull(environment, "Execution Environment must not be null.");
		this.transformation = Preconditions.checkNotNull(transformation, "Stream Transformation must not be null.");
	}

	/**
	 * Returns the ID of the {@link DataStream} in the current {@link StreamExecutionEnvironment}.
	 *
	 * @return ID of the DataStream
	 */
	@Internal
	public int getId() {
		return transformation.getId();
	}

	/**
	 * Gets the parallelism for this operator.
	 *
	 * @return The parallelism set for this operator.
	 */
	public int getParallelism() {
		return transformation.getParallelism();
	}

	/**
	 * Gets the type of the stream.
	 *
	 * @return The type of the datastream.
	 */
	public TypeInformation<T> getType() {
		return transformation.getOutputType();
	}

	/**
	 * Invokes the {@link org.apache.flink.api.java.ClosureCleaner}
	 * on the given function if closure cleaning is enabled in the {@link ExecutionConfig}.
	 *
	 * @return The cleaned Function
	 */
	protected <F> F clean(F f) {
		return getExecutionEnvironment().clean(f);
	}

	/**
	 * Returns the {@link StreamExecutionEnvironment} that was used to create this
	 * {@link DataStream}
	 *
	 * @return The Execution Environment
	 */
	public StreamExecutionEnvironment getExecutionEnvironment() {
		return environment;
	}

	public ExecutionConfig getExecutionConfig() {
		return environment.getConfig();
	}

	/**
	 * Creates a new {@link DataStream} by merging {@link DataStream} outputs of
	 * the same type with each other. The DataStreams merged using this operator
	 * will be transformed simultaneously.
	 *
	 * @param streams
	 *            The DataStreams to union output with.
	 * @return The {@link DataStream}.
	 */
	@SafeVarargs
	public final DataStream<T> union(DataStream<T>... streams) {
		List<StreamTransformation<T>> unionedTransforms = new ArrayList<>();
		unionedTransforms.add(this.transformation);

		for (DataStream<T> newStream : streams) {
			if (!getType().equals(newStream.getType())) {
				throw new IllegalArgumentException("Cannot union streams of different types: "
						+ getType() + " and " + newStream.getType());
			}
			
			unionedTransforms.add(newStream.getTransformation());
		}
		return new DataStream<>(this.environment, new UnionTransformation<>(unionedTransforms));
	}

	/**
	 * Operator used for directing tuples to specific named outputs using an
	 * {@link org.apache.flink.streaming.api.collector.selector.OutputSelector}.
	 * Calling this method on an operator creates a new {@link SplitStream}.
	 *
	 * @param outputSelector
	 *            The user defined
	 *            {@link org.apache.flink.streaming.api.collector.selector.OutputSelector}
	 *            for directing the tuples.
	 * @return The {@link SplitStream}
	 */
	public SplitStream<T> split(OutputSelector<T> outputSelector) {
		return new SplitStream<>(this, clean(outputSelector));
	}

	/**
	 * Creates a new {@link ConnectedStreams} by connecting
	 * {@link DataStream} outputs of (possible) different types with each other.
	 * The DataStreams connected using this operator can be used with
	 * CoFunctions to apply joint transformations.
	 *
	 * @param dataStream
	 *            The DataStream with which this stream will be connected.
	 * @return The {@link ConnectedStreams}.
	 */
	public <R> ConnectedStreams<T, R> connect(DataStream<R> dataStream) {
		return new ConnectedStreams<>(environment, this, dataStream);
	}

	/**
	 *
	 * It creates a new {@link KeyedStream} that uses the provided key for partitioning
	 * its operator states. 
	 *
	 * @param key
	 *            The KeySelector to be used for extracting the key for partitioning
	 * @return The {@link DataStream} with partitioned state (i.e. KeyedStream)
	 */
	public <K> KeyedStream<T, K> keyBy(KeySelector<T, K> key) {
		return new KeyedStream<>(this, clean(key));
	}

	/**
	 * Partitions the operator state of a {@link DataStream} by the given key positions. 
	 *
	 * @param fields
	 *            The position of the fields on which the {@link DataStream}
	 *            will be grouped.
	 * @return The {@link DataStream} with partitioned state (i.e. KeyedStream)
	 */
	public KeyedStream<T, Tuple> keyBy(int... fields) {
		if (getType() instanceof BasicArrayTypeInfo || getType() instanceof PrimitiveArrayTypeInfo) {
			return keyBy(KeySelectorUtil.getSelectorForArray(fields, getType()));
		} else {
			return keyBy(new Keys.ExpressionKeys<>(fields, getType()));
		}
	}

	/**
	 * Partitions the operator state of a {@link DataStream}using field expressions. 
	 * A field expression is either the name of a public field or a getter method with parentheses
	 * of the {@link DataStream}S underlying type. A dot can be used to drill
	 * down into objects, as in {@code "field1.getInnerField2()" }.
	 *
	 * @param fields
	 *            One or more field expressions on which the state of the {@link DataStream} operators will be
	 *            partitioned.
	 * @return The {@link DataStream} with partitioned state (i.e. KeyedStream)
	 **/
	public KeyedStream<T, Tuple> keyBy(String... fields) {
		return keyBy(new Keys.ExpressionKeys<>(fields, getType()));
	}

	private KeyedStream<T, Tuple> keyBy(Keys<T> keys) {
		return new KeyedStream<>(this, clean(KeySelectorUtil.getSelectorForKeys(keys,
				getType(), getExecutionConfig())));
	}

	/**
	 * Partitions a tuple DataStream on the specified key fields using a custom partitioner.
	 * This method takes the key position to partition on, and a partitioner that accepts the key type.
	 * <p>
	 * Note: This method works only on single field keys.
	 *
	 * @param partitioner The partitioner to assign partitions to keys.
	 * @param field The field index on which the DataStream is to partitioned.
	 * @return The partitioned DataStream.
	 */
	public <K> DataStream<T> partitionCustom(Partitioner<K> partitioner, int field) {
		Keys.ExpressionKeys<T> outExpressionKeys = new Keys.ExpressionKeys<>(new int[]{field}, getType());
		return partitionCustom(partitioner, outExpressionKeys);
	}

	/**
	 * Partitions a POJO DataStream on the specified key fields using a custom partitioner.
	 * This method takes the key expression to partition on, and a partitioner that accepts the key type.
	 * <p>
	 * Note: This method works only on single field keys.
	 *
	 * @param partitioner The partitioner to assign partitions to keys.
	 * @param field The expression for the field on which the DataStream is to partitioned.
	 * @return The partitioned DataStream.
	 */
	public <K> DataStream<T> partitionCustom(Partitioner<K> partitioner, String field) {
		Keys.ExpressionKeys<T> outExpressionKeys = new Keys.ExpressionKeys<>(new String[]{field}, getType());
		return partitionCustom(partitioner, outExpressionKeys);
	}


	/**
	 * Partitions a DataStream on the key returned by the selector, using a custom partitioner.
	 * This method takes the key selector to get the key to partition on, and a partitioner that
	 * accepts the key type.
	 * <p>
	 * Note: This method works only on single field keys, i.e. the selector cannot return tuples
	 * of fields.
	 *
	 * @param partitioner
	 * 		The partitioner to assign partitions to keys.
	 * @param keySelector
	 * 		The KeySelector with which the DataStream is partitioned.
	 * @return The partitioned DataStream.
	 * @see KeySelector
	 */
	public <K> DataStream<T> partitionCustom(Partitioner<K> partitioner, KeySelector<T, K> keySelector) {
		return setConnectionType(new CustomPartitionerWrapper<>(clean(partitioner),
				clean(keySelector)));
	}

	//	private helper method for custom partitioning
	private <K> DataStream<T> partitionCustom(Partitioner<K> partitioner, Keys<T> keys) {
		KeySelector<T, K> keySelector = KeySelectorUtil.getSelectorForOneKey(keys, partitioner, getType(), getExecutionConfig());

		return setConnectionType(
				new CustomPartitionerWrapper<>(
						clean(partitioner),
						clean(keySelector)));
	}

	/**
	 * Sets the partitioning of the {@link DataStream} so that the output elements
	 * are broadcasted to every parallel instance of the next operation.
	 *
	 * @return The DataStream with broadcast partitioning set.
	 */
	public DataStream<T> broadcast() {
		return setConnectionType(new BroadcastPartitioner<T>());
	}

	/**
	 * Sets the partitioning of the {@link DataStream} so that the output elements
	 * are shuffled uniformly randomly to the next operation.
	 *
	 * @return The DataStream with shuffle partitioning set.
	 */
	@PublicEvolving
	public DataStream<T> shuffle() {
		return setConnectionType(new ShufflePartitioner<T>());
	}

	/**
	 * Sets the partitioning of the {@link DataStream} so that the output elements
	 * are forwarded to the local subtask of the next operation.
	 *
	 * @return The DataStream with forward partitioning set.
	 */
	public DataStream<T> forward() {
		return setConnectionType(new ForwardPartitioner<T>());
	}

	/**
	 * Sets the partitioning of the {@link DataStream} so that the output elements
	 * are distributed evenly to instances of the next operation in a round-robin
	 * fashion.
	 *
	 * @return The DataStream with rebalance partitioning set.
	 */
	public DataStream<T> rebalance() {
		return setConnectionType(new RebalancePartitioner<T>());
	}

	/**
	 * Sets the partitioning of the {@link DataStream} so that the output elements
	 * are distributed evenly to a subset of instances of the next operation in a round-robin
	 * fashion.
	 *
	 * <p>The subset of downstream operations to which the upstream operation sends
	 * elements depends on the degree of parallelism of both the upstream and downstream operation.
	 * For example, if the upstream operation has parallelism 2 and the downstream operation
	 * has parallelism 4, then one upstream operation would distribute elements to two
	 * downstream operations while the other upstream operation would distribute to the other
	 * two downstream operations. If, on the other hand, the downstream operation has parallelism
	 * 2 while the upstream operation has parallelism 4 then two upstream operations will
	 * distribute to one downstream operation while the other two upstream operations will
	 * distribute to the other downstream operations.
	 *
	 * <p>In cases where the different parallelisms are not multiples of each other one or several
	 * downstream operations will have a differing number of inputs from upstream operations.
	 *
	 * @return The DataStream with rescale partitioning set.
	 */
	@PublicEvolving
	public DataStream<T> rescale() {
		return setConnectionType(new RescalePartitioner<T>());
	}

	/**
	 * Sets the partitioning of the {@link DataStream} so that the output values
	 * all go to the first instance of the next processing operator. Use this
	 * setting with care since it might cause a serious performance bottleneck
	 * in the application.
	 *
	 * @return The DataStream with shuffle partitioning set.
	 */
	@PublicEvolving
	public DataStream<T> global() {
		return setConnectionType(new GlobalPartitioner<T>());
	}

	/**
	 * Initiates an iterative part of the program that feeds back data streams.
	 * The iterative part needs to be closed by calling
	 * {@link IterativeStream#closeWith(DataStream)}. The transformation of
	 * this IterativeStream will be the iteration head. The data stream
	 * given to the {@link IterativeStream#closeWith(DataStream)} method is
	 * the data stream that will be fed back and used as the input for the
	 * iteration head. The user can also use different feedback type than the
	 * input of the iteration and treat the input and feedback streams as a
	 * {@link ConnectedStreams} be calling
	 * {@link IterativeStream#withFeedbackType(TypeInformation)}
	 * <p>
	 * A common usage pattern for streaming iterations is to use output
	 * splitting to send a part of the closing data stream to the head. Refer to
	 * {@link #split(OutputSelector)} for more information.
	 * <p>
	 * The iteration edge will be partitioned the same way as the first input of
	 * the iteration head unless it is changed in the
	 * {@link IterativeStream#closeWith(DataStream)} call.
	 * <p>
	 * By default a DataStream with iteration will never terminate, but the user
	 * can use the maxWaitTime parameter to set a max waiting time for the
	 * iteration head. If no data received in the set time, the stream
	 * terminates.
	 *
	 * @return The iterative data stream created.
	 */
	@PublicEvolving
	public IterativeStream<T> iterate() {
		return new IterativeStream<>(this, 0);
	}

	/**
	 * Initiates an iterative part of the program that feeds back data streams.
	 * The iterative part needs to be closed by calling
	 * {@link IterativeStream#closeWith(DataStream)}. The transformation of
	 * this IterativeStream will be the iteration head. The data stream
	 * given to the {@link IterativeStream#closeWith(DataStream)} method is
	 * the data stream that will be fed back and used as the input for the
	 * iteration head. The user can also use different feedback type than the
	 * input of the iteration and treat the input and feedback streams as a
	 * {@link ConnectedStreams} be calling
	 * {@link IterativeStream#withFeedbackType(TypeInformation)}
	 * <p>
	 * A common usage pattern for streaming iterations is to use output
	 * splitting to send a part of the closing data stream to the head. Refer to
	 * {@link #split(OutputSelector)} for more information.
	 * <p>
	 * The iteration edge will be partitioned the same way as the first input of
	 * the iteration head unless it is changed in the
	 * {@link IterativeStream#closeWith(DataStream)} call.
	 * <p>
	 * By default a DataStream with iteration will never terminate, but the user
	 * can use the maxWaitTime parameter to set a max waiting time for the
	 * iteration head. If no data received in the set time, the stream
	 * terminates.
	 *
	 * @param maxWaitTimeMillis
	 *            Number of milliseconds to wait between inputs before shutting
	 *            down
	 *
	 * @return The iterative data stream created.
	 */
	@PublicEvolving
	public IterativeStream<T> iterate(long maxWaitTimeMillis) {
		return new IterativeStream<>(this, maxWaitTimeMillis);
	}

	/**
	 * Applies a Map transformation on a {@link DataStream}. The transformation
	 * calls a {@link MapFunction} for each element of the DataStream. Each
	 * MapFunction call returns exactly one element. The user can also extend
	 * {@link RichMapFunction} to gain access to other features provided by the
	 * {@link org.apache.flink.api.common.functions.RichFunction} interface.
	 *
	 * @param mapper
	 *            The MapFunction that is called for each element of the
	 *            DataStream.
	 * @param <R>
	 *            output type
	 * @return The transformed {@link DataStream}.
	 */
	public <R> SingleOutputStreamOperator<R, ?> map(MapFunction<T, R> mapper) {

		TypeInformation<R> outType = TypeExtractor.getMapReturnTypes(clean(mapper), getType(),
				Utils.getCallLocationName(), true);

		return transform("Map", outType, new StreamMap<>(clean(mapper)));
	}

	/**
	 * Applies a FlatMap transformation on a {@link DataStream}. The
	 * transformation calls a {@link FlatMapFunction} for each element of the
	 * DataStream. Each FlatMapFunction call can return any number of elements
	 * including none. The user can also extend {@link RichFlatMapFunction} to
	 * gain access to other features provided by the
	 * {@link org.apache.flink.api.common.functions.RichFunction} interface.
	 *
	 * @param flatMapper
	 *            The FlatMapFunction that is called for each element of the
	 *            DataStream
	 *
	 * @param <R>
	 *            output type
	 * @return The transformed {@link DataStream}.
	 */
	public <R> SingleOutputStreamOperator<R, ?> flatMap(FlatMapFunction<T, R> flatMapper) {

		TypeInformation<R> outType = TypeExtractor.getFlatMapReturnTypes(clean(flatMapper),
				getType(), Utils.getCallLocationName(), true);

		return transform("Flat Map", outType, new StreamFlatMap<>(clean(flatMapper)));

	}

	/**
	 * Applies a Filter transformation on a {@link DataStream}. The
	 * transformation calls a {@link FilterFunction} for each element of the
	 * DataStream and retains only those element for which the function returns
	 * true. Elements for which the function returns false are filtered. The
	 * user can also extend {@link RichFilterFunction} to gain access to other
	 * features provided by the
	 * {@link org.apache.flink.api.common.functions.RichFunction} interface.
	 *
	 * @param filter
	 *            The FilterFunction that is called for each element of the
	 *            DataStream.
	 * @return The filtered DataStream.
	 */
	public SingleOutputStreamOperator<T, ?> filter(FilterFunction<T> filter) {
		return transform("Filter", getType(), new StreamFilter<>(clean(filter)));

	}

	/**
	 * Initiates a Project transformation on a {@link Tuple} {@link DataStream}.<br>
	 * <b>Note: Only Tuple DataStreams can be projected.</b>
	 *
	 * <p>
	 * The transformation projects each Tuple of the DataSet onto a (sub)set of
	 * fields.
	 *
	 * @param fieldIndexes
	 *            The field indexes of the input tuples that are retained. The
	 *            order of fields in the output tuple corresponds to the order
	 *            of field indexes.
	 * @return The projected DataStream
	 *
	 * @see Tuple
	 * @see DataStream
	 */
	@PublicEvolving
	public <R extends Tuple> SingleOutputStreamOperator<R, ?> project(int... fieldIndexes) {
		return new StreamProjection<>(this, fieldIndexes).projectTupleX();
	}

	/**
	 * Creates a join operation. See {@link CoGroupedStreams} for an example of how the keys
	 * and window can be specified.
	 */
	public <T2> CoGroupedStreams<T, T2> coGroup(DataStream<T2> otherStream) {
		return new CoGroupedStreams<>(this, otherStream);
	}

	/**
	 * Creates a join operation. See {@link JoinedStreams} for an example of how the keys
	 * and window can be specified.
	 */
	public <T2> JoinedStreams<T, T2> join(DataStream<T2> otherStream) {
		return new JoinedStreams<>(this, otherStream);
	}

	/**
	 * Windows this {@code DataStream} into tumbling time windows.
	 *
	 * <p>
	 * This is a shortcut for either {@code .window(TumblingEventTimeWindows.of(size))} or
	 * {@code .window(TumblingProcessingTimeWindows.of(size))} depending on the time characteristic
	 * set using
	 *
	 * <p>
	 * Note: This operation can be inherently non-parallel since all elements have to pass through
	 * the same operator instance. (Only for special cases, such as aligned time windows is
	 * it possible to perform this operation in parallel).
	 *
	 * {@link org.apache.flink.streaming.api.environment.StreamExecutionEnvironment#setStreamTimeCharacteristic(org.apache.flink.streaming.api.TimeCharacteristic)}
	 *
	 * @param size The size of the window.
	 */
	public AllWindowedStream<T, TimeWindow> timeWindowAll(Time size) {
		if (environment.getStreamTimeCharacteristic() == TimeCharacteristic.ProcessingTime) {
			return windowAll(TumblingProcessingTimeWindows.of(size));
		} else {
			return windowAll(TumblingEventTimeWindows.of(size));
		}
	}

	/**
	 * Windows this {@code DataStream} into sliding time windows.
	 *
	 * <p>
	 * This is a shortcut for either {@code .window(SlidingEventTimeWindows.of(size, slide))} or
	 * {@code .window(SlidingProcessingTimeWindows.of(size, slide))} depending on the time characteristic
	 * set using
	 * {@link org.apache.flink.streaming.api.environment.StreamExecutionEnvironment#setStreamTimeCharacteristic(org.apache.flink.streaming.api.TimeCharacteristic)}
	 *
	 * <p>
	 * Note: This operation can be inherently non-parallel since all elements have to pass through
	 * the same operator instance. (Only for special cases, such as aligned time windows is
	 * it possible to perform this operation in parallel).
	 *
	 * @param size The size of the window.
	 */
	public AllWindowedStream<T, TimeWindow> timeWindowAll(Time size, Time slide) {
		if (environment.getStreamTimeCharacteristic() == TimeCharacteristic.ProcessingTime) {
			return windowAll(SlidingProcessingTimeWindows.of(size, slide));
		} else {
			return windowAll(SlidingEventTimeWindows.of(size, slide));
		}
	}

	/**
	 * Windows this {@code DataStream} into tumbling count windows.
	 *
	 * <p>
	 * Note: This operation can be inherently non-parallel since all elements have to pass through
	 * the same operator instance. (Only for special cases, such as aligned time windows is
	 * it possible to perform this operation in parallel).
	 *
	 * @param size The size of the windows in number of elements.
	 */
	public AllWindowedStream<T, GlobalWindow> countWindowAll(long size) {
		return windowAll(GlobalWindows.create()).trigger(PurgingTrigger.of(CountTrigger.of(size)));
	}

	/**
	 * Windows this {@code DataStream} into sliding count windows.
	 *
	 * <p>
	 * Note: This operation can be inherently non-parallel since all elements have to pass through
	 * the same operator instance. (Only for special cases, such as aligned time windows is
	 * it possible to perform this operation in parallel).
	 *
	 * @param size The size of the windows in number of elements.
	 * @param slide The slide interval in number of elements.
	 */
	public AllWindowedStream<T, GlobalWindow> countWindowAll(long size, long slide) {
		return windowAll(GlobalWindows.create())
				.evictor(CountEvictor.of(size))
				.trigger(CountTrigger.of(slide));
	}

	/**
	 * Windows this data stream to a {@code KeyedTriggerWindowDataStream}, which evaluates windows
	 * over a key grouped stream. Elements are put into windows by a
	 * {@link org.apache.flink.streaming.api.windowing.assigners.WindowAssigner}. The grouping of
	 * elements is done both by key and by window.
	 *
	 * <p>
	 * A {@link org.apache.flink.streaming.api.windowing.triggers.Trigger} can be defined to specify
	 * when windows are evaluated. However, {@code WindowAssigners} have a default {@code Trigger}
	 * that is used if a {@code Trigger} is not specified.
	 *
	 * <p>
	 * Note: This operation can be inherently non-parallel since all elements have to pass through
	 * the same operator instance. (Only for special cases, such as aligned time windows is
	 * it possible to perform this operation in parallel).
	 *
	 * @param assigner The {@code WindowAssigner} that assigns elements to windows.
	 * @return The trigger windows data stream.
	 */
	@PublicEvolving
	public <W extends Window> AllWindowedStream<T, W> windowAll(WindowAssigner<? super T, W> assigner) {
		return new AllWindowedStream<>(this, assigner);
	}

	// ------------------------------------------------------------------------
	//  Timestamps and watermarks
	// ------------------------------------------------------------------------
	
	/**
	 * Extracts a timestamp from an element and assigns it as the internal timestamp of that element.
	 * The internal timestamps are, for example, used to to event-time window operations.
	 *
	 * <p>
	 * If you know that the timestamps are strictly increasing you can use an
	 * {@link org.apache.flink.streaming.api.functions.AscendingTimestampExtractor}. Otherwise,
	 * you should provide a {@link TimestampExtractor} that also implements
	 * {@link TimestampExtractor#getCurrentWatermark()} to keep track of watermarks.
	 *
	 * @param extractor The TimestampExtractor that is called for each element of the DataStream.
	 * 
	 * @deprecated Please use {@link #assignTimestampsAndWatermarks(AssignerWithPeriodicWatermarks)}
	 *             of {@link #assignTimestampsAndWatermarks(AssignerWithPunctuatedWatermarks)}
	 *             instread.
	 * @see #assignTimestampsAndWatermarks(AssignerWithPeriodicWatermarks)
	 * @see #assignTimestampsAndWatermarks(AssignerWithPunctuatedWatermarks)
	 */
	@Deprecated
	public SingleOutputStreamOperator<T, ?> assignTimestamps(TimestampExtractor<T> extractor) {
		// match parallelism to input, otherwise dop=1 sources could lead to some strange
		// behaviour: the watermark will creep along very slowly because the elements
		// from the source go to each extraction operator round robin.
		int inputParallelism = getTransformation().getParallelism();
		ExtractTimestampsOperator<T> operator = new ExtractTimestampsOperator<>(clean(extractor));
		return transform("ExtractTimestamps", getTransformation().getOutputType(), operator)
				.setParallelism(inputParallelism);
	}

	/**
	 * Assigns timestamps to the elements in the data stream and periodically creates
	 * watermarks to signal event time progress.
	 * 
	 * <p>This method creates watermarks periodically (for example every second), based
	 * on the watermarks indicated by the given watermark generator. Even when no new elements
	 * in the stream arrive, the given watermark generator will be periodically checked for
	 * new watermarks. The interval in which watermarks are generated is defined in
	 * {@link ExecutionConfig#setAutoWatermarkInterval(long)}.
	 * 
	 * <p>Use this method for the common cases, where some characteristic over all elements
	 * should generate the watermarks, or where watermarks are simply trailing behind the
	 * wall clock time by a certain amount.
	 * 
	 * <p>For cases where watermarks should be created in an irregular fashion, for example
	 * based on certain markers that some element carry, use the
	 * {@link AssignerWithPunctuatedWatermarks}.
	 * 
	 * @param timestampAndWatermarkAssigner The implementation of the timestamp assigner and
	 *                                      watermark generator.   
	 * @return The stream after the transformation, with assigned timestamps and watermarks.
	 * 
	 * @see AssignerWithPeriodicWatermarks
	 * @see AssignerWithPunctuatedWatermarks
	 * @see #assignTimestampsAndWatermarks(AssignerWithPunctuatedWatermarks) 
	 */
	public SingleOutputStreamOperator<T, ?> assignTimestampsAndWatermarks(
			AssignerWithPeriodicWatermarks<T> timestampAndWatermarkAssigner) {
		
		// match parallelism to input, otherwise dop=1 sources could lead to some strange
		// behaviour: the watermark will creep along very slowly because the elements
		// from the source go to each extraction operator round robin.
		final int inputParallelism = getTransformation().getParallelism();
		final AssignerWithPeriodicWatermarks<T> cleanedAssigner = clean(timestampAndWatermarkAssigner);
		
		TimestampsAndPeriodicWatermarksOperator<T> operator = 
				new TimestampsAndPeriodicWatermarksOperator<>(cleanedAssigner);
		
		return transform("Timestamps/Watermarks", getTransformation().getOutputType(), operator)
				.setParallelism(inputParallelism);
	}

	/**
	 * Assigns timestamps to the elements in the data stream and periodically creates
	 * watermarks to signal event time progress.
	 *
	 * <p>This method creates watermarks based purely on stream elements. For each element
	 * that is handled via {@link AssignerWithPunctuatedWatermarks#extractTimestamp(Object, long)},
	 * the {@link AssignerWithPunctuatedWatermarks#checkAndGetNextWatermark(Object, long)}
	 * method is called, and a new watermark is emitted, if the returned watermark value is
	 * non-negative and greater than the previous watermark.
	 *
	 * <p>This method is useful when the data stream embeds watermark elements, or certain elements
	 * carry a marker that can be used to determine the current event time watermark. 
	 * This operation gives the programmer full control over the watermark generation. Users
	 * should be aware that too aggressive watermark generation (i.e., generating hundreds of
	 * watermarks every second) can cost some performance.
	 *
	 * <p>For cases where watermarks should be created in a regular fashion, for example
	 * every x milliseconds, use the {@link AssignerWithPeriodicWatermarks}.
	 *
	 * @param timestampAndWatermarkAssigner The implementation of the timestamp assigner and
	 *                                      watermark generator.   
	 * @return The stream after the transformation, with assigned timestamps and watermarks.
	 *
	 * @see AssignerWithPunctuatedWatermarks
	 * @see AssignerWithPeriodicWatermarks
	 * @see #assignTimestampsAndWatermarks(AssignerWithPeriodicWatermarks)
	 */
	public SingleOutputStreamOperator<T, ?> assignTimestampsAndWatermarks(
			AssignerWithPunctuatedWatermarks<T> timestampAndWatermarkAssigner) {
		
		// match parallelism to input, otherwise dop=1 sources could lead to some strange
		// behaviour: the watermark will creep along very slowly because the elements
		// from the source go to each extraction operator round robin.
		final int inputParallelism = getTransformation().getParallelism();
		final AssignerWithPunctuatedWatermarks<T> cleanedAssigner = clean(timestampAndWatermarkAssigner);

		TimestampsAndPunctuatedWatermarksOperator<T> operator = 
				new TimestampsAndPunctuatedWatermarksOperator<>(cleanedAssigner);
		
		return transform("Timestamps/Watermarks", getTransformation().getOutputType(), operator)
				.setParallelism(inputParallelism);
	}

	// ------------------------------------------------------------------------
	//  Data sinks
	// ------------------------------------------------------------------------
	
	/**
	 * Writes a DataStream to the standard output stream (stdout).
	 *
	 * <p>
	 * For each element of the DataStream the result of
	 * {@link Object#toString()} is written.
	 *
	 * @return The closed DataStream.
	 */
	@PublicEvolving
	public DataStreamSink<T> print() {
		PrintSinkFunction<T> printFunction = new PrintSinkFunction<>();
		return addSink(printFunction);
	}

	/**
	 * Writes a DataStream to the standard output stream (stderr).
	 *
	 * <p>
	 * For each element of the DataStream the result of
	 * {@link Object#toString()} is written.
	 *
	 * @return The closed DataStream.
	 */
	@PublicEvolving
	public DataStreamSink<T> printToErr() {
		PrintSinkFunction<T> printFunction = new PrintSinkFunction<>(true);
		return addSink(printFunction);
	}

	/**
	 * Writes a DataStream to the file specified by path in text format.
	 *
	 * <p>
	 * For every element of the DataStream the result of {@link Object#toString()}
	 * is written.
	 *
	 * @param path
	 *            The path pointing to the location the text file is written to.
	 *
	 * @return The closed DataStream.
	 */
	@PublicEvolving
	public DataStreamSink<T> writeAsText(String path) {
		return writeUsingOutputFormat(new TextOutputFormat<T>(new Path(path)));
	}


	/**
	 * Writes a DataStream to the file specified by path in text format.
	 *
	 * <p>
	 * For every element of the DataStream the result of {@link Object#toString()}
	 * is written.
	 *
	 * @param path
	 *            The path pointing to the location the text file is written to
	 * @param writeMode
	 *            Controls the behavior for existing files. Options are
	 *            NO_OVERWRITE and OVERWRITE.
	 *
	 * @return The closed DataStream.
	 */
	@PublicEvolving
	public DataStreamSink<T> writeAsText(String path, WriteMode writeMode) {
		TextOutputFormat<T> tof = new TextOutputFormat<>(new Path(path));
		tof.setWriteMode(writeMode);
		return writeUsingOutputFormat(tof);
	}


	/**
	 * Writes a DataStream to the file specified by the path parameter.
	 *
	 * <p>
	 * For every field of an element of the DataStream the result of {@link Object#toString()}
	 * is written. This method can only be used on data streams of tuples.
	 *
	 * @param path
	 *            the path pointing to the location the text file is written to
	 *
	 * @return the closed DataStream
	 */
	@PublicEvolving
	public DataStreamSink<T> writeAsCsv(String path) {
		return writeAsCsv(path, null, CsvOutputFormat.DEFAULT_LINE_DELIMITER, CsvOutputFormat.DEFAULT_FIELD_DELIMITER);
	}


	/**
	 * Writes a DataStream to the file specified by the path parameter.
	 *
	 * <p>
	 * For every field of an element of the DataStream the result of {@link Object#toString()}
	 * is written. This method can only be used on data streams of tuples.
	 *
	 * @param path
	 *            the path pointing to the location the text file is written to
	 * @param writeMode
	 *            Controls the behavior for existing files. Options are
	 *            NO_OVERWRITE and OVERWRITE.
	 *
	 * @return the closed DataStream
	 */
	@PublicEvolving
	public DataStreamSink<T> writeAsCsv(String path, WriteMode writeMode) {
		return writeAsCsv(path, writeMode, CsvOutputFormat.DEFAULT_LINE_DELIMITER, CsvOutputFormat.DEFAULT_FIELD_DELIMITER);
	}

	/**
	 * Writes a DataStream to the file specified by the path parameter. The
	 * writing is performed periodically every millis milliseconds.
	 *
	 * <p>
	 * For every field of an element of the DataStream the result of {@link Object#toString()}
	 * is written. This method can only be used on data streams of tuples.
	 *
	 * @param path
	 *            the path pointing to the location the text file is written to
	 * @param writeMode
	 *            Controls the behavior for existing files. Options are
	 *            NO_OVERWRITE and OVERWRITE.
	 * @param rowDelimiter
	 *            the delimiter for two rows
	 * @param fieldDelimiter
	 *            the delimiter for two fields
	 *
	 * @return the closed DataStream
	 */
	@SuppressWarnings("unchecked")
	@PublicEvolving
	public <X extends Tuple> DataStreamSink<T> writeAsCsv(
			String path,
			WriteMode writeMode,
			String rowDelimiter,
			String fieldDelimiter) {
		Preconditions.checkArgument(
			getType().isTupleType(),
			"The writeAsCsv() method can only be used on data streams of tuples.");

		CsvOutputFormat<X> of = new CsvOutputFormat<>(
			new Path(path),
			rowDelimiter,
			fieldDelimiter);

		if (writeMode != null) {
			of.setWriteMode(writeMode);
		}

		return writeUsingOutputFormat((OutputFormat<T>) of);
	}

	/**
	 * Writes the DataStream to a socket as a byte array. The format of the
	 * output is specified by a {@link SerializationSchema}.
	 *
	 * @param hostName
	 *            host of the socket
	 * @param port
	 *            port of the socket
	 * @param schema
	 *            schema for serialization
	 * @return the closed DataStream
	 */
	@PublicEvolving
	public DataStreamSink<T> writeToSocket(String hostName, int port, SerializationSchema<T> schema) {
		DataStreamSink<T> returnStream = addSink(new SocketClientSink<>(hostName, port, schema, 0));
		returnStream.setParallelism(1); // It would not work if multiple instances would connect to the same port
		return returnStream;
	}

	/**
	 * Writes the dataStream into an output, described by an OutputFormat.
	 *
	 * The output is not participating in Flink's checkpointing!
	 *
	 * For writing to a file system periodically, the use of the "flink-connector-filesystem" is recommended.
	 *
	 * @param format The output format
	 * @return The closed DataStream
	 */
	@PublicEvolving
	public DataStreamSink<T> writeUsingOutputFormat(OutputFormat<T> format) {
		return addSink(new OutputFormatSinkFunction<>(format));
	}

	/**
	 * Method for passing user defined operators along with the type
	 * information that will transform the DataStream.
	 *
	 * @param operatorName
	 *            name of the operator, for logging purposes
	 * @param outTypeInfo
	 *            the output type of the operator
	 * @param operator
	 *            the object containing the transformation logic
	 * @param <R>
	 *            type of the return stream
	 * @return the data stream constructed
	 */
	@PublicEvolving
	public <R> SingleOutputStreamOperator<R, ?> transform(String operatorName, TypeInformation<R> outTypeInfo, OneInputStreamOperator<T, R> operator) {

		// read the output type of the input Transform to coax out errors about MissingTypeInfo
		transformation.getOutputType();

		OneInputTransformation<T, R> resultTransform = new OneInputTransformation<>(
				this.transformation,
				operatorName,
				operator,
				outTypeInfo,
				environment.getParallelism());

		@SuppressWarnings({ "unchecked", "rawtypes" })
		SingleOutputStreamOperator<R, ?> returnStream = new SingleOutputStreamOperator(environment, resultTransform);

		getExecutionEnvironment().addOperator(resultTransform);

		return returnStream;
	}

	/**
	 * Internal function for setting the partitioner for the DataStream
	 *
	 * @param partitioner
	 *            Partitioner to set.
	 * @return The modified DataStream.
	 */
	protected DataStream<T> setConnectionType(StreamPartitioner<T> partitioner) {
		return new DataStream<>(this.getExecutionEnvironment(), new PartitionTransformation<>(this.getTransformation(), partitioner));
	}

	/**
	 * Adds the given sink to this DataStream. Only streams with sinks added
	 * will be executed once the {@link StreamExecutionEnvironment#execute()}
	 * method is called.
	 *
	 * @param sinkFunction
	 *            The object containing the sink's invoke function.
	 * @return The closed DataStream.
	 */
	public DataStreamSink<T> addSink(SinkFunction<T> sinkFunction) {

		// read the output type of the input Transform to coax out errors about MissingTypeInfo
		transformation.getOutputType();

		// configure the type if needed
		if (sinkFunction instanceof InputTypeConfigurable) {
			((InputTypeConfigurable) sinkFunction).setInputType(getType(), getExecutionConfig() );
		}

		StreamSink<T> sinkOperator = new StreamSink<>(clean(sinkFunction));

		DataStreamSink<T> sink = new DataStreamSink<>(this, sinkOperator);

		getExecutionEnvironment().addOperator(sink.getTransformation());
		return sink;
	}

	/**
	 * Returns the {@link StreamTransformation} that represents the operation that logically creates
	 * this {@link DataStream}.
	 *
	 * @return The Transformation
	 */
	@Internal
	public StreamTransformation<T> getTransformation() {
		return transformation;
	}
}
