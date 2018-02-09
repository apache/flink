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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.functions.aggregation.ComparableAggregator;
import org.apache.flink.streaming.api.functions.aggregation.SumAggregator;
import org.apache.flink.streaming.api.functions.query.QueryableAppendingStateOperator;
import org.apache.flink.streaming.api.functions.query.QueryableValueStateOperator;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamGroupedFold;
import org.apache.flink.streaming.api.operators.StreamGroupedReduce;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.UUID;

/**
 * A {@link KeyedStream} represents a {@link DataStream} on which operator state is
 * partitioned by key using a provided {@link KeySelector}. Typical operations supported by a
 * {@code DataStream} are also possible on a {@code KeyedStream}, with the exception of
 * partitioning methods such as shuffle, forward and keyBy.
 *
 * <p>Reduce-style operations, such as {@link #reduce}, {@link #sum} and {@link #fold} work on
 * elements that have the same key.
 *
 * @param <T> The type of the elements in the Keyed Stream.
 * @param <KEY> The type of the key in the Keyed Stream.
 */
@Public
public class KeyedStream<T, KEY> extends DataStream<T> {

	/**
	 * The key selector that can get the key by which the stream if partitioned from the elements.
	 */
	private final KeySelector<T, KEY> keySelector;

	/** The type of the key by which the stream is partitioned. */
	private final TypeInformation<KEY> keyType;

	/**
	 * Creates a new {@link KeyedStream} using the given {@link KeySelector}
	 * to partition operator state by key.
	 *
	 * @param dataStream
	 *            Base stream of data
	 * @param keySelector
	 *            Function for determining state partitions
	 */
	public KeyedStream(DataStream<T> dataStream, KeySelector<T, KEY> keySelector) {
		this(dataStream, keySelector, TypeExtractor.getKeySelectorTypes(keySelector, dataStream.getType()));
	}

	/**
	 * Creates a new {@link KeyedStream} using the given {@link KeySelector}
	 * to partition operator state by key.
	 *
	 * @param dataStream
	 *            Base stream of data
	 * @param keySelector
	 *            Function for determining state partitions
	 */
	public KeyedStream(DataStream<T> dataStream, KeySelector<T, KEY> keySelector, TypeInformation<KEY> keyType) {
		this(
			dataStream,
			new PartitionTransformation<>(
				dataStream.getTransformation(),
				new KeyGroupStreamPartitioner<>(keySelector, StreamGraphGenerator.DEFAULT_LOWER_BOUND_MAX_PARALLELISM)),
			keySelector,
			keyType);
	}

	/**
	 * Creates a new {@link KeyedStream} using the given {@link KeySelector} and {@link TypeInformation}
	 * to partition operator state by key, where the partitioning is defined by a {@link PartitionTransformation}.
	 *
	 * @param stream
	 *            Base stream of data
	 * @param partitionTransformation
	 *            Function that determines how the keys are distributed to downstream operator(s)
	 * @param keySelector
	 *            Function to extract keys from the base stream
	 * @param keyType
	 *            Defines the type of the extracted keys
	 */
	@Internal
	public KeyedStream(
		DataStream<T> stream,
		PartitionTransformation<T> partitionTransformation,
		KeySelector<T, KEY> keySelector,
		TypeInformation<KEY> keyType) {

		super(stream.getExecutionEnvironment(), partitionTransformation);
		this.keySelector = clean(keySelector);
		this.keyType = validateKeyType(keyType);
	}

	/**
	 * Validates that a given type of element (as encoded by the provided {@link TypeInformation}) can be
	 * used as a key in the {@code DataStream.keyBy()} operation. This is done by searching depth-first the
	 * key type and checking if each of the composite types satisfies the required conditions
	 * (see {@link #validateKeyTypeIsHashable(TypeInformation)}).
	 *
	 * @param keyType The {@link TypeInformation} of the key.
	 */
	private TypeInformation<KEY> validateKeyType(TypeInformation<KEY> keyType) {
		Stack<TypeInformation<?>> stack = new Stack<>();
		stack.push(keyType);

		List<TypeInformation<?>> unsupportedTypes = new ArrayList<>();

		while (!stack.isEmpty()) {
			TypeInformation<?> typeInfo = stack.pop();

			if (!validateKeyTypeIsHashable(typeInfo)) {
				unsupportedTypes.add(typeInfo);
			}

			if (typeInfo instanceof TupleTypeInfoBase) {
				for (int i = 0; i < typeInfo.getArity(); i++) {
					stack.push(((TupleTypeInfoBase) typeInfo).getTypeAt(i));
				}
			}
		}

		if (!unsupportedTypes.isEmpty()) {
			throw new InvalidProgramException("Type " + keyType + " cannot be used as key. Contained " +
					"UNSUPPORTED key types: " + StringUtils.join(unsupportedTypes, ", ") + ". Look " +
					"at the keyBy() documentation for the conditions a type has to satisfy in order to be " +
					"eligible for a key.");
		}

		return keyType;
	}

	/**
	 * Validates that a given type of element (as encoded by the provided {@link TypeInformation}) can be
	 * used as a key in the {@code DataStream.keyBy()} operation.
	 *
	 * @param type The {@link TypeInformation} of the type to check.
	 * @return {@code false} if:
	 * <ol>
	 *     <li>it is a POJO type but does not override the {@link #hashCode()} method and relies on
	 *     the {@link Object#hashCode()} implementation.</li>
	 *     <li>it is an array of any type (see {@link PrimitiveArrayTypeInfo}, {@link BasicArrayTypeInfo},
	 *     {@link ObjectArrayTypeInfo}).</li>
	 * </ol>,
	 * {@code true} otherwise.
	 */
	private boolean validateKeyTypeIsHashable(TypeInformation<?> type) {
		try {
			return (type instanceof PojoTypeInfo)
					? !type.getTypeClass().getMethod("hashCode").getDeclaringClass().equals(Object.class)
					: !(type instanceof PrimitiveArrayTypeInfo || type instanceof BasicArrayTypeInfo || type instanceof ObjectArrayTypeInfo);
		} catch (NoSuchMethodException ignored) {
			// this should never happen as we are just searching for the hashCode() method.
		}
		return false;
	}

	// ------------------------------------------------------------------------
	//  properties
	// ------------------------------------------------------------------------

	/**
	 * Gets the key selector that can get the key by which the stream if partitioned from the elements.
	 * @return The key selector for the key.
	 */
	@Internal
	public KeySelector<T, KEY> getKeySelector() {
		return this.keySelector;
	}

	/**
	 * Gets the type of the key by which the stream is partitioned.
	 * @return The type of the key by which the stream is partitioned.
	 */
	@Internal
	public TypeInformation<KEY> getKeyType() {
		return keyType;
	}

	@Override
	protected DataStream<T> setConnectionType(StreamPartitioner<T> partitioner) {
		throw new UnsupportedOperationException("Cannot override partitioning for KeyedStream.");
	}

	// ------------------------------------------------------------------------
	//  basic transformations
	// ------------------------------------------------------------------------

	@Override
	@PublicEvolving
	public <R> SingleOutputStreamOperator<R> transform(String operatorName,
			TypeInformation<R> outTypeInfo, OneInputStreamOperator<T, R> operator) {

		SingleOutputStreamOperator<R> returnStream = super.transform(operatorName, outTypeInfo, operator);

		// inject the key selector and key type
		OneInputTransformation<T, R> transform = (OneInputTransformation<T, R>) returnStream.getTransformation();
		transform.setStateKeySelector(keySelector);
		transform.setStateKeyType(keyType);

		return returnStream;
	}

	@Override
	public DataStreamSink<T> addSink(SinkFunction<T> sinkFunction) {
		DataStreamSink<T> result = super.addSink(sinkFunction);
		result.getTransformation().setStateKeySelector(keySelector);
		result.getTransformation().setStateKeyType(keyType);
		return result;
	}

	/**
	 * Applies the given {@link ProcessFunction} on the input stream, thereby
	 * creating a transformed output stream.
	 *
	 * <p>The function will be called for every element in the input streams and can produce zero
	 * or more output elements. Contrary to the {@link DataStream#flatMap(FlatMapFunction)}
	 * function, this function can also query the time and set timers. When reacting to the firing
	 * of set timers the function can directly emit elements and/or register yet more timers.
	 *
	 * @param processFunction The {@link ProcessFunction} that is called for each element
	 *                      in the stream.
	 *
	 * @param <R> The type of elements emitted by the {@code ProcessFunction}.
	 *
	 * @return The transformed {@link DataStream}.
	 */
	@Override
	@PublicEvolving
	public <R> SingleOutputStreamOperator<R> process(ProcessFunction<T, R> processFunction) {

		TypeInformation<R> outType = TypeExtractor.getUnaryOperatorReturnType(
			processFunction,
			ProcessFunction.class,
			0,
			1,
			TypeExtractor.NO_INDEX,
			TypeExtractor.NO_INDEX,
			getType(),
			Utils.getCallLocationName(),
			true);

		return process(processFunction, outType);
	}

	/**
	 * Applies the given {@link ProcessFunction} on the input stream, thereby
	 * creating a transformed output stream.
	 *
	 * <p>The function will be called for every element in the input streams and can produce zero
	 * or more output elements. Contrary to the {@link DataStream#flatMap(FlatMapFunction)}
	 * function, this function can also query the time and set timers. When reacting to the firing
	 * of set timers the function can directly emit elements and/or register yet more timers.
	 *
	 * @param processFunction The {@link ProcessFunction} that is called for each element
	 *                      in the stream.
	 * @param outputType {@link TypeInformation} for the result type of the function.
	 *
	 * @param <R> The type of elements emitted by the {@code ProcessFunction}.
	 *
	 * @return The transformed {@link DataStream}.
	 */
	@Override
	@Internal
	public <R> SingleOutputStreamOperator<R> process(
			ProcessFunction<T, R> processFunction,
			TypeInformation<R> outputType) {

		KeyedProcessOperator<KEY, T, R> operator =
				new KeyedProcessOperator<>(clean(processFunction));

		return transform("Process", outputType, operator);
	}


	// ------------------------------------------------------------------------
	//  Windowing
	// ------------------------------------------------------------------------

	/**
	 * Windows this {@code KeyedStream} into tumbling time windows.
	 *
	 * <p>This is a shortcut for either {@code .window(TumblingEventTimeWindows.of(size))} or
	 * {@code .window(TumblingProcessingTimeWindows.of(size))} depending on the time characteristic
	 * set using
	 * {@link org.apache.flink.streaming.api.environment.StreamExecutionEnvironment#setStreamTimeCharacteristic(org.apache.flink.streaming.api.TimeCharacteristic)}
	 *
	 * @param size The size of the window.
	 */
	public WindowedStream<T, KEY, TimeWindow> timeWindow(Time size) {
		if (environment.getStreamTimeCharacteristic() == TimeCharacteristic.ProcessingTime) {
			return window(TumblingProcessingTimeWindows.of(size));
		} else {
			return window(TumblingEventTimeWindows.of(size));
		}
	}

	/**
	 * Windows this {@code KeyedStream} into sliding time windows.
	 *
	 * <p>This is a shortcut for either {@code .window(SlidingEventTimeWindows.of(size, slide))} or
	 * {@code .window(SlidingProcessingTimeWindows.of(size, slide))} depending on the time
	 * characteristic set using
	 * {@link org.apache.flink.streaming.api.environment.StreamExecutionEnvironment#setStreamTimeCharacteristic(org.apache.flink.streaming.api.TimeCharacteristic)}
	 *
	 * @param size The size of the window.
	 */
	public WindowedStream<T, KEY, TimeWindow> timeWindow(Time size, Time slide) {
		if (environment.getStreamTimeCharacteristic() == TimeCharacteristic.ProcessingTime) {
			return window(SlidingProcessingTimeWindows.of(size, slide));
		} else {
			return window(SlidingEventTimeWindows.of(size, slide));
		}
	}

	/**
	 * Windows this {@code KeyedStream} into tumbling count windows.
	 *
	 * @param size The size of the windows in number of elements.
	 */
	public WindowedStream<T, KEY, GlobalWindow> countWindow(long size) {
		return window(GlobalWindows.create()).trigger(PurgingTrigger.of(CountTrigger.of(size)));
	}

	/**
	 * Windows this {@code KeyedStream} into sliding count windows.
	 *
	 * @param size The size of the windows in number of elements.
	 * @param slide The slide interval in number of elements.
	 */
	public WindowedStream<T, KEY, GlobalWindow> countWindow(long size, long slide) {
		return window(GlobalWindows.create())
				.evictor(CountEvictor.of(size))
				.trigger(CountTrigger.of(slide));
	}

	/**
	 * Windows this data stream to a {@code WindowedStream}, which evaluates windows
	 * over a key grouped stream. Elements are put into windows by a {@link WindowAssigner}. The
	 * grouping of elements is done both by key and by window.
	 *
	 * <p>A {@link org.apache.flink.streaming.api.windowing.triggers.Trigger} can be defined to
	 * specify when windows are evaluated. However, {@code WindowAssigners} have a default
	 * {@code Trigger} that is used if a {@code Trigger} is not specified.
	 *
	 * @param assigner The {@code WindowAssigner} that assigns elements to windows.
	 * @return The trigger windows data stream.
	 */
	@PublicEvolving
	public <W extends Window> WindowedStream<T, KEY, W> window(WindowAssigner<? super T, W> assigner) {
		return new WindowedStream<>(this, assigner);
	}

	// ------------------------------------------------------------------------
	//  Non-Windowed aggregation operations
	// ------------------------------------------------------------------------

	/**
	 * Applies a reduce transformation on the grouped data stream grouped on by
	 * the given key position. The {@link ReduceFunction} will receive input
	 * values based on the key value. Only input values with the same key will
	 * go to the same reducer.
	 *
	 * @param reducer
	 *            The {@link ReduceFunction} that will be called for every
	 *            element of the input values with the same key.
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> reduce(ReduceFunction<T> reducer) {
		return transform("Keyed Reduce", getType(), new StreamGroupedReduce<T>(
				clean(reducer), getType().createSerializer(getExecutionConfig())));
	}

	/**
	 * Applies a fold transformation on the grouped data stream grouped on by
	 * the given key position. The {@link FoldFunction} will receive input
	 * values based on the key value. Only input values with the same key will
	 * go to the same folder.
	 *
	 * @param folder
	 *            The {@link FoldFunction} that will be called for every element
	 *            of the input values with the same key.
	 * @param initialValue
	 *            The initialValue passed to the folders for each key.
	 * @return The transformed DataStream.
	 *
	 * @deprecated will be removed in a future version
	 */
	@Deprecated
	public <R> SingleOutputStreamOperator<R> fold(R initialValue, FoldFunction<T, R> folder) {

		TypeInformation<R> outType = TypeExtractor.getFoldReturnTypes(
				clean(folder), getType(), Utils.getCallLocationName(), true);

		return transform("Keyed Fold", outType, new StreamGroupedFold<>(clean(folder), initialValue));
	}

	/**
	 * Applies an aggregation that gives a rolling sum of the data stream at the
	 * given position grouped by the given key. An independent aggregate is kept
	 * per key.
	 *
	 * @param positionToSum
	 *            The field position in the data points to sum. This is applicable to
	 *            Tuple types, basic and primitive array types, Scala case classes,
	 *            and primitive types (which is considered as having one field).
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> sum(int positionToSum) {
		return aggregate(new SumAggregator<>(positionToSum, getType(), getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that gives the current sum of the data
	 * stream at the given field by the given key. An independent
	 * aggregate is kept per key.
	 *
	 * @param field
	 *            In case of a POJO, Scala case class, or Tuple type, the
	 *            name of the (public) field on which to perform the aggregation.
	 *            Additionally, a dot can be used to drill down into nested
	 *            objects, as in {@code "field1.fieldxy" }.
	 *            Furthermore "*" can be specified in case of a basic type
	 *            (which is considered as having only one field).
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> sum(String field) {
		return aggregate(new SumAggregator<>(field, getType(), getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that gives the current minimum of the data
	 * stream at the given position by the given key. An independent aggregate
	 * is kept per key.
	 *
	 * @param positionToMin
	 *            The field position in the data points to minimize. This is applicable to
	 *            Tuple types, Scala case classes, and primitive types (which is considered
	 *            as having one field).
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> min(int positionToMin) {
		return aggregate(new ComparableAggregator<>(positionToMin, getType(), AggregationFunction.AggregationType.MIN,
				getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that gives the current minimum of the
	 * data stream at the given field expression by the given key. An
	 * independent aggregate is kept per key. A field expression is either the
	 * name of a public field or a getter method with parentheses of the
	 * {@link DataStream}'s underlying type. A dot can be used to drill down into
	 * objects, as in {@code "field1.fieldxy" }.
	 *
	 * @param field
	 *            In case of a POJO, Scala case class, or Tuple type, the
	 *            name of the (public) field on which to perform the aggregation.
	 *            Additionally, a dot can be used to drill down into nested
	 *            objects, as in {@code "field1.fieldxy" }.
	 *            Furthermore "*" can be specified in case of a basic type
	 *            (which is considered as having only one field).
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> min(String field) {
		return aggregate(new ComparableAggregator<>(field, getType(), AggregationFunction.AggregationType.MIN,
				false, getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that gives the current maximum of the data stream
	 * at the given position by the given key. An independent aggregate is kept
	 * per key.
	 *
	 * @param positionToMax
	 *            The field position in the data points to minimize. This is applicable to
	 *            Tuple types, Scala case classes, and primitive types (which is considered
	 *            as having one field).
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> max(int positionToMax) {
		return aggregate(new ComparableAggregator<>(positionToMax, getType(), AggregationFunction.AggregationType.MAX,
				getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that gives the current maximum of the
	 * data stream at the given field expression by the given key. An
	 * independent aggregate is kept per key. A field expression is either the
	 * name of a public field or a getter method with parentheses of the
	 * {@link DataStream}'s underlying type. A dot can be used to drill down into
	 * objects, as in {@code "field1.fieldxy" }.
	 *
	 * @param field
	 *            In case of a POJO, Scala case class, or Tuple type, the
	 *            name of the (public) field on which to perform the aggregation.
	 *            Additionally, a dot can be used to drill down into nested
	 *            objects, as in {@code "field1.fieldxy" }.
	 *            Furthermore "*" can be specified in case of a basic type
	 *            (which is considered as having only one field).
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> max(String field) {
		return aggregate(new ComparableAggregator<>(field, getType(), AggregationFunction.AggregationType.MAX,
				false, getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that gives the current minimum element of the
	 * data stream by the given field expression by the given key. An
	 * independent aggregate is kept per key. A field expression is either the
	 * name of a public field or a getter method with parentheses of the
	 * {@link DataStream}'s underlying type. A dot can be used to drill down into
	 * objects, as in {@code "field1.fieldxy" }.
	 *
	 * @param field
	 *            In case of a POJO, Scala case class, or Tuple type, the
	 *            name of the (public) field on which to perform the aggregation.
	 *            Additionally, a dot can be used to drill down into nested
	 *            objects, as in {@code "field1.fieldxy" }.
	 *            Furthermore "*" can be specified in case of a basic type
	 *            (which is considered as having only one field).
	 * @param first
	 *            If True then in case of field equality the first object will
	 *            be returned
	 * @return The transformed DataStream.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public SingleOutputStreamOperator<T> minBy(String field, boolean first) {
		return aggregate(new ComparableAggregator(field, getType(), AggregationFunction.AggregationType.MINBY,
				first, getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that gives the current maximum element of the
	 * data stream by the given field expression by the given key. An
	 * independent aggregate is kept per key. A field expression is either the
	 * name of a public field or a getter method with parentheses of the
	 * {@link DataStream}'s underlying type. A dot can be used to drill down into
	 * objects, as in {@code "field1.fieldxy" }.
	 *
	 * @param field
	 *            In case of a POJO, Scala case class, or Tuple type, the
	 *            name of the (public) field on which to perform the aggregation.
	 *            Additionally, a dot can be used to drill down into nested
	 *            objects, as in {@code "field1.fieldxy" }.
	 *            Furthermore "*" can be specified in case of a basic type
	 *            (which is considered as having only one field).
	 * @param first
	 *            If True then in case of field equality the first object will
	 *            be returned
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> maxBy(String field, boolean first) {
		return aggregate(new ComparableAggregator<>(field, getType(), AggregationFunction.AggregationType.MAXBY,
				first, getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that gives the current element with the
	 * minimum value at the given position by the given key. An independent
	 * aggregate is kept per key. If more elements have the minimum value at the
	 * given position, the operator returns the first one by default.
	 *
	 * @param positionToMinBy
	 *            The field position in the data points to minimize. This is applicable to
	 *            Tuple types, Scala case classes, and primitive types (which is considered
	 *            as having one field).
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> minBy(int positionToMinBy) {
		return this.minBy(positionToMinBy, true);
	}

	/**
	 * Applies an aggregation that gives the current element with the
	 * minimum value at the given position by the given key. An independent
	 * aggregate is kept per key. If more elements have the minimum value at the
	 * given position, the operator returns the first one by default.
	 *
	 * @param positionToMinBy
	 *            In case of a POJO, Scala case class, or Tuple type, the
	 *            name of the (public) field on which to perform the aggregation.
	 *            Additionally, a dot can be used to drill down into nested
	 *            objects, as in {@code "field1.fieldxy" }.
	 *            Furthermore "*" can be specified in case of a basic type
	 *            (which is considered as having only one field).
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> minBy(String positionToMinBy) {
		return this.minBy(positionToMinBy, true);
	}

	/**
	 * Applies an aggregation that gives the current element with the
	 * minimum value at the given position by the given key. An independent
	 * aggregate is kept per key. If more elements have the minimum value at the
	 * given position, the operator returns either the first or last one,
	 * depending on the parameter set.
	 *
	 * @param positionToMinBy
	 *            The field position in the data points to minimize. This is applicable to
	 *            Tuple types, Scala case classes, and primitive types (which is considered
	 *            as having one field).
	 * @param first
	 *            If true, then the operator return the first element with the
	 *            minimal value, otherwise returns the last
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> minBy(int positionToMinBy, boolean first) {
		return aggregate(new ComparableAggregator<T>(positionToMinBy, getType(), AggregationFunction.AggregationType.MINBY, first,
				getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that gives the current element with the
	 * maximum value at the given position by the given key. An independent
	 * aggregate is kept per key. If more elements have the maximum value at the
	 * given position, the operator returns the first one by default.
	 *
	 * @param positionToMaxBy
	 *            The field position in the data points to minimize. This is applicable to
	 *            Tuple types, Scala case classes, and primitive types (which is considered
	 *            as having one field).
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> maxBy(int positionToMaxBy) {
		return this.maxBy(positionToMaxBy, true);
	}

	/**
	 * Applies an aggregation that gives the current element with the
	 * maximum value at the given position by the given key. An independent
	 * aggregate is kept per key. If more elements have the maximum value at the
	 * given position, the operator returns the first one by default.
	 *
	 * @param positionToMaxBy
	 *            In case of a POJO, Scala case class, or Tuple type, the
	 *            name of the (public) field on which to perform the aggregation.
	 *            Additionally, a dot can be used to drill down into nested
	 *            objects, as in {@code "field1.fieldxy" }.
	 *            Furthermore "*" can be specified in case of a basic type
	 *            (which is considered as having only one field).
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> maxBy(String positionToMaxBy) {
		return this.maxBy(positionToMaxBy, true);
	}

	/**
	 * Applies an aggregation that gives the current element with the
	 * maximum value at the given position by the given key. An independent
	 * aggregate is kept per key. If more elements have the maximum value at the
	 * given position, the operator returns either the first or last one,
	 * depending on the parameter set.
	 *
	 * @param positionToMaxBy
	 *            The field position in the data points to minimize. This is applicable to
	 *            Tuple types, Scala case classes, and primitive types (which is considered
	 *            as having one field).
	 * @param first
	 *            If true, then the operator return the first element with the
	 *            maximum value, otherwise returns the last
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> maxBy(int positionToMaxBy, boolean first) {
		return aggregate(new ComparableAggregator<>(positionToMaxBy, getType(), AggregationFunction.AggregationType.MAXBY, first,
				getExecutionConfig()));
	}

	protected SingleOutputStreamOperator<T> aggregate(AggregationFunction<T> aggregate) {
		StreamGroupedReduce<T> operator = new StreamGroupedReduce<T>(
				clean(aggregate), getType().createSerializer(getExecutionConfig()));
		return transform("Keyed Aggregation", getType(), operator);
	}

	/**
	 * Publishes the keyed stream as queryable ValueState instance.
	 *
	 * @param queryableStateName Name under which to the publish the queryable state instance
	 * @return Queryable state instance
	 */
	@PublicEvolving
	public QueryableStateStream<KEY, T> asQueryableState(String queryableStateName) {
		ValueStateDescriptor<T> valueStateDescriptor = new ValueStateDescriptor<T>(
				UUID.randomUUID().toString(),
				getType());

		return asQueryableState(queryableStateName, valueStateDescriptor);
	}

	/**
	 * Publishes the keyed stream as a queryable ValueState instance.
	 *
	 * @param queryableStateName Name under which to the publish the queryable state instance
	 * @param stateDescriptor State descriptor to create state instance from
	 * @return Queryable state instance
	 */
	@PublicEvolving
	public QueryableStateStream<KEY, T> asQueryableState(
			String queryableStateName,
			ValueStateDescriptor<T> stateDescriptor) {

		transform("Queryable state: " + queryableStateName,
				getType(),
				new QueryableValueStateOperator<>(queryableStateName, stateDescriptor));

		stateDescriptor.initializeSerializerUnlessSet(getExecutionConfig());

		return new QueryableStateStream<>(
				queryableStateName,
				stateDescriptor,
				getKeyType().createSerializer(getExecutionConfig()));
	}

	/**
	 * Publishes the keyed stream as a queryable FoldingState instance.
	 *
	 * @param queryableStateName Name under which to the publish the queryable state instance
	 * @param stateDescriptor State descriptor to create state instance from
	 * @return Queryable state instance
	 *
	 * @deprecated will be removed in a future version
	 */
	@PublicEvolving
	@Deprecated
	public <ACC> QueryableStateStream<KEY, ACC> asQueryableState(
			String queryableStateName,
			FoldingStateDescriptor<T, ACC> stateDescriptor) {

		transform("Queryable state: " + queryableStateName,
				getType(),
				new QueryableAppendingStateOperator<>(queryableStateName, stateDescriptor));

		stateDescriptor.initializeSerializerUnlessSet(getExecutionConfig());

		return new QueryableStateStream<>(
				queryableStateName,
				stateDescriptor,
				getKeyType().createSerializer(getExecutionConfig()));
	}

	/**
	 * Publishes the keyed stream as a queryable ReducingState instance.
	 *
	 * @param queryableStateName Name under which to the publish the queryable state instance
	 * @param stateDescriptor State descriptor to create state instance from
	 * @return Queryable state instance
	 */
	@PublicEvolving
	public QueryableStateStream<KEY, T> asQueryableState(
			String queryableStateName,
			ReducingStateDescriptor<T> stateDescriptor) {

		transform("Queryable state: " + queryableStateName,
				getType(),
				new QueryableAppendingStateOperator<>(queryableStateName, stateDescriptor));

		stateDescriptor.initializeSerializerUnlessSet(getExecutionConfig());

		return new QueryableStateStream<>(
				queryableStateName,
				stateDescriptor,
				getKeyType().createSerializer(getExecutionConfig()));
	}

}
