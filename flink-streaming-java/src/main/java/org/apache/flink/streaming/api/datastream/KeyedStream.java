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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.TimelyFlatMapFunction;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.functions.aggregation.ComparableAggregator;
import org.apache.flink.streaming.api.functions.aggregation.SumAggregator;
import org.apache.flink.streaming.api.functions.query.QueryableAppendingStateOperator;
import org.apache.flink.streaming.api.functions.query.QueryableValueStateOperator;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.KeyedStreamFilter;
import org.apache.flink.streaming.api.operators.KeyedStreamFlatMap;
import org.apache.flink.streaming.api.operators.KeyedStreamMap;
import org.apache.flink.streaming.api.operators.KeyedStreamSink;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamGroupedFold;
import org.apache.flink.streaming.api.operators.StreamGroupedReduce;
import org.apache.flink.streaming.api.operators.StreamTimelyFlatMap;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
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
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;

import java.util.UUID;

/**
 * A {@code KeyedStream} represents a {@link DataStream} on which operator state is
 * partitioned by key using a provided {@link KeySelector}. Typical operations supported by a
 * {@code DataStream} are also possible on a {@code KeyedStream}, with the exception of
 * partitioning methods such as shuffle, forward and keyBy.
 *
 * <p>
 * Reduce-style operations, such as {@link #reduce}, {@link #sum} and {@link #fold} work on elements
 * that have the same key.
 *
 * @param <T> The type of the elements in the Keyed Stream.
 * @param <KEY> The type of the key in the Keyed Stream.
 */
@Public
public class KeyedStream<T, KEY> extends DataStream<T> {

	/** The key selector that can get the key by which the stream if partitioned from the elements */
	private final KeySelector<T, KEY> keySelector;

	/** The type of the key by which the stream is partitioned */
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
		super(
			dataStream.getExecutionEnvironment(),
			new PartitionTransformation<>(
				dataStream.getTransformation(),
				new KeyGroupStreamPartitioner<>(
					keySelector, KeyGroupRangeAssignment.DEFAULT_MAX_PARALLELISM)));
		this.keySelector = keySelector;
		this.keyType = keyType;
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

		// the only difference from DataStream.transform() is that we allow keyed operators here

		// read the output type of the input Transform to coax out errors about MissingTypeInfo
		transformation.getOutputType();

		OneInputTransformation<T, R> resultTransform = new OneInputTransformation<>(
				this.transformation,
				operatorName,
				operator,
				outTypeInfo,
				environment.getParallelism());

		@SuppressWarnings({ "unchecked", "rawtypes" })
		SingleOutputStreamOperator<R> returnStream = new SingleOutputStreamOperator(environment, resultTransform);

		getExecutionEnvironment().addOperator(resultTransform);

		return returnStream;
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
	@Override
	public <R> SingleOutputStreamOperator<R> map(MapFunction<T, R> mapper) {

		TypeInformation<R> outType = TypeExtractor.getMapReturnTypes(clean(mapper), getType(),
				Utils.getCallLocationName(), true);

		KeyedStreamMap<KEY, T, R> mapOperator = new KeyedStreamMap<>(
				clean(mapper),
				keyType.createSerializer(getExecutionConfig()),
				keySelector);

		return transform("Map", outType,
				mapOperator);
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
	@Override
	public <R> SingleOutputStreamOperator<R> flatMap(FlatMapFunction<T, R> flatMapper) {

		TypeInformation<R> outType = TypeExtractor.getFlatMapReturnTypes(clean(flatMapper),
				getType(), Utils.getCallLocationName(), true);

		KeyedStreamFlatMap<KEY, T, R> flatMapOperator = new KeyedStreamFlatMap<>(
				clean(flatMapper),
				keyType.createSerializer(getExecutionConfig()),
				keySelector);

		return transform("Flat Map", outType, flatMapOperator);

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
	@Override
	public SingleOutputStreamOperator<T> filter(FilterFunction<T> filter) {
		KeyedStreamFilter<KEY, T> filterOperator = new KeyedStreamFilter<>(
				clean(filter),
				keyType.createSerializer(getExecutionConfig()),
				keySelector);

		return transform("Filter", getType(), filterOperator);
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
	@Override
	public DataStreamSink<T> addSink(SinkFunction<T> sinkFunction) {

		// read the output type of the input Transform to coax out errors about MissingTypeInfo
		transformation.getOutputType();

		// configure the type if needed
		if (sinkFunction instanceof InputTypeConfigurable) {
			((InputTypeConfigurable) sinkFunction).setInputType(getType(), getExecutionConfig() );
		}

		KeyedStreamSink<KEY, T> sinkOperator = new KeyedStreamSink<>(
				clean(sinkFunction), keyType.createSerializer(getExecutionConfig()),
				keySelector
		);

		DataStreamSink<T> sink = new DataStreamSink<>(this, sinkOperator);

		getExecutionEnvironment().addOperator(sink.getTransformation());
		return sink;
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
	public <R> SingleOutputStreamOperator<R> flatMap(TimelyFlatMapFunction<T, R> flatMapper) {

		TypeInformation<R> outType = TypeExtractor.getUnaryOperatorReturnType(
				flatMapper,
				TimelyFlatMapFunction.class,
				false,
				true,
				getType(),
				Utils.getCallLocationName(),
				true);

		StreamTimelyFlatMap<KEY, T, R> operator =
				new StreamTimelyFlatMap<>(
						keyType.createSerializer(getExecutionConfig()),
						keySelector,
						clean(flatMapper));

		return transform("Flat Map", outType, operator);

	}


	// ------------------------------------------------------------------------
	//  Windowing
	// ------------------------------------------------------------------------

	/**
	 * Windows this {@code KeyedStream} into tumbling time windows.
	 *
	 * <p>
	 * This is a shortcut for either {@code .window(TumblingEventTimeWindows.of(size))} or
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
	 * <p>
	 * This is a shortcut for either {@code .window(SlidingEventTimeWindows.of(size, slide))} or
	 * {@code .window(SlidingProcessingTimeWindows.of(size, slide))} depending on the time characteristic
	 * set using
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
	 * <p>
	 * A {@link org.apache.flink.streaming.api.windowing.triggers.Trigger} can be defined to specify
	 * when windows are evaluated. However, {@code WindowAssigners} have a default {@code Trigger}
	 * that is used if a {@code Trigger} is not specified.
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
		StreamGroupedReduce<KEY, T> reduceOperator = new StreamGroupedReduce<>(
				keyType.createSerializer(getExecutionConfig()),
				keySelector,
				clean(reducer),
				getType().createSerializer(getExecutionConfig()));

		return transform("Keyed Reduce", getType(),
				reduceOperator);
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
	 */
	public <R> SingleOutputStreamOperator<R> fold(R initialValue, FoldFunction<T, R> folder) {

		TypeInformation<R> outType = TypeExtractor.getFoldReturnTypes(
				clean(folder), getType(), Utils.getCallLocationName(), true);

		StreamGroupedFold<KEY, T, R> foldOperator = new StreamGroupedFold<>(
				keyType.createSerializer(new ExecutionConfig()),
				keySelector,
				clean(folder),
				initialValue);

		return transform("Keyed Fold", outType,
				foldOperator);
	}

	/**
	 * Applies an aggregation that gives a rolling sum of the data stream at the
	 * given position grouped by the given key. An independent aggregate is kept
	 * per key.
	 *
	 * @param positionToSum
	 *            The position in the data point to sum
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> sum(int positionToSum) {
		return aggregate(new SumAggregator<>(positionToSum, getType(), getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that that gives the current sum of the pojo data
	 * stream at the given field expressionby the given key. An independent
	 * aggregate is kept per key. A field expression is either the name of a
	 * public field or a getter method with parentheses of the
	 * {@link DataStream}S underlying type. A dot can be used to drill down into
	 * objects, as in {@code "field1.getInnerField2()" }.
	 *
	 * @param field
	 *            The field expression based on which the aggregation will be
	 *            applied.
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> sum(String field) {
		return aggregate(new SumAggregator<>(field, getType(), getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that that gives the current minimum of the data
	 * stream at the given position by the given key. An independent aggregate
	 * is kept per key.
	 *
	 * @param positionToMin
	 *            The position in the data point to minimize
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> min(int positionToMin) {
		return aggregate(new ComparableAggregator<>(positionToMin, getType(), AggregationFunction.AggregationType.MIN,
				getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that that gives the current minimum of the pojo
	 * data stream at the given field expression by the given key. An
	 * independent aggregate is kept per key. A field expression is either the
	 * name of a public field or a getter method with parentheses of the
	 * {@link DataStream}S underlying type. A dot can be used to drill down into
	 * objects, as in {@code "field1.getInnerField2()" }.
	 *
	 * @param field
	 *            The field expression based on which the aggregation will be
	 *            applied.
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
	 *            The position in the data point to maximize
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> max(int positionToMax) {
		return aggregate(new ComparableAggregator<>(positionToMax, getType(), AggregationFunction.AggregationType.MAX,
				getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that that gives the current maximum of the pojo
	 * data stream at the given field expression by the given key. An
	 * independent aggregate is kept per key. A field expression is either the
	 * name of a public field or a getter method with parentheses of the
	 * {@link DataStream}S underlying type. A dot can be used to drill down into
	 * objects, as in {@code "field1.getInnerField2()" }.
	 *
	 * @param field
	 *            The field expression based on which the aggregation will be
	 *            applied.
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> max(String field) {
		return aggregate(new ComparableAggregator<>(field, getType(), AggregationFunction.AggregationType.MAX,
				false, getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that that gives the current minimum element of the
	 * pojo data stream by the given field expression by the given key. An
	 * independent aggregate is kept per key. A field expression is either the
	 * name of a public field or a getter method with parentheses of the
	 * {@link DataStream}S underlying type. A dot can be used to drill down into
	 * objects, as in {@code "field1.getInnerField2()" }.
	 *
	 * @param field
	 *            The field expression based on which the aggregation will be
	 *            applied.
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
	 * Applies an aggregation that that gives the current maximum element of the
	 * pojo data stream by the given field expression by the given key. An
	 * independent aggregate is kept per key. A field expression is either the
	 * name of a public field or a getter method with parentheses of the
	 * {@link DataStream}S underlying type. A dot can be used to drill down into
	 * objects, as in {@code "field1.getInnerField2()" }.
	 *
	 * @param field
	 *            The field expression based on which the aggregation will be
	 *            applied.
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
	 * Applies an aggregation that that gives the current element with the
	 * minimum value at the given position by the given key. An independent
	 * aggregate is kept per key. If more elements have the minimum value at the
	 * given position, the operator returns the first one by default.
	 *
	 * @param positionToMinBy
	 *            The position in the data point to minimize
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> minBy(int positionToMinBy) {
		return this.minBy(positionToMinBy, true);
	}

	/**
	 * Applies an aggregation that that gives the current element with the
	 * minimum value at the given position by the given key. An independent
	 * aggregate is kept per key. If more elements have the minimum value at the
	 * given position, the operator returns the first one by default.
	 *
	 * @param positionToMinBy
	 *            The position in the data point to minimize
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> minBy(String positionToMinBy) {
		return this.minBy(positionToMinBy, true);
	}

	/**
	 * Applies an aggregation that that gives the current element with the
	 * minimum value at the given position by the given key. An independent
	 * aggregate is kept per key. If more elements have the minimum value at the
	 * given position, the operator returns either the first or last one,
	 * depending on the parameter set.
	 *
	 * @param positionToMinBy
	 *            The position in the data point to minimize
	 * @param first
	 *            If true, then the operator return the first element with the
	 *            minimal value, otherwise returns the last
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> minBy(int positionToMinBy, boolean first) {
		return aggregate(new ComparableAggregator<>(positionToMinBy, getType(), AggregationFunction.AggregationType.MINBY, first,
				getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that that gives the current element with the
	 * maximum value at the given position by the given key. An independent
	 * aggregate is kept per key. If more elements have the maximum value at the
	 * given position, the operator returns the first one by default.
	 *
	 * @param positionToMaxBy
	 *            The position in the data point to maximize
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> maxBy(int positionToMaxBy) {
		return this.maxBy(positionToMaxBy, true);
	}

	/**
	 * Applies an aggregation that that gives the current element with the
	 * maximum value at the given position by the given key. An independent
	 * aggregate is kept per key. If more elements have the maximum value at the
	 * given position, the operator returns the first one by default.
	 *
	 * @param positionToMaxBy
	 *            The position in the data point to maximize
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> maxBy(String positionToMaxBy) {
		return this.maxBy(positionToMaxBy, true);
	}

	/**
	 * Applies an aggregation that that gives the current element with the
	 * maximum value at the given position by the given key. An independent
	 * aggregate is kept per key. If more elements have the maximum value at the
	 * given position, the operator returns either the first or last one,
	 * depending on the parameter set.
	 *
	 * @param positionToMaxBy
	 *            The position in the data point to maximize.
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
		StreamGroupedReduce<KEY, T> reduceOperator = new StreamGroupedReduce<>(
				keyType.createSerializer(getExecutionConfig()),
				keySelector,
				clean(aggregate),
				getType().createSerializer(getExecutionConfig()));

		return transform("Keyed Aggregation", getType(), reduceOperator);
	}

	/**
	 * Publishes the keyed stream as queryable ValueState instance.
	 *
	 * @param queryableStateName Name under which to the publish the queryable state instance
	 * @return Queryable state instance
	 */
	@PublicEvolving
	public QueryableStateStream<KEY, T> asQueryableState(String queryableStateName) {
		ValueStateDescriptor<T> valueStateDescriptor = new ValueStateDescriptor<>(
				UUID.randomUUID().toString(),
				getType(),
				null);

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
				new QueryableValueStateOperator<>(
						getKeyType().createSerializer(getExecutionConfig()),
						keySelector,
						queryableStateName, stateDescriptor));

		stateDescriptor.initializeSerializerUnlessSet(getExecutionConfig());

		return new QueryableStateStream<>(
				queryableStateName,
				stateDescriptor.getSerializer(),
				getKeyType().createSerializer(getExecutionConfig()));
	}

	/**
	 * Publishes the keyed stream as a queryable ListStance instance.
	 *
	 * @param queryableStateName Name under which to the publish the queryable state instance
	 * @param stateDescriptor State descriptor to create state instance from
	 * @return Queryable state instance
	 */
	@PublicEvolving
	public QueryableStateStream<KEY, T> asQueryableState(
			String queryableStateName,
			ListStateDescriptor<T> stateDescriptor) {

		transform("Queryable state: " + queryableStateName,
				getType(),
				new QueryableAppendingStateOperator<>(
						getKeyType().createSerializer(getExecutionConfig()),
						keySelector,
						queryableStateName, stateDescriptor));

		stateDescriptor.initializeSerializerUnlessSet(getExecutionConfig());

		return new QueryableStateStream<>(
				queryableStateName,
				getType().createSerializer(getExecutionConfig()),
				getKeyType().createSerializer(getExecutionConfig()));
	}

	/**
	 * Publishes the keyed stream as a queryable FoldingState instance.
	 *
	 * @param queryableStateName Name under which to the publish the queryable state instance
	 * @param stateDescriptor State descriptor to create state instance from
	 * @return Queryable state instance
	 */
	@PublicEvolving
	public <ACC> QueryableStateStream<KEY, ACC> asQueryableState(
			String queryableStateName,
			FoldingStateDescriptor<T, ACC> stateDescriptor) {

		transform("Queryable state: " + queryableStateName,
				getType(),
				new QueryableAppendingStateOperator<>(
						getKeyType().createSerializer(getExecutionConfig()),
						keySelector,
						queryableStateName, stateDescriptor));

		stateDescriptor.initializeSerializerUnlessSet(getExecutionConfig());

		return new QueryableStateStream<>(
				queryableStateName,
				stateDescriptor.getSerializer(),
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
				new QueryableAppendingStateOperator<>(
						getKeyType().createSerializer(getExecutionConfig()),
						keySelector,
						queryableStateName, stateDescriptor));

		stateDescriptor.initializeSerializerUnlessSet(getExecutionConfig());

		return new QueryableStateStream<>(
				queryableStateName,
				stateDescriptor.getSerializer(),
				getKeyType().createSerializer(getExecutionConfig()));
	}

}
