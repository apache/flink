/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.functions.aggregation.ComparableAggregator;
import org.apache.flink.streaming.api.functions.aggregation.SumAggregator;
import org.apache.flink.streaming.api.functions.windowing.AggregateApplyAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.FoldApplyAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.FoldApplyProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.PassThroughAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ReduceApplyAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ReduceApplyProcessAllWindowFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.BaseAlignedWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.EvictingWindowOperator;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalAggregateProcessAllWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableAllWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableProcessAllWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalSingleValueAllWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalSingleValueProcessAllWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@code AllWindowedStream} represents a data stream where the stream of
 * elements is split into windows based on a
 * {@link org.apache.flink.streaming.api.windowing.assigners.WindowAssigner}. Window emission
 * is triggered based on a {@link org.apache.flink.streaming.api.windowing.triggers.Trigger}.
 *
 * <p>If an {@link org.apache.flink.streaming.api.windowing.evictors.Evictor} is specified it will be
 * used to evict elements from the window after
 * evaluation was triggered by the {@code Trigger} but before the actual evaluation of the window.
 * When using an evictor, window performance will degrade significantly, since
 * pre-aggregation of window results cannot be used.
 *
 * <p>Note that the {@code AllWindowedStream} is purely an API construct, during runtime
 * the {@code AllWindowedStream} will be collapsed together with the
 * operation over the window into one single operation.
 *
 * @param <T> The type of elements in the stream.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns the elements to.
 */
@Public
public class AllWindowedStream<T, W extends Window> {

	/** The keyed data stream that is windowed by this stream. */
	private final KeyedStream<T, Byte> input;

	/** The window assigner. */
	private final WindowAssigner<? super T, W> windowAssigner;

	/** The trigger that is used for window evaluation/emission. */
	private Trigger<? super T, ? super W> trigger;

	/** The evictor that is used for evicting elements before window evaluation. */
	private Evictor<? super T, ? super W> evictor;

	/** The user-specified allowed lateness. */
	private long allowedLateness = 0L;

	/**
	 * Side output {@code OutputTag} for late data. If no tag is set late data will simply be dropped.
	 */
	private OutputTag<T> lateDataOutputTag;

	@PublicEvolving
	public AllWindowedStream(DataStream<T> input,
			WindowAssigner<? super T, W> windowAssigner) {
		this.input = input.keyBy(new NullByteKeySelector<T>());
		this.windowAssigner = windowAssigner;
		this.trigger = windowAssigner.getDefaultTrigger(input.getExecutionEnvironment());
	}

	/**
	 * Sets the {@code Trigger} that should be used to trigger window emission.
	 */
	@PublicEvolving
	public AllWindowedStream<T, W> trigger(Trigger<? super T, ? super W> trigger) {
		if (windowAssigner instanceof MergingWindowAssigner && !trigger.canMerge()) {
			throw new UnsupportedOperationException("A merging window assigner cannot be used with a trigger that does not support merging.");
		}

		this.trigger = trigger;
		return this;
	}

	/**
	 * Sets the time by which elements are allowed to be late. Elements that
	 * arrive behind the watermark by more than the specified time will be dropped.
	 * By default, the allowed lateness is {@code 0L}.
	 *
	 * <p>Setting an allowed lateness is only valid for event-time windows.
	 */
	@PublicEvolving
	public AllWindowedStream<T, W> allowedLateness(Time lateness) {
		final long millis = lateness.toMilliseconds();
		checkArgument(millis >= 0, "The allowed lateness cannot be negative.");

		this.allowedLateness = millis;
		return this;
	}

	/**
	 * Send late arriving data to the side output identified by the given {@link OutputTag}. Data
	 * is considered late after the watermark has passed the end of the window plus the allowed
	 * lateness set using {@link #allowedLateness(Time)}.
	 *
	 * <p>You can get the stream of late data using
	 * {@link SingleOutputStreamOperator#getSideOutput(OutputTag)} on the
	 * {@link SingleOutputStreamOperator} resulting from the windowed operation
	 * with the same {@link OutputTag}.
	 */
	@PublicEvolving
	public AllWindowedStream<T, W> sideOutputLateData(OutputTag<T> outputTag) {
		Preconditions.checkNotNull(outputTag, "Side output tag must not be null.");
		this.lateDataOutputTag = input.getExecutionEnvironment().clean(outputTag);
		return this;
	}

	/**
	 * Sets the {@code Evictor} that should be used to evict elements from a window before emission.
	 *
	 * <p>Note: When using an evictor window performance will degrade significantly, since
	 * incremental aggregation of window results cannot be used.
	 */
	@PublicEvolving
	public AllWindowedStream<T, W> evictor(Evictor<? super T, ? super W> evictor) {
		if (windowAssigner instanceof BaseAlignedWindowAssigner) {
			throw new UnsupportedOperationException("Cannot use a " + windowAssigner.getClass().getSimpleName() + " with an Evictor.");
		}

		this.evictor = evictor;
		return this;
	}


	// ------------------------------------------------------------------------
	//  Operations on the keyed windows
	// ------------------------------------------------------------------------

	/**
	 * Applies a reduce function to the window. The window function is called for each evaluation
	 * of the window for each key individually. The output of the reduce function is interpreted
	 * as a regular non-windowed stream.
	 *
	 * <p>This window will try and incrementally aggregate data as much as the window policies permit.
	 * For example, tumbling time windows can aggregate the data, meaning that only one element per
	 * key is stored. Sliding time windows will aggregate on the granularity of the slide interval,
	 * so a few elements are stored per key (one per slide interval).
	 * Custom windows may not be able to incrementally aggregate, or may need to store extra values
	 * in an aggregation tree.
	 *
	 * @param function The reduce function.
	 * @return The data stream that is the result of applying the reduce function to the window.
	 */
	@SuppressWarnings("unchecked")
	public SingleOutputStreamOperator<T> reduce(ReduceFunction<T> function) {
		if (function instanceof RichFunction) {
			throw new UnsupportedOperationException("ReduceFunction of reduce can not be a RichFunction. " +
					"Please use reduce(ReduceFunction, WindowFunction) instead.");
		}

		//clean the closure
		function = input.getExecutionEnvironment().clean(function);

		String callLocation = Utils.getCallLocationName();
		String udfName = "AllWindowedStream." + callLocation;

		return reduce(function, new PassThroughAllWindowFunction<W, T>());
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>Arriving data is incrementally aggregated using the given reducer.
	 *
	 * @param reduceFunction The reduce function that is used for incremental aggregation.
	 * @param function The window function.
	 * @return The data stream that is the result of applying the window function to the window.
	 */
	@PublicEvolving
	public <R> SingleOutputStreamOperator<R> reduce(
			ReduceFunction<T> reduceFunction,
			AllWindowFunction<T, R, W> function) {

		TypeInformation<T> inType = input.getType();
		TypeInformation<R> resultType = getAllWindowFunctionReturnType(function, inType);

		return reduce(reduceFunction, function, resultType);
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>Arriving data is incrementally aggregated using the given reducer.
	 *
	 * @param reduceFunction The reduce function that is used for incremental aggregation.
	 * @param function The window function.
	 * @param resultType Type information for the result type of the window function
	 * @return The data stream that is the result of applying the window function to the window.
	 */
	@PublicEvolving
	public <R> SingleOutputStreamOperator<R> reduce(
			ReduceFunction<T> reduceFunction,
			AllWindowFunction<T, R, W> function,
			TypeInformation<R> resultType) {

		if (reduceFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("ReduceFunction of reduce can not be a RichFunction.");
		}

		//clean the closures
		function = input.getExecutionEnvironment().clean(function);
		reduceFunction = input.getExecutionEnvironment().clean(reduceFunction);

		String callLocation = Utils.getCallLocationName();
		String udfName = "AllWindowedStream." + callLocation;

		String opName;
		KeySelector<T, Byte> keySel = input.getKeySelector();

		OneInputStreamOperator<T, R> operator;

		if (evictor != null) {
			@SuppressWarnings({"unchecked", "rawtypes"})
			TypeSerializer<StreamRecord<T>> streamRecordSerializer =
				(TypeSerializer<StreamRecord<T>>) new StreamElementSerializer(input.getType().createSerializer(getExecutionEnvironment().getConfig()));

			ListStateDescriptor<StreamRecord<T>> stateDesc =
				new ListStateDescriptor<>("window-contents", streamRecordSerializer);

			opName = "TriggerWindow(" + windowAssigner + ", " + stateDesc + ", " + trigger + ", " + evictor + ", " + udfName + ")";

			operator =
				new EvictingWindowOperator<>(windowAssigner,
					windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
					keySel,
					input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
					stateDesc,
					new InternalIterableAllWindowFunction<>(new ReduceApplyAllWindowFunction<>(reduceFunction, function)),
					trigger,
					evictor,
					allowedLateness,
					lateDataOutputTag);

		} else {
			ReducingStateDescriptor<T> stateDesc = new ReducingStateDescriptor<>("window-contents",
				reduceFunction,
				input.getType().createSerializer(getExecutionEnvironment().getConfig()));

			opName = "TriggerWindow(" + windowAssigner + ", " + stateDesc + ", " + trigger + ", " + udfName + ")";

			operator =
				new WindowOperator<>(windowAssigner,
					windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
					keySel,
					input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
					stateDesc,
					new InternalSingleValueAllWindowFunction<>(function),
					trigger,
					allowedLateness,
					lateDataOutputTag);
		}

		return input.transform(opName, resultType, operator).forceNonParallel();
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>Arriving data is incrementally aggregated using the given reducer.
	 *
	 * @param reduceFunction The reduce function that is used for incremental aggregation.
	 * @param function The process window function.
	 * @return The data stream that is the result of applying the window function to the window.
	 */
	@PublicEvolving
	public <R> SingleOutputStreamOperator<R> reduce(
			ReduceFunction<T> reduceFunction,
			ProcessAllWindowFunction<T, R, W> function) {

		TypeInformation<R> resultType = getProcessAllWindowFunctionReturnType(function, input.getType());

		return reduce(reduceFunction, function, resultType);
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>Arriving data is incrementally aggregated using the given reducer.
	 *
	 * @param reduceFunction The reduce function that is used for incremental aggregation.
	 * @param function The process window function.
	 * @param resultType Type information for the result type of the window function
	 * @return The data stream that is the result of applying the window function to the window.
	 */
	@PublicEvolving
	public <R> SingleOutputStreamOperator<R> reduce(ReduceFunction<T> reduceFunction, ProcessAllWindowFunction<T, R, W> function, TypeInformation<R> resultType) {
		if (reduceFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("ReduceFunction of reduce can not be a RichFunction.");
		}

		//clean the closures
		function = input.getExecutionEnvironment().clean(function);
		reduceFunction = input.getExecutionEnvironment().clean(reduceFunction);

		String callLocation = Utils.getCallLocationName();
		String udfName = "AllWindowedStream." + callLocation;

		String opName;
		KeySelector<T, Byte> keySel = input.getKeySelector();

		OneInputStreamOperator<T, R> operator;

		if (evictor != null) {
			@SuppressWarnings({"unchecked", "rawtypes"})
			TypeSerializer<StreamRecord<T>> streamRecordSerializer =
				(TypeSerializer<StreamRecord<T>>) new StreamElementSerializer(input.getType().createSerializer(getExecutionEnvironment().getConfig()));

			ListStateDescriptor<StreamRecord<T>> stateDesc =
				new ListStateDescriptor<>("window-contents", streamRecordSerializer);

			opName = "TriggerWindow(" + windowAssigner + ", " + stateDesc + ", " + trigger + ", " + evictor + ", " + udfName + ")";

			operator =
				new EvictingWindowOperator<>(windowAssigner,
					windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
					keySel,
					input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
					stateDesc,
					new InternalIterableProcessAllWindowFunction<>(new ReduceApplyProcessAllWindowFunction<>(reduceFunction, function)),
					trigger,
					evictor,
					allowedLateness,
					lateDataOutputTag);

		} else {
			ReducingStateDescriptor<T> stateDesc = new ReducingStateDescriptor<>("window-contents",
				reduceFunction,
				input.getType().createSerializer(getExecutionEnvironment().getConfig()));

			opName = "TriggerWindow(" + windowAssigner + ", " + stateDesc + ", " + trigger + ", " + udfName + ")";

			operator =
				new WindowOperator<>(windowAssigner,
					windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
					keySel,
					input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
					stateDesc,
					new InternalSingleValueProcessAllWindowFunction<>(function),
					trigger,
					allowedLateness,
					lateDataOutputTag);
		}

		return input.transform(opName, resultType, operator).forceNonParallel();
	}

	// ------------------------------------------------------------------------
	//  AggregateFunction
	// ------------------------------------------------------------------------

	/**
	 * Applies the given {@code AggregateFunction} to each window. The AggregateFunction
	 * aggregates all elements of a window into a single result element. The stream of these
	 * result elements (one per window) is interpreted as a regular non-windowed stream.
	 *
	 * @param function The aggregation function.
	 * @return The data stream that is the result of applying the fold function to the window.
	 *
	 * @param <ACC> The type of the AggregateFunction's accumulator
	 * @param <R> The type of the elements in the resulting stream, equal to the
	 *            AggregateFunction's result type
	 */
	@PublicEvolving
	public <ACC, R> SingleOutputStreamOperator<R> aggregate(AggregateFunction<T, ACC, R> function) {
		checkNotNull(function, "function");

		if (function instanceof RichFunction) {
			throw new UnsupportedOperationException("This aggregation function cannot be a RichFunction.");
		}

		TypeInformation<ACC> accumulatorType = TypeExtractor.getAggregateFunctionAccumulatorType(
				function, input.getType(), null, false);

		TypeInformation<R> resultType = TypeExtractor.getAggregateFunctionReturnType(
				function, input.getType(), null, false);

		return aggregate(function, accumulatorType, resultType);
	}

	/**
	 * Applies the given {@code AggregateFunction} to each window. The AggregateFunction
	 * aggregates all elements of a window into a single result element. The stream of these
	 * result elements (one per window) is interpreted as a regular non-windowed stream.
	 *
	 * @param function The aggregation function.
	 * @return The data stream that is the result of applying the aggregation function to the window.
	 *
	 * @param <ACC> The type of the AggregateFunction's accumulator
	 * @param <R> The type of the elements in the resulting stream, equal to the
	 *            AggregateFunction's result type
	 */
	@PublicEvolving
	public <ACC, R> SingleOutputStreamOperator<R> aggregate(
			AggregateFunction<T, ACC, R> function,
			TypeInformation<ACC> accumulatorType,
			TypeInformation<R> resultType) {

		checkNotNull(function, "function");
		checkNotNull(accumulatorType, "accumulatorType");
		checkNotNull(resultType, "resultType");

		if (function instanceof RichFunction) {
			throw new UnsupportedOperationException("This aggregation function cannot be a RichFunction.");
		}

		return aggregate(function, new PassThroughAllWindowFunction<W, R>(),
				accumulatorType, resultType);
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>Arriving data is incrementally aggregated using the given aggregate function. This means
	 * that the window function typically has only a single value to process when called.
	 *
	 * @param aggFunction The aggregate function that is used for incremental aggregation.
	 * @param windowFunction The window function.
	 *
	 * @return The data stream that is the result of applying the window function to the window.
	 *
	 * @param <ACC> The type of the AggregateFunction's accumulator
	 * @param <V> The type of AggregateFunction's result, and the WindowFunction's input
	 * @param <R> The type of the elements in the resulting stream, equal to the
	 *            WindowFunction's result type
	 */
	@PublicEvolving
	public <ACC, V, R> SingleOutputStreamOperator<R> aggregate(
			AggregateFunction<T, ACC, V> aggFunction,
			AllWindowFunction<V, R, W> windowFunction) {

		checkNotNull(aggFunction, "aggFunction");
		checkNotNull(windowFunction, "windowFunction");

		TypeInformation<ACC> accumulatorType = TypeExtractor.getAggregateFunctionAccumulatorType(
				aggFunction, input.getType(), null, false);

		TypeInformation<V> aggResultType = TypeExtractor.getAggregateFunctionReturnType(
				aggFunction, input.getType(), null, false);

		TypeInformation<R> resultType = getAllWindowFunctionReturnType(windowFunction, aggResultType);

		return aggregate(aggFunction, windowFunction, accumulatorType, resultType);
	}

	private static <IN, OUT> TypeInformation<OUT> getAllWindowFunctionReturnType(
			AllWindowFunction<IN, OUT, ?> function,
			TypeInformation<IN> inType) {
		return TypeExtractor.getUnaryOperatorReturnType(
			function,
			AllWindowFunction.class,
			0,
			1,
			new int[]{2, 0},
			inType,
			null,
			false);
	}

	private static <IN, OUT> TypeInformation<OUT> getProcessAllWindowFunctionReturnType(
			ProcessAllWindowFunction<IN, OUT, ?> function,
			TypeInformation<IN> inType) {
		return TypeExtractor.getUnaryOperatorReturnType(
			function,
			ProcessAllWindowFunction.class,
			0,
			1,
			TypeExtractor.NO_INDEX,
			inType,
			null,
			false);
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>Arriving data is incrementally aggregated using the given aggregate function. This means
	 * that the window function typically has only a single value to process when called.
	 *
	 * @param aggregateFunction The aggregation function that is used for incremental aggregation.
	 * @param windowFunction The window function.
	 * @param accumulatorType Type information for the internal accumulator type of the aggregation function
	 * @param resultType Type information for the result type of the window function
	 *
	 * @return The data stream that is the result of applying the window function to the window.
	 *
	 * @param <ACC> The type of the AggregateFunction's accumulator
	 * @param <V> The type of AggregateFunction's result, and the WindowFunction's input
	 * @param <R> The type of the elements in the resulting stream, equal to the
	 *            WindowFunction's result type
	 */
	@PublicEvolving
	public <ACC, V, R> SingleOutputStreamOperator<R> aggregate(
			AggregateFunction<T, ACC, V> aggregateFunction,
			AllWindowFunction<V, R, W> windowFunction,
			TypeInformation<ACC> accumulatorType,
			TypeInformation<R> resultType) {

		checkNotNull(aggregateFunction, "aggregateFunction");
		checkNotNull(windowFunction, "windowFunction");
		checkNotNull(accumulatorType, "accumulatorType");
		checkNotNull(resultType, "resultType");

		if (aggregateFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("This aggregate function cannot be a RichFunction.");
		}

		//clean the closures
		windowFunction = input.getExecutionEnvironment().clean(windowFunction);
		aggregateFunction = input.getExecutionEnvironment().clean(aggregateFunction);

		final String callLocation = Utils.getCallLocationName();
		final String udfName = "AllWindowedStream." + callLocation;

		final String opName;
		final KeySelector<T, Byte> keySel = input.getKeySelector();

		OneInputStreamOperator<T, R> operator;

		if (evictor != null) {
			@SuppressWarnings({"unchecked", "rawtypes"})
			TypeSerializer<StreamRecord<T>> streamRecordSerializer =
					(TypeSerializer<StreamRecord<T>>) new StreamElementSerializer(
							input.getType().createSerializer(getExecutionEnvironment().getConfig()));

			ListStateDescriptor<StreamRecord<T>> stateDesc =
					new ListStateDescriptor<>("window-contents", streamRecordSerializer);

			opName = "TriggerWindow(" + windowAssigner + ", " + stateDesc + ", " + trigger + ", " + evictor + ", " + udfName + ")";

			operator =
					new EvictingWindowOperator<>(windowAssigner,
							windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
							keySel,
							input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
							stateDesc,
							new InternalIterableAllWindowFunction<>(
									new AggregateApplyAllWindowFunction<>(aggregateFunction, windowFunction)),
							trigger,
							evictor,
							allowedLateness,
							lateDataOutputTag);

		} else {
			AggregatingStateDescriptor<T, ACC, V> stateDesc = new AggregatingStateDescriptor<>(
					"window-contents",
					aggregateFunction,
					accumulatorType.createSerializer(getExecutionEnvironment().getConfig()));

			opName = "TriggerWindow(" + windowAssigner + ", " + stateDesc + ", " + trigger + ", " + udfName + ")";

			operator = new WindowOperator<>(
							windowAssigner,
							windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
							keySel,
							input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
							stateDesc,
							new InternalSingleValueAllWindowFunction<>(windowFunction),
							trigger,
							allowedLateness,
							lateDataOutputTag);
		}

		return input.transform(opName, resultType, operator).forceNonParallel();
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>Arriving data is incrementally aggregated using the given aggregate function. This means
	 * that the window function typically has only a single value to process when called.
	 *
	 * @param aggFunction The aggregate function that is used for incremental aggregation.
	 * @param windowFunction The process window function.
	 *
	 * @return The data stream that is the result of applying the window function to the window.
	 *
	 * @param <ACC> The type of the AggregateFunction's accumulator
	 * @param <V> The type of AggregateFunction's result, and the WindowFunction's input
	 * @param <R> The type of the elements in the resulting stream, equal to the
	 *            WindowFunction's result type
	 */
	@PublicEvolving
	public <ACC, V, R> SingleOutputStreamOperator<R> aggregate(
			AggregateFunction<T, ACC, V> aggFunction,
			ProcessAllWindowFunction<V, R, W> windowFunction) {

		checkNotNull(aggFunction, "aggFunction");
		checkNotNull(windowFunction, "windowFunction");

		TypeInformation<ACC> accumulatorType = TypeExtractor.getAggregateFunctionAccumulatorType(
				aggFunction, input.getType(), null, false);

		TypeInformation<V> aggResultType = TypeExtractor.getAggregateFunctionReturnType(
				aggFunction, input.getType(), null, false);

		TypeInformation<R> resultType = getProcessAllWindowFunctionReturnType(windowFunction, aggResultType);

		return aggregate(aggFunction, windowFunction, accumulatorType, aggResultType, resultType);
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>Arriving data is incrementally aggregated using the given aggregate function. This means
	 * that the window function typically has only a single value to process when called.
	 *
	 * @param aggregateFunction The aggregation function that is used for incremental aggregation.
	 * @param windowFunction The process window function.
	 * @param accumulatorType Type information for the internal accumulator type of the aggregation function
	 * @param resultType Type information for the result type of the window function
	 *
	 * @return The data stream that is the result of applying the window function to the window.
	 *
	 * @param <ACC> The type of the AggregateFunction's accumulator
	 * @param <V> The type of AggregateFunction's result, and the WindowFunction's input
	 * @param <R> The type of the elements in the resulting stream, equal to the
	 *            WindowFunction's result type
	 */
	@PublicEvolving
	public <ACC, V, R> SingleOutputStreamOperator<R> aggregate(
			AggregateFunction<T, ACC, V> aggregateFunction,
			ProcessAllWindowFunction<V, R, W> windowFunction,
			TypeInformation<ACC> accumulatorType,
			TypeInformation<V> aggregateResultType,
			TypeInformation<R> resultType) {

		checkNotNull(aggregateFunction, "aggregateFunction");
		checkNotNull(windowFunction, "windowFunction");
		checkNotNull(accumulatorType, "accumulatorType");
		checkNotNull(aggregateResultType, "aggregateResultType");
		checkNotNull(resultType, "resultType");

		if (aggregateFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("This aggregate function cannot be a RichFunction.");
		}

		//clean the closures
		windowFunction = input.getExecutionEnvironment().clean(windowFunction);
		aggregateFunction = input.getExecutionEnvironment().clean(aggregateFunction);

		final String callLocation = Utils.getCallLocationName();
		final String udfName = "AllWindowedStream." + callLocation;

		final String opName;
		final KeySelector<T, Byte> keySel = input.getKeySelector();

		OneInputStreamOperator<T, R> operator;

		if (evictor != null) {
			@SuppressWarnings({"unchecked", "rawtypes"})
			TypeSerializer<StreamRecord<T>> streamRecordSerializer =
					(TypeSerializer<StreamRecord<T>>) new StreamElementSerializer(
							input.getType().createSerializer(getExecutionEnvironment().getConfig()));

			ListStateDescriptor<StreamRecord<T>> stateDesc =
					new ListStateDescriptor<>("window-contents", streamRecordSerializer);

			opName = "TriggerWindow(" + windowAssigner + ", " + stateDesc + ", " + trigger + ", " + evictor + ", " + udfName + ")";

			operator = new EvictingWindowOperator<>(windowAssigner,
					windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
					keySel,
					input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
					stateDesc,
					new InternalAggregateProcessAllWindowFunction<>(aggregateFunction, windowFunction),
					trigger,
					evictor,
					allowedLateness,
					lateDataOutputTag);

		} else {
			AggregatingStateDescriptor<T, ACC, V> stateDesc = new AggregatingStateDescriptor<>(
					"window-contents",
					aggregateFunction,
					accumulatorType.createSerializer(getExecutionEnvironment().getConfig()));

			opName = "TriggerWindow(" + windowAssigner + ", " + stateDesc + ", " + trigger + ", " + udfName + ")";

			operator = new WindowOperator<>(
					windowAssigner,
					windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
					keySel,
					input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
					stateDesc,
					new InternalSingleValueProcessAllWindowFunction<>(windowFunction),
					trigger,
					allowedLateness,
					lateDataOutputTag);
		}

		return input.transform(opName, resultType, operator).forceNonParallel();
	}

	// ------------------------------------------------------------------------
	//  FoldFunction
	// ------------------------------------------------------------------------

	/**
	 * Applies the given fold function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the reduce function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * @param function The fold function.
	 * @return The data stream that is the result of applying the fold function to the window.
	 *
	 * @deprecated use {@link #aggregate(AggregateFunction)} instead
	 */
	@Deprecated
	public <R> SingleOutputStreamOperator<R> fold(R initialValue, FoldFunction<T, R> function) {
		if (function instanceof RichFunction) {
			throw new UnsupportedOperationException("FoldFunction of fold can not be a RichFunction. " +
					"Please use fold(FoldFunction, WindowFunction) instead.");
		}

		TypeInformation<R> resultType = TypeExtractor.getFoldReturnTypes(function, input.getType(),
				Utils.getCallLocationName(), true);

		return fold(initialValue, function, resultType);
	}

	/**
	 * Applies the given fold function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the reduce function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * @param function The fold function.
	 * @return The data stream that is the result of applying the fold function to the window.
	 *
	 * @deprecated use {@link #aggregate(AggregateFunction, TypeInformation, TypeInformation)} instead
	 */
	@Deprecated
	public <R> SingleOutputStreamOperator<R> fold(R initialValue, FoldFunction<T, R> function, TypeInformation<R> resultType) {
		if (function instanceof RichFunction) {
			throw new UnsupportedOperationException("FoldFunction of fold can not be a RichFunction. " +
					"Please use fold(FoldFunction, WindowFunction) instead.");
		}

		return fold(initialValue, function, new PassThroughAllWindowFunction<W, R>(), resultType, resultType);
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>Arriving data is incrementally aggregated using the given fold function.
	 *
	 * @param initialValue The initial value of the fold.
	 * @param foldFunction The fold function that is used for incremental aggregation.
	 * @param function The window function.
	 * @return The data stream that is the result of applying the window function to the window.
	 *
	 * @deprecated use {@link #aggregate(AggregateFunction, ProcessAllWindowFunction)} instead
	 */
	@PublicEvolving
	@Deprecated
	public <ACC, R> SingleOutputStreamOperator<R> fold(ACC initialValue, FoldFunction<T, ACC> foldFunction, AllWindowFunction<ACC, R, W> function) {

		TypeInformation<ACC> foldAccumulatorType = TypeExtractor.getFoldReturnTypes(foldFunction, input.getType(),
			Utils.getCallLocationName(), true);

		TypeInformation<R> resultType = getAllWindowFunctionReturnType(function, foldAccumulatorType);

		return fold(initialValue, foldFunction, function, foldAccumulatorType, resultType);
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>Arriving data is incrementally aggregated using the given fold function.
	 *
	 * @param initialValue The initial value of the fold.
	 * @param foldFunction The fold function that is used for incremental aggregation.
	 * @param function The window function.
	 * @param foldAccumulatorType Type information for the result type of the fold function
	 * @param resultType Type information for the result type of the window function
	 * @return The data stream that is the result of applying the window function to the window.
	 *
	 * @deprecated use {@link #aggregate(AggregateFunction, AllWindowFunction, TypeInformation, TypeInformation)} instead
	 */
	@PublicEvolving
	@Deprecated
	public <ACC, R> SingleOutputStreamOperator<R> fold(ACC initialValue,
			FoldFunction<T, ACC> foldFunction,
			AllWindowFunction<ACC, R, W> function,
			TypeInformation<ACC> foldAccumulatorType,
			TypeInformation<R> resultType) {
		if (foldFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("FoldFunction of fold can not be a RichFunction.");
		}
		if (windowAssigner instanceof MergingWindowAssigner) {
			throw new UnsupportedOperationException("Fold cannot be used with a merging WindowAssigner.");
		}

		//clean the closures
		function = input.getExecutionEnvironment().clean(function);
		foldFunction = input.getExecutionEnvironment().clean(foldFunction);

		String callLocation = Utils.getCallLocationName();
		String udfName = "AllWindowedStream." + callLocation;

		String opName;
		KeySelector<T, Byte> keySel = input.getKeySelector();

		OneInputStreamOperator<T, R> operator;

		if (evictor != null) {
			@SuppressWarnings({"unchecked", "rawtypes"})
			TypeSerializer<StreamRecord<T>> streamRecordSerializer =
				(TypeSerializer<StreamRecord<T>>) new StreamElementSerializer(input.getType().createSerializer(getExecutionEnvironment().getConfig()));

			ListStateDescriptor<StreamRecord<T>> stateDesc =
				new ListStateDescriptor<>("window-contents", streamRecordSerializer);

			opName = "TriggerWindow(" + windowAssigner + ", " + stateDesc + ", " + trigger + ", " + evictor + ", " + udfName + ")";

			operator =
				new EvictingWindowOperator<>(windowAssigner,
					windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
					keySel,
					input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
					stateDesc,
					new InternalIterableAllWindowFunction<>(new FoldApplyAllWindowFunction<>(initialValue, foldFunction, function, foldAccumulatorType)),
					trigger,
					evictor,
					allowedLateness,
					lateDataOutputTag);

		} else {
			FoldingStateDescriptor<T, ACC> stateDesc = new FoldingStateDescriptor<>("window-contents",
				initialValue, foldFunction, foldAccumulatorType.createSerializer(getExecutionEnvironment().getConfig()));

			opName = "TriggerWindow(" + windowAssigner + ", " + stateDesc + ", " + trigger + ", " + udfName + ")";

			operator =
				new WindowOperator<>(windowAssigner,
					windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
					keySel,
					input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
					stateDesc,
					new InternalSingleValueAllWindowFunction<>(function),
					trigger,
					allowedLateness,
					lateDataOutputTag);
		}

		return input.transform(opName, resultType, operator).forceNonParallel();
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>Arriving data is incrementally aggregated using the given fold function.
	 *
	 * @param initialValue The initial value of the fold.
	 * @param foldFunction The fold function that is used for incremental aggregation.
	 * @param function The window function.
	 * @return The data stream that is the result of applying the window function to the window.
	 *
	 * @deprecated use {@link #aggregate(AggregateFunction, ProcessAllWindowFunction)} instead
	 */
	@PublicEvolving
	@Deprecated
	public <ACC, R> SingleOutputStreamOperator<R> fold(ACC initialValue, FoldFunction<T, ACC> foldFunction, ProcessAllWindowFunction<ACC, R, W> function) {

		TypeInformation<ACC> foldAccumulatorType = TypeExtractor.getFoldReturnTypes(foldFunction, input.getType(),
			Utils.getCallLocationName(), true);

		TypeInformation<R> resultType = getProcessAllWindowFunctionReturnType(function, foldAccumulatorType);

		return fold(initialValue, foldFunction, function, foldAccumulatorType, resultType);
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>Arriving data is incrementally aggregated using the given fold function.
	 *
	 * @param initialValue The initial value of the fold.
	 * @param foldFunction The fold function that is used for incremental aggregation.
	 * @param function The process window function.
	 * @param foldAccumulatorType Type information for the result type of the fold function
	 * @param resultType Type information for the result type of the window function
	 * @return The data stream that is the result of applying the window function to the window.
	 *
	 * @deprecated use {@link #aggregate(AggregateFunction, ProcessAllWindowFunction, TypeInformation, TypeInformation, TypeInformation)} instead
	 */
	@PublicEvolving
	@Deprecated
	public <ACC, R> SingleOutputStreamOperator<R> fold(ACC initialValue,
			FoldFunction<T, ACC> foldFunction,
			ProcessAllWindowFunction<ACC, R, W> function,
			TypeInformation<ACC> foldAccumulatorType,
			TypeInformation<R> resultType) {
		if (foldFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("FoldFunction of fold can not be a RichFunction.");
		}
		if (windowAssigner instanceof MergingWindowAssigner) {
			throw new UnsupportedOperationException("Fold cannot be used with a merging WindowAssigner.");
		}

		//clean the closures
		function = input.getExecutionEnvironment().clean(function);
		foldFunction = input.getExecutionEnvironment().clean(foldFunction);

		String callLocation = Utils.getCallLocationName();
		String udfName = "AllWindowedStream." + callLocation;

		String opName;
		KeySelector<T, Byte> keySel = input.getKeySelector();

		OneInputStreamOperator<T, R> operator;

		if (evictor != null) {
			@SuppressWarnings({"unchecked", "rawtypes"})
			TypeSerializer<StreamRecord<T>> streamRecordSerializer =
				(TypeSerializer<StreamRecord<T>>) new StreamElementSerializer(input.getType().createSerializer(getExecutionEnvironment().getConfig()));

			ListStateDescriptor<StreamRecord<T>> stateDesc =
				new ListStateDescriptor<>("window-contents", streamRecordSerializer);

			opName = "TriggerWindow(" + windowAssigner + ", " + stateDesc + ", " + trigger + ", " + evictor + ", " + udfName + ")";

			operator =
				new EvictingWindowOperator<>(windowAssigner,
					windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
					keySel,
					input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
					stateDesc,
					new InternalIterableProcessAllWindowFunction<>(new FoldApplyProcessAllWindowFunction<>(initialValue, foldFunction, function, foldAccumulatorType)),
					trigger,
					evictor,
					allowedLateness,
					lateDataOutputTag);

		} else {
			FoldingStateDescriptor<T, ACC> stateDesc = new FoldingStateDescriptor<>("window-contents",
				initialValue, foldFunction, foldAccumulatorType.createSerializer(getExecutionEnvironment().getConfig()));

			opName = "TriggerWindow(" + windowAssigner + ", " + stateDesc + ", " + trigger + ", " + udfName + ")";

			operator =
				new WindowOperator<>(windowAssigner,
					windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
					keySel,
					input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
					stateDesc,
					new InternalSingleValueProcessAllWindowFunction<>(function),
					trigger,
					allowedLateness,
					lateDataOutputTag);
		}

		return input.transform(opName, resultType, operator).forceNonParallel();
	}

	// ------------------------------------------------------------------------
	//  Apply (Window Function)
	// ------------------------------------------------------------------------

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>Note that this function requires that all data in the windows is buffered until the window
	 * is evaluated, as the function provides no means of incremental aggregation.
	 *
	 * @param function The window function.
	 * @return The data stream that is the result of applying the window function to the window.
	 */
	public <R> SingleOutputStreamOperator<R> apply(AllWindowFunction<T, R, W> function) {
		String callLocation = Utils.getCallLocationName();
		function = input.getExecutionEnvironment().clean(function);
		TypeInformation<R> resultType = getAllWindowFunctionReturnType(function, getInputType());
		return apply(new InternalIterableAllWindowFunction<>(function), resultType, callLocation);
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>Note that this function requires that all data in the windows is buffered until the window
	 * is evaluated, as the function provides no means of incremental aggregation.
	 *
	 * @param function The window function.
	 * @return The data stream that is the result of applying the window function to the window.
	 */
	public <R> SingleOutputStreamOperator<R> apply(AllWindowFunction<T, R, W> function, TypeInformation<R> resultType) {
		String callLocation = Utils.getCallLocationName();
		function = input.getExecutionEnvironment().clean(function);
		return apply(new InternalIterableAllWindowFunction<>(function), resultType, callLocation);
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>Note that this function requires that all data in the windows is buffered until the window
	 * is evaluated, as the function provides no means of incremental aggregation.
	 *
	 * @param function The process window function.
	 * @return The data stream that is the result of applying the window function to the window.
	 */
	@PublicEvolving
	public <R> SingleOutputStreamOperator<R> process(ProcessAllWindowFunction<T, R, W> function) {
		String callLocation = Utils.getCallLocationName();
		function = input.getExecutionEnvironment().clean(function);
		TypeInformation<R> resultType = getProcessAllWindowFunctionReturnType(function, getInputType());
		return apply(new InternalIterableProcessAllWindowFunction<>(function), resultType, callLocation);
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>Note that this function requires that all data in the windows is buffered until the window
	 * is evaluated, as the function provides no means of incremental aggregation.
	 *
	 * @param function The process window function.
	 * @return The data stream that is the result of applying the window function to the window.
	 */
	@PublicEvolving
	public <R> SingleOutputStreamOperator<R> process(ProcessAllWindowFunction<T, R, W> function, TypeInformation<R> resultType) {
		String callLocation = Utils.getCallLocationName();
		function = input.getExecutionEnvironment().clean(function);
		return apply(new InternalIterableProcessAllWindowFunction<>(function), resultType, callLocation);
	}

	private <R> SingleOutputStreamOperator<R> apply(InternalWindowFunction<Iterable<T>, R, Byte, W> function, TypeInformation<R> resultType, String callLocation) {

		String udfName = "AllWindowedStream." + callLocation;

		String opName;
		KeySelector<T, Byte> keySel = input.getKeySelector();

		WindowOperator<Byte, T, Iterable<T>, R, W> operator;

		if (evictor != null) {
			@SuppressWarnings({"unchecked", "rawtypes"})
			TypeSerializer<StreamRecord<T>> streamRecordSerializer =
					(TypeSerializer<StreamRecord<T>>) new StreamElementSerializer(input.getType().createSerializer(getExecutionEnvironment().getConfig()));

			ListStateDescriptor<StreamRecord<T>> stateDesc =
					new ListStateDescriptor<>("window-contents", streamRecordSerializer);

			opName = "TriggerWindow(" + windowAssigner + ", " + stateDesc + ", " + trigger + ", " + evictor + ", " + udfName + ")";

			operator =
				new EvictingWindowOperator<>(windowAssigner,
					windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
					keySel,
					input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
					stateDesc,
					function,
					trigger,
					evictor,
					allowedLateness,
					lateDataOutputTag);

		} else {
			ListStateDescriptor<T> stateDesc = new ListStateDescriptor<>("window-contents",
					input.getType().createSerializer(getExecutionEnvironment().getConfig()));

			opName = "TriggerWindow(" + windowAssigner + ", " + stateDesc + ", " + trigger + ", " + udfName + ")";

			operator =
				new WindowOperator<>(windowAssigner,
					windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
					keySel,
					input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
					stateDesc,
					function,
					trigger,
					allowedLateness,
					lateDataOutputTag);
		}

		return input.transform(opName, resultType, operator).forceNonParallel();
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>Arriving data is incrementally aggregated using the given reducer.
	 *
	 * @param reduceFunction The reduce function that is used for incremental aggregation.
	 * @param function The window function.
	 * @return The data stream that is the result of applying the window function to the window.
	 *
	 * @deprecated Use {@link #reduce(ReduceFunction, AllWindowFunction)} instead.
	 */
	@Deprecated
	public <R> SingleOutputStreamOperator<R> apply(ReduceFunction<T> reduceFunction, AllWindowFunction<T, R, W> function) {
		TypeInformation<T> inType = input.getType();
		TypeInformation<R> resultType = getAllWindowFunctionReturnType(function, inType);

		return apply(reduceFunction, function, resultType);
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>Arriving data is incrementally aggregated using the given reducer.
	 *
	 * @param reduceFunction The reduce function that is used for incremental aggregation.
	 * @param function The window function.
	 * @param resultType Type information for the result type of the window function
	 * @return The data stream that is the result of applying the window function to the window.
	 *
	 * @deprecated Use {@link #reduce(ReduceFunction, AllWindowFunction, TypeInformation)} instead.
	 */
	@Deprecated
	public <R> SingleOutputStreamOperator<R> apply(ReduceFunction<T> reduceFunction, AllWindowFunction<T, R, W> function, TypeInformation<R> resultType) {
		if (reduceFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("ReduceFunction of apply can not be a RichFunction.");
		}

		//clean the closures
		function = input.getExecutionEnvironment().clean(function);
		reduceFunction = input.getExecutionEnvironment().clean(reduceFunction);

		String callLocation = Utils.getCallLocationName();
		String udfName = "AllWindowedStream." + callLocation;

		String opName;
		KeySelector<T, Byte> keySel = input.getKeySelector();

		OneInputStreamOperator<T, R> operator;

		if (evictor != null) {
			@SuppressWarnings({"unchecked", "rawtypes"})
			TypeSerializer<StreamRecord<T>> streamRecordSerializer =
					(TypeSerializer<StreamRecord<T>>) new StreamElementSerializer(input.getType().createSerializer(getExecutionEnvironment().getConfig()));

			ListStateDescriptor<StreamRecord<T>> stateDesc =
					new ListStateDescriptor<>("window-contents", streamRecordSerializer);

			opName = "TriggerWindow(" + windowAssigner + ", " + stateDesc + ", " + trigger + ", " + evictor + ", " + udfName + ")";

			operator =
				new EvictingWindowOperator<>(windowAssigner,
					windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
					keySel,
					input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
					stateDesc,
					new InternalIterableAllWindowFunction<>(new ReduceApplyAllWindowFunction<>(reduceFunction, function)),
					trigger,
					evictor,
					allowedLateness,
					lateDataOutputTag);

		} else {
			ReducingStateDescriptor<T> stateDesc = new ReducingStateDescriptor<>("window-contents",
					reduceFunction,
					input.getType().createSerializer(getExecutionEnvironment().getConfig()));

			opName = "TriggerWindow(" + windowAssigner + ", " + stateDesc + ", " + trigger + ", " + udfName + ")";

			operator =
				new WindowOperator<>(windowAssigner,
					windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
					keySel,
					input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
					stateDesc,
					new InternalSingleValueAllWindowFunction<>(function),
					trigger,
					allowedLateness,
					lateDataOutputTag);
		}

		return input.transform(opName, resultType, operator).forceNonParallel();
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>Arriving data is incrementally aggregated using the given fold function.
	 *
	 * @param initialValue The initial value of the fold.
	 * @param foldFunction The fold function that is used for incremental aggregation.
	 * @param function The window function.
	 * @return The data stream that is the result of applying the window function to the window.
	 *
	 * @deprecated Use {@link #fold(Object, FoldFunction, AllWindowFunction)} instead.
	 */
	@Deprecated
	public <R> SingleOutputStreamOperator<R> apply(R initialValue, FoldFunction<T, R> foldFunction, AllWindowFunction<R, R, W> function) {

		TypeInformation<R> resultType = TypeExtractor.getFoldReturnTypes(foldFunction, input.getType(),
				Utils.getCallLocationName(), true);

		return apply(initialValue, foldFunction, function, resultType);
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>Arriving data is incrementally aggregated using the given fold function.
	 *
	 * @param initialValue The initial value of the fold.
	 * @param foldFunction The fold function that is used for incremental aggregation.
	 * @param function The window function.
	 * @param resultType Type information for the result type of the window function
	 * @return The data stream that is the result of applying the window function to the window.
	 *
	 * @deprecated Use {@link #fold(Object, FoldFunction, AllWindowFunction, TypeInformation, TypeInformation)} instead.
	 */
	@Deprecated
	public <R> SingleOutputStreamOperator<R> apply(R initialValue, FoldFunction<T, R> foldFunction, AllWindowFunction<R, R, W> function, TypeInformation<R> resultType) {
		if (foldFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("ReduceFunction of apply can not be a RichFunction.");
		}
		if (windowAssigner instanceof MergingWindowAssigner) {
			throw new UnsupportedOperationException("Fold cannot be used with a merging WindowAssigner.");
		}

		//clean the closures
		function = input.getExecutionEnvironment().clean(function);
		foldFunction = input.getExecutionEnvironment().clean(foldFunction);

		String callLocation = Utils.getCallLocationName();
		String udfName = "AllWindowedStream." + callLocation;

		String opName;
		KeySelector<T, Byte> keySel = input.getKeySelector();

		OneInputStreamOperator<T, R> operator;

		if (evictor != null) {
			@SuppressWarnings({"unchecked", "rawtypes"})
			TypeSerializer<StreamRecord<T>> streamRecordSerializer =
					(TypeSerializer<StreamRecord<T>>) new StreamElementSerializer(input.getType().createSerializer(getExecutionEnvironment().getConfig()));

			ListStateDescriptor<StreamRecord<T>> stateDesc =
					new ListStateDescriptor<>("window-contents", streamRecordSerializer);

			opName = "TriggerWindow(" + windowAssigner + ", " + stateDesc + ", " + trigger + ", " + evictor + ", " + udfName + ")";

			operator =
				new EvictingWindowOperator<>(windowAssigner,
					windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
					keySel,
					input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
					stateDesc,
					new InternalIterableAllWindowFunction<>(new FoldApplyAllWindowFunction<>(initialValue, foldFunction, function, resultType)),
					trigger,
					evictor,
					allowedLateness,
					lateDataOutputTag);

		} else {
			FoldingStateDescriptor<T, R> stateDesc = new FoldingStateDescriptor<>("window-contents",
					initialValue, foldFunction, resultType.createSerializer(getExecutionEnvironment().getConfig()));

			opName = "TriggerWindow(" + windowAssigner + ", " + stateDesc + ", " + trigger + ", " + udfName + ")";

			operator =
				new WindowOperator<>(windowAssigner,
					windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
					keySel,
					input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
					stateDesc,
					new InternalSingleValueAllWindowFunction<>(function),
					trigger,
					allowedLateness,
					lateDataOutputTag);
		}

		return input.transform(opName, resultType, operator).forceNonParallel();
	}

	// ------------------------------------------------------------------------
	//  Aggregations on the all windows
	// ------------------------------------------------------------------------

	/**
	 * Applies an aggregation that sums every window of the data stream at the
	 * given position.
	 *
	 * @param positionToSum The position in the tuple/array to sum
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> sum(int positionToSum) {
		return aggregate(new SumAggregator<>(positionToSum, input.getType(), input.getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that sums every window of the pojo data stream at
	 * the given field for every window.
	 *
	 * <p>A field expression is either the name of a public field or a getter method with
	 * parentheses of the stream's underlying type. A dot can be used to drill down into objects,
	 * as in {@code "field1.getInnerField2()" }.
	 *
	 * @param field The field to sum
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> sum(String field) {
		return aggregate(new SumAggregator<>(field, input.getType(), input.getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that that gives the minimum value of every window
	 * of the data stream at the given position.
	 *
	 * @param positionToMin The position to minimize
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> min(int positionToMin) {
		return aggregate(new ComparableAggregator<>(positionToMin, input.getType(), AggregationFunction.AggregationType.MIN, input.getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that that gives the minimum value of the pojo data
	 * stream at the given field expression for every window.
	 *
	 * <p>A field expression is either the name of a public field or a getter method with
	 * parentheses of the {@link DataStream}S underlying type. A dot can be used to drill down into
	 * objects, as in {@code "field1.getInnerField2()" }.
	 *
	 * @param field The field expression based on which the aggregation will be applied.
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> min(String field) {
		return aggregate(new ComparableAggregator<>(field, input.getType(), AggregationFunction.AggregationType.MIN, false, input.getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that gives the minimum element of every window of
	 * the data stream by the given position. If more elements have the same
	 * minimum value the operator returns the first element by default.
	 *
	 * @param positionToMinBy
	 *            The position to minimize by
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> minBy(int positionToMinBy) {
		return this.minBy(positionToMinBy, true);
	}

	/**
	 * Applies an aggregation that gives the minimum element of every window of
	 * the data stream by the given position. If more elements have the same
	 * minimum value the operator returns the first element by default.
	 *
	 * @param positionToMinBy The position to minimize by
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> minBy(String positionToMinBy) {
		return this.minBy(positionToMinBy, true);
	}

	/**
	 * Applies an aggregation that gives the minimum element of every window of
	 * the data stream by the given position. If more elements have the same
	 * minimum value the operator returns either the first or last one depending
	 * on the parameter setting.
	 *
	 * @param positionToMinBy The position to minimize
	 * @param first If true, then the operator return the first element with the minimum value, otherwise returns the last
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> minBy(int positionToMinBy, boolean first) {
		return aggregate(new ComparableAggregator<>(positionToMinBy, input.getType(), AggregationFunction.AggregationType.MINBY, first, input.getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that that gives the minimum element of the pojo
	 * data stream by the given field expression for every window. A field
	 * expression is either the name of a public field or a getter method with
	 * parentheses of the {@link DataStream DataStreams} underlying type. A dot can be used
	 * to drill down into objects, as in {@code "field1.getInnerField2()" }.
	 *
	 * @param field The field expression based on which the aggregation will be applied.
	 * @param first If True then in case of field equality the first object will be returned
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> minBy(String field, boolean first) {
		return aggregate(new ComparableAggregator<>(field, input.getType(), AggregationFunction.AggregationType.MINBY, first, input.getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that gives the maximum value of every window of
	 * the data stream at the given position.
	 *
	 * @param positionToMax The position to maximize
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> max(int positionToMax) {
		return aggregate(new ComparableAggregator<>(positionToMax, input.getType(), AggregationFunction.AggregationType.MAX, input.getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that that gives the maximum value of the pojo data
	 * stream at the given field expression for every window. A field expression
	 * is either the name of a public field or a getter method with parentheses
	 * of the {@link DataStream DataStreams} underlying type. A dot can be used to drill
	 * down into objects, as in {@code "field1.getInnerField2()" }.
	 *
	 * @param field The field expression based on which the aggregation will be applied.
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> max(String field) {
		return aggregate(new ComparableAggregator<>(field, input.getType(), AggregationFunction.AggregationType.MAX, false, input.getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that gives the maximum element of every window of
	 * the data stream by the given position. If more elements have the same
	 * maximum value the operator returns the first by default.
	 *
	 * @param positionToMaxBy
	 *            The position to maximize by
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> maxBy(int positionToMaxBy) {
		return this.maxBy(positionToMaxBy, true);
	}

	/**
	 * Applies an aggregation that gives the maximum element of every window of
	 * the data stream by the given position. If more elements have the same
	 * maximum value the operator returns the first by default.
	 *
	 * @param positionToMaxBy
	 *            The position to maximize by
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> maxBy(String positionToMaxBy) {
		return this.maxBy(positionToMaxBy, true);
	}

	/**
	 * Applies an aggregation that gives the maximum element of every window of
	 * the data stream by the given position. If more elements have the same
	 * maximum value the operator returns either the first or last one depending
	 * on the parameter setting.
	 *
	 * @param positionToMaxBy The position to maximize by
	 * @param first If true, then the operator return the first element with the maximum value, otherwise returns the last
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> maxBy(int positionToMaxBy, boolean first) {
		return aggregate(new ComparableAggregator<>(positionToMaxBy, input.getType(), AggregationFunction.AggregationType.MAXBY, first, input.getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that that gives the maximum element of the pojo
	 * data stream by the given field expression for every window. A field
	 * expression is either the name of a public field or a getter method with
	 * parentheses of the {@link DataStream}S underlying type. A dot can be used
	 * to drill down into objects, as in {@code "field1.getInnerField2()" }.
	 *
	 * @param field The field expression based on which the aggregation will be applied.
	 * @param first If True then in case of field equality the first object will be returned
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T> maxBy(String field, boolean first) {
		return aggregate(new ComparableAggregator<>(field, input.getType(), AggregationFunction.AggregationType.MAXBY, first, input.getExecutionConfig()));
	}

	private SingleOutputStreamOperator<T> aggregate(AggregationFunction<T> aggregator) {
		return reduce(aggregator);
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	public StreamExecutionEnvironment getExecutionEnvironment() {
		return input.getExecutionEnvironment();
	}

	public TypeInformation<T> getInputType() {
		return input.getType();
	}
}
