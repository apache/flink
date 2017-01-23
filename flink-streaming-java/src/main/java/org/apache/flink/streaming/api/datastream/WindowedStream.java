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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.functions.aggregation.ComparableAggregator;
import org.apache.flink.streaming.api.functions.aggregation.SumAggregator;
import org.apache.flink.streaming.api.functions.windowing.FoldApplyWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.PassThroughWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ReduceApplyWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.BaseAlignedWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.SlidingAlignedProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingAlignedProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.AccumulatingProcessingTimeWindowOperator;
import org.apache.flink.streaming.runtime.operators.windowing.AggregatingProcessingTimeWindowOperator;
import org.apache.flink.streaming.runtime.operators.windowing.EvictingWindowOperator;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalSingleValueWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A {@code WindowedStream} represents a data stream where elements are grouped by
 * key, and for each key, the stream of elements is split into windows based on a
 * {@link org.apache.flink.streaming.api.windowing.assigners.WindowAssigner}. Window emission
 * is triggered based on a {@link org.apache.flink.streaming.api.windowing.triggers.Trigger}.
 *
 * <p>
 * The windows are conceptually evaluated for each key individually, meaning windows can trigger at
 * different points for each key.
 *
 * <p>
 * If an {@link Evictor} is specified it will be used to evict elements from the window after
 * evaluation was triggered by the {@code Trigger} but before the actual evaluation of the window.
 * When using an evictor window performance will degrade significantly, since
 * incremental aggregation of window results cannot be used.
 *
 * <p>
 * Note that the {@code WindowedStream} is purely and API construct, during runtime
 * the {@code WindowedStream} will be collapsed together with the
 * {@code KeyedStream} and the operation over the window into one single operation.
 * 
 * @param <T> The type of elements in the stream.
 * @param <K> The type of the key by which elements are grouped.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns the elements to.
 */
@Public
public class WindowedStream<T, K, W extends Window> {

	/** The keyed data stream that is windowed by this stream */
	private final KeyedStream<T, K> input;

	/** The window assigner */
	private final WindowAssigner<? super T, W> windowAssigner;

	/** The trigger that is used for window evaluation/emission. */
	private Trigger<? super T, ? super W> trigger;

	/** The evictor that is used for evicting elements before window evaluation. */
	private Evictor<? super T, ? super W> evictor;

	/** The user-specified allowed lateness. */
	private long allowedLateness = 0L;

	@PublicEvolving
	public WindowedStream(KeyedStream<T, K> input,
			WindowAssigner<? super T, W> windowAssigner) {
		this.input = input;
		this.windowAssigner = windowAssigner;
		this.trigger = windowAssigner.getDefaultTrigger(input.getExecutionEnvironment());
	}

	/**
	 * Sets the {@code Trigger} that should be used to trigger window emission.
	 */
	@PublicEvolving
	public WindowedStream<T, K, W> trigger(Trigger<? super T, ? super W> trigger) {
		if (windowAssigner instanceof MergingWindowAssigner && !trigger.canMerge()) {
			throw new UnsupportedOperationException("A merging window assigner cannot be used with a trigger that does not support merging.");
		}

		if (windowAssigner instanceof BaseAlignedWindowAssigner) {
			throw new UnsupportedOperationException("Cannot use a " + windowAssigner.getClass().getSimpleName() + " with a custom trigger.");
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
	public WindowedStream<T, K, W> allowedLateness(Time lateness) {
		final long millis = lateness.toMilliseconds();
		checkArgument(millis >= 0, "The allowed lateness cannot be negative.");

		this.allowedLateness = millis;
		return this;
	}

	/**
	 * Sets the {@code Evictor} that should be used to evict elements from a window before emission.
	 *
	 * <p>
	 * Note: When using an evictor window performance will degrade significantly, since
	 * incremental aggregation of window results cannot be used.
	 */
	@PublicEvolving
	public WindowedStream<T, K, W> evictor(Evictor<? super T, ? super W> evictor) {
		if (windowAssigner instanceof MergingWindowAssigner) {
			throw new UnsupportedOperationException("Cannot use a merging WindowAssigner with an Evictor.");
		}

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
	 * <p>
	 * This window will try and incrementally aggregate data as much as the window policies permit.
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
		String udfName = "WindowedStream." + callLocation;

		SingleOutputStreamOperator<T> result = createFastTimeOperatorIfValid(function, input.getType(), udfName);
		if (result != null) {
			return result;
		}

		LegacyWindowOperatorType legacyOpType = getLegacyWindowType(function);
		return reduce(function, new PassThroughWindowFunction<K, W, T>(), legacyOpType);
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>
	 * Arriving data is incrementally aggregated using the given reducer.
	 *
	 * @param reduceFunction The reduce function that is used for incremental aggregation.
	 * @param function The window function.
	 * @return The data stream that is the result of applying the window function to the window.
	 */
	@PublicEvolving
	public <R> SingleOutputStreamOperator<R> reduce(ReduceFunction<T> reduceFunction, WindowFunction<T, R, K, W> function) {
		return reduce(reduceFunction, function, LegacyWindowOperatorType.NONE);
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>
	 * Arriving data is incrementally aggregated using the given reducer.
	 *
	 * @param reduceFunction The reduce function that is used for incremental aggregation.
	 * @param function The window function.
	 * @param resultType Type information for the result type of the window function
	 * @return The data stream that is the result of applying the window function to the window.
	 */
	@PublicEvolving
	public <R> SingleOutputStreamOperator<R> reduce(
		ReduceFunction<T> reduceFunction,
		WindowFunction<T, R, K, W> function,
		TypeInformation<R> resultType) {
		return reduce(reduceFunction, function, resultType, LegacyWindowOperatorType.NONE);
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>
	 * Arriving data is incrementally aggregated using the given reducer.
	 *
	 * @param reduceFunction The reduce function that is used for incremental aggregation.
	 * @param function The window function.
	 * @param legacyWindowOpType When migrating from an older Flink version, this flag indicates
	 *                           the type of the previous operator whose state we inherit.
	 * @return The data stream that is the result of applying the window function to the window.
	 */
	private <R> SingleOutputStreamOperator<R> reduce(
			ReduceFunction<T> reduceFunction,
			WindowFunction<T, R, K, W> function,
			LegacyWindowOperatorType legacyWindowOpType) {

		TypeInformation<T> inType = input.getType();
		TypeInformation<R> resultType = TypeExtractor.getUnaryOperatorReturnType(
			function, WindowFunction.class, true, true, inType, null, false);

		return reduce(reduceFunction, function, resultType, legacyWindowOpType);
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>
	 * Arriving data is incrementally aggregated using the given reducer.
	 *
	 * @param reduceFunction The reduce function that is used for incremental aggregation.
	 * @param function The window function.
	 * @param resultType Type information for the result type of the window function.
	 * @param legacyWindowOpType When migrating from an older Flink version, this flag indicates
	 *                           the type of the previous operator whose state we inherit.
	 * @return The data stream that is the result of applying the window function to the window.
	 */
	private <R> SingleOutputStreamOperator<R> reduce(
			ReduceFunction<T> reduceFunction,
			WindowFunction<T, R, K, W> function,
			TypeInformation<R> resultType,
			LegacyWindowOperatorType legacyWindowOpType) {

		if (reduceFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("ReduceFunction of reduce can not be a RichFunction.");
		}

		//clean the closures
		function = input.getExecutionEnvironment().clean(function);
		reduceFunction = input.getExecutionEnvironment().clean(reduceFunction);

		String callLocation = Utils.getCallLocationName();
		String udfName = "WindowedStream." + callLocation;

		String opName;
		KeySelector<T, K> keySel = input.getKeySelector();

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
					new InternalIterableWindowFunction<>(new ReduceApplyWindowFunction<>(reduceFunction, function)),
					trigger,
					evictor,
					allowedLateness);

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
					new InternalSingleValueWindowFunction<>(function),
					trigger,
					allowedLateness,
					legacyWindowOpType);
		}

		return input.transform(opName, resultType, operator);
	}

	/**
	 * Applies the given fold function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the reduce function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * @param function The fold function.
	 * @return The data stream that is the result of applying the fold function to the window.
	 */
	public <R> SingleOutputStreamOperator<R> fold(R initialValue, FoldFunction<T, R> function) {
		if (function instanceof RichFunction) {
			throw new UnsupportedOperationException("FoldFunction can not be a RichFunction. " +
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
	 */
	public <R> SingleOutputStreamOperator<R> fold(R initialValue, FoldFunction<T, R> function, TypeInformation<R> resultType) {
		if (function instanceof RichFunction) {
			throw new UnsupportedOperationException("FoldFunction can not be a RichFunction. " +
				"Please use fold(FoldFunction, WindowFunction) instead.");
		}

		return fold(initialValue, function, new PassThroughWindowFunction<K, W, R>(), resultType, resultType);
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>
	 * Arriving data is incrementally aggregated using the given fold function.
	 *
	 * @param initialValue The initial value of the fold.
	 * @param foldFunction The fold function that is used for incremental aggregation.
	 * @param function The window function.
	 * @return The data stream that is the result of applying the window function to the window.
	 */
	@PublicEvolving
	public <ACC, R> SingleOutputStreamOperator<R> fold(ACC initialValue, FoldFunction<T, ACC> foldFunction, WindowFunction<ACC, R, K, W> function) {

		TypeInformation<ACC> foldAccumulatorType = TypeExtractor.getFoldReturnTypes(foldFunction, input.getType(),
			Utils.getCallLocationName(), true);

		TypeInformation<R> resultType = TypeExtractor.getUnaryOperatorReturnType(
			function, WindowFunction.class, true, true, foldAccumulatorType, null, false);

		return fold(initialValue, foldFunction, function, foldAccumulatorType, resultType);
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>
	 * Arriving data is incrementally aggregated using the given fold function.
	 *
	 * @param initialValue The initial value of the fold.
	 * @param foldFunction The fold function that is used for incremental aggregation.
	 * @param function The window function.
	 * @param foldAccumulatorType Type information for the result type of the fold function
	 * @param resultType Type information for the result type of the window function
	 * @return The data stream that is the result of applying the window function to the window.
	 */
	@PublicEvolving
	public <ACC, R> SingleOutputStreamOperator<R> fold(ACC initialValue,
			FoldFunction<T, ACC> foldFunction,
			WindowFunction<ACC, R, K, W> function,
			TypeInformation<ACC> foldAccumulatorType,
			TypeInformation<R> resultType) {
		if (foldFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("FoldFunction of fold can not be a RichFunction.");
		}
		if (windowAssigner instanceof MergingWindowAssigner) {
			throw new UnsupportedOperationException("Fold cannot be used with a merging WindowAssigner.");
		}

		if (windowAssigner instanceof BaseAlignedWindowAssigner) {
			throw new UnsupportedOperationException("Fold cannot be used with a " +
				windowAssigner.getClass().getSimpleName() + " assigner.");
		}

		//clean the closures
		function = input.getExecutionEnvironment().clean(function);
		foldFunction = input.getExecutionEnvironment().clean(foldFunction);

		String callLocation = Utils.getCallLocationName();
		String udfName = "WindowedStream." + callLocation;

		String opName;
		KeySelector<T, K> keySel = input.getKeySelector();

		OneInputStreamOperator<T, R> operator;

		if (evictor != null) {
			@SuppressWarnings({"unchecked", "rawtypes"})
			TypeSerializer<StreamRecord<T>> streamRecordSerializer =
				(TypeSerializer<StreamRecord<T>>) new StreamElementSerializer(input.getType().createSerializer(getExecutionEnvironment().getConfig()));

			ListStateDescriptor<StreamRecord<T>> stateDesc =
				new ListStateDescriptor<>("window-contents", streamRecordSerializer);

			opName = "TriggerWindow(" + windowAssigner + ", " + stateDesc + ", " + trigger + ", " + evictor + ", " + udfName + ")";

			operator = new EvictingWindowOperator<>(windowAssigner,
				windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
				keySel,
				input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
				stateDesc,
				new InternalIterableWindowFunction<>(new FoldApplyWindowFunction<>(initialValue, foldFunction, function, foldAccumulatorType)),
				trigger,
				evictor,
				allowedLateness);

		} else {
			FoldingStateDescriptor<T, ACC> stateDesc = new FoldingStateDescriptor<>("window-contents",
				initialValue, foldFunction, foldAccumulatorType.createSerializer(getExecutionEnvironment().getConfig()));

			opName = "TriggerWindow(" + windowAssigner + ", " + stateDesc + ", " + trigger + ", " + udfName + ")";

			operator = new WindowOperator<>(windowAssigner,
				windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
				keySel,
				input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
				stateDesc,
				new InternalSingleValueWindowFunction<>(function),
				trigger,
				allowedLateness);
		}

		return input.transform(opName, resultType, operator);
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>
	 * Not that this function requires that all data in the windows is buffered until the window
	 * is evaluated, as the function provides no means of incremental aggregation.
	 * 
	 * @param function The window function.
	 * @return The data stream that is the result of applying the window function to the window.
	 */
	public <R> SingleOutputStreamOperator<R> apply(WindowFunction<T, R, K, W> function) {
		TypeInformation<R> resultType = TypeExtractor.getUnaryOperatorReturnType(
				function, WindowFunction.class, true, true, getInputType(), null, false);

		return apply(function, resultType);
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>
	 * Note that this function requires that all data in the windows is buffered until the window
	 * is evaluated, as the function provides no means of incremental aggregation.
	 *
	 * @param function The window function.
	 * @param resultType Type information for the result type of the window function
	 * @return The data stream that is the result of applying the window function to the window.
	 */
	public <R> SingleOutputStreamOperator<R> apply(WindowFunction<T, R, K, W> function, TypeInformation<R> resultType) {

		//clean the closure
		function = input.getExecutionEnvironment().clean(function);

		String callLocation = Utils.getCallLocationName();
		String udfName = "WindowedStream." + callLocation;

		SingleOutputStreamOperator<R> result = createFastTimeOperatorIfValid(function, resultType, udfName);
		if (result != null) {
			return result;
		}

		LegacyWindowOperatorType legacyWindowOpType = getLegacyWindowType(function);
		String opName;
		KeySelector<T, K> keySel = input.getKeySelector();

		WindowOperator<K, T, Iterable<T>, R, W> operator;

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
					new InternalIterableWindowFunction<>(function),
					trigger,
					evictor,
					allowedLateness);

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
					new InternalIterableWindowFunction<>(function),
					trigger,
					allowedLateness,
					legacyWindowOpType);
		}

		return input.transform(opName, resultType, operator);
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>
	 * Arriving data is incrementally aggregated using the given reducer.
	 *
	 * @param reduceFunction The reduce function that is used for incremental aggregation.
	 * @param function The window function.
	 * @return The data stream that is the result of applying the window function to the window.
	 *
	 * @deprecated Use {@link #reduce(ReduceFunction, WindowFunction)} instead.
	 */
	@Deprecated
	public <R> SingleOutputStreamOperator<R> apply(ReduceFunction<T> reduceFunction, WindowFunction<T, R, K, W> function) {
		TypeInformation<T> inType = input.getType();
		TypeInformation<R> resultType = TypeExtractor.getUnaryOperatorReturnType(
				function, WindowFunction.class, true, true, inType, null, false);

		return apply(reduceFunction, function, resultType);
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>
	 * Arriving data is incrementally aggregated using the given reducer.
	 *
	 * @param reduceFunction The reduce function that is used for incremental aggregation.
	 * @param function The window function.
	 * @param resultType Type information for the result type of the window function
	 * @return The data stream that is the result of applying the window function to the window.
	 *
	 * @deprecated Use {@link #reduce(ReduceFunction, WindowFunction, TypeInformation)} instead.
	 */
	@Deprecated
	public <R> SingleOutputStreamOperator<R> apply(ReduceFunction<T> reduceFunction, WindowFunction<T, R, K, W> function, TypeInformation<R> resultType) {
		if (reduceFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("ReduceFunction of apply can not be a RichFunction.");
		}

		//clean the closures
		function = input.getExecutionEnvironment().clean(function);
		reduceFunction = input.getExecutionEnvironment().clean(reduceFunction);

		String callLocation = Utils.getCallLocationName();
		String udfName = "WindowedStream." + callLocation;

		String opName;
		KeySelector<T, K> keySel = input.getKeySelector();

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
					new InternalIterableWindowFunction<>(new ReduceApplyWindowFunction<>(reduceFunction, function)),
					trigger,
					evictor,
					allowedLateness);

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
					new InternalSingleValueWindowFunction<>(function),
					trigger,
					allowedLateness);
		}

		return input.transform(opName, resultType, operator);
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>
	 * Arriving data is incrementally aggregated using the given fold function.
	 *
	 * @param initialValue The initial value of the fold.
	 * @param foldFunction The fold function that is used for incremental aggregation.
	 * @param function The window function.
	 * @return The data stream that is the result of applying the window function to the window.
	 *
	 * @deprecated Use {@link #fold(R, FoldFunction, WindowFunction)} instead.
	 */
	@Deprecated
	public <R> SingleOutputStreamOperator<R> apply(R initialValue, FoldFunction<T, R> foldFunction, WindowFunction<R, R, K, W> function) {

		TypeInformation<R> resultType = TypeExtractor.getFoldReturnTypes(foldFunction, input.getType(),
			Utils.getCallLocationName(), true);

		return apply(initialValue, foldFunction, function, resultType);
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>
	 * Arriving data is incrementally aggregated using the given fold function.
	 *
	 * @param initialValue The initial value of the fold.
	 * @param foldFunction The fold function that is used for incremental aggregation.
	 * @param function The window function.
	 * @param resultType Type information for the result type of the window function
	 * @return The data stream that is the result of applying the window function to the window.
	 *
	 * @deprecated Use {@link #fold(R, FoldFunction, WindowFunction, TypeInformation, TypeInformation)} instead.
	 */
	@Deprecated
	public <R> SingleOutputStreamOperator<R> apply(R initialValue, FoldFunction<T, R> foldFunction, WindowFunction<R, R, K, W> function, TypeInformation<R> resultType) {
		if (foldFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("FoldFunction of apply can not be a RichFunction.");
		}
		if (windowAssigner instanceof MergingWindowAssigner) {
			throw new UnsupportedOperationException("Fold cannot be used with a merging WindowAssigner.");
		}

		//clean the closures
		function = input.getExecutionEnvironment().clean(function);
		foldFunction = input.getExecutionEnvironment().clean(foldFunction);

		String callLocation = Utils.getCallLocationName();
		String udfName = "WindowedStream." + callLocation;

		String opName;
		KeySelector<T, K> keySel = input.getKeySelector();

		OneInputStreamOperator<T, R> operator;

		if (evictor != null) {
			@SuppressWarnings({"unchecked", "rawtypes"})
			TypeSerializer<StreamRecord<T>> streamRecordSerializer =
					(TypeSerializer<StreamRecord<T>>) new StreamElementSerializer(input.getType().createSerializer(getExecutionEnvironment().getConfig()));

			ListStateDescriptor<StreamRecord<T>> stateDesc =
					new ListStateDescriptor<>("window-contents", streamRecordSerializer);

			opName = "TriggerWindow(" + windowAssigner + ", " + stateDesc + ", " + trigger + ", " + evictor + ", " + udfName + ")";

			operator = new EvictingWindowOperator<>(windowAssigner,
				windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
				keySel,
				input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
				stateDesc,
				new InternalIterableWindowFunction<>(new FoldApplyWindowFunction<>(initialValue, foldFunction, function, resultType)),
				trigger,
				evictor,
				allowedLateness);

		} else {
			FoldingStateDescriptor<T, R> stateDesc = new FoldingStateDescriptor<>("window-contents",
				initialValue, foldFunction, resultType.createSerializer(getExecutionEnvironment().getConfig()));

			opName = "TriggerWindow(" + windowAssigner + ", " + stateDesc + ", " + trigger + ", " + udfName + ")";

			operator = new WindowOperator<>(windowAssigner,
				windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
				keySel,
				input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
				stateDesc,
				new InternalSingleValueWindowFunction<>(function),
				trigger,
				allowedLateness);
		}

		return input.transform(opName, resultType, operator);
	}

	// ------------------------------------------------------------------------
	//  Aggregations on the keyed windows
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
	 * <p>
	 * A field expression is either
	 * the name of a public field or a getter method with parentheses of the
	 * stream's underlying type. A dot can be used to drill down into objects,
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
	 * <p>
	 * A field
	 * expression is either the name of a public field or a getter method with
	 * parentheses of the {@link DataStream}S underlying type. A dot can be used
	 * to drill down into objects, as in {@code "field1.getInnerField2()" }.
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

	private LegacyWindowOperatorType getLegacyWindowType(Function function) {
		if (windowAssigner instanceof SlidingProcessingTimeWindows && trigger instanceof ProcessingTimeTrigger && evictor == null) {
			if (function instanceof ReduceFunction) {
				return LegacyWindowOperatorType.FAST_AGGREGATING;
			} else if (function instanceof WindowFunction) {
				return LegacyWindowOperatorType.FAST_ACCUMULATING;
			}
		} else if (windowAssigner instanceof TumblingProcessingTimeWindows && trigger instanceof ProcessingTimeTrigger && evictor == null) {
			if (function instanceof ReduceFunction) {
				return LegacyWindowOperatorType.FAST_AGGREGATING;
			} else if (function instanceof WindowFunction) {
				return LegacyWindowOperatorType.FAST_ACCUMULATING;
			}
		}
		return LegacyWindowOperatorType.NONE;
	}

	private <R> SingleOutputStreamOperator<R> createFastTimeOperatorIfValid(
			Function function,
			TypeInformation<R> resultType,
			String functionName) {

		if (windowAssigner.getClass() == SlidingAlignedProcessingTimeWindows.class && trigger == null && evictor == null) {
			SlidingAlignedProcessingTimeWindows timeWindows = (SlidingAlignedProcessingTimeWindows) windowAssigner;
			final long windowLength = timeWindows.getSize();
			final long windowSlide = timeWindows.getSlide();

			String opName = "Fast " + timeWindows + " of " + functionName;

			if (function instanceof ReduceFunction) {
				@SuppressWarnings("unchecked")
				ReduceFunction<T> reducer = (ReduceFunction<T>) function;

				@SuppressWarnings("unchecked")
				OneInputStreamOperator<T, R> op = (OneInputStreamOperator<T, R>)
						new AggregatingProcessingTimeWindowOperator<>(
								reducer, input.getKeySelector(), 
								input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
								input.getType().createSerializer(getExecutionEnvironment().getConfig()),
								windowLength, windowSlide);
				return input.transform(opName, resultType, op);
			}
			else if (function instanceof WindowFunction) {
				@SuppressWarnings("unchecked")
				WindowFunction<T, R, K, TimeWindow> wf = (WindowFunction<T, R, K, TimeWindow>) function;

				OneInputStreamOperator<T, R> op = new AccumulatingProcessingTimeWindowOperator<>(
						wf, input.getKeySelector(),
						input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
						input.getType().createSerializer(getExecutionEnvironment().getConfig()),
						windowLength, windowSlide);
				return input.transform(opName, resultType, op);
			}
		} else if (windowAssigner.getClass() == TumblingAlignedProcessingTimeWindows.class && trigger == null && evictor == null) {
			TumblingAlignedProcessingTimeWindows timeWindows = (TumblingAlignedProcessingTimeWindows) windowAssigner;
			final long windowLength = timeWindows.getSize();
			final long windowSlide = timeWindows.getSize();

			String opName = "Fast " + timeWindows + " of " + functionName;

			if (function instanceof ReduceFunction) {
				@SuppressWarnings("unchecked")
				ReduceFunction<T> reducer = (ReduceFunction<T>) function;

				@SuppressWarnings("unchecked")
				OneInputStreamOperator<T, R> op = (OneInputStreamOperator<T, R>)
						new AggregatingProcessingTimeWindowOperator<>(
								reducer,
								input.getKeySelector(),
								input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
								input.getType().createSerializer(getExecutionEnvironment().getConfig()),
								windowLength, windowSlide);
				return input.transform(opName, resultType, op);
			}
			else if (function instanceof WindowFunction) {
				@SuppressWarnings("unchecked")
				WindowFunction<T, R, K, TimeWindow> wf = (WindowFunction<T, R, K, TimeWindow>) function;

				OneInputStreamOperator<T, R> op = new AccumulatingProcessingTimeWindowOperator<>(
						wf, input.getKeySelector(),
						input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
						input.getType().createSerializer(getExecutionEnvironment().getConfig()),
						windowLength, windowSlide);
				return input.transform(opName, resultType, op);
			}
		}

		return null;
	}

	public StreamExecutionEnvironment getExecutionEnvironment() {
		return input.getExecutionEnvironment();
	}

	public TypeInformation<T> getInputType() {
		return input.getType();
	}
}
