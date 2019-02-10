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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.SliceAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.slicing.Slice;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.IterableSliceOperator;
import org.apache.flink.streaming.runtime.operators.windowing.SliceOperator;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@code SlicedStream} represents a data stream where elements are grouped by key, and
 * for each key, the stream of elements is split into zero or one window based on a
 * {@link SliceAssigner}. Window emission is triggered based on a {@link Trigger}.
 *
 * <p>The windows are conceptually evaluated for each key individually, meaning they can trigger
 * at different points for each key. These windows elements are assigned to are called slices.
 *
 * <p>Note that the {@code SlicedStream} is purely and API construct, during runtime the
 * {@code SlicedStream} will be collapsed together with the {@code KeyedStream} and the operation
 * over the window into one single operation.
 *
 * @param <T> The type of elements in the stream.
 * @param <K> The type of the key by which elements are grouped.
 * @param <W> The type of {@code Window} that the {@code SliceAssigner} assigns the elements to.
 */
@Public
public class SlicedStream<T, K, W extends Window> extends WindowedStream<T, K, W> {

	/**
	 * Side output {@code OutputTag} for late data. If no tag is set late data will simply be
	 * dropped.
 	 */
	private OutputTag<T> lateDataOutputTag;

	private SliceAssigner<? super T, W> sliceAssigner;

	@PublicEvolving
	public SlicedStream(KeyedStream<T, K> input,
						SliceAssigner<? super T, W> sliceAssigner) {
		super(input, sliceAssigner);
		this.sliceAssigner = sliceAssigner;
		this.trigger = sliceAssigner.getDefaultTrigger(input.getExecutionEnvironment());
	}

	/**
	 * Sets the {@code Trigger} that should be used to trigger window emission.
	 */
	@PublicEvolving
	public SlicedStream<T, K, W> trigger(Trigger<? super T, ? super W> trigger) {
		super.trigger(trigger);
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
	public SlicedStream<T, K, W> allowedLateness(Time lateness) {
		super.allowedLateness(lateness);
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
	public SlicedStream<T, K, W> sideOutputLateData(OutputTag<T> outputTag) {
		Preconditions.checkNotNull(outputTag, "Side output tag must not be null.");
		this.lateDataOutputTag = input.getExecutionEnvironment().clean(outputTag);
		return this;
	}

	// ------------------------------------------------------------------------
	//  Operations on the keyed windows
	// ------------------------------------------------------------------------
	/**
	 * Applies a reduce function to the sliced stream, each of the window for each key are applied
	 * individually. The output of this reduce function can be re-interpreted as a keyed stream.
	 *
	 * @param reduceFunction The reduce function.
	 * @return The data stream that is the result of applying the reduce function to the window.
	 */
	public SingleOutputStreamOperator<Slice<T, K, W>> reduceToSlice(
		ReduceFunction<T> reduceFunction) {

		TypeInformation<Slice<T, K, W>> resultType = getSliceTypeInfo(input.getType(),
			input.getKeyType(),
			sliceAssigner.getWindowType());

		return reduceToSlice(reduceFunction, resultType);
	}

	/**
	 * Applies a reduce function to the sliced stream.
	 *
	 * @param reduceFunction The reduce function.
	 * @param resultType The type information for the slice result type
	 * @return The data stream that is the result of applying the reduce function to the window.
	 */
	private SingleOutputStreamOperator<Slice<T, K, W>> reduceToSlice(
		ReduceFunction<T> reduceFunction,
		TypeInformation<Slice<T, K, W>> resultType) {

		if (reduceFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("ReduceFunction of reduce can not be a RichFunction.");
		}

		//clean the closures
		reduceFunction = input.getExecutionEnvironment().clean(reduceFunction);

		final String opName = generateOperatorName(windowAssigner, trigger, evictor, reduceFunction, null);
		KeySelector<T, K> keySel = input.getKeySelector();

		ReducingStateDescriptor<T> stateDesc = new ReducingStateDescriptor<>("window-contents",
			reduceFunction,
			input.getType().createSerializer(getExecutionEnvironment().getConfig()));

		OneInputStreamOperator<T, Slice<T, K, W>> operator =
			new SliceOperator<>(sliceAssigner,
				sliceAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
				keySel,
				input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
				stateDesc,
				trigger,
				allowedLateness,
				lateDataOutputTag);

		return input.transform(opName, resultType, operator);
	}

	// ------------------------------------------------------------------------
	//  Aggregation Function
	// ------------------------------------------------------------------------

	/**
	 * Applies the given aggregation function to each window. The aggregation function is called for
	 * each element, aggregating values incrementally and keeping the state to one accumulator
	 * per key and window. The output of the window function can be re-interpreted as a keyed stream.
	 *
	 * @param function The aggregation function.
	 * @return The data stream that is the result of applying the aggregate function to the window.
	 *
	 * @param <ACC> The type of the AggregateFunction's accumulator
	 */
	@PublicEvolving
	public <ACC> SingleOutputStreamOperator<Slice<ACC, K, W>> aggregateToSlice(AggregateFunction<T, ACC, ?> function) {
		checkNotNull(function, "function");

		if (function instanceof RichFunction) {
			throw new UnsupportedOperationException("This aggregation function cannot be a RichFunction.");
		}
		TypeInformation<ACC> accumulatorType = TypeExtractor.getAggregateFunctionAccumulatorType(
				function, input.getType(), null, false);

		return aggregateToSlice(function, accumulatorType);
	}

	/**
	 * Applies the given aggregation function to each window.
	 *
	 * @param function The aggregation function.
	 * @param accumulatorType The type information of the accumulator result.
	 * @return The data stream that is the result of applying the aggregate function to the window.
	 *
	 * @param <ACC> The type of the AggregateFunction's accumulator
	 */
	@PublicEvolving
	public <ACC> SingleOutputStreamOperator<Slice<ACC, K, W>> aggregateToSlice(
		AggregateFunction<T, ACC, ?> function,
		TypeInformation<ACC> accumulatorType) {
		checkNotNull(function, "function");

		if (function instanceof RichFunction) {
			throw new UnsupportedOperationException("This aggregation function cannot be a RichFunction.");
		}

		AggregateFunction<T, ACC, ACC> wrappedFunction = wrapSlicedStreamAggregateFunction(function);

		TypeInformation<Slice<ACC, K, W>> resultType = getSliceTypeInfo(accumulatorType,
			input.getKeyType(),
			sliceAssigner.getWindowType());

		return aggregateToSlice(wrappedFunction, accumulatorType, resultType);
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function can
	 * be re-interpreted as a keyed stream.
	 *
	 * <p>Arriving data is incrementally aggregated using the given aggregate function. This means
	 * that the window function typically has only a single value to process when called.
	 *
	 * @param aggregateFunction The aggregation function that is used for incremental aggregation.
	 * @param accumulatorType Type information for the internal accumulator type of the aggregation function
	 * @param resultType Type information for the slice result type
	 *
	 * @return The data stream that is the result of applying the window function to the window.
	 *
	 * @param <ACC> The type of the AggregateFunction's accumulator
	 */
	@PublicEvolving
	private <ACC> SingleOutputStreamOperator<Slice<ACC, K, W>> aggregateToSlice(
			AggregateFunction<T, ACC, ACC> aggregateFunction,
			TypeInformation<ACC> accumulatorType,
			TypeInformation<Slice<ACC, K, W>> resultType) {

		checkNotNull(aggregateFunction, "aggregateFunction");
		checkNotNull(accumulatorType, "accumulatorType");

		if (aggregateFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("This aggregate function cannot be a RichFunction.");
		}

		//clean the closures
		aggregateFunction = input.getExecutionEnvironment().clean(aggregateFunction);

		final String opName = generateOperatorName(windowAssigner, trigger, evictor, aggregateFunction, null);
		KeySelector<T, K> keySel = input.getKeySelector();

		AggregatingStateDescriptor<T, ACC, ACC> stateDesc = new AggregatingStateDescriptor<>("window-contents",
				aggregateFunction, accumulatorType.createSerializer(getExecutionEnvironment().getConfig()));

		OneInputStreamOperator<T, Slice<ACC, K, W>> operator = new SliceOperator<>(sliceAssigner,
			sliceAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
			keySel,
			input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
			stateDesc,
			trigger,
			allowedLateness,
			lateDataOutputTag);

		return input.transform(opName, resultType, operator);
	}

	// ------------------------------------------------------------------------
	//  Window Function - Slice into iterable objects
	// ------------------------------------------------------------------------

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function can
	 * be re-interpreted as a keyed stream.
	 *
	 * <p>Note that this function requires that all data in the windows is buffered until the window
	 * is evaluated, as the function provides no means of incremental aggregation. The result is
	 * directly forwarded to downstream operators. This is especially useful if multiple window
	 * functions or multiple overlapping windows are applied toward this same slice multiple times.
	 *
	 * @return The data stream that is the iterable result of the buffering operation.
	 */
	public SingleOutputStreamOperator<Slice<List<T>, K, W>> applyToSlice() {
		final String opName = generateOperatorName(windowAssigner, trigger, evictor, null, null);

		TypeInformation<List<T>> contentType = Types.LIST(input.getType());
		TypeInformation<Slice<List<T>, K, W>> resultType = getSliceTypeInfo(contentType,
			input.getKeyType(),
			sliceAssigner.getWindowType());

		KeySelector<T, K> keySel = input.getKeySelector();

		ListStateDescriptor<T> stateDesc = new ListStateDescriptor<>("window-contents",
			input.getType().createSerializer(getExecutionEnvironment().getConfig()));

		IterableSliceOperator<K, T, W> operator = new IterableSliceOperator<>(sliceAssigner,
			sliceAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
			keySel,
			input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
			stateDesc,
			trigger,
			allowedLateness,
			lateDataOutputTag);

		return input.transform(opName, resultType, operator);
	}

	private static String generateOperatorName(
		WindowAssigner<?, ?> assigner,
		Trigger<?, ?> trigger,
		@Nullable Evictor<?, ?> evictor,
		@Nullable Function function1,
		@Nullable Function function2) {
		return "Window(" +
			assigner + ", " +
			trigger.getClass().getSimpleName() + ", " +
			(evictor == null ? "" : (evictor.getClass().getSimpleName() + ", ")) +
			(function1 == null ? "" : (", " + generateFunctionName(function1))) +
			(function2 == null ? "" : (", " + generateFunctionName(function2))) +
			")";
	}

	public StreamExecutionEnvironment getExecutionEnvironment() {
		return input.getExecutionEnvironment();
	}

	public TypeInformation<T> getInputType() {
		return input.getType();
	}

	private <R> KeySelector<Slice<R, K, W>, K> getSliceToKeySelector(TypeInformation<R> contentType) {
		return (KeySelector<Slice<R, K, W>, K>) rkwSlice -> rkwSlice.getKey();
	}

	private <ACC> AggregateFunction<T, ACC, ACC> wrapSlicedStreamAggregateFunction(AggregateFunction<T, ACC, ?> function) {
		return new AggregateFunction<T, ACC, ACC>() {
			@Override
			public ACC createAccumulator() {
				return function.createAccumulator();
			}

			@Override
			public ACC add(T value, ACC accumulator) {
				return function.add(value, accumulator);
			}

			@Override
			public ACC getResult(ACC accumulator) {
				return accumulator;
			}

			@Override
			public ACC merge(ACC a, ACC b) {
				return function.merge(a, b);
			}
		};
	}

	<C> TupleTypeInfo<Slice<C, K, W>> getSliceTypeInfo(
		TypeInformation<C> contentType,
		TypeInformation<K> keyType,
		TypeInformation<W> windowType) {
		return new TupleTypeInfo<>(contentType, keyType, windowType);
	}

	// -------------------- Testing Methods --------------------

	@VisibleForTesting
	long getAllowedLateness() {
		return allowedLateness;
	}
}
