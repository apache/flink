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

import org.apache.commons.lang.SerializationUtils;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ReduceWindowFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.AccumulatingProcessingTimeWindowOperator;
import org.apache.flink.streaming.runtime.operators.windowing.AggregatingProcessingTimeWindowOperator;
import org.apache.flink.streaming.runtime.operators.windowing.EvictingWindowOperator;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;
import org.apache.flink.streaming.runtime.operators.windowing.buffers.HeapWindowBuffer;
import org.apache.flink.streaming.runtime.operators.windowing.buffers.PreAggregatingHeapWindowBuffer;

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
 * pre-aggregation of window results cannot be used.
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
public class WindowedStream<T, K, W extends Window> {

	/** The keyed data stream that is windowed by this stream */
	private final KeyedStream<T, K> input;

	/** The window assigner */
	private final WindowAssigner<? super T, W> windowAssigner;

	/** The trigger that is used for window evaluation/emission. */
	private Trigger<? super T, ? super W> trigger;

	/** The evictor that is used for evicting elements before window evaluation. */
	private Evictor<? super T, ? super W> evictor;


	public WindowedStream(KeyedStream<T, K> input,
			WindowAssigner<? super T, W> windowAssigner) {
		this.input = input;
		this.windowAssigner = windowAssigner;
		this.trigger = windowAssigner.getDefaultTrigger();
	}

	/**
	 * Sets the {@code Trigger} that should be used to trigger window emission.
	 */
	public WindowedStream<T, K, W> trigger(Trigger<? super T, ? super W> trigger) {
		this.trigger = trigger;
		return this;
	}

	/**
	 * Sets the {@code Evictor} that should be used to evict elements from a window before emission.
	 *
	 * <p>
	 * Note: When using an evictor window performance will degrade significantly, since
	 * pre-aggregation of window results cannot be used.
	 */
	public WindowedStream<T, K, W> evictor(Evictor<? super T, ? super W> evictor) {
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
	 * <p>
	 * This window will try and pre-aggregate data as much as the window policies permit. For example,
	 * tumbling time windows can perfectly pre-aggregate the data, meaning that only one element per
	 * key is stored. Sliding time windows will pre-aggregate on the granularity of the slide interval,
	 * so a few elements are stored per key (one per slide interval).
	 * Custom windows may not be able to pre-aggregate, or may need to store extra values in an
	 * aggregation tree.
	 * 
	 * @param function The reduce function.
	 * @return The data stream that is the result of applying the reduce function to the window. 
	 */
	public DataStream<T> reduceWindow(ReduceFunction<T> function) {
		String callLocation = Utils.getCallLocationName();
		String udfName = "Reduce at " + callLocation;

		DataStream<T> result = createFastTimeOperatorIfValid(function, input.getType(), udfName);
		if (result != null) {
			return result;
		}

		String opName = "TriggerWindow(" + windowAssigner + ", " + trigger + ", " + udfName + ")";
		KeySelector<T, K> keySel = input.getKeySelector();

		OneInputStreamOperator<T, T> operator;

		if (evictor != null) {
			operator = new EvictingWindowOperator<>(windowAssigner,
					keySel,
					new HeapWindowBuffer.Factory<T>(),
					new ReduceWindowFunction<K, W, T>(function),
					trigger,
					evictor);

		} else {
			// we need to copy because we need our own instance of the pre aggregator
			@SuppressWarnings("unchecked")
			ReduceFunction<T> functionCopy = (ReduceFunction<T>) SerializationUtils.clone(function);

			operator = new WindowOperator<>(windowAssigner,
					keySel,
					new PreAggregatingHeapWindowBuffer.Factory<>(functionCopy),
					new ReduceWindowFunction<K, W, T>(function),
					trigger);
		}

		return input.transform(opName, input.getType(), operator);
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>
	 * Not that this function requires that all data in the windows is buffered until the window
	 * is evaluated, as the function provides no means of pre-aggregation.
	 * 
	 * @param function The window function.
	 * @return The data stream that is the result of applying the window function to the window.
	 */
	public <R> DataStream<R> apply(WindowFunction<T, R, K, W> function) {
		TypeInformation<T> inType = input.getType();
		TypeInformation<R> resultType = TypeExtractor.getUnaryOperatorReturnType(
				function, WindowFunction.class, true, true, inType, null, false);

		return apply(function, resultType);
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>
	 * Not that this function requires that all data in the windows is buffered until the window
	 * is evaluated, as the function provides no means of pre-aggregation.
	 *
	 * @param function The window function.
	 * @return The data stream that is the result of applying the window function to the window.
	 */
	public <R> DataStream<R> apply(WindowFunction<T, R, K, W> function, TypeInformation<R> resultType) {
		//clean the closure
		function = input.getExecutionEnvironment().clean(function);

		String callLocation = Utils.getCallLocationName();
		String udfName = "MapWindow at " + callLocation;

		DataStream<R> result = createFastTimeOperatorIfValid(function, resultType, udfName);
		if (result != null) {
			return result;
		}


		String opName = "TriggerWindow(" + windowAssigner + ", " + trigger + ", " + udfName + ")";
		KeySelector<T, K> keySel = input.getKeySelector();

		OneInputStreamOperator<T, R> operator;

		if (evictor != null) {
			operator = new EvictingWindowOperator<>(windowAssigner,
					keySel,
					new HeapWindowBuffer.Factory<T>(),
					function,
					trigger,
					evictor);

		} else {
			operator = new WindowOperator<>(windowAssigner,
					keySel,
					new HeapWindowBuffer.Factory<T>(),
					function,
					trigger);
		}

		return input.transform(opName, resultType, operator);
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private <R> DataStream<R> createFastTimeOperatorIfValid(
			Function function,
			TypeInformation<R> resultType,
			String functionName) {

		if (windowAssigner instanceof SlidingProcessingTimeWindows && trigger instanceof ProcessingTimeTrigger && evictor == null) {
			SlidingProcessingTimeWindows timeWindows = (SlidingProcessingTimeWindows) windowAssigner;
			final long windowLength = timeWindows.getSize();
			final long windowSlide = timeWindows.getSlide();

			String opName = "Fast " + timeWindows + " of " + functionName;

			if (function instanceof ReduceFunction) {
				@SuppressWarnings("unchecked")
				ReduceFunction<T> reducer = (ReduceFunction<T>) function;

				@SuppressWarnings("unchecked")
				OneInputStreamOperator<T, R> op = (OneInputStreamOperator<T, R>)
						new AggregatingProcessingTimeWindowOperator<>(
								reducer, input.getKeySelector(), windowLength, windowSlide);
				return input.transform(opName, resultType, op);
			}
			else if (function instanceof WindowFunction) {
				@SuppressWarnings("unchecked")
				WindowFunction<T, R, K, TimeWindow> wf = (WindowFunction<T, R, K, TimeWindow>) function;

				OneInputStreamOperator<T, R> op = new AccumulatingProcessingTimeWindowOperator<>(
						wf, input.getKeySelector(), windowLength, windowSlide);
				return input.transform(opName, resultType, op);
			}
		} else if (windowAssigner instanceof TumblingProcessingTimeWindows && trigger instanceof ProcessingTimeTrigger && evictor == null) {
			TumblingProcessingTimeWindows timeWindows = (TumblingProcessingTimeWindows) windowAssigner;
			final long windowLength = timeWindows.getSize();
			final long windowSlide = timeWindows.getSize();

			String opName = "Fast " + timeWindows + " of " + functionName;

			if (function instanceof ReduceFunction) {
				@SuppressWarnings("unchecked")
				ReduceFunction<T> reducer = (ReduceFunction<T>) function;

				@SuppressWarnings("unchecked")
				OneInputStreamOperator<T, R> op = (OneInputStreamOperator<T, R>)
						new AggregatingProcessingTimeWindowOperator<>(
								reducer, input.getKeySelector(), windowLength, windowSlide);
				return input.transform(opName, resultType, op);
			}
			else if (function instanceof WindowFunction) {
				@SuppressWarnings("unchecked")
				WindowFunction<T, R, K, TimeWindow> wf = (WindowFunction<T, R, K, TimeWindow>) function;

				OneInputStreamOperator<T, R> op = new AccumulatingProcessingTimeWindowOperator<>(
						wf, input.getKeySelector(), windowLength, windowSlide);
				return input.transform(opName, resultType, op);
			}
		}

		return null;
	}

	public StreamExecutionEnvironment getExecutionEnvironment() {
		return input.getExecutionEnvironment();
	}
}
