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

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.commons.lang.SerializationUtils;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.windowing.KeyedWindowFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.functions.windowing.ReduceWindowFunction;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.evictors.DeltaEvictor;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousWatermarkTrigger;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.WatermarkTrigger;
import org.apache.flink.streaming.api.windowing.windowpolicy.Count;
import org.apache.flink.streaming.api.windowing.windowpolicy.Delta;
import org.apache.flink.streaming.api.windowing.windowpolicy.EventTime;
import org.apache.flink.streaming.api.windowing.windowpolicy.ProcessingTime;
import org.apache.flink.streaming.api.windowing.windowpolicy.WindowPolicy;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.buffers.EvictingWindowBuffer;
import org.apache.flink.streaming.runtime.operators.windowing.buffers.HeapWindowBuffer;
import org.apache.flink.streaming.runtime.operators.windowing.buffers.PreAggregatingHeapWindowBuffer;
import org.apache.flink.streaming.runtime.operators.windowing.buffers.WindowBuffer;
import org.apache.flink.streaming.runtime.operators.windowing.buffers.WindowBufferFactory;

/**
 * This class implements the conversion from window policies to concrete operator
 * implementations.
 */
public class PolicyToOperator {

	/**
	 * Entry point to create an operator for the given window policies and the window function.
	 */
	public static <IN, OUT, KEY> OneInputStreamOperator<IN, OUT> createOperatorForPolicies(
			WindowPolicy window, WindowPolicy slide, Function function, KeySelector<IN, KEY> keySelector)
	{
		if (window == null || function == null) {
			throw new NullPointerException();
		}
		
		// -- case 1: both policies are processing time policies
		if (window instanceof ProcessingTime && (slide == null || slide instanceof ProcessingTime)) {
			final long windowLength = ((ProcessingTime) window).toMilliseconds();
			final long windowSlide = slide == null ? windowLength : ((ProcessingTime) slide).toMilliseconds();
			
			if (function instanceof ReduceFunction) {
				@SuppressWarnings("unchecked")
				ReduceFunction<IN> reducer = (ReduceFunction<IN>) function;

				@SuppressWarnings("unchecked")
				OneInputStreamOperator<IN, OUT> op = (OneInputStreamOperator<IN, OUT>)
						new AggregatingProcessingTimeWindowOperator<>(
								reducer, keySelector, windowLength, windowSlide);
				return op;
			}
			else if (function instanceof KeyedWindowFunction) {
				@SuppressWarnings("unchecked")
				KeyedWindowFunction<IN, OUT, KEY, Window> wf = (KeyedWindowFunction<IN, OUT, KEY, Window>) function;

				return new AccumulatingProcessingTimeWindowOperator<>(
								wf, keySelector, windowLength, windowSlide);
			}
		}

		// -- case 2: both policies are event time policies
		if (window instanceof EventTime && (slide == null || slide instanceof EventTime)) {
			final long windowLength = ((EventTime) window).toMilliseconds();
			final long windowSlide = slide == null ? windowLength : ((EventTime) slide).toMilliseconds();

			WindowAssigner<? super IN, TimeWindow> assigner;
			if (windowSlide == windowLength) {
				assigner = TumblingTimeWindows.of(windowLength);
			} else {
				assigner = SlidingTimeWindows.of(windowLength, windowSlide);
			}
			WindowBufferFactory<IN, ? extends WindowBuffer<IN>> windowBuffer;
			if (function instanceof ReduceFunction) {
				@SuppressWarnings("unchecked")
				ReduceFunction<IN> reducer = (ReduceFunction<IN>) SerializationUtils.clone(function);
				function = new ReduceWindowFunction<>(reducer);
				windowBuffer = new PreAggregatingHeapWindowBuffer.Factory<>(reducer);
			} else {
				windowBuffer = new HeapWindowBuffer.Factory<>();
			}
			@SuppressWarnings("unchecked")
			KeyedWindowFunction<IN, OUT, KEY, TimeWindow> windowFunction = (KeyedWindowFunction<IN, OUT, KEY, TimeWindow>) function;

			return new WindowOperator<>(
					assigner,
					keySelector,
					windowBuffer,
					windowFunction,
					WatermarkTrigger.create());
		}

		// -- case 3: arbitrary trigger, no eviction
		if (slide == null) {
			Trigger<? super IN, GlobalWindow> trigger = policyToTrigger(window);
			// we need to make them purging triggers because the trigger/eviction policy model
			// expects that the window is purged when no slide is used
			Trigger<? super IN, GlobalWindow> purgingTrigger = PurgingTrigger.of(trigger);

			WindowBufferFactory<IN, ? extends WindowBuffer<IN>> windowBuffer;
			if (function instanceof ReduceFunction) {
				@SuppressWarnings("unchecked")
				ReduceFunction<IN> reducer = (ReduceFunction<IN>) SerializationUtils.clone(function);
				function = new ReduceWindowFunction<>(reducer);
				windowBuffer = new PreAggregatingHeapWindowBuffer.Factory<>(reducer);
			} else {
				windowBuffer = new HeapWindowBuffer.Factory<>();
			}

			if (!(function instanceof KeyedWindowFunction)) {
				throw new IllegalStateException("Windowing function is not of type EvaluateKeyedWindowFunction.");
			}
			@SuppressWarnings("unchecked")
			KeyedWindowFunction<IN, OUT, KEY, GlobalWindow> windowFunction = (KeyedWindowFunction<IN, OUT, KEY, GlobalWindow>) function;

			return new WindowOperator<>(
					GlobalWindows.<IN>create(),
					keySelector,
					windowBuffer,
					windowFunction,
					purgingTrigger);
		}

		// -- case 4: arbitrary trigger, arbitrary eviction
		Trigger<? super IN, GlobalWindow> trigger = policyToTrigger(slide);
		Evictor<? super IN, GlobalWindow> evictor = policyToEvictor(window);

		WindowBufferFactory<IN, ? extends EvictingWindowBuffer<IN>> windowBuffer = new HeapWindowBuffer.Factory<>();
		if (function instanceof ReduceFunction) {
			@SuppressWarnings("unchecked")
			ReduceFunction<IN> reducer = (ReduceFunction<IN>) SerializationUtils.clone(function);
			function = new ReduceWindowFunction<>(reducer);
		}

		if (!(function instanceof KeyedWindowFunction)) {
			throw new IllegalStateException("Windowing function is not of type EvaluateKeyedWindowFunction.");
		}

		@SuppressWarnings("unchecked")
		KeyedWindowFunction<IN, OUT, KEY, GlobalWindow> windowFunction = (KeyedWindowFunction<IN, OUT, KEY, GlobalWindow>) function;

		EvictingWindowOperator<KEY, IN, OUT, GlobalWindow> op = new EvictingWindowOperator<>(
				GlobalWindows.<IN>create(),
				keySelector,
				windowBuffer,
				windowFunction,
				trigger,
				evictor);

		if (window instanceof ProcessingTime) {
			// special case, we need to instruct the window operator to store the processing time in
			// the elements so that the evictor can work on that
			op.enableSetProcessingTime(true);
		}

		return op;
	}

	private static <IN> Trigger<? super IN, GlobalWindow> policyToTrigger(WindowPolicy policy) {
		if (policy instanceof EventTime) {
			EventTime eventTime = (EventTime) policy;
			return ContinuousWatermarkTrigger.of(eventTime.getSize());
		} else if (policy instanceof ProcessingTime) {
			ProcessingTime processingTime = (ProcessingTime) policy;
			return ContinuousProcessingTimeTrigger.of(processingTime.getSize());
		} else if (policy instanceof Count) {
			Count count = (Count) policy;
			return CountTrigger.of(count.getSize());
		} else if (policy instanceof Delta) {
			@SuppressWarnings("unchecked,rawtypes")
			Delta<IN> delta = (Delta) policy;
			return DeltaTrigger.of(delta.getThreshold(), delta.getDeltaFunction());

		}

		throw new UnsupportedOperationException("Unsupported policy " + policy);
	}

	private static <IN> Evictor<? super IN, GlobalWindow> policyToEvictor(WindowPolicy policy) {
		if (policy instanceof EventTime) {
			EventTime eventTime = (EventTime) policy;
			return TimeEvictor.of(eventTime.getSize());
		} else if (policy instanceof ProcessingTime) {
			ProcessingTime processingTime = (ProcessingTime) policy;
			return TimeEvictor.of(processingTime.getSize());
		} else if (policy instanceof Count) {
			Count count = (Count) policy;
			return CountEvictor.of(count.getSize());
		} else if (policy instanceof Delta) {
			@SuppressWarnings("unchecked,rawtypes")
			Delta<IN> delta = (Delta) policy;
			return DeltaEvictor.of(delta.getThreshold(), delta.getDeltaFunction());

		}


		throw new UnsupportedOperationException("Unsupported policy " + policy);
	}
	
	// ------------------------------------------------------------------------
	
	/** Don't instantiate */
	private PolicyToOperator() {}
}
