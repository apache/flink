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

package org.apache.flink.streaming.runtime.operators.windows;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.windows.KeyedWindowFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.windowing.windowpolicy.EventTime;
import org.apache.flink.streaming.api.windowing.windowpolicy.ProcessingTime;
import org.apache.flink.streaming.api.windowing.windowpolicy.WindowPolicy;

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
						new AggregatingProcessingTimeWindowOperator<KEY, IN>(
								reducer, keySelector, windowLength, windowSlide);
				return op;
			}
			else if (function instanceof KeyedWindowFunction) {
				@SuppressWarnings("unchecked")
				KeyedWindowFunction<IN, OUT, KEY> wf = (KeyedWindowFunction<IN, OUT, KEY>) function;

				return new AccumulatingProcessingTimeWindowOperator<KEY, IN, OUT>(
								wf, keySelector, windowLength, windowSlide);
			}
		}

		// -- case 2: both policies are event time policies
		if (window instanceof EventTime && (slide == null || slide instanceof EventTime)) {
			// add event time implementation
		}
		
		throw new UnsupportedOperationException("The windowing mechanism does not yet support " + window.toString(slide));
	}
	
	// ------------------------------------------------------------------------
	
	/** Don't instantiate */
	private PolicyToOperator() {}
}
