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

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.windowing.KeyedWindowFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.windowing.windowpolicy.WindowPolicy;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.PolicyToOperator;

/**
 * A KeyedWindowDataStream represents a data stream where elements are grouped by key, and 
 * for each key, the stream of elements is split into windows. The windows are conceptually
 * evaluated for each key individually, meaning windows can trigger at different points
 * for each key.
 * <p>
 * In many cases, however, the windows are "aligned", meaning they trigger at the
 * same time for all keys. The most common example for that are the regular time windows.
 * <p>
 * Note that the KeyedWindowDataStream is purely and API construct, during runtime the
 * KeyedWindowDataStream will be collapsed together with the KeyedDataStream and the operation
 * over the window into one single operation.
 * 
 * @param <T> The type of elements in the stream.
 * @param <K> The type of the key by which elements are grouped.
 */
public class KeyedWindowDataStream<T, K> {

	/** The keyed data stream that is windowed by this stream */
	private final KeyedDataStream<T, K> input;

	/** The core window policy */
	private final WindowPolicy windowPolicy;

	/** The optional additional slide policy */
	private final WindowPolicy slidePolicy;
	
	
	public KeyedWindowDataStream(KeyedDataStream<T, K> input, WindowPolicy windowPolicy) {
		this(input, windowPolicy, null);
	}

	public KeyedWindowDataStream(KeyedDataStream<T, K> input,
								WindowPolicy windowPolicy, WindowPolicy slidePolicy) 
	{
		TimeCharacteristic time = input.getExecutionEnvironment().getStreamTimeCharacteristic();

		this.input = input;
		this.windowPolicy = windowPolicy.makeSpecificBasedOnTimeCharacteristic(time);
		this.slidePolicy = slidePolicy == null ? null : slidePolicy.makeSpecificBasedOnTimeCharacteristic(time);
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
		return createWindowOperator(function, input.getType(), "Reduce at " + callLocation);
	}

	/**
	 * Applies a window function to the window. The window function is called for each evaluation
	 * of the window for each key individually. The output of the window function is interpreted
	 * as a regular non-windowed stream.
	 * <p>
	 * Not that this function requires that all data in the windows is buffered until the window
	 * is evaluated, as the function provides no means of pre-aggregation.
	 * 
	 * @param function The window function.
	 * @return The data stream that is the result of applying the window function to the window.
	 */
	public <Result> DataStream<Result> mapWindow(KeyedWindowFunction<T, Result, K, Window> function) {
		String callLocation = Utils.getCallLocationName();

		TypeInformation<T> inType = input.getType();
		TypeInformation<Result> resultType = TypeExtractor.getUnaryOperatorReturnType(
				function, KeyedWindowFunction.class, true, true, inType, null, false);

		return createWindowOperator(function, resultType, "KeyedWindowFunction at " + callLocation);
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------
	
	private <Result> DataStream<Result> createWindowOperator(
			Function function, TypeInformation<Result> resultType, String functionName) {

		String opName = windowPolicy.toString(slidePolicy) + " of " + functionName;
		KeySelector<T, K> keySel = input.getKeySelector();
		
		OneInputStreamOperator<T, Result> operator =
				PolicyToOperator.createOperatorForPolicies(windowPolicy, slidePolicy, function, keySel);
		
		return input.transform(opName, resultType, operator);
	}
}
