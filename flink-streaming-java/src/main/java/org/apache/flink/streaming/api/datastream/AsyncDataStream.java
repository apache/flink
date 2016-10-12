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

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;

/**
 * A helper class to apply {@link AsyncFunction} to a data stream.
 * <p>
 * <pre>{@code
 * DataStream<String> input = ...
 * AsyncFunction<String, Tuple<String, String>> asyncFunc = ...
 *
 * AsyncDataStream.orderedWait(input, asyncFunc, 100);
 * }
 * </pre>
 */
public class AsyncDataStream {
	public enum OutputMode { ORDERED, UNORDERED }

	private static final int DEFAULT_BUFFER_SIZE = 100;

	private static <IN, OUT> SingleOutputStreamOperator<OUT> addOperator(DataStream<IN> in,
																		 AsyncFunction<IN, OUT> func,
																		 int bufSize, OutputMode mode) {
		TypeInformation<OUT> outTypeInfo =
			TypeExtractor.getUnaryOperatorReturnType((Function) func, AsyncFunction.class, false,
				true, in.getType(), Utils.getCallLocationName(), true);

		// create transform
		AsyncWaitOperator<IN, OUT> operator = new AsyncWaitOperator<>(in.getExecutionEnvironment().clean(func));
		operator.setBufferSize(bufSize);
		operator.setMode(mode);

		OneInputTransformation<IN, OUT> resultTransform = new OneInputTransformation<>(
			in.getTransformation(),
			"async wait operator",
			operator,
			outTypeInfo,
			in.getExecutionEnvironment().getParallelism());

		SingleOutputStreamOperator<OUT> returnStream =
			new SingleOutputStreamOperator<>(in.getExecutionEnvironment(), resultTransform);

		returnStream.getExecutionEnvironment().addOperator(resultTransform);

		return returnStream;
	}

	/**
	 * Add an AsyncWaitOperator. The order of output stream records may be reordered.
	 *
	 * @param in Input {@link DataStream}
	 * @param func AsyncFunction
	 * @bufSize The max number of async i/o operation that can be triggered
	 * @return A new {@link SingleOutputStreamOperator}.
	 */
	public static <IN, OUT> SingleOutputStreamOperator<OUT> unorderedWait(DataStream<IN> in,
																		  AsyncFunction<IN, OUT> func,
																		  int bufSize) {
		return addOperator(in, func, bufSize, OutputMode.UNORDERED);
	}

	public static <IN, OUT> SingleOutputStreamOperator<OUT> unorderedWait(DataStream<IN> in,
																		  AsyncFunction<IN, OUT> func) {
		return addOperator(in, func, DEFAULT_BUFFER_SIZE, OutputMode.UNORDERED);
	}

	/**
	 * Add an AsyncWaitOperator. The order of output stream records is guaranteed to be the same as input ones.
	 *
	 * @param in Input data stream
	 * @param func {@link AsyncFunction}
	 * @bufSize The max number of async i/o operation that can be triggered
	 * @return A new {@link DataStream}.
	 */
	public static <IN, OUT> SingleOutputStreamOperator<OUT> orderedWait(DataStream<IN> in,
																		AsyncFunction<IN, OUT> func,
																		int bufSize) {
		return addOperator(in, func, bufSize, OutputMode.ORDERED);
	}

	public static <IN, OUT> SingleOutputStreamOperator<OUT> orderedWait(DataStream<IN> in,
																		AsyncFunction<IN, OUT> func) {
		return addOperator(in, func, DEFAULT_BUFFER_SIZE, OutputMode.ORDERED);
	}
}
