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

package org.apache.flink.streaming.api.functions.co;

import java.io.Serializable;

import org.apache.flink.api.common.functions.Function;

/**
 * The CoReduceFunction interface represents a Reduce transformation with two
 * different input streams. The reduce1 function combine groups of elements of
 * the first input with the same key to a single value, while reduce2 combine
 * groups of elements of the second input with the same key to a single value.
 * Each produced values are mapped to the same type by map1 and map2,
 * respectively, to form one output stream.
 * 
 * The basic syntax for using a grouped ReduceFunction is as follows:
 * 
 * <pre>
 * <blockquote>
 * ConnectedDataStream<X> input = ...;
 * 
 * ConnectedDataStream<X> result = input.groupBy(keyPosition1, keyPosition2)
 *          .reduce(new MyCoReduceFunction(), keyPosition1, keyPosition2).addSink(...);
 * </blockquote>
 * </pre>
 * <p>
 * 
 * @param <IN1>
 *            Type of the first input.
 * @param <IN2>
 *            Type of the second input.
 * @param <OUT>
 *            Output type.
 */
public interface CoReduceFunction<IN1, IN2, OUT> extends Function, Serializable {

	/**
	 * The core method of CoReduceFunction, combining two values of the first
	 * input into one value of the same type. The reduce1 function is
	 * consecutively applied to all values of a group until only a single value
	 * remains.
	 *
	 * @param value1
	 *            The first value to combine.
	 * @param value2
	 *            The second value to combine.
	 * @return The combined value of both input values.
	 *
	 * @throws Exception
	 *             This method may throw exceptions. Throwing an exception will
	 *             cause the operation to fail and may trigger recovery.
	 */
	IN1 reduce1(IN1 value1, IN1 value2) throws Exception;

	/**
	 * The core method of ReduceFunction, combining two values of the second
	 * input into one value of the same type. The reduce2 function is
	 * consecutively applied to all values of a group until only a single value
	 * remains.
	 *
	 * @param value1
	 *            The first value to combine.
	 * @param value2
	 *            The second value to combine.
	 * @return The combined value of both input values.
	 *
	 * @throws Exception
	 *             This method may throw exceptions. Throwing an exception will
	 *             cause the operation to fail and may trigger recovery.
	 */
	IN2 reduce2(IN2 value1, IN2 value2) throws Exception;

	/**
	 * Maps the reduced first input to the output type.
	 * 
	 * @param value
	 *            Type of the first input.
	 * @return the output type.
	 */
	OUT map1(IN1 value) throws Exception;

	/**
	 * Maps the reduced second input to the output type.
	 * 
	 * @param value
	 *            Type of the second input.
	 * @return the output type.
	 */
	OUT map2(IN2 value) throws Exception;
}
