/**
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

package org.apache.flink.api.common.functions;

import java.io.Serializable;

/**
 * Interface for Cross functions. Cross functions are applied to the Cartesian produce of their inputs
 * and call are called for each pair of elements.
 * 
 * They are optional, a means of convenience the can be used to directly produce manipulate the
 * pair of elements, instead of processing 2-tuples that contain the pairs.
 * <p>
 * The basic syntax for using Cross on two data sets is as follows:
 * <pre><blockquote>
 * DataSet<X> set1 = ...;
 * DataSet<Y> set2 = ...;
 * 
 * set1.cross(set2).with(new MyCrossFunction());
 * </blockquote></pre>
 * <p>
 * {@code set1} is here considered the first input, {@code set2} the second input.
 * 
 * @param <IN1> The type of the elements in the first input.
 * @param <IN2> The type of the elements in the second input.
 * @param <OUT> The type of the result elements.
 */
public interface CrossFunction<IN1, IN2, OUT> extends Function, Serializable {

	/**
	 * Cross UDF method. Called once per pair of elements in the Cartesian product of the inputs.
	 * 
	 * @param val1 Element from first input.
	 * @param val2 Element from the second input.
	 * @return The result element.
	 * 
	 * @throws Exception The function may throw Exceptions, which will cause the program to cancel,
	 *                   and may trigger the recovery logic.
	 */
	OUT cross(IN1 val1, IN2 val2) throws Exception;

}
