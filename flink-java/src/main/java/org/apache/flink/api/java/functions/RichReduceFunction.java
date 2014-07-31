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

package org.apache.flink.api.java.functions;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * The abstract base class for Reduce functions. Reduce functions combine groups of elements to
 * a single value, by taking always two elements and combining them into one. Reduce functions
 * may be used on entire data sets, or on grouped data sets. In the latter case, each group is reduced
 * individually.
 * <p>
 * For a reduce functions that work on an entire group at the same time (such as the 
 * MapReduce/Hadoop-style reduce), see {@link RichGroupReduceFunction}, called via
 * {@link org.apache.flink.api.java.DataSet#reduceGroup(RichGroupReduceFunction)}. In the general case,
 * ReduceFunctions are considered faster, because they allow the system to use hash-based
 * execution strategies.
 * <p>
 * The basic syntax for using a grouped ReduceFunction is as follows:
 * <pre><blockquote>
 * DataSet<X> input = ...;
 * 
 * DataSet<X> result = input.groupBy(<key-definition>).reduce(new MyReduceFunction());
 * </blockquote></pre>
 * <p>
 * Like all functions, the ReduceFunction needs to be serializable, as defined in {@link java.io.Serializable}.
 * 
 * @param <T> Type of the elements that this function processes.
 */
public abstract class RichReduceFunction<T> extends AbstractRichFunction implements ReduceFunction<T> {
	
	private static final long serialVersionUID = 1L;

	/**
	 * The core method of the ReduceFunction, combining two values into one value of the same type.
	 * The reduce function is consecutively applied to all values of a group until only a single value remains.
	 *
	 * @param value1 The first value to combine.
	 * @param value2 The second value to combine.
	 * @return The combined value of both input values.
	 *
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
	 *                   to fail and may trigger recovery.
	 */
	public abstract T reduce(T value1, T value2) throws Exception;
}
