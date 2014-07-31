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
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.util.Collector;

/**
 * The abstract base class for Join functions. Join functions combine two data sets by joining their
 * elements on specified keys and calling this function with each pair of joining elements.
 * By default, this follows strictly the semantics of an "inner join" in SQL.
 * the semantics are those of an "inner join", meaning that elements are filtered out
 * if their key is not contained in the other data set.
 * <p>
 * Per the semantics of an inner join, the function is 
 * <p>
 * The basic syntax for using Join on two data sets is as follows:
 * <pre><blockquote>
 * DataSet<X> set1 = ...;
 * DataSet<Y> set2 = ...;
 * 
 * set1.join(set2).where(<key-definition>).equalTo(<key-definition>).with(new MyJoinFunction());
 * </blockquote></pre>
 * <p>
 * {@code set1} is here considered the first input, {@code set2} the second input.
 * The keys can be defined through tuple field positions or key extractors.
 * See {@link org.apache.flink.api.java.operators.Keys} for details.
 * <p>
 * The Join function is actually not a necessary part of a join operation. If no JoinFunction is provided,
 * the result of the operation is a sequence of Tuple2, where the elements in the tuple are those that
 * the JoinFunction would have been invoked with.
 * <P>
 * Note: You can use a {@link RichCoGroupFunction} to perform an outer join.
 * <p>
 * All functions need to be serializable, as defined in {@link java.io.Serializable}.
 * 
 * @param <IN1> The type of the elements in the first input.
 * @param <IN2> The type of the elements in the second input.
 * @param <OUT> The type of the result elements.
 */
public abstract class RichFlatJoinFunction<IN1, IN2, OUT> extends AbstractRichFunction implements FlatJoinFunction<IN1, IN2, OUT> {

	private static final long serialVersionUID = 1L;

	/**
	 * The user-defined method for performing transformations after a join.
	 * The method is called with matching pairs of elements from the inputs.
	 * 
	 * @param first The element from first input.
	 * @param second The element from second input.
	 * @return The resulting element.
	 * 
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
	 *                   to fail and may trigger recovery.
	 */
	@Override
	public abstract void join(IN1 first, IN2 second, Collector<OUT> out) throws Exception;
}
