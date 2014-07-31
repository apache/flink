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
import org.apache.flink.api.common.functions.CrossFunction;


/**
 * The abstract base class for Cross functions. Cross functions build a Cartesian produce of their inputs
 * and call the function or each pair of elements.
 * They are a means of convenience and can be used to directly produce manipulate the
 * pair of elements, instead of having the operator build 2-tuples, and then using a
 * MapFunction over those 2-tuples.
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
 * <p>
 * All functions need to be serializable, as defined in {@link java.io.Serializable}.
 * 
 * @param <IN1> The type of the elements in the first input.
 * @param <IN2> The type of the elements in the second input.
 * @param <OUT> The type of the result elements.
 */
public abstract class RichCrossFunction<IN1, IN2, OUT> extends AbstractRichFunction implements CrossFunction<IN1, IN2, OUT> {
	
	private static final long serialVersionUID = 1L;
	

	/**
	 * The core method of the cross operation. The method will be invoked for each pair of elements
	 * in the Cartesian product.
	 * 
	 * @param first The element from the first input.
	 * @param second The element from the second input.
	 * @return The result element.
	 * 
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
	 *                   to fail and may trigger recovery.
	 */
	@Override
	public abstract OUT cross(IN1 first, IN2 second) throws Exception;

}
