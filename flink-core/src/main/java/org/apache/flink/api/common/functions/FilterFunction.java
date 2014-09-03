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
 * base interface for Filter functions. A filter function take elements and evaluates a
 * predicate on them to decide whether to keep the element, or to discard it.
 * <p>
 * The basic syntax for using a FilterFunction is as follows:
 * <pre><blockquote>
 * DataSet<X> input = ...;
 * 
 * DataSet<X> result = input.filter(new MyFilterFunction());
 * </blockquote></pre>
 * 
 * @param <T> The type of the filtered elements.
 */
public interface FilterFunction<T> extends Function, Serializable {
	
	/**
	 * The filter function that evaluates the predicate.
	 * 
	 * @param value The value to be filtered.
	 * @return True for values that should be retained, false for values to be filtered out.
	 * 
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
	 *                   to fail and may trigger recovery.
	 */
	boolean filter(T value) throws Exception;
}
