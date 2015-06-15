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

package org.apache.flink.api.common.functions;

/**
 * This is like a ReduceFunction, except that it also contains the inverse of the function
 * (eg. subtraction for addition) which will be used to speed up a {@link DataStream.reduceWindow}:
 * the system will keep track of the reduced value as the window changes, and update it in O(1) time on evictions, by
 * "subtracting" the evicted element from the reduced value.
 *
 * Like all functions, the ReduceFunction needs to be serializable, as defined in {@link java.io.Serializable}.
 *
 * @param <T> Type of the elements that this function processes.
 */
public interface ReduceFunctionWithInverse<T> extends ReduceFunction<T> {

	/**
	 * This should be the inverse of reduce, that is
	 * {@code invReduce(reduce(a, b), a)}
	 * should return b.
	 *
	 * @param value1 The value to "subtract" from
	 * @param value2 The value to be "subtracted"
	 * @return value1 - value2
	 *
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
	 *                   to fail and may trigger recovery.
	 */
	T invReduce(T value1, T value2) throws Exception;
}
