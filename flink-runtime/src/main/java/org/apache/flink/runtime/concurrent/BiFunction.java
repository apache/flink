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

package org.apache.flink.runtime.concurrent;

/**
 * Function which is called with two arguments and returns a value.
 *
 * @param <T> type of the first argument
 * @param <U> type of the second argument
 * @param <R> type of the return value
 */
public interface BiFunction<T, U, R> {

	/**
	 * Method which handles the function call.
	 *
	 * @param t first argument
	 * @param u second argument
	 * @return the function value
	 */
	R apply(T t, U u);
}
