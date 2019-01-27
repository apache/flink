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

package org.apache.flink.util.function;

import org.apache.flink.util.ExceptionUtils;

import java.util.function.BiConsumer;

/**
 * A checked extension of the {@link BiConsumer} interface.
 *
 * @param <T> type of the first argument
 * @param <U> type of the second argument
 * @param <E> type of the thrown exception
 */
@FunctionalInterface
public interface BiConsumerWithException<T, U, E extends Throwable> extends BiConsumer<T, U> {

	/**
	 * Performs this operation on the given arguments.
	 *
	 * @param t the first input argument
	 * @param u the second input argument
	 * @throws E in case of an error
	 */
	void acceptWithException(T t, U u) throws E;

	@Override
	default void accept(T t, U u) {
		try {
			acceptWithException(t, u);
		} catch (Throwable e) {
			ExceptionUtils.rethrow(e);
		}
	}
}
