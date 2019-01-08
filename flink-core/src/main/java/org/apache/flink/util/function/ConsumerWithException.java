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

import java.util.function.Consumer;

/**
 * A checked extension of the {@link Consumer} interface.
 *
 * @param <T> type of the first argument
 * @param <E> type of the thrown exception
 */
public interface ConsumerWithException<T, E extends Throwable> extends Consumer<T> {

	void acceptWithException(T value) throws E;

	@Override
	default void accept(T value) {
		try {
			acceptWithException(value);
		} catch (Throwable t) {
			ExceptionUtils.rethrow(t);
		}
	}
}
