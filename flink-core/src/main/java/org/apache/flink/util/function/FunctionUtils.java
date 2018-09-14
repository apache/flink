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
import java.util.function.Function;

/**
 * Utility class for Flink's functions.
 */
public class FunctionUtils {

	private FunctionUtils() {
		throw new UnsupportedOperationException("This class should never be instantiated.");
	}

	/**
	 * Convert at {@link FunctionWithException} into a {@link Function}.
	 *
	 * @param functionWithException function with exception to convert into a function
	 * @param <A> input type
	 * @param <B> output type
	 * @return {@link Function} which throws all checked exception as an unchecked exception.
	 */
	public static <A, B> Function<A, B> uncheckedFunction(FunctionWithException<A, B, ?> functionWithException) {
		return (A value) -> {
			try {
				return functionWithException.apply(value);
			} catch (Throwable t) {
				ExceptionUtils.rethrow(t);
				// we need this to appease the compiler :-(
				return null;
			}
		};
	}

	/**
	 * Converts a {@link ThrowingConsumer} into a {@link Consumer} which throws checked exceptions
	 * as unchecked.
	 *
	 * @param throwingConsumer to convert into a {@link Consumer}
	 * @param <A> input type
	 * @return {@link Consumer} which throws all checked exceptions as unchecked
	 */
	public static <A> Consumer<A> uncheckedConsumer(ThrowingConsumer<A, ?> throwingConsumer) {
		return (A value) -> {
			try {
				throwingConsumer.accept(value);
			} catch (Throwable t) {
				ExceptionUtils.rethrow(t);
			}
		};
	}
}
