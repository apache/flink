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

import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Utility class for Flink's functions.
 */
public class FunctionUtils {

	private FunctionUtils() {
		throw new UnsupportedOperationException("This class should never be instantiated.");
	}

	private static final Function<Object, Void> NULL_FN = ignored -> null;

	private static final Consumer<Object> IGNORE_FN = ignored -> {};

	/**
	 * Function which returns {@code null} (type: Void).
	 *
	 * @param <T> input type
	 * @return Function which returns {@code null}.
	 */
	@SuppressWarnings("unchecked")
	public static <T> Function<T, Void> nullFn() {
		return (Function<T, Void>) NULL_FN;
	}

	/**
	 * Consumer which ignores the input.
	 *
	 * @param <T> type of the input
	 * @return Ignoring {@link Consumer}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Consumer<T> ignoreFn() {
		return (Consumer<T>) IGNORE_FN;
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

	/**
	 * Converts a {@link SupplierWithException} into a {@link Supplier} which throws all checked exceptions
	 * as unchecked.
	 *
	 * @param supplierWithException to convert into a {@link Supplier}
	 * @return {@link Supplier} which throws all checked exceptions as unchecked.
	 */
	public static <T> Supplier<T> uncheckedSupplier(SupplierWithException<T, ?> supplierWithException) {
		return () -> {
			T result = null;
			try {
				result = supplierWithException.get();
			} catch (Throwable t) {
				ExceptionUtils.rethrow(t);
			}
			return result;
		};
	}

	/**
	 * Converts {@link RunnableWithException} into a {@link Callable} that will return the {@code result}.
	 */
	public static <T> Callable<T> asCallable(RunnableWithException command, T result) {
		return () -> {
			command.run();
			return result;
		};
	}
}
