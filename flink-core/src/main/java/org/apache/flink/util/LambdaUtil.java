/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util;

import org.apache.flink.core.plugin.TemporaryClassLoaderContext;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.util.function.ThrowingConsumer;
import org.apache.flink.util.function.ThrowingRunnable;

/**
 * This class offers utility functions for Java's lambda features.
 */
public final class LambdaUtil {

	private LambdaUtil() {
		throw new AssertionError();
	}

	/**
	 * This method supplies all elements from the input to the consumer. Exceptions that happen on elements are
	 * suppressed until all elements are processed. If exceptions happened for one or more of the inputs, they are
	 * reported in a combining suppressed exception.
	 *
	 * @param inputs iterator for all inputs to the throwingConsumer.
	 * @param throwingConsumer this consumer will be called for all elements delivered by the input iterator.
	 * @param <T> the type of input.
	 * @throws Exception collected exceptions that happened during the invocation of the consumer on the input elements.
	 */
	public static <T> void applyToAllWhileSuppressingExceptions(
		Iterable<T> inputs,
		ThrowingConsumer<T, ? extends Exception> throwingConsumer) throws Exception {

		if (inputs != null && throwingConsumer != null) {
			Exception exception = null;

			for (T input : inputs) {

				if (input != null) {
					try {
						throwingConsumer.accept(input);
					} catch (Exception ex) {
						exception = ExceptionUtils.firstOrSuppressed(ex, exception);
					}
				}
			}

			if (exception != null) {
				throw exception;
			}
		}
	}

	/**
	 * Runs the given runnable with the given ClassLoader as the thread's
	 * {@link Thread#setContextClassLoader(ClassLoader) context class loader}.
	 *
	 * <p>The method will make sure to set the context class loader of the calling thread
	 * back to what it was before after the runnable completed.
	 */
	public static <E extends Throwable> void withContextClassLoader(
			final ClassLoader cl,
			final ThrowingRunnable<E> r) throws E {

		try (TemporaryClassLoaderContext tmpCl = new TemporaryClassLoaderContext(cl)) {
			r.run();
		}
	}

	/**
	 * Runs the given runnable with the given ClassLoader as the thread's
	 * {@link Thread#setContextClassLoader(ClassLoader) context class loader}.
	 *
	 * <p>The method will make sure to set the context class loader of the calling thread
	 * back to what it was before after the runnable completed.
	 */
	public static <R, E extends Throwable> R withContextClassLoader(
			final ClassLoader cl,
			final SupplierWithException<R, E> s) throws E {

		try (TemporaryClassLoaderContext tmpCl = new TemporaryClassLoaderContext(cl)) {
			return s.get();
		}
	}
}
