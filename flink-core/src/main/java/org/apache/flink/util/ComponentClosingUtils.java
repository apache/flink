/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.flink.util;

import org.apache.flink.util.function.ThrowingRunnable;

import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * A util class to help with a clean component shutdown.
 */
public class ComponentClosingUtils {
	// The default timeout to close a component.
	private static final long DEFAULT_CLOSE_TIMEOUT = 60_000L;

	private ComponentClosingUtils() {}

	/**
	 * Close a component asynchronously.
	 *
	 * @param componentName the name of the component.
	 * @param closingSequence the closing logic which is a {@link ThrowingRunnable} that can throw exceptions.
	 * @param exceptionHandler the exception handler that handles exceptions thrown from the closing sequence.
	 */
	public static void closeAsynchronously(
			final String componentName,
			final ThrowingRunnable<Exception> closingSequence,
			final Consumer<Throwable> exceptionHandler) {
		closeAsynchronously(componentName, closingSequence, exceptionHandler, DEFAULT_CLOSE_TIMEOUT);
	}

	/**
	 * Close a component asynchronously with a timeout.
	 *
	 * @param componentName the name of the component.
	 * @param closingSequence the closing logic which is a {@link ThrowingRunnable} that can throw exceptions.
	 * @param exceptionHandler the exception handler that handles exceptions thrown from the closing sequence.
	 * @param timeoutMs the max time in milliseconds to wait for the component to close. A {@link TimeoutException}
	 *                  will be passed to the exceptionHandler if the component did not close before timeout.
	 */
	public static void closeAsynchronously(
			final String componentName,
			final ThrowingRunnable<Exception> closingSequence,
			final Consumer<Throwable> exceptionHandler,
			long timeoutMs) {
		Thread t = new Thread(() ->
				closeWithTimeout(componentName, closingSequence, timeoutMs)
						.ifPresent(exceptionHandler));
		t.setUncaughtExceptionHandler((ignored, error) -> exceptionHandler.accept(error));
		t.start();
	}

	/**
	 * Close a component with a timeout.
	 *
	 * <p>Note that this method does not guarantee the component has been successfully closed
	 * when it returns.
	 *
	 * @param componentName the name of the component.
	 * @param closingSequence the closing logic which is a {@link ThrowingRunnable} that can throw exceptions.
	 * @param closeTimeoutMs the timeout in milliseconds to waif for the component to close.
	 * @return An optional throwable which is non-empty if an error occurred when closing the component.
	 */
	public static Optional<Throwable> closeWithTimeout(
			String componentName,
			ThrowingRunnable<Exception> closingSequence,
			long closeTimeoutMs) {
		return closeWithTimeout(
				componentName,
				(Runnable) () -> {
					try {
						closingSequence.run();
					} catch (Exception e) {
						throw new ClosingException(componentName, e);
					}
				}, closeTimeoutMs);
	}

	/**
	 * Close a component with a timeout.
	 *
	 * <p>Note that this method does not guarantee the component has been successfully closed
	 * when it returns.
	 *
	 * @param componentName the name of the component.
	 * @param closingSequence the closing logic.
	 * @param closeTimeoutMs the timeout in milliseconds to waif for the component to close.
	 * @return An optional throwable which is non-empty if an error occurred when closing the component.
	 */
	public static Optional<Throwable> closeWithTimeout(
			String componentName,
			Runnable closingSequence,
			long closeTimeoutMs) {
		final AtomicReference<Throwable> closingError = new AtomicReference<>();
		Thread t = new Thread(closingSequence, componentName + " Closing Thread");
		t.setUncaughtExceptionHandler((thread, error) -> reportError(closingError, error));
		t.start();
		try {
			t.join(closeTimeoutMs);
		} catch (InterruptedException e) {
			// Only add the interrupted exception to suppressed if it is the first error.
			// Otherwise, either a user error or a TimeoutException would have been added.
			reportError(closingError, e);
		}
		if (t.isAlive()) {
			reportError(closingError, new TimeoutException(
					String.format("Failed to close the %s before timeout of %d milliseconds",
							componentName, closeTimeoutMs)));
			t.interrupt();
		}
		return Optional.ofNullable(closingError.get());
	}

	// ---------------------------

	private static void reportError(AtomicReference<Throwable> errorRef, Throwable toReport) {
		if (!errorRef.compareAndSet(null, toReport)) {
			errorRef.get().addSuppressed(toReport);
		}
	}

	private static class ClosingException extends RuntimeException {
		private static final long serialVersionUID = 2527474477287706295L;

		private ClosingException(String componentName, Exception e) {
			super(String.format("Caught exception when closing %s", componentName), e);
		}
	}
}
