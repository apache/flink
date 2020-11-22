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

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.util.function.ThrowingRunnable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A util class to help with a clean component shutdown.
 */
public class ComponentClosingUtils {

	/** Utility class, not meant to be instantiated. */
	private ComponentClosingUtils() {}

	/**
	 * Close a component with a timeout.
	 *
	 * @param componentName the name of the component.
	 * @param closingSequence the closing logic which is a callable that can throw exceptions.
	 * @param closeTimeoutMs the timeout in milliseconds to waif for the component to close.
	 * @return An optional throwable which is non-empty if an error occurred when closing the component.
	 */
	public static CompletableFuture<Void> closeAsyncWithTimeout(
			String componentName,
			ThrowingRunnable<Exception> closingSequence,
			long closeTimeoutMs) {
		return closeAsyncWithTimeout(
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
	 * @param componentName the name of the component.
	 * @param closingSequence the closing logic.
	 * @param closeTimeoutMs the timeout in milliseconds to waif for the component to close.
	 * @return An optional throwable which is non-empty if an error occurred when closing the component.
	 */
	public static CompletableFuture<Void> closeAsyncWithTimeout(
			String componentName,
			Runnable closingSequence,
			long closeTimeoutMs) {
		final CompletableFuture<Void> future = new CompletableFuture<>();
		// Start a dedicate thread to close the component.
		final Thread t = new Thread(() -> {
			closingSequence.run();
			future.complete(null);
		});
		// Use uncaught exception handler to handle exceptions during closing.
		t.setUncaughtExceptionHandler((thread, error) -> future.completeExceptionally(error));
		t.start();

		// if the future fails due to a timeout, we interrupt the thread
		future.exceptionally((error) -> {
			if (error instanceof TimeoutException && t.isAlive()) {
				abortThread(t);
			}
			return null;
		});

		FutureUtils.orTimeout(
				future,
				closeTimeoutMs, TimeUnit.MILLISECONDS,
				String.format("Failed to close the %s before timeout of %d milliseconds", componentName, closeTimeoutMs));

		return future;
	}

	static void abortThread(Thread t) {
		// the abortion strategy is pretty simple here...
		t.interrupt();
	}

	// ---------------------------

	private static class ClosingException extends RuntimeException {
		private static final long serialVersionUID = 2527474477287706295L;

		private ClosingException(String componentName, Exception e) {
			super(String.format("Caught exception when closing %s", componentName), e);
		}
	}
}
