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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A util class to help with a clean component shutdown.
 */
public class ComponentClosingUtils {
	private static final Logger LOG = LoggerFactory.getLogger(ComponentClosingUtils.class);
	// A shared watchdog executor to handle the timeout closing.
	private static final ScheduledExecutorService watchDog =
			Executors.newSingleThreadScheduledExecutor((ThreadFactory) r -> {
				Thread t = new Thread(r, "ComponentClosingUtil");
				t.setUncaughtExceptionHandler((thread, exception) -> {
					LOG.error("FATAL: The component closing util thread caught exception ", exception);
					System.exit(-17);
				});
				return t;
			});

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
		Thread t = new Thread(() -> {
			closingSequence.run();
			future.complete(null);
		});
		// Use uncaught exception handler to handle exceptions during closing.
		t.setUncaughtExceptionHandler((thread, error) -> future.completeExceptionally(error));
		t.start();
		// Schedule a watch dog job to the watching executor to detect timeout when
		// closing the component.
		watchDog.schedule(() -> {
				if (t.isAlive()) {
					t.interrupt();
					future.completeExceptionally(new TimeoutException(
							String.format("Failed to close the %s before timeout of %d milliseconds",
									componentName, closeTimeoutMs)));
				}
			}, closeTimeoutMs, TimeUnit.MILLISECONDS);
		return future;
	}

	// ---------------------------

	private static class ClosingException extends RuntimeException {
		private static final long serialVersionUID = 2527474477287706295L;

		private ClosingException(String componentName, Exception e) {
			super(String.format("Caught exception when closing %s", componentName), e);
		}
	}
}
