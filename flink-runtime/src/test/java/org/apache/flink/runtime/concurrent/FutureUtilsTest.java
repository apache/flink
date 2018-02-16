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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the utility methods in {@link FutureUtils}.
 */
public class FutureUtilsTest extends TestLogger {

	/**
	 * Tests that we can retry an operation.
	 */
	@Test
	public void testRetrySuccess() throws Exception {
		final int retries = 10;
		final AtomicInteger atomicInteger = new AtomicInteger(0);
		CompletableFuture<Boolean> retryFuture = FutureUtils.retry(
			() ->
				CompletableFuture.supplyAsync(
					() -> {
						if (atomicInteger.incrementAndGet() == retries) {
							return true;
						} else {
							throw new CompletionException(new FlinkException("Test exception"));
						}
					},
					TestingUtils.defaultExecutor()),
			retries,
			TestingUtils.defaultExecutor());

		assertTrue(retryFuture.get());
		assertTrue(retries == atomicInteger.get());
	}

	/**
	 * Tests that a retry future is failed after all retries have been consumed.
	 */
	@Test(expected = FutureUtils.RetryException.class)
	public void testRetryFailure() throws Throwable {
		final int retries = 3;

		CompletableFuture<?> retryFuture = FutureUtils.retry(
			() -> FutureUtils.completedExceptionally(new FlinkException("Test exception")),
			retries,
			TestingUtils.defaultExecutor());

		try {
			retryFuture.get();
		} catch (ExecutionException ee) {
			throw ExceptionUtils.stripExecutionException(ee);
		}
	}

	/**
	 * Tests that we can cancel a retry future.
	 */
	@Test
	public void testRetryCancellation() throws Exception {
		final int retries = 10;
		final AtomicInteger atomicInteger = new AtomicInteger(0);
		final OneShotLatch notificationLatch = new OneShotLatch();
		final OneShotLatch waitLatch = new OneShotLatch();
		final AtomicReference<Throwable> atomicThrowable = new AtomicReference<>(null);

		CompletableFuture<?> retryFuture = FutureUtils.retry(
			() ->
				CompletableFuture.supplyAsync(
					() -> {
						if (atomicInteger.incrementAndGet() == 2) {
							notificationLatch.trigger();
							try {
								waitLatch.await();
							} catch (InterruptedException e) {
								atomicThrowable.compareAndSet(null, e);
							}
						}

						throw new CompletionException(new FlinkException("Test exception"));
					},
					TestingUtils.defaultExecutor()),
			retries,
			TestingUtils.defaultExecutor());

		// await that we have failed once
		notificationLatch.await();

		assertFalse(retryFuture.isDone());

		// cancel the retry future
		retryFuture.cancel(false);

		// let the retry operation continue
		waitLatch.trigger();

		assertTrue(retryFuture.isCancelled());
		assertEquals(2, atomicInteger.get());

		if (atomicThrowable.get() != null) {
			throw new FlinkException("Exception occurred in the retry operation.", atomicThrowable.get());
		}
	}

	/**
	 * Tests that retry with delay fails after having exceeded all retries.
	 */
	@Test(expected = FutureUtils.RetryException.class)
	public void testRetryWithDelayFailure() throws Throwable {
		CompletableFuture<?> retryFuture = FutureUtils.retryWithDelay(
			() -> FutureUtils.completedExceptionally(new FlinkException("Test exception")),
			3,
			Time.milliseconds(1L),
			TestingUtils.defaultScheduledExecutor());

		try {
			retryFuture.get(TestingUtils.TIMEOUT().toMilliseconds(), TimeUnit.MILLISECONDS);
		} catch (ExecutionException ee) {
			throw ExceptionUtils.stripExecutionException(ee);
		}
	}

	/**
	 * Tests that the delay is respected between subsequent retries of a retry future with retry delay.
	 */
	@Test
	public void testRetryWithDelay() throws Exception {
		final int retries = 4;
		final Time delay = Time.milliseconds(5L);
		final AtomicInteger countDown = new AtomicInteger(retries);

		long start = System.currentTimeMillis();

		CompletableFuture<Boolean> retryFuture = FutureUtils.retryWithDelay(
			() -> {
				if (countDown.getAndDecrement() == 0) {
					return CompletableFuture.completedFuture(true);
				} else {
					return FutureUtils.completedExceptionally(new FlinkException("Test exception."));
				}
			},
			retries,
			delay,
			TestingUtils.defaultScheduledExecutor());

		Boolean result = retryFuture.get();

		long completionTime = System.currentTimeMillis() - start;

		assertTrue(result);
		assertTrue("The completion time should be at least rertries times delay between retries.", completionTime >= retries * delay.toMilliseconds());
	}

	/**
	 * Tests that all scheduled tasks are canceled if the retry future is being cancelled.
	 */
	@Test
	public void testRetryWithDelayCancellation() {
		final ManuallyTriggeredScheduledExecutor scheduledExecutor = new ManuallyTriggeredScheduledExecutor();

		CompletableFuture<?> retryFuture = FutureUtils.retryWithDelay(
			() -> FutureUtils.completedExceptionally(new FlinkException("Test exception")),
			1,
			TestingUtils.infiniteTime(),
			scheduledExecutor);

		assertFalse(retryFuture.isDone());

		final Collection<ScheduledFuture<?>> scheduledTasks = scheduledExecutor.getScheduledTasks();

		assertFalse(scheduledTasks.isEmpty());

		final ScheduledFuture<?> scheduledFuture = scheduledTasks.iterator().next();

		assertFalse(scheduledFuture.isDone());

		retryFuture.cancel(false);

		assertTrue(retryFuture.isCancelled());
		assertTrue(scheduledFuture.isCancelled());
	}

	/**
	 * Tests that a future is timed out after the specified timeout.
	 */
	@Test
	public void testOrTimeout() throws Exception {
		final CompletableFuture<String> future = new CompletableFuture<>();
		final long timeout = 10L;

		FutureUtils.orTimeout(future, timeout, TimeUnit.MILLISECONDS);

		try {
			future.get();
		} catch (ExecutionException e) {
			assertTrue(ExceptionUtils.stripExecutionException(e) instanceof TimeoutException);
		}
	}

	@Test
	public void testRetryWithDelayAndPredicate() throws Exception {
		final ScheduledExecutorService retryExecutor = Executors.newSingleThreadScheduledExecutor();
		final String retryableExceptionMessage = "first exception";
		class TestStringSupplier implements Supplier<CompletableFuture<String>> {
			private final AtomicInteger counter = new AtomicInteger();

			@Override
			public CompletableFuture<String> get() {
				if (counter.getAndIncrement() == 0) {
					return FutureUtils.completedExceptionally(new RuntimeException(retryableExceptionMessage));
				} else {
					return FutureUtils.completedExceptionally(new RuntimeException("should propagate"));
				}
			}
		}

		try {
			FutureUtils.retryWithDelay(
				new TestStringSupplier(),
				1,
				Time.seconds(0),
				throwable ->
					throwable instanceof RuntimeException && throwable.getMessage().contains(retryableExceptionMessage),
				new ScheduledExecutorServiceAdapter(retryExecutor)).get();
		} catch (final ExecutionException e) {
			assertThat(e.getMessage(), containsString("Could not complete the operation"));
		} finally {
			retryExecutor.shutdownNow();
		}
	}
}
