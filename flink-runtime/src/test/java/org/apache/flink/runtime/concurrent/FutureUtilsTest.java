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
import org.mockito.invocation.InvocationOnMock;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

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
		final Time delay = Time.milliseconds(50L);
		final AtomicInteger countDown = new AtomicInteger(retries);

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

		long start = System.currentTimeMillis();

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
		ScheduledFuture<?> scheduledFutureMock = mock(ScheduledFuture.class);
		ScheduledExecutor scheduledExecutorMock = mock(ScheduledExecutor.class);
		doReturn(scheduledFutureMock).when(scheduledExecutorMock).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
		doAnswer(
			(InvocationOnMock invocation) -> {
				invocation.getArgumentAt(0, Runnable.class).run();
				return null;
			}).when(scheduledExecutorMock).execute(any(Runnable.class));

		CompletableFuture<?> retryFuture = FutureUtils.retryWithDelay(
			() -> FutureUtils.completedExceptionally(new FlinkException("Test exception")),
			1,
			TestingUtils.infiniteTime(),
			scheduledExecutorMock);

		assertFalse(retryFuture.isDone());

		verify(scheduledExecutorMock).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));

		retryFuture.cancel(false);

		assertTrue(retryFuture.isCancelled());
		verify(scheduledFutureMock).cancel(anyBoolean());
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
}
