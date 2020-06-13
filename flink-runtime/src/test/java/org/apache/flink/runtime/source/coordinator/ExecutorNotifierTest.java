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

package org.apache.flink.runtime.source.coordinator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for ExecutorNotifier.
 */
public class ExecutorNotifierTest {
	private ScheduledExecutorService workerExecutor;
	private ExecutorService executorToNotify;
	private ExecutorNotifier notifier;
	private Throwable exceptionInHandler;

	@Before
	public void setup() {
		this.exceptionInHandler = null;
		this.workerExecutor = Executors.newSingleThreadScheduledExecutor(r ->
				new Thread(r, "worker-thread"));
		this.executorToNotify = Executors.newSingleThreadExecutor(r -> {
			Thread t = new Thread(r, "main-thread");
			t.setUncaughtExceptionHandler((thread, e) -> exceptionInHandler = e);
			return t;
		});
		this.notifier = new ExecutorNotifier(
				this.workerExecutor,
				this.executorToNotify,
				(t, e) -> exceptionInHandler = e);
	}

	@After
	public void tearDown() throws InterruptedException {
		notifier.close();
		closeExecutorToNotify();
	}

	@Test
	public void testBasic() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(1);
		AtomicInteger result = new AtomicInteger(0);
		notifier.notifyReadyAsync(() -> 1234, (v, e) -> {
			result.set(v);
			latch.countDown();
		});
		latch.await();
		closeExecutorToNotify();
		assertEquals(1234, result.get());
	}

	@Test
	public void testExceptionInCallable() {
		Exception exception = new Exception("Expected exception.");
		notifier.notifyReadyAsync(
				() -> {
					throw exception;
				},
				(v, e) -> {
					assertEquals(exception, e);
					assertNull(v);
				});
	}

	@Test
	public void testExceptionInHandler() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(1);
		RuntimeException exception =  new RuntimeException("Expected exception.");
		notifier.notifyReadyAsync(() -> 1234, (v, e) -> {
			latch.countDown();
			throw exception;
		});
		latch.await();
		closeExecutorToNotify();
		assertEquals(exception, exceptionInHandler);
	}

	@Test
	public void testCancelCallable() throws InterruptedException {
		CountDownLatch syncLatch = new CountDownLatch(1);
		CountDownLatch blockingLatch = new CountDownLatch(1);
		testCancel(
				syncLatch,
				blockingLatch,
				() -> {
					syncLatch.countDown();
					blockingLatch.await();
					return 0;
				},
				(v, e) -> fail("The callable should be canceled."));
	}

	@Test
	public void testCancelHandler() throws InterruptedException {
		CountDownLatch syncLatch = new CountDownLatch(1);
		CountDownLatch blockingLatch = new CountDownLatch(1);
		testCancel(
				syncLatch,
				blockingLatch,
				() -> 1234, (v, e) -> {
					try {
						syncLatch.countDown();
						blockingLatch.await();
					} catch (InterruptedException e1) {
						throw new RuntimeException("Interrupted");
					}
				});
	}

	// ------------- private helpers -------------

	private void testCancel(
			CountDownLatch syncLatch,
			CountDownLatch blockingLatch,
			Callable<Integer> callable,
			BiConsumer<Integer, Throwable> handler) throws InterruptedException {
		List<Integer> results = new ArrayList<>();
		AtomicInteger i = new AtomicInteger(0);
		// Invoke the given callable first.
		notifier.notifyReadyAsync(callable, handler);
		// Add two new async calls.
		notifier.notifyReadyAsync(() -> i.incrementAndGet() * 100, (v, e) -> results.add(v + 1));
		notifier.notifyReadyAsync(() -> i.incrementAndGet() * 100, (v, e) -> results.add(v + 1), 0, 100);
		syncLatch.await();
		// Close the notifier to ensure all the async calls finish.
		Thread t = new Thread(() -> {
			try {
				notifier.close();
			} catch (InterruptedException e) {
				fail("Interrupted unexpectedly while closing the notifier.");
			}
		});
		t.start();
		// Blocking wait util the closing thread blocks or finishes.
		while (t.isAlive() && t.getState() != Thread.State.WAITING && t.getState() != Thread.State.TIMED_WAITING) {
			Thread.sleep(1);
		}
		blockingLatch.countDown();
		t.join();
		// Shutdown the executorToNotify to ensure everything has been executed.
		executorToNotify.shutdown();
		executorToNotify.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
		// The results should be empty.
		assertTrue(results.isEmpty());
	}

	private void closeExecutorToNotify() throws InterruptedException {
		executorToNotify.shutdown();
		executorToNotify.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
	}
}
