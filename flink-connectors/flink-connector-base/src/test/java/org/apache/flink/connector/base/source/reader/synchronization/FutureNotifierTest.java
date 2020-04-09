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

package org.apache.flink.connector.base.source.reader.synchronization;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The unit tests for {@link FutureNotifier}.
 */
public class FutureNotifierTest {

	@Test
	public void testGetFuture() {
		FutureNotifier notifier = new FutureNotifier();
		CompletableFuture<Void> future = notifier.future();
		// The future should not be null.
		assertNotNull(future);
		// Calling the future again should return the same future.
		assertEquals(future, notifier.future());
	}

	@Test
	public void testCompleteFuture() {
		FutureNotifier notifier = new FutureNotifier();
		CompletableFuture<Void> future = notifier.future();
		assertFalse(future.isDone());
		notifier.notifyComplete();
		assertTrue(future.isDone());
	}

	@Test
	public void testConcurrency() throws InterruptedException, ExecutionException {
		final int times = 1_000_000;
		final int nThreads = 5;
		FutureNotifier notifier = new FutureNotifier();
		// A thread pool that simply gets futures out of the notifier.
		ExecutorService listenerExecutor = Executors.newFixedThreadPool(nThreads);
		// A thread pool that completes the futures.
		ExecutorService notifierExecutor = Executors.newFixedThreadPool(nThreads);

		CountDownLatch runningListeners = new CountDownLatch(nThreads);
		CountDownLatch startCommand = new CountDownLatch(1);
		CountDownLatch finishLine = new CountDownLatch(1);

		List<Future<?>> executionFutures = new ArrayList<>();
		// Start nThreads thread getting futures out of the notifier.
		for (int i = 0; i < nThreads; i++) {
			executionFutures.add(listenerExecutor.submit(() -> {
				try {
					List<CompletableFuture<Void>> futures = new ArrayList<>(times);
					startCommand.await();
					for (int j = 0; j < times; j++) {
						futures.add(notifier.future());
					}
					runningListeners.countDown();
					// Wait for the notifying thread to finish.
					finishLine.await();
					// All the futures should have been completed.
					futures.forEach(f -> {
						assertNotNull(f);
						assertTrue(f.isDone());
					});
				} catch (Exception e) {
					fail();
				}
			}));
		}

		// Start nThreads thread notifying the completion.
		for (int i = 0; i < nThreads; i++) {
			notifierExecutor.submit(() -> {
				try {
					startCommand.await();
					while (runningListeners.getCount() > 0) {
						notifier.notifyComplete();
					}
					notifier.notifyComplete();
					finishLine.countDown();
				} catch (Exception e) {
					fail();
				}
			});
		}

		// Kick off the threads.
		startCommand.countDown();

		try {
			for (Future<?> executionFuture : executionFutures) {
				executionFuture.get();
			}
		} finally {
			listenerExecutor.shutdown();
			notifierExecutor.shutdown();
			listenerExecutor.awaitTermination(30L, TimeUnit.SECONDS);
			notifierExecutor.awaitTermination(30L, TimeUnit.SECONDS);
		}
	}
}
