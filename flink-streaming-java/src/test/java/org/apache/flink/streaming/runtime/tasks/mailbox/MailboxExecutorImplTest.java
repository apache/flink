/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks.mailbox;

import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.streaming.api.operators.MailboxExecutor;
import org.apache.flink.util.Preconditions;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link MailboxExecutorImpl}.
 */
public class MailboxExecutorImplTest {

	public static final int DEFAULT_PRIORITY = 0;
	private MailboxExecutor mailboxExecutor;
	private ExecutorService otherThreadExecutor;
	private TaskMailboxImpl mailbox;

	@Before
	public void setUp() throws Exception {
		this.mailbox = new TaskMailboxImpl();
		this.mailboxExecutor = new MailboxExecutorImpl(mailbox, DEFAULT_PRIORITY);
		this.otherThreadExecutor = Executors.newSingleThreadScheduledExecutor();
	}

	@After
	public void tearDown() {
		otherThreadExecutor.shutdown();
		try {
			if (!otherThreadExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
				otherThreadExecutor.shutdownNow();
				if (!otherThreadExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
					throw new IllegalStateException("Thread pool did not terminate on time!");
				}
			}
		} catch (InterruptedException ie) {
			otherThreadExecutor.shutdownNow();
			Thread.currentThread().interrupt();
		}
	}

	@Test
	public void testOperations() throws Exception {
		final TestRunnable testRunnable = new TestRunnable();
		mailboxExecutor.execute(testRunnable, "testRunnable");
		Assert.assertEquals(testRunnable, mailbox.take(DEFAULT_PRIORITY).getRunnable());

		CompletableFuture.runAsync(
			() -> mailboxExecutor.execute(testRunnable, "testRunnable"),
			otherThreadExecutor).get();
		Assert.assertEquals(testRunnable, mailbox.take(DEFAULT_PRIORITY).getRunnable());

		final TestRunnable yieldRun = new TestRunnable();
		final TestRunnable leftoverRun = new TestRunnable();
		mailboxExecutor.execute(yieldRun, "yieldRun");
		Future<?> leftoverFuture = CompletableFuture.supplyAsync(
			() -> mailboxExecutor.submit(leftoverRun, "leftoverRun"),
			otherThreadExecutor).get();

		assertTrue(mailboxExecutor.tryYield());
		Assert.assertEquals(Thread.currentThread(), yieldRun.wasExecutedBy());
		assertFalse(leftoverFuture.isDone());

		List<Mail> leftoverTasks = mailbox.close();
		Assert.assertEquals(1, leftoverTasks.size());
		assertFalse(leftoverFuture.isCancelled());
		FutureUtils.cancelRunnableFutures(leftoverTasks.stream().map(Mail::getRunnable).collect(Collectors.toList()));
		assertTrue(leftoverFuture.isCancelled());

		try {
			mailboxExecutor.tryYield();
			Assert.fail("yielding should not work after shutdown().");
		} catch (IllegalStateException expected) {
		}

		try {
			mailboxExecutor.yield();
			Assert.fail("yielding should not work after shutdown().");
		} catch (IllegalStateException expected) {
		}
	}

	@Test
	public void testTryYield() throws Exception {
		final TestRunnable testRunnable = new TestRunnable();
		CompletableFuture.runAsync(
			() -> mailboxExecutor.execute(testRunnable, "testRunnable"),
			otherThreadExecutor)
			.get();
		assertTrue(mailboxExecutor.tryYield());
		assertFalse(mailboxExecutor.tryYield());
		Assert.assertEquals(Thread.currentThread(), testRunnable.wasExecutedBy());
	}

	@Test
	public void testYield() throws Exception {
		final AtomicReference<Exception> exceptionReference = new AtomicReference<>();
		final TestRunnable testRunnable = new TestRunnable();
		final Thread submitThread = new Thread(() -> {
			try {
				mailboxExecutor.execute(testRunnable, "testRunnable");
			} catch (Exception e) {
				exceptionReference.set(e);
			}
		});

		submitThread.start();
		mailboxExecutor.yield();
		submitThread.join();

		Assert.assertNull(exceptionReference.get());
		Assert.assertEquals(Thread.currentThread(), testRunnable.wasExecutedBy());
	}

	@Test
	public void testExecutorView() throws Exception {
		CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {}, mailboxExecutor.asExecutor("runAsync"));
		assertFalse(future.isDone());

		mailboxExecutor.yield();
		assertTrue(future.isDone());
	}

	/**
	 * Test {@link Runnable} that tracks execution.
	 */
	static class TestRunnable implements Runnable {

		private Thread executedByThread = null;

		@Override
		public void run() {
			Preconditions.checkState(!isExecuted(), "Runnable was already executed before by " + executedByThread);
			executedByThread = Thread.currentThread();
		}

		boolean isExecuted() {
			return executedByThread != null;
		}

		Thread wasExecutedBy() {
			return executedByThread;
		}
	}
}
