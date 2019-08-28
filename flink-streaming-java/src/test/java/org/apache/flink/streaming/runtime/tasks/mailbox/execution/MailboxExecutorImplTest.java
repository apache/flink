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

package org.apache.flink.streaming.runtime.tasks.mailbox.execution;

import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;
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
		this.mailbox.open();
		this.mailboxExecutor = new MailboxExecutorImpl(mailbox.getDownstreamMailbox(DEFAULT_PRIORITY));
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
		mailboxExecutor.execute(testRunnable);
		Assert.assertEquals(testRunnable, mailbox.tryTakeMail(DEFAULT_PRIORITY).get());
		CompletableFuture.runAsync(() -> mailboxExecutor.execute(testRunnable), otherThreadExecutor).get();
		Assert.assertEquals(testRunnable, mailbox.takeMail(DEFAULT_PRIORITY));
		final TestRunnable yieldRun = new TestRunnable();
		final TestRunnable leftoverRun = new TestRunnable();
		mailboxExecutor.execute(yieldRun);
		Future<?> leftoverFuture = CompletableFuture.supplyAsync(
				() -> mailboxExecutor.submit(leftoverRun),
				otherThreadExecutor).get();

		Assert.assertTrue(mailboxExecutor.tryYield());
		Assert.assertEquals(Thread.currentThread(), yieldRun.wasExecutedBy());
		Assert.assertFalse(leftoverFuture.isDone());

		List<Runnable> leftoverTasks = mailbox.close();
		Assert.assertEquals(1, leftoverTasks.size());
		Assert.assertFalse(leftoverFuture.isCancelled());
		FutureUtils.cancelRunnableFutures(leftoverTasks);
		Assert.assertTrue(leftoverFuture.isCancelled());

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
		CompletableFuture.runAsync(() -> mailboxExecutor.execute(testRunnable), otherThreadExecutor).get();
		Assert.assertTrue(mailboxExecutor.tryYield());
		Assert.assertFalse(mailbox.tryTakeMail(DEFAULT_PRIORITY).isPresent());
		Assert.assertEquals(Thread.currentThread(), testRunnable.wasExecutedBy());
	}

	@Test
	public void testYield() throws Exception {
		final AtomicReference<Exception> exceptionReference = new AtomicReference<>();
		final TestRunnable testRunnable = new TestRunnable();
		final Thread submitThread = new Thread(() -> {
			try {
				mailboxExecutor.execute(testRunnable);
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
