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
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxImpl;
import org.apache.flink.util.Preconditions;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests for {@link MailboxExecutorServiceImpl}.
 */
public class MailboxExecutorServiceImplTest {

	private MailboxExecutorServiceImpl mailboxExecutorService;
	private ExecutorService otherThreadExecutor;
	private MailboxImpl mailbox;

	@Before
	public void setUp() throws Exception {
		this.mailbox = new MailboxImpl();
		this.mailbox.open();
		this.mailboxExecutorService = new MailboxExecutorServiceImpl(mailbox);
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
	public void testOpsAndLifecycle() throws Exception {
		Assert.assertFalse(mailboxExecutorService.isShutdown());
		Assert.assertFalse(mailboxExecutorService.isTerminated());
		final TestRunnable testRunnable = new TestRunnable();
		mailboxExecutorService.execute(testRunnable);
		Assert.assertEquals(testRunnable, mailbox.tryTakeMail().get());
		CompletableFuture.runAsync(() -> mailboxExecutorService.execute(testRunnable), otherThreadExecutor).get();
		Assert.assertEquals(testRunnable, mailbox.takeMail());
		final TestRunnable yieldRun = new TestRunnable();
		final TestRunnable leftoverRun = new TestRunnable();
		mailboxExecutorService.execute(yieldRun);
		Future<?> leftoverFuture = CompletableFuture.supplyAsync(
			() -> mailboxExecutorService.submit(leftoverRun), otherThreadExecutor).get();
		mailboxExecutorService.shutdown();
		Assert.assertTrue(mailboxExecutorService.isShutdown());
		Assert.assertFalse(mailboxExecutorService.isTerminated());

		try {
			CompletableFuture.runAsync(() -> mailboxExecutorService.execute(testRunnable), otherThreadExecutor).get();
			Assert.fail("execution should not work after shutdown().");
		} catch (ExecutionException expected) {
			Assert.assertTrue(expected.getCause() instanceof RejectedExecutionException);
		}

		try {
			CompletableFuture.runAsync(() -> mailboxExecutorService.execute(testRunnable), otherThreadExecutor).get();
			Assert.fail("execution should not work after shutdown().");
		} catch (ExecutionException expected) {
			Assert.assertTrue(expected.getCause() instanceof RejectedExecutionException);
		}

		Assert.assertTrue(mailboxExecutorService.tryYield());
		Assert.assertEquals(Thread.currentThread(), yieldRun.wasExecutedBy());
		Assert.assertFalse(leftoverFuture.isDone());

		List<Runnable> leftoverTasks = mailboxExecutorService.shutdownNow();
		Assert.assertEquals(1, leftoverTasks.size());
		Assert.assertFalse(leftoverFuture.isCancelled());
		FutureUtils.cancelRunnableFutures(leftoverTasks);
		Assert.assertTrue(leftoverFuture.isCancelled());

		try {
			mailboxExecutorService.tryYield();
			Assert.fail("yielding should not work after shutdown().");
		} catch (IllegalStateException expected) {
		}

		try {
			mailboxExecutorService.yield();
			Assert.fail("yielding should not work after shutdown().");
		} catch (IllegalStateException expected) {
		}
	}

	@Test
	public void testTryYield() throws Exception {
		final TestRunnable testRunnable = new TestRunnable();
		CompletableFuture.runAsync(() -> mailboxExecutorService.execute(testRunnable), otherThreadExecutor).get();
		Assert.assertTrue(mailboxExecutorService.tryYield());
		Assert.assertFalse(mailbox.tryTakeMail().isPresent());
		Assert.assertEquals(Thread.currentThread(), testRunnable.wasExecutedBy());
	}

	@Test
	public void testYield() throws Exception {
		final AtomicReference<Exception> exceptionReference = new AtomicReference<>();
		final TestRunnable testRunnable = new TestRunnable();
		final Thread submitThread = new Thread(() -> {
			try {
				mailboxExecutorService.execute(testRunnable);
			} catch (Exception e) {
				exceptionReference.set(e);
			}
		});

		submitThread.start();
		mailboxExecutorService.yield();
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
