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

import org.apache.flink.streaming.api.operators.MailboxExecutor;
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.RunnableWithException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
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
	private MailboxProcessor mailboxProcessor;

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Before
	public void setUp() throws Exception {
		this.mailbox = new TaskMailboxImpl();
		this.mailboxExecutor = new MailboxExecutorImpl(mailbox, DEFAULT_PRIORITY, StreamTaskActionExecutor.IMMEDIATE);
		this.otherThreadExecutor = Executors.newSingleThreadScheduledExecutor();
		this.mailboxProcessor = new MailboxProcessor(c -> { }, mailbox, StreamTaskActionExecutor.IMMEDIATE);
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
		AtomicBoolean wasExecuted = new AtomicBoolean(false);
		CompletableFuture.runAsync(() -> mailboxExecutor.execute(() -> wasExecuted.set(true), ""), otherThreadExecutor).get();

		mailbox.take(DEFAULT_PRIORITY).run();
		Assert.assertTrue(wasExecuted.get());
	}

	@Test
	public void testClose() throws Exception {
		final TestRunnable yieldRun = new TestRunnable();
		final TestRunnable leftoverRun = new TestRunnable();
		mailboxExecutor.execute(yieldRun, "yieldRun");
		final Future<?> leftoverFuture = CompletableFuture.supplyAsync(
			() -> mailboxExecutor.submit(leftoverRun, "leftoverRun"),
			otherThreadExecutor).get();

		assertTrue(mailboxExecutor.tryYield());
		assertEquals(Thread.currentThread(), yieldRun.wasExecutedBy());

		assertFalse(leftoverFuture.isDone());
		assertFalse(leftoverFuture.isCancelled());

		mailboxProcessor.close();
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

	/**
	 * Test {@link Runnable} that tracks execution.
	 */
	static class TestRunnable implements RunnableWithException {

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
