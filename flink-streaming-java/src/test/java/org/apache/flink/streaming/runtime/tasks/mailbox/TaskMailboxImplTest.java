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

package org.apache.flink.streaming.runtime.tasks.mailbox;

import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

/**
 * Unit tests for {@link TaskMailboxImpl}.
 */
public class TaskMailboxImplTest {

	private static final Runnable NO_OP = () -> {};
	private static final Runnable POISON_LETTER = NO_OP;
	private static final int DEFAULT_PRIORITY = 0;
	/**
	 * Object under test.
	 */
	private TaskMailbox taskMailbox;

	@Before
	public void setUp() {
		taskMailbox = new TaskMailboxImpl();
		taskMailbox.open();
	}

	@After
	public void tearDown() {
		taskMailbox.close();
	}

	@Test
	public void testPutAsHead() throws Exception {

		Runnable instanceA = () -> {};
		Runnable instanceB = () -> {};
		Runnable instanceC = () -> {};
		Runnable instanceD = () -> {};

		taskMailbox.putMail(instanceC, DEFAULT_PRIORITY);
		taskMailbox.putFirst(instanceB);
		taskMailbox.putMail(instanceD, DEFAULT_PRIORITY);
		taskMailbox.putFirst(instanceA);

		Assert.assertSame(instanceA, taskMailbox.takeMail(DEFAULT_PRIORITY));
		Assert.assertSame(instanceB, taskMailbox.takeMail(DEFAULT_PRIORITY));
		Assert.assertSame(instanceC, taskMailbox.takeMail(DEFAULT_PRIORITY));
		Assert.assertSame(instanceD, taskMailbox.takeMail(DEFAULT_PRIORITY));

		Assert.assertFalse(taskMailbox.tryTakeMail(DEFAULT_PRIORITY).isPresent());
	}

	@Test
	public void testContracts() throws Exception {
		final Queue<Runnable> testObjects = new LinkedList<>();
		Assert.assertFalse(taskMailbox.hasMail());

		for (int i = 0; i < 10; ++i) {
			Runnable letter = NO_OP;
			testObjects.add(letter);
			taskMailbox.putMail(letter, DEFAULT_PRIORITY);
			Assert.assertTrue(taskMailbox.hasMail());
		}

		while (!testObjects.isEmpty()) {
			Assert.assertEquals(testObjects.remove(), taskMailbox.tryTakeMail(DEFAULT_PRIORITY).get());
			Assert.assertEquals(!testObjects.isEmpty(), taskMailbox.hasMail());
		}
	}

	/**
	 * Test the producer-consumer pattern using the blocking methods on the mailbox.
	 */
	@Test
	public void testConcurrentPutTakeBlocking() throws Exception {
		testPutTake(Mailbox::takeMail, Mailbox::putMail);
	}

	/**
	 * Test the producer-consumer pattern using the non-blocking methods & waits on the mailbox.
	 */
	@Test
	public void testConcurrentPutTakeNonBlockingAndWait() throws Exception {
		testPutTake((mailbox -> {
					Optional<Runnable> optionalLetter = mailbox.tryTakeMail();
				while (!optionalLetter.isPresent()) {
					optionalLetter = mailbox.tryTakeMail();
				}
				return optionalLetter.get();
			}),
			MailboxSender::putMail);
	}

	/**
	 * Test that closing the mailbox unblocks pending accesses with correct exceptions.
	 */
	@Test
	public void testCloseUnblocks() throws InterruptedException {
		testAllPuttingUnblocksInternal(TaskMailbox::close);
		setUp();
		testUnblocksInternal(() -> taskMailbox.takeMail(DEFAULT_PRIORITY), TaskMailbox::close);
	}

	/**
	 * Test that silencing the mailbox unblocks pending accesses with correct exceptions.
	 */
	@Test
	public void testQuiesceUnblocks() throws Exception {
		testAllPuttingUnblocksInternal(TaskMailbox::quiesce);
	}

	@Test
	public void testLifeCycleQuiesce() throws Exception {
		taskMailbox.putMail(NO_OP, DEFAULT_PRIORITY);
		taskMailbox.putMail(NO_OP, DEFAULT_PRIORITY);
		taskMailbox.quiesce();
		testLifecyclePuttingInternal();
		taskMailbox.takeMail(DEFAULT_PRIORITY);
		Assert.assertTrue(taskMailbox.tryTakeMail(DEFAULT_PRIORITY).isPresent());
		Assert.assertFalse(taskMailbox.tryTakeMail(DEFAULT_PRIORITY).isPresent());
	}

	@Test
	public void testLifeCycleClose() throws Exception {
		taskMailbox.close();
		testLifecyclePuttingInternal();

		try {
			taskMailbox.takeMail(DEFAULT_PRIORITY);
			Assert.fail();
		} catch (MailboxStateException ignore) {
		}

		try {
			taskMailbox.tryTakeMail(DEFAULT_PRIORITY);
			Assert.fail();
		} catch (MailboxStateException ignore) {
		}
	}

	private void testLifecyclePuttingInternal() throws Exception {
		try {
			taskMailbox.putMail(NO_OP, DEFAULT_PRIORITY);
			Assert.fail();
		} catch (MailboxStateException ignore) {
		}
		try {
			taskMailbox.putFirst(NO_OP);
			Assert.fail();
		} catch (MailboxStateException ignore) {
		}
	}

	private void testAllPuttingUnblocksInternal(Consumer<TaskMailbox> unblockMethod) throws InterruptedException {
		testUnblocksInternal(() -> taskMailbox.putMail(NO_OP, DEFAULT_PRIORITY), unblockMethod);
		setUp();
		testUnblocksInternal(() -> taskMailbox.putFirst(NO_OP), unblockMethod);
	}

	private void testUnblocksInternal(
		RunnableWithException testMethod,
			Consumer<TaskMailbox> unblockMethod) throws InterruptedException {
		final Thread[] blockedThreads = new Thread[8];
		final Exception[] exceptions = new Exception[blockedThreads.length];

		CountDownLatch countDownLatch = new CountDownLatch(blockedThreads.length);

		for (int i = 0; i < blockedThreads.length; ++i) {
			final int id = i;
			Thread blocked = new Thread(() -> {
				try {
					countDownLatch.countDown();
					while (true) {
						testMethod.run();
					}
				} catch (Exception ex) {
					exceptions[id] = ex;
				}
			});
			blockedThreads[i] = blocked;
			blocked.start();
		}

		countDownLatch.await();
		unblockMethod.accept(taskMailbox);

		for (Thread blockedThread : blockedThreads) {
			blockedThread.join();
		}

		for (Exception exception : exceptions) {
			Assert.assertEquals(MailboxStateException.class, exception.getClass());
		}

	}

	/**
	 * Test producer-consumer pattern through the mailbox in a concurrent setting (n-writer / 1-reader).
	 */
	private void testPutTake(
			FunctionWithException<Mailbox, Runnable, Exception> takeMethod,
			BiConsumerWithException<Mailbox, Runnable, Exception> putMethod) throws Exception {
		final int numThreads = 10;
		final int numLettersPerThread = 1000;
		final int[] results = new int[numThreads];
		Thread[] writerThreads = new Thread[numThreads];
		Mailbox mailbox = taskMailbox.getDownstreamMailbox(DEFAULT_PRIORITY);
		Thread readerThread = new Thread(ThrowingRunnable.unchecked(() -> {
			Runnable letter;
			while ((letter = takeMethod.apply(mailbox)) != POISON_LETTER) {
				letter.run();
			}
		}));

		readerThread.start();
		for (int i = 0; i < writerThreads.length; ++i) {
			final int threadId = i;
			writerThreads[i] = new Thread(ThrowingRunnable.unchecked(() -> {
				for (int k = 0; k < numLettersPerThread; ++k) {
					putMethod.accept(mailbox, () -> ++results[threadId]);
				}
			}));
		}

		for (Thread writerThread : writerThreads) {
			writerThread.start();
		}

		for (Thread writerThread : writerThreads) {
			writerThread.join();
		}

		taskMailbox.putMail(POISON_LETTER, DEFAULT_PRIORITY);

		readerThread.join();
		for (int perThreadResult : results) {
			Assert.assertEquals(numLettersPerThread, perThreadResult);
		}
	}

	/**
	 * Tests the downstream view of {@link TaskMailbox}.
	 */
	public static class DownstreamMailboxTest {
		/**
		 * Object under test.
		 */
		private TaskMailboxImpl taskMailbox;

		@Before
		public void setUp() {
			taskMailbox = new TaskMailboxImpl();
			taskMailbox.open();
		}

		@After
		public void tearDown() {
			taskMailbox.close();
		}

		@Test
		public void testPutAsHeadInDownstream() throws Exception {

			Runnable instanceA = () -> {};
			Runnable instanceB = () -> {};
			Runnable instanceC = () -> {};
			Runnable instanceD = () -> {};

			taskMailbox.getDownstreamMailbox(1).putMail(instanceC);
			taskMailbox.getDownstreamMailbox(2).putMail(instanceB);
			taskMailbox.getDownstreamMailbox(1).putMail(instanceD);
			taskMailbox.getDownstreamMailbox(2).putFirst(instanceA);

			Assert.assertSame(instanceA, taskMailbox.getDownstreamMailbox(2).takeMail());
			Assert.assertSame(instanceB, taskMailbox.getDownstreamMailbox(2).takeMail());
			Assert.assertFalse(taskMailbox.getDownstreamMailbox(2).tryTakeMail().isPresent());

			Assert.assertSame(instanceC, taskMailbox.getDownstreamMailbox(1).takeMail());
			Assert.assertSame(instanceD, taskMailbox.getDownstreamMailbox(1).takeMail());

			Assert.assertFalse(taskMailbox.getDownstreamMailbox(1).tryTakeMail().isPresent());
		}

		@Test
		public void testPutInDownstreamAndReadingFromTaskMailbox() throws Exception {

			Runnable instanceA = () -> {};
			Runnable instanceB = () -> {};
			Runnable instanceC = () -> {};
			Runnable instanceD = () -> {};

			taskMailbox.getDownstreamMailbox(1).putMail(instanceC);
			taskMailbox.getDownstreamMailbox(2).putMail(instanceB);
			taskMailbox.getDownstreamMailbox(1).putMail(instanceD);
			taskMailbox.getDownstreamMailbox(2).putFirst(instanceA);

			// same order for non-priority and priority on top
			Assert.assertSame(instanceA, taskMailbox.takeMail(TaskMailbox.MIN_PRIORITY));
			Assert.assertSame(instanceC, taskMailbox.takeMail(TaskMailbox.MIN_PRIORITY));
			Assert.assertSame(instanceB, taskMailbox.takeMail(TaskMailbox.MIN_PRIORITY));
			Assert.assertSame(instanceD, taskMailbox.takeMail(TaskMailbox.MIN_PRIORITY));
		}
	}
}
