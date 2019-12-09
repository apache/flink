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

import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox.MAX_PRIORITY;
import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link TaskMailboxImpl}.
 */
public class TaskMailboxImplTest {

	private static final RunnableWithException NO_OP = () -> {};
	private static final int DEFAULT_PRIORITY = 0;
	/**
	 * Object under test.
	 */
	private TaskMailbox taskMailbox;

	@Before
	public void setUp() {
		taskMailbox = new TaskMailboxImpl();
	}

	@After
	public void tearDown() {
		taskMailbox.close();
	}

	@Test
	public void testPutAsHead() throws InterruptedException {

		Mail mailA = new Mail(() -> {}, MAX_PRIORITY, "mailA");
		Mail mailB = new Mail(() -> {}, MAX_PRIORITY, "mailB");
		Mail mailC = new Mail(() -> {}, DEFAULT_PRIORITY, "mailC, DEFAULT_PRIORITY");
		Mail mailD = new Mail(() -> {}, DEFAULT_PRIORITY, "mailD, DEFAULT_PRIORITY");

		taskMailbox.put(mailC);
		taskMailbox.putFirst(mailB);
		taskMailbox.put(mailD);
		taskMailbox.putFirst(mailA);

		Assert.assertSame(mailA, taskMailbox.take(DEFAULT_PRIORITY));
		Assert.assertSame(mailB, taskMailbox.take(DEFAULT_PRIORITY));
		Assert.assertSame(mailC, taskMailbox.take(DEFAULT_PRIORITY));
		Assert.assertSame(mailD, taskMailbox.take(DEFAULT_PRIORITY));

		Assert.assertFalse(taskMailbox.tryTake(DEFAULT_PRIORITY).isPresent());
	}

	@Test
	public void testContracts() throws InterruptedException {
		final Queue<Mail> testObjects = new LinkedList<>();
		Assert.assertFalse(taskMailbox.hasMail());

		for (int i = 0; i < 10; ++i) {
			final Mail mail = new Mail(NO_OP, DEFAULT_PRIORITY, "mail, DEFAULT_PRIORITY");
			testObjects.add(mail);
			taskMailbox.put(mail);
			Assert.assertTrue(taskMailbox.hasMail());
		}

		while (!testObjects.isEmpty()) {
			assertEquals(testObjects.remove(), taskMailbox.take(DEFAULT_PRIORITY));
			assertEquals(!testObjects.isEmpty(), taskMailbox.hasMail());
		}
	}

	/**
	 * Test the producer-consumer pattern using the blocking methods on the mailbox.
	 */
	@Test
	public void testConcurrentPutTakeBlocking() throws Exception {
		testPutTake(mailbox -> mailbox.take(DEFAULT_PRIORITY));
	}

	/**
	 * Test the producer-consumer pattern using the non-blocking methods & waits on the mailbox.
	 */
	@Test
	public void testConcurrentPutTakeNonBlockingAndWait() throws Exception {
		testPutTake((mailbox -> {
				Optional<Mail> optionalMail = mailbox.tryTake(DEFAULT_PRIORITY);
				while (!optionalMail.isPresent()) {
					optionalMail = mailbox.tryTake(DEFAULT_PRIORITY);
				}
				return optionalMail.get();
			}));
	}

	/**
	 * Test that closing the mailbox unblocks pending accesses with correct exceptions.
	 */
	@Test
	public void testCloseUnblocks() throws InterruptedException {
		testAllPuttingUnblocksInternal(TaskMailbox::close);
		setUp();
		testUnblocksInternal(() -> taskMailbox.take(DEFAULT_PRIORITY), TaskMailbox::close);
	}

	/**
	 * Test that silencing the mailbox unblocks pending accesses with correct exceptions.
	 */
	@Test
	public void testQuiesceUnblocks() throws InterruptedException {
		testAllPuttingUnblocksInternal(TaskMailbox::quiesce);
	}

	@Test
	public void testLifeCycleQuiesce() throws InterruptedException {
		taskMailbox.put(new Mail(NO_OP, DEFAULT_PRIORITY, "NO_OP, DEFAULT_PRIORITY"));
		taskMailbox.put(new Mail(NO_OP, DEFAULT_PRIORITY, "NO_OP, DEFAULT_PRIORITY"));
		taskMailbox.quiesce();
		testLifecyclePuttingInternal();
		taskMailbox.take(DEFAULT_PRIORITY);
		Assert.assertTrue(taskMailbox.tryTake(DEFAULT_PRIORITY).isPresent());
		Assert.assertFalse(taskMailbox.tryTake(DEFAULT_PRIORITY).isPresent());
	}

	@Test
	public void testLifeCycleClose() throws InterruptedException {
		taskMailbox.close();
		testLifecyclePuttingInternal();

		try {
			taskMailbox.take(DEFAULT_PRIORITY);
			Assert.fail();
		} catch (IllegalStateException ignore) {
		}

		try {
			taskMailbox.tryTake(DEFAULT_PRIORITY);
			Assert.fail();
		} catch (IllegalStateException ignore) {
		}
	}

	private void testLifecyclePuttingInternal() {
		try {
			taskMailbox.put(new Mail(NO_OP, DEFAULT_PRIORITY, "NO_OP, DEFAULT_PRIORITY"));
			Assert.fail();
		} catch (IllegalStateException ignore) {
		}
		try {
			taskMailbox.putFirst(new Mail(NO_OP, MAX_PRIORITY, "NO_OP"));
			Assert.fail();
		} catch (IllegalStateException ignore) {
		}
	}

	private void testAllPuttingUnblocksInternal(Consumer<TaskMailbox> unblockMethod) throws InterruptedException {
		testUnblocksInternal(
				() -> taskMailbox.put(new Mail(NO_OP, DEFAULT_PRIORITY, "NO_OP, DEFAULT_PRIORITY")),
				unblockMethod);
		setUp();
		testUnblocksInternal(() -> taskMailbox.putFirst(new Mail(NO_OP, MAX_PRIORITY, "NO_OP")), unblockMethod);
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
			assertEquals(IllegalStateException.class, exception.getClass());
		}

	}

	/**
	 * Test producer-consumer pattern through the mailbox in a concurrent setting (n-writer / 1-reader).
	 */
	private void testPutTake(FunctionWithException<TaskMailbox, Mail, InterruptedException> takeMethod) throws Exception {
		final int numThreads = 10;
		final int numMailsPerThread = 1000;
		final int[] results = new int[numThreads];
		Thread[] writerThreads = new Thread[numThreads];

		for (int i = 0; i < writerThreads.length; ++i) {
			final int threadId = i;
			writerThreads[i] = new Thread(ThrowingRunnable.unchecked(() -> {
				for (int k = 0; k < numMailsPerThread; ++k) {
					taskMailbox.put(new Mail(() -> ++results[threadId], DEFAULT_PRIORITY, "result " + k));
				}
			}));
		}

		for (Thread writerThread : writerThreads) {
			writerThread.start();
		}

		for (Thread writerThread : writerThreads) {
			writerThread.join();
		}

		AtomicBoolean isRunning = new AtomicBoolean(true);
		taskMailbox.put(new Mail(() -> isRunning.set(false), DEFAULT_PRIORITY, "POISON_MAIL, DEFAULT_PRIORITY"));

		while (isRunning.get()) {
			takeMethod.apply(taskMailbox).run();
		}
		for (int perThreadResult : results) {
			assertEquals(numMailsPerThread, perThreadResult);
		}
	}

	@Test
	public void testPutAsHeadWithPriority() throws InterruptedException {

		Mail mailA = new Mail(() -> {}, 2, "mailA");
		Mail mailB = new Mail(() -> {}, 2, "mailB");
		Mail mailC = new Mail(() -> {}, 1, "mailC");
		Mail mailD = new Mail(() -> {}, 1, "mailD");

		taskMailbox.put(mailC);
		taskMailbox.put(mailB);
		taskMailbox.put(mailD);
		taskMailbox.putFirst(mailA);

		Assert.assertSame(mailA, taskMailbox.take(2));
		Assert.assertSame(mailB, taskMailbox.take(2));
		Assert.assertFalse(taskMailbox.tryTake(2).isPresent());

		Assert.assertSame(mailC, taskMailbox.take(1));
		Assert.assertSame(mailD, taskMailbox.take(1));

		Assert.assertFalse(taskMailbox.tryTake(1).isPresent());
	}

	@Test
	public void testPutWithPriorityAndReadingFromMainMailbox() throws InterruptedException {

		Mail mailA = new Mail(() -> {}, 2, "mailA");
		Mail mailB = new Mail(() -> {}, 2, "mailB");
		Mail mailC = new Mail(() -> {}, 1, "mailC");
		Mail mailD = new Mail(() -> {}, 1, "mailD");

		taskMailbox.put(mailC);
		taskMailbox.put(mailB);
		taskMailbox.put(mailD);
		taskMailbox.putFirst(mailA);

		// same order for non-priority and priority on top
		Assert.assertSame(mailA, taskMailbox.take(TaskMailbox.MIN_PRIORITY));
		Assert.assertSame(mailC, taskMailbox.take(TaskMailbox.MIN_PRIORITY));
		Assert.assertSame(mailB, taskMailbox.take(TaskMailbox.MIN_PRIORITY));
		Assert.assertSame(mailD, taskMailbox.take(TaskMailbox.MIN_PRIORITY));
	}

	/**
	 * Tests the interaction of batch and non-batch methods.
	 *
	 * <p>Both {@link TaskMailbox#take(int)} and {@link TaskMailbox#tryTake(int)} consume the batch but once drained
	 * will fetch elements from the remaining mails.
	 *
	 * <p>In contrast, {@link TaskMailbox#tryTakeFromBatch()} will not return any mail once the batch is drained.
	 */
	@Test
	public void testBatchAndNonBatchTake() throws InterruptedException {
		final List<Mail> mails = IntStream.range(0, 6).mapToObj(i -> new Mail(
			NO_OP,
			DEFAULT_PRIORITY,
			String.valueOf(i))).collect(Collectors.toList());

		// create a batch with 3 mails
		mails.subList(0, 3).forEach(taskMailbox::put);
		Assert.assertTrue(taskMailbox.createBatch());
		// add 3 more mails after the batch
		mails.subList(3, 6).forEach(taskMailbox::put);

		// now take all mails in the batch with all available methods
		assertEquals(Optional.ofNullable(mails.get(0)), taskMailbox.tryTakeFromBatch());
		assertEquals(Optional.ofNullable(mails.get(1)), taskMailbox.tryTake(DEFAULT_PRIORITY));
		assertEquals(mails.get(2), taskMailbox.take(DEFAULT_PRIORITY));

		// batch empty, so only regular methods work
		assertEquals(Optional.empty(), taskMailbox.tryTakeFromBatch());
		assertEquals(Optional.ofNullable(mails.get(3)), taskMailbox.tryTake(DEFAULT_PRIORITY));
		assertEquals(mails.get(4), taskMailbox.take(DEFAULT_PRIORITY));

		// one unprocessed mail left
		assertEquals(Collections.singletonList(mails.get(5)), taskMailbox.close());
	}

	@Test
	public void testBatchDrain() throws Exception {

		Mail mailA = new Mail(() -> {}, MAX_PRIORITY, "mailA");
		Mail mailB = new Mail(() -> {}, MAX_PRIORITY, "mailB");

		taskMailbox.put(mailA);
		Assert.assertTrue(taskMailbox.createBatch());
		taskMailbox.put(mailB);

		assertEquals(Arrays.asList(mailA, mailB), taskMailbox.drain());
	}

	@Test
	public void testBatchPriority() throws Exception {

		Mail mailA = new Mail(() -> {}, 1, "mailA");
		Mail mailB = new Mail(() -> {}, 2, "mailB");

		taskMailbox.put(mailA);
		Assert.assertTrue(taskMailbox.createBatch());
		taskMailbox.put(mailB);

		assertEquals(mailB, taskMailbox.take(2));
		assertEquals(Optional.of(mailA), taskMailbox.tryTakeFromBatch());
	}

	/**
	 * Testing that we cannot close while running exclusively.
	 */
	@Test
	public void testRunExclusively() throws InterruptedException {
		CountDownLatch exclusiveCodeStarted = new CountDownLatch(1);

		final int numMails = 10;

		// send 10 mails in an atomic operation
		new Thread(() ->
			taskMailbox.runExclusively(() -> {
				exclusiveCodeStarted.countDown();
				for (int index = 0; index < numMails; index++) {
					try {
						taskMailbox.put(new Mail(() -> {}, 1, "mailD"));
						Thread.sleep(1);
					} catch (Exception e) {
					}
				}
			})).start();

		exclusiveCodeStarted.await();
		// make sure that all 10 messages have been actually enqueued.
		assertEquals(numMails, taskMailbox.close().size());
	}
}
