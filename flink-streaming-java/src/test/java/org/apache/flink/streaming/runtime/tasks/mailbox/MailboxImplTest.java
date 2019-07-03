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
 * Unit tests for {@link MailboxImpl}.
 */
public class MailboxImplTest {

	private static final Runnable POISON_LETTER = () -> {};
	/**
	 * Object under test.
	 */
	private Mailbox mailbox;

	@Before
	public void setUp() {
		mailbox = new MailboxImpl();
		mailbox.open();
	}

	@After
	public void tearDown() {
		mailbox.close();
	}

	@Test
	public void testPutAsHead() throws Exception {

		Runnable instanceA = () -> {};
		Runnable instanceB = () -> {};
		Runnable instanceC = () -> {};
		Runnable instanceD = () -> {};

		mailbox.putMail(instanceC);
		mailbox.putFirst(instanceB);
		mailbox.putMail(instanceD);
		mailbox.putFirst(instanceA);

		Assert.assertSame(instanceA, mailbox.takeMail());
		Assert.assertSame(instanceB, mailbox.takeMail());
		Assert.assertSame(instanceC, mailbox.takeMail());
		Assert.assertSame(instanceD, mailbox.takeMail());

		Assert.assertFalse(mailbox.tryTakeMail().isPresent());
	}

	@Test
	public void testContracts() throws Exception {
		final Queue<Runnable> testObjects = new LinkedList<>();
		Assert.assertFalse(mailbox.hasMail());

		for (int i = 0; i < 10; ++i) {
			Runnable letter = () -> {};
			testObjects.add(letter);
			mailbox.putMail(letter);
			Assert.assertTrue(mailbox.hasMail());
		}

		while (!testObjects.isEmpty()) {
			Assert.assertEquals(testObjects.remove(), mailbox.tryTakeMail().get());
			Assert.assertEquals(!testObjects.isEmpty(), mailbox.hasMail());
		}
	}

	/**
	 * Test the producer-consumer pattern using the blocking methods on the mailbox.
	 */
	@Test
	public void testConcurrentPutTakeBlocking() throws Exception {
		testPutTake(MailboxReceiver::takeMail, MailboxSender::putMail);
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
		testAllPuttingUnblocksInternal(Mailbox::close);
		setUp();
		testUnblocksInternal(() -> mailbox.takeMail(), Mailbox::close);
	}

	/**
	 * Test that silencing the mailbox unblocks pending accesses with correct exceptions.
	 */
	@Test
	public void testQuiesceUnblocks() throws Exception {
		testAllPuttingUnblocksInternal(Mailbox::quiesce);
	}

	@Test
	public void testLifeCycleQuiesce() throws Exception {
		mailbox.putMail(() -> {});
		mailbox.putMail(() -> {});
		mailbox.quiesce();
		testLifecyclePuttingInternal();
		mailbox.takeMail();
		Assert.assertTrue(mailbox.tryTakeMail().isPresent());
		Assert.assertFalse(mailbox.tryTakeMail().isPresent());
	}

	@Test
	public void testLifeCycleClose() throws Exception {
		mailbox.close();
		testLifecyclePuttingInternal();

		try {
			mailbox.takeMail();
			Assert.fail();
		} catch (MailboxStateException ignore) {
		}

		try {
			mailbox.tryTakeMail();
			Assert.fail();
		} catch (MailboxStateException ignore) {
		}
	}

	private void testLifecyclePuttingInternal() throws Exception {
		try {
			mailbox.putMail(() -> {});
			Assert.fail();
		} catch (MailboxStateException ignore) {
		}
		try {
			mailbox.putFirst(() -> {});
			Assert.fail();
		} catch (MailboxStateException ignore) {
		}
	}

	private void testAllPuttingUnblocksInternal(Consumer<Mailbox> unblockMethod) throws InterruptedException {
		testUnblocksInternal(() -> mailbox.putMail(() -> {}), unblockMethod);
		setUp();
		testUnblocksInternal(() -> mailbox.putFirst(() -> {}), unblockMethod);
	}

	private void testUnblocksInternal(
		RunnableWithException testMethod,
		Consumer<Mailbox> unblockMethod) throws InterruptedException {
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
		unblockMethod.accept(mailbox);

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

		mailbox.putMail(POISON_LETTER);

		readerThread.join();
		for (int perThreadResult : results) {
			Assert.assertEquals(numLettersPerThread, perThreadResult);
		}
	}
}
