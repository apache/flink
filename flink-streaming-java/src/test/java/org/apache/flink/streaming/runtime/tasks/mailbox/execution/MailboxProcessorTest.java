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

import org.apache.flink.core.testutils.OneShotLatch;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for {@link MailboxProcessor}.
 */
public class MailboxProcessorTest {

	@Test
	public void testRejectIfNotOpen() {
		MailboxProcessor mailboxProcessor = new MailboxProcessor((ctx) -> {});
		try {
			mailboxProcessor.getMailboxExecutor().execute(() -> {});
			Assert.fail("Should not be able to accept runnables if not opened.");
		} catch (RejectedExecutionException expected) {
		}
	}

	@Test
	public void testShutdown() {
		MailboxProcessor mailboxProcessor = new MailboxProcessor((ctx) -> {});
		FutureTask<Void> testRunnableFuture = new FutureTask<>(() -> {}, null);
		mailboxProcessor.open();
		mailboxProcessor.getMailboxExecutor().execute(testRunnableFuture);
		mailboxProcessor.prepareClose();

		try {
			mailboxProcessor.getMailboxExecutor().execute(() -> {});
			Assert.fail("Should not be able to accept runnables if not opened.");
		} catch (RejectedExecutionException expected) {
		}

		Assert.assertFalse(testRunnableFuture.isDone());

		mailboxProcessor.close();
		Assert.assertTrue(testRunnableFuture.isCancelled());
	}

	@Test
	public void testRunDefaultActionAndLetters() throws Exception {
		AtomicBoolean stop = new AtomicBoolean(false);
		MailboxThread mailboxThread = new MailboxThread() {
			@Override
			public void runDefaultAction(DefaultActionContext context) throws Exception {
				if (stop.get()) {
					context.allActionsCompleted();
				} else {
					Thread.sleep(10L);
				}
			}
		};

		MailboxProcessor mailboxProcessor = start(mailboxThread);
		mailboxProcessor.getMailboxExecutor().execute(() -> stop.set(true));
		stop(mailboxThread);
	}

	@Test
	public void testRunDefaultAction() throws Exception {

		final int expectedInvocations = 3;
		final AtomicInteger counter = new AtomicInteger(0);
		MailboxThread mailboxThread = new MailboxThread() {
			@Override
			public void runDefaultAction(DefaultActionContext context) {
				if (counter.incrementAndGet() == expectedInvocations) {
					context.allActionsCompleted();
				}
			}
		};

		start(mailboxThread);
		stop(mailboxThread);
		Assert.assertEquals(expectedInvocations, counter.get());
	}

	@Test
	public void testSignalUnAvailable() throws Exception {

		final AtomicInteger counter = new AtomicInteger(0);
		final AtomicReference<SuspendedMailboxDefaultAction> suspendedActionRef = new AtomicReference<>();
		final OneShotLatch actionSuspendedLatch = new OneShotLatch();
		final int blockAfterInvocations = 3;
		final int totalInvocations = blockAfterInvocations * 2;

		MailboxThread mailboxThread = new MailboxThread() {
			@Override
			public void runDefaultAction(DefaultActionContext context) {
				if (counter.incrementAndGet() == blockAfterInvocations) {
					suspendedActionRef.set(context.suspendDefaultAction());
					actionSuspendedLatch.trigger();
				} else if (counter.get() == totalInvocations) {
					context.allActionsCompleted();
				}
			}
		};

		MailboxProcessor mailboxProcessor = start(mailboxThread);
		actionSuspendedLatch.await();
		Assert.assertEquals(blockAfterInvocations, counter.get());

		SuspendedMailboxDefaultAction suspendedMailboxDefaultAction = suspendedActionRef.get();
		mailboxProcessor.getMailboxExecutor().execute(suspendedMailboxDefaultAction::resume);
		stop(mailboxThread);
		Assert.assertEquals(totalInvocations, counter.get());
	}

	@Test
	public void testSignalUnAvailablePingPong() throws Exception {
		final AtomicReference<SuspendedMailboxDefaultAction> suspendedActionRef = new AtomicReference<>();
		final int totalSwitches = 10000;
		final MailboxThread mailboxThread = new MailboxThread() {
			int count = 0;

			@Override
			public void runDefaultAction(DefaultActionContext context) {

				// If this is violated, it means that the default action was invoked while we assumed suspension
				Assert.assertTrue(suspendedActionRef.compareAndSet(null, context.suspendDefaultAction()));

				++count;

				if (count == totalSwitches) {
					context.allActionsCompleted();
				} else if (count % 1000 == 0) {
					try {
						Thread.sleep(1L);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				}
			}
		};

		mailboxThread.start();
		final MailboxProcessor mailboxProcessor = mailboxThread.getMailboxProcessor();
		mailboxProcessor.open();

		final Thread asyncUnblocker = new Thread(() -> {
			int count = 0;
			while (!Thread.currentThread().isInterrupted()) {

				final SuspendedMailboxDefaultAction resume =
					suspendedActionRef.getAndSet(null);
				if (resume != null) {
					mailboxProcessor.getMailboxExecutor().execute(resume::resume);
				} else {
					try {
						mailboxProcessor.getMailboxExecutor().execute(() -> { });
					} catch (RejectedExecutionException ignore) {
					}
				}

				++count;
				if (count % 5000 == 0) {
					try {
						Thread.sleep(1L);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				}
			}
		});

		asyncUnblocker.start();
		mailboxThread.signalStart();
		mailboxThread.join();
		asyncUnblocker.interrupt();
		asyncUnblocker.join();
		mailboxProcessor.prepareClose();
		mailboxProcessor.close();
		mailboxThread.checkException();
	}

	private static MailboxProcessor start(MailboxThread mailboxThread) {
		mailboxThread.start();
		final MailboxProcessor mailboxProcessor = mailboxThread.getMailboxProcessor();
		mailboxProcessor.open();
		mailboxThread.signalStart();
		return mailboxProcessor;
	}

	private static void stop(MailboxThread mailboxThread) throws Exception {
		mailboxThread.join();
		MailboxProcessor mailboxProcessor = mailboxThread.getMailboxProcessor();
		mailboxProcessor.prepareClose();
		mailboxProcessor.close();
		mailboxThread.checkException();
	}

	static class MailboxThread extends Thread implements MailboxDefaultAction {

		MailboxProcessor mailboxProcessor;
		OneShotLatch mailboxCreatedLatch = new OneShotLatch();
		OneShotLatch canRun = new OneShotLatch();
		private Throwable caughtException;

		@Override
		public final void run() {
			mailboxProcessor = new MailboxProcessor(this);
			mailboxCreatedLatch.trigger();
			try {
				canRun.await();
				mailboxProcessor.runMailboxLoop();
			} catch (Throwable t) {
				this.caughtException = t;
			}
		}

		@Override
		public void runDefaultAction(DefaultActionContext context) throws Exception {
			context.allActionsCompleted();
		}

		final MailboxProcessor getMailboxProcessor() {
			try {
				mailboxCreatedLatch.await();
				return mailboxProcessor;
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new RuntimeException(e);
			}
		}

		final void signalStart() {
			if (mailboxCreatedLatch.isTriggered()) {
				canRun.trigger();
			}
		}

		void checkException() throws Exception {
			if (caughtException != null) {
				throw new Exception(caughtException);
			}
		}
	}

}
