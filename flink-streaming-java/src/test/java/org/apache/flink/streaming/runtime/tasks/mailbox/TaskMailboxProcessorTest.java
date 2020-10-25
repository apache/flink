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

import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.concurrent.FutureTaskWithException;
import org.apache.flink.streaming.api.operators.MailboxExecutor;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.function.RunnableWithException;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for {@link MailboxProcessor}.
 */
public class TaskMailboxProcessorTest {

	public static final int DEFAULT_PRIORITY = 0;

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Test
	public void testRejectIfNotOpen() {
		MailboxProcessor mailboxProcessor = new MailboxProcessor(controller -> {});
		mailboxProcessor.prepareClose();
		try {
			mailboxProcessor.getMailboxExecutor(DEFAULT_PRIORITY).execute(() -> {}, "dummy");
			Assert.fail("Should not be able to accept runnables if not opened.");
		} catch (RejectedExecutionException expected) {
		}
	}

	@Test
	public void testSubmittingRunnableWithException() throws Exception {
		expectedException.expectMessage("Expected");
		try (MailboxProcessor mailboxProcessor = new MailboxProcessor(controller -> {})) {
			final Thread submitThread = new Thread(() -> {
				mailboxProcessor.getMainMailboxExecutor().execute(
					this::throwFlinkException,
					"testSubmittingRunnableWithException");
			});

			submitThread.start();
			mailboxProcessor.runMailboxLoop();
			submitThread.join();
		}
	}

	private void throwFlinkException() throws FlinkException {
		throw new FlinkException("Expected");
	}

	@Test
	public void testShutdown() {
		MailboxProcessor mailboxProcessor = new MailboxProcessor(controller -> {});
		FutureTaskWithException<Void> testRunnableFuture = new FutureTaskWithException<>(() -> {});
		mailboxProcessor.getMailboxExecutor(DEFAULT_PRIORITY).execute(testRunnableFuture, "testRunnableFuture");
		mailboxProcessor.prepareClose();

		try {
			mailboxProcessor.getMailboxExecutor(DEFAULT_PRIORITY).execute(() -> {}, "dummy");
			Assert.fail("Should not be able to accept runnables if not opened.");
		} catch (RejectedExecutionException expected) {
		}

		Assert.assertFalse(testRunnableFuture.isDone());

		mailboxProcessor.close();
		Assert.assertTrue(testRunnableFuture.isCancelled());
	}

	@Test
	public void testRunDefaultActionAndMails() throws Exception {
		AtomicBoolean stop = new AtomicBoolean(false);
		MailboxThread mailboxThread = new MailboxThread() {
			@Override
			public void runDefaultAction(Controller controller) throws Exception {
				if (stop.get()) {
					controller.allActionsCompleted();
				} else {
					Thread.sleep(10L);
				}
			}
		};

		MailboxProcessor mailboxProcessor = start(mailboxThread);
		mailboxProcessor.getMailboxExecutor(DEFAULT_PRIORITY).execute(() -> stop.set(true), "stop");
		mailboxThread.join();
	}

	@Test
	public void testRunDefaultAction() throws Exception {

		final int expectedInvocations = 3;
		final AtomicInteger counter = new AtomicInteger(0);
		MailboxThread mailboxThread = new MailboxThread() {
			@Override
			public void runDefaultAction(Controller controller) {
				if (counter.incrementAndGet() == expectedInvocations) {
					controller.allActionsCompleted();
				}
			}
		};

		start(mailboxThread);
		mailboxThread.join();
		Assert.assertEquals(expectedInvocations, counter.get());
	}

	@Test
	public void testSignalUnAvailable() throws Exception {

		final AtomicInteger counter = new AtomicInteger(0);
		final AtomicReference<MailboxDefaultAction.Suspension> suspendedActionRef = new AtomicReference<>();
		final OneShotLatch actionSuspendedLatch = new OneShotLatch();
		final int blockAfterInvocations = 3;
		final int totalInvocations = blockAfterInvocations * 2;

		MailboxThread mailboxThread = new MailboxThread() {
			@Override
			public void runDefaultAction(Controller controller) {
				if (counter.incrementAndGet() == blockAfterInvocations) {
					suspendedActionRef.set(controller.suspendDefaultAction());
					actionSuspendedLatch.trigger();
				} else if (counter.get() == totalInvocations) {
					controller.allActionsCompleted();
				}
			}
		};

		MailboxProcessor mailboxProcessor = start(mailboxThread);
		actionSuspendedLatch.await();
		Assert.assertEquals(blockAfterInvocations, counter.get());

		MailboxDefaultAction.Suspension suspension = suspendedActionRef.get();
		mailboxProcessor.getMailboxExecutor(DEFAULT_PRIORITY).execute(suspension::resume, "resume");
		mailboxThread.join();
		Assert.assertEquals(totalInvocations, counter.get());
	}

	@Test
	public void testSignalUnAvailablePingPong() throws Exception {
		final AtomicReference<MailboxDefaultAction.Suspension> suspendedActionRef = new AtomicReference<>();
		final int totalSwitches = 10000;
		final MailboxThread mailboxThread = new MailboxThread() {
			int count = 0;

			@Override
			public void runDefaultAction(Controller controller) {

				// If this is violated, it means that the default action was invoked while we assumed suspension
				Assert.assertTrue(suspendedActionRef.compareAndSet(null, controller.suspendDefaultAction()));

				++count;

				if (count == totalSwitches) {
					controller.allActionsCompleted();
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

		final Thread asyncUnblocker = new Thread(() -> {
			int count = 0;
			while (!Thread.currentThread().isInterrupted()) {

				final MailboxDefaultAction.Suspension resume =
						suspendedActionRef.getAndSet(null);
				if (resume != null) {
					mailboxProcessor.getMailboxExecutor(DEFAULT_PRIORITY).execute(resume::resume, "resume");
				} else {
					try {
						mailboxProcessor.getMailboxExecutor(DEFAULT_PRIORITY).execute(() -> {}, "dummy");
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
		mailboxThread.checkException();
	}

	/**
	 * Testing that canceling after closing will not lead to an exception.
	 */
	@Test
	public void testCancelAfterClose() {
		MailboxProcessor mailboxProcessor = new MailboxProcessor((ctx) -> {});
		mailboxProcessor.close();
		mailboxProcessor.allActionsCompleted();
	}

	@Test
	public void testNoIdleTimeWhenBusy() throws InterruptedException {
		final AtomicReference<MailboxDefaultAction.Suspension> suspendedActionRef = new AtomicReference<>();
		final int totalSwitches = 10;

		AtomicInteger count = new AtomicInteger();
		MailboxThread mailboxThread = new MailboxThread() {
			@Override
			public void runDefaultAction(Controller controller) {
				int currentCount = count.incrementAndGet();
				if (currentCount == totalSwitches) {
					controller.allActionsCompleted();
				}
			}
		};
		mailboxThread.start();
		final MailboxProcessor mailboxProcessor = mailboxThread.getMailboxProcessor();

		mailboxThread.signalStart();
		mailboxThread.join();

		Assert.assertEquals(0, mailboxProcessor.getIdleTime().getCount());
		Assert.assertEquals(totalSwitches, count.get());
	}

	@Test
	public void testIdleTime() throws InterruptedException {
		final AtomicReference<MailboxDefaultAction.Suspension> suspendedActionRef = new AtomicReference<>();
		final int totalSwitches = 2;

		CountDownLatch syncLock = new CountDownLatch(1);
		MailboxThread mailboxThread = new MailboxThread() {
			int count = 0;

			@Override
			public void runDefaultAction(Controller controller) {
				// If this is violated, it means that the default action was invoked while we assumed suspension
				Assert.assertTrue(suspendedActionRef.compareAndSet(null, controller.suspendDefaultAction()));
				++count;
				if (count == totalSwitches) {
					controller.allActionsCompleted();
				}
				syncLock.countDown();
			}
		};
		mailboxThread.start();
		final MailboxProcessor mailboxProcessor = mailboxThread.getMailboxProcessor();
		mailboxThread.signalStart();

		syncLock.await();
		Thread.sleep(10);
		mailboxProcessor.getMailboxExecutor(DEFAULT_PRIORITY).execute(suspendedActionRef.get()::resume, "resume");
		mailboxThread.join();
		Assert.assertThat(mailboxProcessor.getIdleTime().getCount(), Matchers.greaterThan(0L));
	}

	private static MailboxProcessor start(MailboxThread mailboxThread) {
		mailboxThread.start();
		final MailboxProcessor mailboxProcessor = mailboxThread.getMailboxProcessor();
		mailboxThread.signalStart();
		return mailboxProcessor;
	}

	/**
	 * FLINK-14304: Avoid newly spawned letters to prevent input processing from ever happening.
	 */
	@Test
	public void testAvoidStarvation() throws Exception {

		final int expectedInvocations = 3;
		final AtomicInteger counter = new AtomicInteger(0);
		MailboxThread mailboxThread = new MailboxThread() {
			@Override
			public void runDefaultAction(Controller controller) {
				if (counter.incrementAndGet() == expectedInvocations) {
					controller.allActionsCompleted();
				}
			}
		};

		mailboxThread.start();
		final MailboxProcessor mailboxProcessor = mailboxThread.getMailboxProcessor();
		final MailboxExecutor mailboxExecutor = mailboxProcessor.getMailboxExecutor(DEFAULT_PRIORITY);
		AtomicInteger index = new AtomicInteger();
		mailboxExecutor.execute(
			new RunnableWithException() {
				@Override
				public void run() {
					mailboxExecutor.execute(this, "Blocking mail" + index.incrementAndGet());
				}
			},
			"Blocking mail" + index.get());

		mailboxThread.signalStart();
		mailboxThread.join();

		Assert.assertEquals(expectedInvocations, counter.get());
		Assert.assertEquals(expectedInvocations, index.get());
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
		public void runDefaultAction(Controller controller) throws Exception {
			controller.allActionsCompleted();
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
