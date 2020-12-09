/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.operators.coordination.TestEventSender.EventWithSubtask;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Test;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * A test that ensures the before/after conditions around event sending and checkpoint are met.
 * concurrency
 */
@SuppressWarnings("serial")
public class OperatorCoordinatorHolderTest extends TestLogger {

	private final Consumer<Throwable> globalFailureHandler = (t) -> globalFailure = t;
	private Throwable globalFailure;

	@After
	public void checkNoGlobalFailure() throws Exception {
		if (globalFailure != null) {
			ExceptionUtils.rethrowException(globalFailure);
		}
	}

	// ------------------------------------------------------------------------

	@Test
	public void checkpointFutureInitiallyNotDone() throws Exception {
		final TestEventSender sender = new TestEventSender();
		final OperatorCoordinatorHolder holder = createCoordinatorHolder(sender, TestingOperatorCoordinator::new);

		final CompletableFuture<byte[]> checkpointFuture = new CompletableFuture<>();
		holder.checkpointCoordinator(1L, checkpointFuture);

		assertFalse(checkpointFuture.isDone());
	}

	@Test
	public void completedCheckpointFuture() throws Exception {
		final TestEventSender sender = new TestEventSender();
		final OperatorCoordinatorHolder holder = createCoordinatorHolder(sender, TestingOperatorCoordinator::new);

		final byte[] testData = new byte[] {11, 22, 33, 44};

		final CompletableFuture<byte[]> checkpointFuture = new CompletableFuture<>();
		holder.checkpointCoordinator(9L, checkpointFuture);
		getCoordinator(holder).getLastTriggeredCheckpoint().complete(testData);

		assertTrue(checkpointFuture.isDone());
		assertArrayEquals(testData, checkpointFuture.get());
	}

	@Test
	public void eventsBeforeCheckpointFutureCompletionPassThrough() throws Exception {
		final TestEventSender sender = new TestEventSender();
		final OperatorCoordinatorHolder holder = createCoordinatorHolder(sender, TestingOperatorCoordinator::new);

		holder.checkpointCoordinator(1L, new CompletableFuture<>());
		getCoordinator(holder).getContext().sendEvent(new TestOperatorEvent(1), 1);

		assertThat(sender.events, contains(
			new EventWithSubtask(new TestOperatorEvent(1), 1)
		));
	}

	@Test
	public void eventsAreBlockedAfterCheckpointFutureCompletes() throws Exception {
		final TestEventSender sender = new TestEventSender();
		final OperatorCoordinatorHolder holder = createCoordinatorHolder(sender, TestingOperatorCoordinator::new);

		triggerAndCompleteCheckpoint(holder, 10L);
		getCoordinator(holder).getContext().sendEvent(new TestOperatorEvent(1337), 0);

		assertTrue(sender.events.isEmpty());
	}

	@Test
	public void abortedCheckpointReleasesBlockedEvents() throws Exception {
		final TestEventSender sender = new TestEventSender();
		final OperatorCoordinatorHolder holder = createCoordinatorHolder(sender, TestingOperatorCoordinator::new);

		triggerAndCompleteCheckpoint(holder, 123L);
		getCoordinator(holder).getContext().sendEvent(new TestOperatorEvent(1337), 0);
		holder.abortCurrentTriggering();

		assertThat(sender.events, contains(
			new EventWithSubtask(new TestOperatorEvent(1337), 0)
		));
	}

	@Test
	public void sourceBarrierInjectionReleasesBlockedEvents() throws Exception {
		final TestEventSender sender = new TestEventSender();
		final OperatorCoordinatorHolder holder = createCoordinatorHolder(sender, TestingOperatorCoordinator::new);

		triggerAndCompleteCheckpoint(holder, 1111L);
		getCoordinator(holder).getContext().sendEvent(new TestOperatorEvent(1337), 0);
		holder.afterSourceBarrierInjection(1111L);

		assertThat(sender.events, contains(
			new EventWithSubtask(new TestOperatorEvent(1337), 0)
		));
	}

	@Test
	public void failedTasksDropsBlockedEvents() throws Exception {
		final TestEventSender sender = new TestEventSender();
		final OperatorCoordinatorHolder holder = createCoordinatorHolder(sender, TestingOperatorCoordinator::new);

		triggerAndCompleteCheckpoint(holder, 1000L);
		getCoordinator(holder).getContext().sendEvent(new TestOperatorEvent(0), 0);
		getCoordinator(holder).getContext().sendEvent(new TestOperatorEvent(1), 1);
		holder.subtaskFailed(1, null);
		holder.abortCurrentTriggering();

		assertThat(sender.events, contains(
			new EventWithSubtask(new TestOperatorEvent(0), 0)
		));
	}

	@Test
	public void restoreOpensValveEvents() throws Exception {
		final TestEventSender sender = new TestEventSender();
		final OperatorCoordinatorHolder holder = createCoordinatorHolder(sender, TestingOperatorCoordinator::new);

		triggerAndCompleteCheckpoint(holder, 1000L);
		holder.resetToCheckpoint(1L, new byte[0]);
		getCoordinator(holder).getContext().sendEvent(new TestOperatorEvent(999), 1);

		assertThat(sender.events, contains(
			new EventWithSubtask(new TestOperatorEvent(999), 1)
		));
	}

	@Test
	public void restoreDropsBlockedEvents() throws Exception {
		final TestEventSender sender = new TestEventSender();
		final OperatorCoordinatorHolder holder = createCoordinatorHolder(sender, TestingOperatorCoordinator::new);

		triggerAndCompleteCheckpoint(holder, 1000L);
		getCoordinator(holder).getContext().sendEvent(new TestOperatorEvent(0), 0);
		getCoordinator(holder).getContext().sendEvent(new TestOperatorEvent(1), 1);
		holder.resetToCheckpoint(2L, new byte[0]);

		assertTrue(sender.events.isEmpty());
	}

	@Test
	public void lateCompleteCheckpointFutureDoesNotBlockEvents() throws Exception {
		final TestEventSender sender = new TestEventSender();
		final OperatorCoordinatorHolder holder = createCoordinatorHolder(sender, TestingOperatorCoordinator::new);

		final CompletableFuture<byte[]> holderFuture = new CompletableFuture<>();
		holder.checkpointCoordinator(1000L, holderFuture);

		final CompletableFuture<byte[]> future1 = getCoordinator(holder).getLastTriggeredCheckpoint();
		holder.abortCurrentTriggering();

		triggerAndCompleteCheckpoint(holder, 1010L);
		holder.afterSourceBarrierInjection(1010L);

		future1.complete(new byte[0]);

		getCoordinator(holder).getContext().sendEvent(new TestOperatorEvent(123), 0);

		assertThat(sender.events, contains(
			new EventWithSubtask(new TestOperatorEvent(123), 0)
		));
	}

	@Test
	public void triggeringFailsIfOtherTriggeringInProgress() throws Exception {
		final TestEventSender sender = new TestEventSender();
		final OperatorCoordinatorHolder holder = createCoordinatorHolder(sender, TestingOperatorCoordinator::new);

		holder.checkpointCoordinator(11L, new CompletableFuture<>());

		final CompletableFuture<byte[]> future = new CompletableFuture<>();
		holder.checkpointCoordinator(12L, future);

		assertTrue(future.isCompletedExceptionally());
		assertNotNull(globalFailure);
		globalFailure = null;
	}

	@Test
	public void takeCheckpointAfterSuccessfulCheckpoint() throws Exception {
		final TestEventSender sender = new TestEventSender();
		final OperatorCoordinatorHolder holder = createCoordinatorHolder(sender, TestingOperatorCoordinator::new);

		getCoordinator(holder).getContext().sendEvent(new TestOperatorEvent(0), 0);

		triggerAndCompleteCheckpoint(holder, 22L);
		getCoordinator(holder).getContext().sendEvent(new TestOperatorEvent(1), 0);
		holder.afterSourceBarrierInjection(22L);

		getCoordinator(holder).getContext().sendEvent(new TestOperatorEvent(2), 0);

		triggerAndCompleteCheckpoint(holder, 23L);
		getCoordinator(holder).getContext().sendEvent(new TestOperatorEvent(3), 0);
		holder.afterSourceBarrierInjection(23L);

		assertThat(sender.events, contains(
			new EventWithSubtask(new TestOperatorEvent(0), 0),
			new EventWithSubtask(new TestOperatorEvent(1), 0),
			new EventWithSubtask(new TestOperatorEvent(2), 0),
			new EventWithSubtask(new TestOperatorEvent(3), 0)
		));
	}

	@Test
	public void takeCheckpointAfterAbortedCheckpoint() throws Exception {
		final TestEventSender sender = new TestEventSender();
		final OperatorCoordinatorHolder holder = createCoordinatorHolder(sender, TestingOperatorCoordinator::new);

		getCoordinator(holder).getContext().sendEvent(new TestOperatorEvent(0), 0);

		triggerAndCompleteCheckpoint(holder, 22L);
		getCoordinator(holder).getContext().sendEvent(new TestOperatorEvent(1), 0);
		holder.abortCurrentTriggering();

		getCoordinator(holder).getContext().sendEvent(new TestOperatorEvent(2), 0);

		triggerAndCompleteCheckpoint(holder, 23L);
		getCoordinator(holder).getContext().sendEvent(new TestOperatorEvent(3), 0);
		holder.afterSourceBarrierInjection(23L);

		assertThat(sender.events, contains(
			new EventWithSubtask(new TestOperatorEvent(0), 0),
			new EventWithSubtask(new TestOperatorEvent(1), 0),
			new EventWithSubtask(new TestOperatorEvent(2), 0),
			new EventWithSubtask(new TestOperatorEvent(3), 0)
		));
	}

	@Test
	public void testFailingJobMultipleTimesNotCauseCascadingJobFailure() throws Exception {
		Function<OperatorCoordinator.Context, OperatorCoordinator> coordinatorProvider =
			context -> new TestingOperatorCoordinator(context) {
				@Override
				public void handleEventFromOperator(int subtask, OperatorEvent event) {
					context.failJob(new RuntimeException("Artificial Exception"));
				}
			};
		final TestEventSender sender = new TestEventSender();
		final OperatorCoordinatorHolder holder = createCoordinatorHolder(sender, coordinatorProvider);

		holder.handleEventFromOperator(0, new TestOperatorEvent());
		assertNotNull(globalFailure);
		final Throwable firstGlobalFailure = globalFailure;

		holder.handleEventFromOperator(1, new TestOperatorEvent());
		assertEquals("The global failure should be the same instance because the context"
						+ "should only take the first request from the coordinator to fail the job.",
				firstGlobalFailure, globalFailure);

		holder.resetToCheckpoint(0L, new byte[0]);
		holder.handleEventFromOperator(1, new TestOperatorEvent());
		assertNotEquals("The new failures should be propagated after the coordinator "
							+ "is reset.", firstGlobalFailure, globalFailure);
		// Reset global failure to null to make the after method check happy.
		globalFailure = null;
	}

	/**
	 * This test verifies that the order of Checkpoint Completion and Event Sending observed from the
	 * outside matches that from within the OperatorCoordinator.
	 *
	 * <p>Extreme case 1: The coordinator immediately completes the checkpoint future and sends an
	 * event directly after that.
	 */
	@Test
	public void verifyCheckpointEventOrderWhenCheckpointFutureCompletedImmediately() throws Exception {
		checkpointEventValueAtomicity(FutureCompletedInstantlyTestCoordinator::new);
	}

	/**
	 * This test verifies that the order of Checkpoint Completion and Event Sending observed from the
	 * outside matches that from within the OperatorCoordinator.
	 *
	 * <p>Extreme case 2: After the checkpoint triggering, the coordinator flushes a bunch of events
	 * before completing the checkpoint future.
	 */
	@Test
	public void verifyCheckpointEventOrderWhenCheckpointFutureCompletesLate() throws Exception {
		checkpointEventValueAtomicity(FutureCompletedAfterSendingEventsCoordinator::new);
	}

	private void checkpointEventValueAtomicity(
			final Function<OperatorCoordinator.Context, OperatorCoordinator> coordinatorCtor) throws Exception {

		final ManuallyTriggeredScheduledExecutorService executor = new ManuallyTriggeredScheduledExecutorService();
		final ComponentMainThreadExecutor mainThreadExecutor = new ComponentMainThreadExecutorServiceAdapter(
				(ScheduledExecutorService) executor, Thread.currentThread());

		final TestEventSender sender = new TestEventSender();
		final OperatorCoordinatorHolder holder = createCoordinatorHolder(
				sender, coordinatorCtor, mainThreadExecutor);

		// give the coordinator some time to emit some events
		Thread.sleep(new Random().nextInt(10) + 20);
		executor.triggerAll();

		// trigger the checkpoint - this should also shut the valve as soon as the future is completed
		final CompletableFuture<byte[]> checkpointFuture = new CompletableFuture<>();
		holder.checkpointCoordinator(0L, checkpointFuture);
		executor.triggerAll();

		// give the coordinator some time to emit some events
		Thread.sleep(new Random().nextInt(10) + 10);
		holder.close();
		executor.triggerAll();

		assertTrue(checkpointFuture.isDone());
		final int checkpointedNumber = bytesToInt(checkpointFuture.get());

		assertEquals(checkpointedNumber, sender.events.size());
		for (int i = 0; i < checkpointedNumber; i++) {
			assertEquals(i, ((TestOperatorEvent) sender.events.get(i).event).getValue());
		}
	}

	// ------------------------------------------------------------------------
	//   test actions
	// ------------------------------------------------------------------------

	private CompletableFuture<byte[]> triggerAndCompleteCheckpoint(
			OperatorCoordinatorHolder holder,
			long checkpointId) throws Exception {

		final CompletableFuture<byte[]> future = new CompletableFuture<>();
		holder.checkpointCoordinator(checkpointId, future);
		getCoordinator(holder).getLastTriggeredCheckpoint().complete(new byte[0]);
		return future;
	}

	// ------------------------------------------------------------------------
	//   miscellaneous helpers
	// ------------------------------------------------------------------------

	static byte[] intToBytes(int value) {
		return ByteBuffer.allocate(4).putInt(value).array();
	}

	static int bytesToInt(byte[] bytes) {
		return ByteBuffer.wrap(bytes).getInt();
	}

	private static TestingOperatorCoordinator getCoordinator(OperatorCoordinatorHolder holder) {
		return (TestingOperatorCoordinator) holder.coordinator();
	}

	private OperatorCoordinatorHolder createCoordinatorHolder(
			final BiFunction<SerializedValue<OperatorEvent>, Integer, CompletableFuture<Acknowledge>> eventSender,
			final Function<OperatorCoordinator.Context, OperatorCoordinator> coordinatorCtor) throws Exception {

		return createCoordinatorHolder(
				eventSender,
				coordinatorCtor,
				ComponentMainThreadExecutorServiceAdapter.forMainThread());
	}

	private OperatorCoordinatorHolder createCoordinatorHolder(
			final BiFunction<SerializedValue<OperatorEvent>, Integer, CompletableFuture<Acknowledge>> eventSender,
			final Function<OperatorCoordinator.Context, OperatorCoordinator> coordinatorCtor,
			final ComponentMainThreadExecutor mainThreadExecutor) throws Exception {

		final OperatorID opId = new OperatorID();
		final OperatorCoordinator.Provider provider = new OperatorCoordinator.Provider() {
			@Override
			public OperatorID getOperatorId() {
				return opId;
			}

			@Override
			public OperatorCoordinator create(OperatorCoordinator.Context context) {
				return coordinatorCtor.apply(context);
			}
		};

		final OperatorCoordinatorHolder holder = OperatorCoordinatorHolder.create(
				opId,
				provider,
				eventSender,
				"test-coordinator-name",
				getClass().getClassLoader(),
				3,
				1775);

		holder.lazyInitialize(globalFailureHandler, mainThreadExecutor);
		holder.start();

		return holder;
	}

	// ------------------------------------------------------------------------
	//   test implementations
	// ------------------------------------------------------------------------

	private static final class FutureCompletedInstantlyTestCoordinator extends CheckpointEventOrderTestBaseCoordinator {

		private final ReentrantLock lock = new ReentrantLock(true);
		private final Condition condition = lock.newCondition();

		@Nullable
		@GuardedBy("lock")
		private CompletableFuture<byte[]> checkpoint;

		private int num;

		FutureCompletedInstantlyTestCoordinator(Context context) {
			super(context);
		}

		@Override
		public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result) throws Exception {
			// before returning from this methof, we wait on a condition.
			// that way, we simulate a "context switch" just at the time when the
			// future would be returned and make the other thread complete the future and send an
			// event before this method returns
			lock.lock();
			try {
				checkpoint = result;
				condition.await();
			} finally {
				lock.unlock();
			}
		}

		@Override
		protected void step() throws Exception {
			lock.lock();
			try {
				// if there is a checkpoint to complete, we complete it and immediately
				// try to send another event, without releasing the lock. that way we
				// force the situation as if the checkpoint get completed and an event gets
				// sent while the triggering thread is stalled
				if (checkpoint != null) {
					checkpoint.complete(intToBytes(num));
					checkpoint = null;
				}
				context.sendEvent(new TestOperatorEvent(num++), 0);
				condition.signalAll();
			} finally {
				lock.unlock();
			}

			Thread.sleep(2);
		}
	}

	private static final class FutureCompletedAfterSendingEventsCoordinator extends CheckpointEventOrderTestBaseCoordinator {

		@Nullable
		private CompletableFuture<byte[]> checkpoint;

		private int num;

		FutureCompletedAfterSendingEventsCoordinator(Context context) {
			super(context);
		}

		@Override
		public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result) throws Exception {
			checkpoint = result;
		}

		@Override
		protected void step() throws Exception {
			Thread.sleep(2);

			context.sendEvent(new TestOperatorEvent(num++), 0);
			context.sendEvent(new TestOperatorEvent(num++), 1);
			context.sendEvent(new TestOperatorEvent(num++), 2);

			if (checkpoint != null) {
				checkpoint.complete(intToBytes(num));
				checkpoint = null;
			}
		}
	}

	private abstract static class CheckpointEventOrderTestBaseCoordinator implements OperatorCoordinator, Runnable {

		private final Thread coordinatorThread;

		protected final Context context;

		private volatile boolean closed;

		CheckpointEventOrderTestBaseCoordinator(Context context) {
			this.context = context;
			this.coordinatorThread = new Thread(this);
		}

		@Override
		public void start() throws Exception {
			coordinatorThread.start();
		}

		@Override
		public void close() throws Exception {
			closed = true;
			coordinatorThread.interrupt();
			coordinatorThread.join();
		}

		@Override
		public void handleEventFromOperator(int subtask, OperatorEvent event){}

		@Override
		public void subtaskFailed(int subtask, @Nullable Throwable reason) {}

		@Override
		public void subtaskReset(int subtask, long checkpointId) {}

		@Override
		public abstract void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result) throws Exception;

		@Override
		public void notifyCheckpointComplete(long checkpointId) {}

		@Override
		public void resetToCheckpoint(long checkpointId, byte[] checkpointData) throws Exception {}

		@Override
		public void run() {
			try {
				while (!closed) {
					step();
				}
			} catch (Throwable t) {
				if (closed) {
					return;
				}

				// this should never happen, but just in case, print and crash the test
				//noinspection CallToPrintStackTrace
				t.printStackTrace();
				System.exit(-1);
			}
		}

		protected abstract void step() throws Exception;
	}
}
