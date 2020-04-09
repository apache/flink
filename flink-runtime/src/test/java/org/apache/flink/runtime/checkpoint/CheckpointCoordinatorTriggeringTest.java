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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.CheckpointCoordinatorBuilder;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.CheckpointCoordinatorConfigurationBuilder;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.annotation.Nullable;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.mockExecutionVertex;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;

/**
 * Tests for checkpoint coordinator triggering.
 */
public class CheckpointCoordinatorTriggeringTest extends TestLogger {
	private static final String TASK_MANAGER_LOCATION_INFO = "Unknown location";

	private ManuallyTriggeredScheduledExecutor timer;
	private ManuallyTriggeredScheduledExecutor mainThreadExecutor;

	@Before
	public void setUp() throws Exception {
		timer = new ManuallyTriggeredScheduledExecutor();
		mainThreadExecutor = new ManuallyTriggeredScheduledExecutor();
	}

	@Test
	public void testPeriodicTriggering() {
		try {
			final JobID jid = new JobID();
			final long start = System.currentTimeMillis();

			// create some mock execution vertices and trigger some checkpoint

			final ExecutionAttemptID triggerAttemptID = new ExecutionAttemptID();
			final ExecutionAttemptID ackAttemptID = new ExecutionAttemptID();
			final ExecutionAttemptID commitAttemptID = new ExecutionAttemptID();

			ExecutionVertex triggerVertex = mockExecutionVertex(triggerAttemptID);
			ExecutionVertex ackVertex = mockExecutionVertex(ackAttemptID);
			ExecutionVertex commitVertex = mockExecutionVertex(commitAttemptID);

			final AtomicInteger numCalls = new AtomicInteger();

			final Execution execution = triggerVertex.getCurrentExecutionAttempt();

			doAnswer(new Answer<Void>() {

				private long lastId = -1;
				private long lastTs = -1;

				@Override
				public Void answer(InvocationOnMock invocation) throws Throwable {
					long id = (Long) invocation.getArguments()[0];
					long ts = (Long) invocation.getArguments()[1];

					assertTrue(id > lastId);
					assertTrue(ts >= lastTs);
					assertTrue(ts >= start);

					lastId = id;
					lastTs = ts;
					numCalls.incrementAndGet();
					return null;
				}
			}).when(execution).triggerCheckpoint(anyLong(), anyLong(), any(CheckpointOptions.class));

			CheckpointCoordinatorConfiguration checkpointCoordinatorConfiguration =
				new CheckpointCoordinatorConfigurationBuilder()
					.setCheckpointInterval(10) // periodic interval is 10 ms
					.setCheckpointTimeout(200000) // timeout is very long (200 s)
					.build();
			CheckpointCoordinator checkpointCoordinator =
				new CheckpointCoordinatorBuilder()
					.setJobId(jid)
					.setCheckpointCoordinatorConfiguration(checkpointCoordinatorConfiguration)
					.setTasksToTrigger(new ExecutionVertex[] { triggerVertex })
					.setTasksToWaitFor(new ExecutionVertex[] { ackVertex })
					.setTasksToCommitTo(new ExecutionVertex[] { commitVertex })
					.setCompletedCheckpointStore(new StandaloneCompletedCheckpointStore(2))
					.setTimer(timer)
					.setMainThreadExecutor(mainThreadExecutor)
					.build();

			checkpointCoordinator.startCheckpointScheduler();

			do {
				timer.triggerPeriodicScheduledTasks();
				mainThreadExecutor.triggerAll();
			}
			while (numCalls.get() < 5);
			assertEquals(5, numCalls.get());

			checkpointCoordinator.stopCheckpointScheduler();

			// no further calls may come.
			timer.triggerPeriodicScheduledTasks();
			mainThreadExecutor.triggerAll();
			assertEquals(5, numCalls.get());

			// start another sequence of periodic scheduling
			numCalls.set(0);
			checkpointCoordinator.startCheckpointScheduler();

			do {
				timer.triggerPeriodicScheduledTasks();
				mainThreadExecutor.triggerAll();
			}
			while (numCalls.get() < 5);
			assertEquals(5, numCalls.get());

			checkpointCoordinator.stopCheckpointScheduler();

			// no further calls may come
			timer.triggerPeriodicScheduledTasks();
			mainThreadExecutor.triggerAll();
			assertEquals(5, numCalls.get());

			checkpointCoordinator.shutdown(JobStatus.FINISHED);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * This test verified that after a completed checkpoint a certain time has passed before
	 * another is triggered.
	 */
	@Test
	public void testMinTimeBetweenCheckpointsInterval() throws Exception {
		final JobID jid = new JobID();

		// create some mock execution vertices and trigger some checkpoint
		final ExecutionAttemptID attemptID = new ExecutionAttemptID();
		final ExecutionVertex vertex = mockExecutionVertex(attemptID);
		final Execution executionAttempt = vertex.getCurrentExecutionAttempt();

		final BlockingQueue<Long> triggerCalls = new LinkedBlockingQueue<>();

		doAnswer(invocation -> {
			triggerCalls.add((Long) invocation.getArguments()[0]);
			return null;
		}).when(executionAttempt).triggerCheckpoint(anyLong(), anyLong(), any(CheckpointOptions.class));

		final long delay = 50;
		final long checkpointInterval = 12;

		CheckpointCoordinatorConfiguration checkpointCoordinatorConfiguration =
			new CheckpointCoordinatorConfigurationBuilder()
				.setCheckpointInterval(checkpointInterval) // periodic interval is 12 ms
				.setCheckpointTimeout(200_000) // timeout is very long (200 s)
				.setMinPauseBetweenCheckpoints(delay) // 50 ms delay between checkpoints
				.setMaxConcurrentCheckpoints(1)
				.build();
		final CheckpointCoordinator checkpointCoordinator =
			new CheckpointCoordinatorBuilder()
				.setJobId(jid)
				.setCheckpointCoordinatorConfiguration(checkpointCoordinatorConfiguration)
				.setTasks(new ExecutionVertex[] { vertex })
				.setCompletedCheckpointStore(new StandaloneCompletedCheckpointStore(2))
				.setTimer(timer)
				.setMainThreadExecutor(mainThreadExecutor)
				.build();

		try {
			checkpointCoordinator.startCheckpointScheduler();
			timer.triggerPeriodicScheduledTasks();
			mainThreadExecutor.triggerAll();

			// wait until the first checkpoint was triggered
			Long firstCallId = triggerCalls.take();
			assertEquals(1L, firstCallId.longValue());

			AcknowledgeCheckpoint ackMsg = new AcknowledgeCheckpoint(jid, attemptID, 1L);

			// tell the coordinator that the checkpoint is done
			final long ackTime = System.nanoTime();
			checkpointCoordinator.receiveAcknowledgeMessage(ackMsg, TASK_MANAGER_LOCATION_INFO);

			timer.triggerPeriodicScheduledTasks();
			mainThreadExecutor.triggerAll();
			while (triggerCalls.isEmpty()) {
				// sleeps for a while to simulate periodic scheduling
				Thread.sleep(checkpointInterval);
				timer.triggerPeriodicScheduledTasks();
				mainThreadExecutor.triggerAll();
			}
			// wait until the next checkpoint is triggered
			Long nextCallId = triggerCalls.take();
			final long nextCheckpointTime = System.nanoTime();
			assertEquals(2L, nextCallId.longValue());

			final long delayMillis = (nextCheckpointTime - ackTime) / 1_000_000;

			// we need to add one ms here to account for rounding errors
			if (delayMillis + 1 < delay) {
				fail("checkpoint came too early: delay was " + delayMillis + " but should have been at least " + delay);
			}
		}
		finally {
			checkpointCoordinator.stopCheckpointScheduler();
			checkpointCoordinator.shutdown(JobStatus.FINISHED);
		}
	}

	@Test
	public void testStopPeriodicScheduler() throws Exception {
		// set up the coordinator and validate the initial state
		CheckpointCoordinator checkpointCoordinator = createCheckpointCoordinator();

		final CompletableFuture<CompletedCheckpoint> onCompletionPromise1 =
			triggerPeriodicCheckpoint(checkpointCoordinator);
		mainThreadExecutor.triggerAll();
		try {
			onCompletionPromise1.get();
			fail("The triggerCheckpoint call expected an exception");
		} catch (ExecutionException e) {
			final Optional<CheckpointException> checkpointExceptionOptional =
				ExceptionUtils.findThrowable(e, CheckpointException.class);
			assertTrue(checkpointExceptionOptional.isPresent());
			assertEquals(CheckpointFailureReason.PERIODIC_SCHEDULER_SHUTDOWN,
				checkpointExceptionOptional.get().getCheckpointFailureReason());
		}

		// Not periodic
		final CompletableFuture<CompletedCheckpoint> onCompletionPromise2 = checkpointCoordinator.triggerCheckpoint(
			System.currentTimeMillis(),
			CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
			null,
			false,
			false);
		mainThreadExecutor.triggerAll();
		assertFalse(onCompletionPromise2.isCompletedExceptionally());
	}

	@Test
	public void testTriggerCheckpointWithShuttingDownCoordinator() throws Exception {
		// set up the coordinator and validate the initial state
		CheckpointCoordinator checkpointCoordinator = createCheckpointCoordinator();
checkpointCoordinator.startCheckpointScheduler();
		final CompletableFuture<CompletedCheckpoint> onCompletionPromise =
			triggerPeriodicCheckpoint(checkpointCoordinator);

		checkpointCoordinator.shutdown(JobStatus.FAILED);
		mainThreadExecutor.triggerAll();
		try {
			onCompletionPromise.get();
			fail("Should not reach here");
		} catch (ExecutionException e) {
			final Optional<CheckpointException> checkpointExceptionOptional =
				ExceptionUtils.findThrowable(e, CheckpointException.class);
			assertTrue(checkpointExceptionOptional.isPresent());
			assertEquals(CheckpointFailureReason.CHECKPOINT_COORDINATOR_SHUTDOWN,
				checkpointExceptionOptional.get().getCheckpointFailureReason());
		}
	}

	@Test
	public void testTriggerCheckpointBeforePreviousOneCompleted() throws Exception {
		// create some mock Execution vertices that receive the checkpoint trigger messages
		final ExecutionAttemptID attemptID = new ExecutionAttemptID();
		final AtomicInteger taskManagerCheckpointTriggeredTimes = new AtomicInteger(0);
		final SimpleAckingTaskManagerGateway.CheckpointConsumer checkpointConsumer =
			(executionAttemptID,
			jobId, checkpointId,
			timestamp,
			checkpointOptions,
			advanceToEndOfEventTime) -> taskManagerCheckpointTriggeredTimes.incrementAndGet();
		ExecutionVertex vertex = mockExecutionVertex(attemptID, checkpointConsumer);

		// set up the coordinator and validate the initial state
		CheckpointCoordinator checkpointCoordinator = createCheckpointCoordinator(vertex);

		checkpointCoordinator.startCheckpointScheduler();
		// start a periodic checkpoint first
		final CompletableFuture<CompletedCheckpoint> onCompletionPromise1 =
			triggerPeriodicCheckpoint(checkpointCoordinator);
		assertTrue(checkpointCoordinator.isTriggering());
		assertEquals(0, checkpointCoordinator.getTriggerRequestQueue().size());
		final CompletableFuture<CompletedCheckpoint> onCompletionPromise2 =
			triggerPeriodicCheckpoint(checkpointCoordinator);
		// another trigger before the prior one finished

		assertTrue(checkpointCoordinator.isTriggering());
		assertEquals(1, checkpointCoordinator.getTriggerRequestQueue().size());

		mainThreadExecutor.triggerAll();
		assertFalse(onCompletionPromise1.isCompletedExceptionally());
		assertFalse(onCompletionPromise2.isCompletedExceptionally());
		assertFalse(checkpointCoordinator.isTriggering());
		assertEquals(0, checkpointCoordinator.getTriggerRequestQueue().size());
		assertEquals(2, taskManagerCheckpointTriggeredTimes.get());
	}

	@Test
	public void testTriggerCheckpointRequestQueuedWithFailure() throws Exception {
		// create some mock Execution vertices that receive the checkpoint trigger messages
		final ExecutionAttemptID attemptID = new ExecutionAttemptID();
		final AtomicInteger taskManagerCheckpointTriggeredTimes = new AtomicInteger(0);
		final SimpleAckingTaskManagerGateway.CheckpointConsumer checkpointConsumer =
			(executionAttemptID,
			jobId, checkpointId,
			timestamp,
			checkpointOptions,
			advanceToEndOfEventTime) -> taskManagerCheckpointTriggeredTimes.incrementAndGet();
		ExecutionVertex vertex = mockExecutionVertex(attemptID, checkpointConsumer);

		// set up the coordinator and validate the initial state
		CheckpointCoordinator checkpointCoordinator =
			new CheckpointCoordinatorBuilder()
				.setTasks(new ExecutionVertex[] { vertex })
				.setCheckpointIDCounter(new UnstableCheckpointIDCounter(id -> id == 0))
				.setMainThreadExecutor(mainThreadExecutor)
				.build();

		checkpointCoordinator.startCheckpointScheduler();
		// start a periodic checkpoint first
		final CompletableFuture<CompletedCheckpoint> onCompletionPromise1 =
			triggerNonPeriodicCheckpoint(checkpointCoordinator);
		assertTrue(checkpointCoordinator.isTriggering());
		assertEquals(0, checkpointCoordinator.getTriggerRequestQueue().size());

		// another trigger before the prior one finished
		final CompletableFuture<CompletedCheckpoint> onCompletionPromise2 =
			triggerNonPeriodicCheckpoint(checkpointCoordinator);

		// another trigger before the first one finished
		final CompletableFuture<CompletedCheckpoint> onCompletionPromise3 =
			triggerNonPeriodicCheckpoint(checkpointCoordinator);
		assertTrue(checkpointCoordinator.isTriggering());
		assertEquals(2, checkpointCoordinator.getTriggerRequestQueue().size());

		mainThreadExecutor.triggerAll();
		// the first triggered checkpoint fails by design through UnstableCheckpointIDCounter
		assertTrue(onCompletionPromise1.isCompletedExceptionally());
		assertFalse(onCompletionPromise2.isCompletedExceptionally());
		assertFalse(onCompletionPromise3.isCompletedExceptionally());
		assertFalse(checkpointCoordinator.isTriggering());
		assertEquals(0, checkpointCoordinator.getTriggerRequestQueue().size());
		assertEquals(2, taskManagerCheckpointTriggeredTimes.get());
	}

	@Test
	public void testTriggerCheckpointRequestCancelled() throws Exception {
		// create some mock Execution vertices that receive the checkpoint trigger messages
		final ExecutionAttemptID attemptID = new ExecutionAttemptID();
		final AtomicInteger taskManagerCheckpointTriggeredTimes = new AtomicInteger(0);
		final SimpleAckingTaskManagerGateway.CheckpointConsumer checkpointConsumer =
			(executionAttemptID,
			jobId, checkpointId,
			timestamp,
			checkpointOptions,
			advanceToEndOfEventTime) -> taskManagerCheckpointTriggeredTimes.incrementAndGet();
		ExecutionVertex vertex = mockExecutionVertex(attemptID, checkpointConsumer);

		// set up the coordinator and validate the initial state
		CheckpointCoordinator checkpointCoordinator = createCheckpointCoordinator(vertex);

		final CompletableFuture<String> masterHookCheckpointFuture = new CompletableFuture<>();
		checkpointCoordinator.addMasterHook(new TestingMasterHook(masterHookCheckpointFuture));
		checkpointCoordinator.startCheckpointScheduler();
		final CompletableFuture<CompletedCheckpoint> onCompletionPromise =
			triggerPeriodicCheckpoint(checkpointCoordinator);

		// checkpoint trigger will not finish since master hook checkpoint is not finished yet
		mainThreadExecutor.triggerAll();
		assertTrue(checkpointCoordinator.isTriggering());

		// trigger cancellation
		mainThreadExecutor.triggerNonPeriodicScheduledTasks();
		assertTrue(checkpointCoordinator.isTriggering());

		try {
			onCompletionPromise.get();
			fail("Should not reach here");
		} catch (ExecutionException e) {
			final Optional<CheckpointException> checkpointExceptionOptional =
				ExceptionUtils.findThrowable(e, CheckpointException.class);
			assertTrue(checkpointExceptionOptional.isPresent());
			assertEquals(CheckpointFailureReason.CHECKPOINT_EXPIRED,
				checkpointExceptionOptional.get().getCheckpointFailureReason());
		}

		// continue triggering
		masterHookCheckpointFuture.complete("finish master hook");

		mainThreadExecutor.triggerAll();
		assertFalse(checkpointCoordinator.isTriggering());
		// it doesn't really trigger task manager to do checkpoint
		assertEquals(0, taskManagerCheckpointTriggeredTimes.get());
		assertEquals(0, checkpointCoordinator.getTriggerRequestQueue().size());
	}

	@Test
	public void testTriggerCheckpointInitializationFailed() throws Exception {
		// create some mock Execution vertices that receive the checkpoint trigger messages
		final ExecutionAttemptID attemptID = new ExecutionAttemptID();
		ExecutionVertex vertex = mockExecutionVertex(attemptID);

		// set up the coordinator and validate the initial state
		CheckpointCoordinator checkpointCoordinator =
			new CheckpointCoordinatorBuilder()
				.setTasks(new ExecutionVertex[] { vertex })
				.setCheckpointIDCounter(new UnstableCheckpointIDCounter(id -> id == 0))
				.setMainThreadExecutor(mainThreadExecutor)
				.build();

		checkpointCoordinator.startCheckpointScheduler();
		final CompletableFuture<CompletedCheckpoint> onCompletionPromise1 =
			triggerPeriodicCheckpoint(checkpointCoordinator);
		assertTrue(checkpointCoordinator.isTriggering());
		assertEquals(0, checkpointCoordinator.getTriggerRequestQueue().size());

		mainThreadExecutor.triggerAll();
		try {
			onCompletionPromise1.get();
			fail("This checkpoint should fail through UnstableCheckpointIDCounter");
		} catch (ExecutionException e) {
			final Optional<CheckpointException> checkpointExceptionOptional =
				ExceptionUtils.findThrowable(e, CheckpointException.class);
			assertTrue(checkpointExceptionOptional.isPresent());
			assertEquals(CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE,
				checkpointExceptionOptional.get().getCheckpointFailureReason());
		}
		assertFalse(checkpointCoordinator.isTriggering());
		assertEquals(0, checkpointCoordinator.getTriggerRequestQueue().size());

		final CompletableFuture<CompletedCheckpoint> onCompletionPromise2 =
			triggerPeriodicCheckpoint(checkpointCoordinator);
		assertTrue(checkpointCoordinator.isTriggering());
		mainThreadExecutor.triggerAll();
		assertFalse(onCompletionPromise2.isCompletedExceptionally());
		assertFalse(checkpointCoordinator.isTriggering());
		assertEquals(0, checkpointCoordinator.getTriggerRequestQueue().size());
	}

	@Test
	public void testTriggerCheckpointSnapshotMasterHookFailed() throws Exception {
		// create some mock Execution vertices that receive the checkpoint trigger messages
		final ExecutionAttemptID attemptID = new ExecutionAttemptID();
		final AtomicInteger taskManagerCheckpointTriggeredTimes = new AtomicInteger(0);
		final SimpleAckingTaskManagerGateway.CheckpointConsumer checkpointConsumer =
			(executionAttemptID,
			jobId, checkpointId,
			timestamp,
			checkpointOptions,
			advanceToEndOfEventTime) -> taskManagerCheckpointTriggeredTimes.incrementAndGet();
		ExecutionVertex vertex = mockExecutionVertex(attemptID, checkpointConsumer);

		// set up the coordinator and validate the initial state
		CheckpointCoordinator checkpointCoordinator = createCheckpointCoordinator(vertex);

		final CompletableFuture<String> masterHookCheckpointFuture = new CompletableFuture<>();
		checkpointCoordinator.addMasterHook(new TestingMasterHook(masterHookCheckpointFuture));
		checkpointCoordinator.startCheckpointScheduler();
		final CompletableFuture<CompletedCheckpoint> onCompletionPromise =
			triggerPeriodicCheckpoint(checkpointCoordinator);

		// checkpoint trigger will not finish since master hook checkpoint is not finished yet
		mainThreadExecutor.triggerAll();
		assertTrue(checkpointCoordinator.isTriggering());

		// continue triggering
		masterHookCheckpointFuture.completeExceptionally(new Exception("by design"));

		mainThreadExecutor.triggerAll();
		assertFalse(checkpointCoordinator.isTriggering());

		try {
			onCompletionPromise.get();
			fail("Should not reach here");
		} catch (ExecutionException e) {
			final Optional<CheckpointException> checkpointExceptionOptional =
				ExceptionUtils.findThrowable(e, CheckpointException.class);
			assertTrue(checkpointExceptionOptional.isPresent());
			assertEquals(CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE,
				checkpointExceptionOptional.get().getCheckpointFailureReason());
		}
		// it doesn't really trigger task manager to do checkpoint
		assertEquals(0, taskManagerCheckpointTriggeredTimes.get());
		assertEquals(0, checkpointCoordinator.getTriggerRequestQueue().size());
	}

	private CheckpointCoordinator createCheckpointCoordinator() {
		return new CheckpointCoordinatorBuilder()
			.setTimer(timer)
			.setMainThreadExecutor(mainThreadExecutor)
			.build();
	}

	private CheckpointCoordinator createCheckpointCoordinator(ExecutionVertex executionVertex) {
		return new CheckpointCoordinatorBuilder()
			.setTasks(new ExecutionVertex[] { executionVertex })
			.setTimer(timer)
			.setMainThreadExecutor(mainThreadExecutor)
			.build();
	}

	private CompletableFuture<CompletedCheckpoint> triggerPeriodicCheckpoint(
		CheckpointCoordinator checkpointCoordinator) {

		return checkpointCoordinator.triggerCheckpoint(
			System.currentTimeMillis(),
			CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
			null,
			true,
			false);
	}

	private CompletableFuture<CompletedCheckpoint> triggerNonPeriodicCheckpoint(
		CheckpointCoordinator checkpointCoordinator) {

		return checkpointCoordinator.triggerCheckpoint(
			System.currentTimeMillis(),
			CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
			null,
			false,
			false);
	}

	private static class TestingMasterHook implements MasterTriggerRestoreHook<String> {

		private final SimpleVersionedSerializer<String> serializer =
			new CheckpointCoordinatorTestingUtils.StringSerializer();

		private final CompletableFuture<String> checkpointFuture;

		private TestingMasterHook(CompletableFuture<String> checkpointFuture) {
			this.checkpointFuture = checkpointFuture;
		}

		@Override
		public String getIdentifier() {
			return "testing master hook";
		}

		@Nullable
		@Override
		public CompletableFuture<String> triggerCheckpoint(
			long checkpointId, long timestamp, Executor executor) {
			return checkpointFuture;
		}

		@Override
		public void restoreCheckpoint(long checkpointId, @Nullable String checkpointData) {

		}

		@Nullable
		@Override
		public SimpleVersionedSerializer<String> createCheckpointDataSerializer() {
			return serializer;
		}
	}

	private static class UnstableCheckpointIDCounter implements CheckpointIDCounter {

		private final Predicate<Long> checkpointFailurePredicate;

		private long id = 0;

		public UnstableCheckpointIDCounter(Predicate<Long> checkpointFailurePredicate) {
			this.checkpointFailurePredicate = checkNotNull(checkpointFailurePredicate);
		}

		@Override
		public void start() {

		}

		@Override
		public void shutdown(JobStatus jobStatus) throws Exception {

		}

		@Override
		public long getAndIncrement() {
			if (checkpointFailurePredicate.test(id++)) {
				throw new RuntimeException("CheckpointIDCounter#getAndIncrement fails by design");
			}
			return id;
		}

		@Override
		public long get() {
			return id;
		}

		@Override
		public void setCount(long newId) {

		}
	}
}
