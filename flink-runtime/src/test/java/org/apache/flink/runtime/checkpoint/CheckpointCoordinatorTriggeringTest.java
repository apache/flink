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
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.CheckpointCoordinatorBuilder;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.CheckpointCoordinatorConfigurationBuilder;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.mockExecutionVertex;
import static org.junit.Assert.assertEquals;
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

	private ManuallyTriggeredScheduledExecutor manuallyTriggeredScheduledExecutor;

	@Before
	public void setUp() throws Exception {
		manuallyTriggeredScheduledExecutor = new ManuallyTriggeredScheduledExecutor();
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
					.setTimer(manuallyTriggeredScheduledExecutor)
					.build();

			checkpointCoordinator.startCheckpointScheduler();

			do {
				manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
			}
			while (numCalls.get() < 5);
			assertEquals(5, numCalls.get());

			checkpointCoordinator.stopCheckpointScheduler();

			// no further calls may come.
			manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
			assertEquals(5, numCalls.get());

			// start another sequence of periodic scheduling
			numCalls.set(0);
			checkpointCoordinator.startCheckpointScheduler();

			do {
				manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
			}
			while (numCalls.get() < 5);
			assertEquals(5, numCalls.get());

			checkpointCoordinator.stopCheckpointScheduler();

			// no further calls may come
			manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
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
				.setTimer(manuallyTriggeredScheduledExecutor)
				.build();

		try {
			checkpointCoordinator.startCheckpointScheduler();
			manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();

			// wait until the first checkpoint was triggered
			Long firstCallId = triggerCalls.take();
			assertEquals(1L, firstCallId.longValue());

			AcknowledgeCheckpoint ackMsg = new AcknowledgeCheckpoint(jid, attemptID, 1L);

			// tell the coordinator that the checkpoint is done
			final long ackTime = System.nanoTime();
			checkpointCoordinator.receiveAcknowledgeMessage(ackMsg, TASK_MANAGER_LOCATION_INFO);

			manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
			while (triggerCalls.isEmpty()) {
				// sleeps for a while to simulate periodic scheduling
				Thread.sleep(checkpointInterval);
				manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
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
		CheckpointCoordinator checkpointCoordinator =
			new CheckpointCoordinatorBuilder()
				.setTimer(manuallyTriggeredScheduledExecutor)
				.build();

		// Periodic
		try {
			checkpointCoordinator.triggerCheckpoint(
				System.currentTimeMillis(),
				CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
				null,
				true,
				false);
			manuallyTriggeredScheduledExecutor.triggerAll();
			fail("The triggerCheckpoint call expected an exception");
		} catch (CheckpointException e) {
			assertEquals(CheckpointFailureReason.PERIODIC_SCHEDULER_SHUTDOWN, e.getCheckpointFailureReason());
		}

		// Not periodic
		try {
			checkpointCoordinator.triggerCheckpoint(
				System.currentTimeMillis(),
				CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
				null,
				false,
				false);
			manuallyTriggeredScheduledExecutor.triggerAll();
		} catch (CheckpointException e) {
			fail("Unexpected exception : " + e.getCheckpointFailureReason().message());
		}
	}

}
