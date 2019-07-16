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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.failover.AdaptedRestartPipelinedRegionStrategyNG;
import org.apache.flink.runtime.executiongraph.failover.FailoverRegion;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategy;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Tests for the interaction between the {@link FailoverStrategy} and the {@link CheckpointCoordinator}.
 */
public class FailoverStrategyCheckpointCoordinatorTest extends TestLogger {

	/**
	 * Tests that {@link CheckpointCoordinator#abortPendingCheckpointsWithTriggerValidation(CheckpointException)}
	 * called by {@link AdaptedRestartPipelinedRegionStrategyNG} or {@link FailoverRegion} could handle
	 * the {@code currentPeriodicTrigger} null situation well.
	 */
	@Test
	public void testAbortPendingCheckpointsWithTriggerValidation() {
		ExecutionVertex executionVertex = mockExecutionVertex();
		CheckpointCoordinatorConfiguration checkpointCoordinatorConfiguration = new CheckpointCoordinatorConfiguration(
			Integer.MAX_VALUE,
			Integer.MAX_VALUE,
			0,
			1,
			CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
			true,
			false,
			0);
		CheckpointCoordinator checkpointCoordinator = new CheckpointCoordinator(
			new JobID(),
			checkpointCoordinatorConfiguration,
			new ExecutionVertex[] { executionVertex },
			new ExecutionVertex[] { executionVertex },
			new ExecutionVertex[] { executionVertex },
			new StandaloneCheckpointIDCounter(),
			new StandaloneCompletedCheckpointStore(1),
			new MemoryStateBackend(),
			Executors.directExecutor(),
			SharedStateRegistry.DEFAULT_FACTORY,
			mock(CheckpointFailureManager.class));

		// switch current execution's state to running to allow checkpoint could be triggered.
		mockExecutionRunning(executionVertex);

		checkpointCoordinator.startCheckpointScheduler();
		checkpointCoordinator.triggerCheckpoint(System.currentTimeMillis(), false);
		assertTrue(checkpointCoordinator.isCurrentPeriodicTriggerAvailable());
		checkpointCoordinator.triggerCheckpoint(System.currentTimeMillis() + 1, false);
		// as we only support single concurrent checkpoint, after twice checkpoint trigger,
		// the currentPeriodicTrigger would been assigned as null.
		assertFalse(checkpointCoordinator.isCurrentPeriodicTriggerAvailable());
		assertFalse(checkpointCoordinator.getPendingCheckpoints().isEmpty());

		checkpointCoordinator.abortPendingCheckpointsWithTriggerValidation(
			new CheckpointException(CheckpointFailureReason.JOB_FAILOVER_REGION));
		// after aborting checkpoints, we ensure currentPeriodicTrigger still available.
		assertTrue(checkpointCoordinator.isCurrentPeriodicTriggerAvailable());
		assertTrue(checkpointCoordinator.getPendingCheckpoints().isEmpty());
	}

	private ExecutionVertex mockExecutionVertex() {
		ExecutionAttemptID executionAttemptID = new ExecutionAttemptID();
		ExecutionVertex executionVertex = mock(ExecutionVertex.class);
		Execution execution = Mockito.mock(Execution.class);
		when(execution.getAttemptId()).thenReturn(executionAttemptID);
		when(executionVertex.getCurrentExecutionAttempt()).thenReturn(execution);
		return executionVertex;
	}

	private void mockExecutionRunning(ExecutionVertex executionVertex) {
		when(executionVertex.getCurrentExecutionAttempt().getState()).thenReturn(ExecutionState.RUNNING);
	}

}
