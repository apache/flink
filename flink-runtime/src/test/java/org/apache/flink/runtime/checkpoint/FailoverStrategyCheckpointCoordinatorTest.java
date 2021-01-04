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
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/** Tests for actions of {@link CheckpointCoordinator} on task failures. */
public class FailoverStrategyCheckpointCoordinatorTest extends TestLogger {
    private ManuallyTriggeredScheduledExecutor manualThreadExecutor;

    @Before
    public void setUp() {
        manualThreadExecutor = new ManuallyTriggeredScheduledExecutor();
    }

    /**
     * Tests that {@link CheckpointCoordinator#abortPendingCheckpoints(CheckpointException)} called
     * on job failover could handle the {@code currentPeriodicTrigger} null case well.
     */
    @Test
    public void testAbortPendingCheckpointsWithTriggerValidation() {
        final int maxConcurrentCheckpoints = ThreadLocalRandom.current().nextInt(10) + 1;
        ExecutionVertex executionVertex = mockExecutionVertex();
        CheckpointCoordinatorConfiguration checkpointCoordinatorConfiguration =
                new CheckpointCoordinatorConfiguration(
                        Integer.MAX_VALUE,
                        Integer.MAX_VALUE,
                        0,
                        maxConcurrentCheckpoints,
                        CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
                        true,
                        false,
                        false,
                        0);
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinator(
                        new JobID(),
                        checkpointCoordinatorConfiguration,
                        new ExecutionVertex[] {executionVertex},
                        new ExecutionVertex[] {executionVertex},
                        new ExecutionVertex[] {executionVertex},
                        Collections.emptyList(),
                        new StandaloneCheckpointIDCounter(),
                        new StandaloneCompletedCheckpointStore(1),
                        new MemoryStateBackend(),
                        Executors.directExecutor(),
                        new CheckpointsCleaner(),
                        manualThreadExecutor,
                        SharedStateRegistry.DEFAULT_FACTORY,
                        mock(CheckpointFailureManager.class));

        // switch current execution's state to running to allow checkpoint could be triggered.
        mockExecutionRunning(executionVertex);

        checkpointCoordinator.startCheckpointScheduler();
        assertTrue(checkpointCoordinator.isCurrentPeriodicTriggerAvailable());
        // only trigger the periodic scheduling
        // we can't trigger all scheduled task, because there is also a cancellation scheduled
        manualThreadExecutor.triggerPeriodicScheduledTasks();
        manualThreadExecutor.triggerAll();
        assertEquals(1, checkpointCoordinator.getNumberOfPendingCheckpoints());

        for (int i = 1; i < maxConcurrentCheckpoints; i++) {
            checkpointCoordinator.triggerCheckpoint(false);
            manualThreadExecutor.triggerAll();
            assertEquals(i + 1, checkpointCoordinator.getNumberOfPendingCheckpoints());
            assertTrue(checkpointCoordinator.isCurrentPeriodicTriggerAvailable());
        }

        // as we only support limited concurrent checkpoints, after checkpoint triggered more than
        // the limits,
        // the currentPeriodicTrigger would been assigned as null.
        checkpointCoordinator.triggerCheckpoint(false);
        manualThreadExecutor.triggerAll();
        assertEquals(
                maxConcurrentCheckpoints, checkpointCoordinator.getNumberOfPendingCheckpoints());

        checkpointCoordinator.abortPendingCheckpoints(
                new CheckpointException(CheckpointFailureReason.JOB_FAILOVER_REGION));
        // after aborting checkpoints, we ensure currentPeriodicTrigger still available.
        assertTrue(checkpointCoordinator.isCurrentPeriodicTriggerAvailable());
        assertEquals(0, checkpointCoordinator.getNumberOfPendingCheckpoints());
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
        when(executionVertex.getCurrentExecutionAttempt().getState())
                .thenReturn(ExecutionState.RUNNING);
    }
}
