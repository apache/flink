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
import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.DefaultExecutionGraph;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.executiongraph.TestingDefaultExecutionGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.operators.coordination.CoordinatorStoreImpl;
import org.apache.flink.runtime.scheduler.DefaultVertexParallelismInfo;
import org.apache.flink.runtime.testtasks.NoOpInvokable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** {@link ExecutionTrackingCheckpointCoordinatorDeActivator} test. */
class ExecutionTrackingCheckpointCoordinatorDeActivatorTest {

    private ManuallyTriggeredScheduledExecutorService executor;
    private CheckpointCoordinator checkpointCoordinator;
    private ExecutionTrackingCheckpointCoordinatorDeActivator activator;
    private ExecutionAttemptID execution1;
    private ExecutionAttemptID execution2;
    private JobID jobId;

    @BeforeEach
    void init() throws Exception {
        executor = new ManuallyTriggeredScheduledExecutorService();
        checkpointCoordinator =
                new CheckpointCoordinatorTestingUtils.CheckpointCoordinatorBuilder()
                        .build(executor);
        activator = new ExecutionTrackingCheckpointCoordinatorDeActivator(checkpointCoordinator);
        execution1 = createExecution(executor).getAttemptId();
        execution2 = createExecution(executor).getAttemptId();
        jobId = new JobID();
        assertFalse(checkpointCoordinator.isPeriodicCheckpointingStarted());
    }

    @AfterEach
    void destroy() throws Exception {
        checkpointCoordinator.shutdown();
        executor.shutdownNow();
    }

    @Test
    void testBasicLifecycle() {
        activator.onStateUpdate(execution1, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        activator.onStateUpdate(execution2, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        assertFalse(checkpointCoordinator.isPeriodicCheckpointingStarted());

        activator.jobStatusChanges(jobId, JobStatus.RUNNING, 0);
        activator.onStateUpdate(execution1, ExecutionState.DEPLOYING, ExecutionState.RUNNING);
        activator.onStateUpdate(execution2, ExecutionState.DEPLOYING, ExecutionState.RUNNING);
        assertTrue(checkpointCoordinator.isPeriodicCheckpointingStarted());

        activator.jobStatusChanges(jobId, JobStatus.FINISHED, 0);
        assertFalse(checkpointCoordinator.isPeriodicCheckpointingStarted());
    }

    @Test
    void testTaskFailure() {
        activator.onStateUpdate(execution1, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        activator.onStateUpdate(execution2, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        assertFalse(checkpointCoordinator.isPeriodicCheckpointingStarted());

        activator.jobStatusChanges(jobId, JobStatus.RUNNING, 0);
        activator.onStateUpdate(execution1, ExecutionState.DEPLOYING, ExecutionState.RUNNING);
        activator.onStateUpdate(execution2, ExecutionState.DEPLOYING, ExecutionState.RUNNING);
        assertTrue(checkpointCoordinator.isPeriodicCheckpointingStarted());

        activator.onStateUpdate(execution1, ExecutionState.RUNNING, ExecutionState.FAILED);
        assertFalse(checkpointCoordinator.isPeriodicCheckpointingStarted());
    }

    @Test
    void testJobRestart() {
        activator.onStateUpdate(execution1, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        activator.onStateUpdate(execution2, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        assertFalse(checkpointCoordinator.isPeriodicCheckpointingStarted());

        activator.jobStatusChanges(jobId, JobStatus.RUNNING, 0);
        activator.onStateUpdate(execution1, ExecutionState.DEPLOYING, ExecutionState.RUNNING);
        activator.onStateUpdate(execution2, ExecutionState.DEPLOYING, ExecutionState.RUNNING);
        assertTrue(checkpointCoordinator.isPeriodicCheckpointingStarted());

        for (JobStatus jobStatus :
                new JobStatus[] {JobStatus.FAILED, JobStatus.RESTARTING, JobStatus.INITIALIZING}) {
            activator.jobStatusChanges(jobId, jobStatus, 0);
            assertFalse(checkpointCoordinator.isPeriodicCheckpointingStarted());
        }
        activator.jobStatusChanges(jobId, JobStatus.RUNNING, 0);
        assertTrue(checkpointCoordinator.isPeriodicCheckpointingStarted());
    }

    @Test
    void testJobFinish() {
        assertFalse(checkpointCoordinator.isPeriodicCheckpointingStarted());
        activator.jobStatusChanges(jobId, JobStatus.RUNNING, 0);
        assertTrue(checkpointCoordinator.isPeriodicCheckpointingStarted());
        activator.jobStatusChanges(jobId, JobStatus.FINISHED, 0);
        assertFalse(checkpointCoordinator.isPeriodicCheckpointingStarted());
    }

    private static Execution createExecution(ManuallyTriggeredScheduledExecutorService executor)
            throws JobException, JobExecutionException {
        DefaultExecutionGraph eg = TestingDefaultExecutionGraphBuilder.newBuilder().build(executor);
        JobVertex jv = ExecutionGraphTestUtils.createJobVertex("task1", 1, NoOpInvokable.class);
        ExecutionJobVertex ejv =
                new ExecutionJobVertex(
                        eg,
                        jv,
                        new DefaultVertexParallelismInfo(1, 1, ignored -> Optional.empty()),
                        new CoordinatorStoreImpl(),
                        UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());

        ExecutionVertex evv =
                new ExecutionVertex(
                        ejv, 0, new IntermediateResult[0], Duration.ofSeconds(3), 0, 0, 0);
        return new Execution(executor, evv, 0, 0, Duration.ofSeconds(3));
    }
}
