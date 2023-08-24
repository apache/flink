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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.failover.flip1.TestRestartBackoffTimeStrategy;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.scheduler.DefaultSchedulerBuilder;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.runtime.scheduler.TestingPhysicalSlotProvider;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

/** Validates that suspending out of various states works correctly. */
class ExecutionGraphSuspendTest {

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    /**
     * Going into SUSPENDED out of CREATED should immediately cancel everything and not send out RPC
     * calls.
     */
    @Test
    void testSuspendedOutOfCreated() throws Exception {
        final InteractionsCountingTaskManagerGateway gateway =
                new InteractionsCountingTaskManagerGateway();
        final int parallelism = 10;
        final SchedulerBase scheduler = createScheduler(gateway, parallelism);
        final ExecutionGraph eg = scheduler.getExecutionGraph();

        assertThat(eg.getState()).isEqualTo(JobStatus.CREATED);

        // suspend

        scheduler.closeAsync();

        assertThat(eg.getState()).isEqualTo(JobStatus.SUSPENDED);
        validateAllVerticesInState(eg, ExecutionState.CANCELED);
        validateCancelRpcCalls(gateway, 0);

        ensureCannotLeaveSuspendedState(scheduler, gateway);
    }

    /**
     * Going into SUSPENDED out of DEPLOYING vertices should cancel all vertices once with RPC
     * calls.
     */
    @Test
    void testSuspendedOutOfDeploying() throws Exception {
        final int parallelism = 10;
        final InteractionsCountingTaskManagerGateway gateway =
                new InteractionsCountingTaskManagerGateway(parallelism);
        final SchedulerBase scheduler = createScheduler(gateway, parallelism);
        final ExecutionGraph eg = scheduler.getExecutionGraph();

        scheduler.startScheduling();
        assertThat(eg.getState()).isEqualTo(JobStatus.RUNNING);
        validateAllVerticesInState(eg, ExecutionState.DEPLOYING);

        // suspend
        scheduler.closeAsync();

        assertThat(eg.getState()).isEqualTo(JobStatus.SUSPENDED);
        validateCancelRpcCalls(gateway, parallelism);

        ensureCannotLeaveSuspendedState(scheduler, gateway);
    }

    /**
     * Going into SUSPENDED out of RUNNING vertices should cancel all vertices once with RPC calls.
     */
    @Test
    void testSuspendedOutOfRunning() throws Exception {
        final int parallelism = 10;
        final InteractionsCountingTaskManagerGateway gateway =
                new InteractionsCountingTaskManagerGateway(parallelism);
        final SchedulerBase scheduler = createScheduler(gateway, parallelism);
        final ExecutionGraph eg = scheduler.getExecutionGraph();

        scheduler.startScheduling();
        ExecutionGraphTestUtils.switchAllVerticesToRunning(eg);

        assertThat(eg.getState()).isEqualTo(JobStatus.RUNNING);
        validateAllVerticesInState(eg, ExecutionState.RUNNING);

        // suspend
        scheduler.closeAsync();

        assertThat(eg.getState()).isEqualTo(JobStatus.SUSPENDED);
        validateCancelRpcCalls(gateway, parallelism);

        ensureCannotLeaveSuspendedState(scheduler, gateway);
    }

    /** Suspending from FAILING goes to SUSPENDED and sends no additional RPC calls. */
    @Test
    void testSuspendedOutOfFailing() throws Exception {
        final int parallelism = 10;
        final InteractionsCountingTaskManagerGateway gateway =
                new InteractionsCountingTaskManagerGateway(parallelism);
        final SchedulerBase scheduler = createScheduler(gateway, parallelism);
        final ExecutionGraph eg = scheduler.getExecutionGraph();

        scheduler.startScheduling();
        ExecutionGraphTestUtils.switchAllVerticesToRunning(eg);

        scheduler.handleGlobalFailure(new Exception("fail global"));

        assertThat(eg.getState()).isEqualTo(JobStatus.FAILING);
        validateCancelRpcCalls(gateway, parallelism);

        // suspend
        scheduler.closeAsync();

        assertThat(eg.getState()).isEqualTo(JobStatus.SUSPENDED);
        ensureCannotLeaveSuspendedState(scheduler, gateway);
    }

    /** Suspending from FAILED should do nothing. */
    @Test
    void testSuspendedOutOfFailed() throws Exception {
        final InteractionsCountingTaskManagerGateway gateway =
                new InteractionsCountingTaskManagerGateway();
        final int parallelism = 10;
        final SchedulerBase scheduler = createScheduler(gateway, parallelism);
        final ExecutionGraph eg = scheduler.getExecutionGraph();

        scheduler.startScheduling();
        ExecutionGraphTestUtils.switchAllVerticesToRunning(eg);

        scheduler.handleGlobalFailure(new Exception("fail global"));

        assertThat(eg.getState()).isEqualTo(JobStatus.FAILING);
        validateCancelRpcCalls(gateway, parallelism);

        ExecutionGraphTestUtils.completeCancellingForAllVertices(eg);
        assertThat(eg.getState()).isEqualTo(JobStatus.FAILED);

        // suspend
        scheduler.closeAsync();

        // still in failed state
        assertThat(eg.getState()).isEqualTo(JobStatus.FAILED);
        validateCancelRpcCalls(gateway, parallelism);
    }

    /** Suspending from CANCELING goes to SUSPENDED and sends no additional RPC calls. */
    @Test
    void testSuspendedOutOfCanceling() throws Exception {
        final int parallelism = 10;
        final InteractionsCountingTaskManagerGateway gateway =
                new InteractionsCountingTaskManagerGateway(parallelism);
        final SchedulerBase scheduler = createScheduler(gateway, parallelism);
        final ExecutionGraph eg = scheduler.getExecutionGraph();

        scheduler.startScheduling();
        ExecutionGraphTestUtils.switchAllVerticesToRunning(eg);

        scheduler.cancel();

        assertThat(eg.getState()).isEqualTo(JobStatus.CANCELLING);
        validateCancelRpcCalls(gateway, parallelism);

        // suspend
        scheduler.closeAsync();

        assertThat(eg.getState()).isEqualTo(JobStatus.SUSPENDED);

        ensureCannotLeaveSuspendedState(scheduler, gateway);
    }

    /** Suspending from CANCELLED should do nothing. */
    @Test
    void testSuspendedOutOfCanceled() throws Exception {
        final InteractionsCountingTaskManagerGateway gateway =
                new InteractionsCountingTaskManagerGateway();
        final int parallelism = 10;
        final SchedulerBase scheduler = createScheduler(gateway, parallelism);
        final ExecutionGraph eg = scheduler.getExecutionGraph();

        scheduler.startScheduling();
        ExecutionGraphTestUtils.switchAllVerticesToRunning(eg);

        scheduler.cancel();

        assertThat(eg.getState()).isEqualTo(JobStatus.CANCELLING);
        validateCancelRpcCalls(gateway, parallelism);

        ExecutionGraphTestUtils.completeCancellingForAllVertices(eg);

        FlinkAssertions.assertThatFuture(eg.getTerminationFuture())
                .eventuallySucceeds()
                .isEqualTo(JobStatus.CANCELED);

        // suspend
        scheduler.closeAsync();

        // still in failed state
        assertThat(eg.getState()).isEqualTo(JobStatus.CANCELED);
        validateCancelRpcCalls(gateway, parallelism);
    }

    /** Tests that we can suspend a job when in state RESTARTING. */
    @Test
    void testSuspendWhileRestarting() throws Exception {
        final ManuallyTriggeredScheduledExecutor taskRestartExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final SchedulerBase scheduler =
                new DefaultSchedulerBuilder(
                                JobGraphTestUtils.emptyJobGraph(),
                                ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                                EXECUTOR_RESOURCE.getExecutor())
                        .setRestartBackoffTimeStrategy(
                                new TestRestartBackoffTimeStrategy(true, Long.MAX_VALUE))
                        .setDelayExecutor(taskRestartExecutor)
                        .build();

        scheduler.startScheduling();

        final ExecutionGraph eg = scheduler.getExecutionGraph();

        assertThat(eg.getState()).isEqualTo(JobStatus.RUNNING);
        ExecutionGraphTestUtils.switchAllVerticesToRunning(eg);

        scheduler.handleGlobalFailure(new Exception("test"));
        assertThat(eg.getState()).isEqualTo(JobStatus.RESTARTING);

        ExecutionGraphTestUtils.completeCancellingForAllVertices(eg);
        assertThat(eg.getState()).isEqualTo(JobStatus.RESTARTING);

        scheduler.closeAsync();

        assertThat(eg.getState()).isEqualTo(JobStatus.SUSPENDED);

        taskRestartExecutor.triggerScheduledTasks();
        assertThat(eg.getState()).isEqualTo(JobStatus.SUSPENDED);
    }

    // ------------------------------------------------------------------------
    //  utilities
    // ------------------------------------------------------------------------

    private static void ensureCannotLeaveSuspendedState(
            SchedulerBase scheduler, InteractionsCountingTaskManagerGateway gateway) {
        final ExecutionGraph eg = scheduler.getExecutionGraph();

        gateway.waitUntilAllTasksAreSubmitted();
        assertThat(eg.getState()).isEqualTo(JobStatus.SUSPENDED);
        gateway.resetCounts();

        scheduler.handleGlobalFailure(new Exception("fail"));
        assertThat(eg.getState()).isEqualTo(JobStatus.SUSPENDED);
        validateNoInteractions(gateway);

        scheduler.cancel();
        assertThat(eg.getState()).isEqualTo(JobStatus.SUSPENDED);
        validateNoInteractions(gateway);

        scheduler.closeAsync();
        assertThat(eg.getState()).isEqualTo(JobStatus.SUSPENDED);
        validateNoInteractions(gateway);

        for (ExecutionVertex ev : eg.getAllExecutionVertices()) {
            assertThat(ev.getCurrentExecutionAttempt().getAttemptNumber()).isZero();
        }
    }

    private static void validateNoInteractions(InteractionsCountingTaskManagerGateway gateway) {
        assertThat(gateway.getInteractionsCount()).isZero();
    }

    private static void validateAllVerticesInState(ExecutionGraph eg, ExecutionState expected) {
        for (ExecutionVertex ev : eg.getAllExecutionVertices()) {
            assertThat(ev.getCurrentExecutionAttempt().getState()).isEqualTo(expected);
        }
    }

    private static void validateCancelRpcCalls(
            InteractionsCountingTaskManagerGateway gateway, int num) {
        assertThat(gateway.getCancelTaskCount()).isEqualTo(num);
    }

    private static SchedulerBase createScheduler(TaskManagerGateway gateway, int parallelism)
            throws Exception {
        final JobVertex vertex = new JobVertex("vertex");
        vertex.setInvokableClass(NoOpInvokable.class);
        vertex.setParallelism(parallelism);

        final SchedulerBase scheduler =
                new DefaultSchedulerBuilder(
                                JobGraphTestUtils.streamingJobGraph(vertex),
                                ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                                EXECUTOR_RESOURCE.getExecutor())
                        .setExecutionSlotAllocatorFactory(
                                SchedulerTestingUtils.newSlotSharingExecutionSlotAllocatorFactory(
                                        TestingPhysicalSlotProvider
                                                .createWithLimitedAmountOfPhysicalSlots(
                                                        parallelism, gateway)))
                        .build();
        return scheduler;
    }
}
