/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.NoOpExecutionDeploymentListener;
import org.apache.flink.runtime.executiongraph.TestingExecutionGraphBuilder;
import org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease.PartitionReleaseStrategyFactoryLoader;
import org.apache.flink.runtime.io.network.partition.NoOpJobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.runtime.shuffle.NettyShuffleMaster;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.function.Consumer;

import static org.apache.flink.runtime.scheduler.adaptive.WaitingForResourcesTest.assertNonNull;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/** Tests for the {@link Restarting} state of the {@link AdaptiveScheduler}. */
public class RestartingTest extends TestLogger {

    @Test
    public void testExecutionGraphCancellationOnEnter() throws Exception {
        try (MockRestartingContext ctx = new MockRestartingContext()) {
            CancellableExecutionGraph cancellableExecutionGraph = new CancellableExecutionGraph();
            createRestartingState(ctx, cancellableExecutionGraph);

            assertThat(cancellableExecutionGraph.isCancelled(), is(true));
        }
    }

    @Test
    public void testTransitionToWaitingForResourcesWhenCancellationComplete() throws Exception {
        try (MockRestartingContext ctx = new MockRestartingContext()) {
            Restarting restarting = createRestartingState(ctx);
            ctx.setExpectWaitingForResources();
            restarting.onGloballyTerminalState(JobStatus.CANCELED);
        }
    }

    @Test
    public void testCancel() throws Exception {
        try (MockRestartingContext ctx = new MockRestartingContext()) {
            Restarting restarting = createRestartingState(ctx);
            ctx.setExpectCancelling(assertNonNull());
            restarting.cancel();
        }
    }

    @Test
    public void testSuspend() throws Exception {
        try (MockRestartingContext ctx = new MockRestartingContext()) {
            CancellableExecutionGraph cancellableExecutionGraph = new CancellableExecutionGraph();
            Restarting restarting = createRestartingState(ctx, cancellableExecutionGraph);
            ctx.setExpectFinished(
                    archivedExecutionGraph ->
                            assertThat(archivedExecutionGraph.getState(), is(JobStatus.SUSPENDED)));
            final Throwable cause = new RuntimeException("suspend");
            restarting.suspend(cause);
        }
    }

    @Test
    public void testGlobalFailuresAreIgnored() throws Exception {
        try (MockRestartingContext ctx = new MockRestartingContext()) {
            CancellableExecutionGraph cancellableExecutionGraph = new CancellableExecutionGraph();
            Restarting restarting = createRestartingState(ctx, cancellableExecutionGraph);
            restarting.handleGlobalFailure(new RuntimeException());
            ctx.assertNoStateTransition();
        }
    }

    @Test
    public void testStateDoesNotExposeGloballyTerminalExecutionGraph() throws Exception {
        try (MockRestartingContext ctx = new MockRestartingContext()) {
            Restarting restarting = createRestartingState(ctx);

            // ideally we'd just delay the state transitions, but the context does not support that
            ctx.setExpectWaitingForResources();

            // this is just a sanity check for the test
            assertThat(restarting.getExecutionGraph().getState(), is(JobStatus.CANCELED));

            assertThat(restarting.getJobStatus(), is(JobStatus.RESTARTING));
            assertThat(restarting.getJob().getState(), is(JobStatus.RESTARTING));
            assertThat(restarting.getJob().getStatusTimestamp(JobStatus.CANCELED), is(0L));
        }
    }

    public Restarting createRestartingState(
            MockRestartingContext ctx, ExecutionGraph executionGraph) {
        final ExecutionGraphHandler executionGraphHandler =
                new ExecutionGraphHandler(
                        executionGraph,
                        log,
                        ctx.getMainThreadExecutor(),
                        ctx.getMainThreadExecutor());
        final OperatorCoordinatorHandler operatorCoordinatorHandler =
                new OperatorCoordinatorHandler(
                        executionGraph,
                        (throwable) -> {
                            throw new RuntimeException("Error in test", throwable);
                        });
        executionGraph.transitionToRunning();
        return new Restarting(
                ctx,
                executionGraph,
                executionGraphHandler,
                operatorCoordinatorHandler,
                log,
                Duration.ZERO);
    }

    public Restarting createRestartingState(MockRestartingContext ctx)
            throws JobException, JobExecutionException {
        ExecutionGraph executionGraph = TestingExecutionGraphBuilder.newBuilder().build();
        return createRestartingState(ctx, executionGraph);
    }

    private static class MockRestartingContext extends MockStateWithExecutionGraphContext
            implements Restarting.Context {

        private final StateValidator<ExecutingTest.CancellingArguments> cancellingStateValidator =
                new StateValidator<>("Cancelling");

        private final StateValidator<Void> waitingForResourcesStateValidator =
                new StateValidator<>("WaitingForResources");

        public void setExpectCancelling(Consumer<ExecutingTest.CancellingArguments> asserter) {
            cancellingStateValidator.expectInput(asserter);
        }

        public void setExpectWaitingForResources() {
            waitingForResourcesStateValidator.expectInput((none) -> {});
        }

        @Override
        public void goToCanceling(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler) {
            cancellingStateValidator.validateInput(
                    new ExecutingTest.CancellingArguments(
                            executionGraph, executionGraphHandler, operatorCoordinatorHandler));
            hadStateTransition = true;
        }

        @Override
        public void goToWaitingForResources() {
            waitingForResourcesStateValidator.validateInput(null);
            hadStateTransition = true;
        }

        @Override
        public void runIfState(State expectedState, Runnable action, Duration delay) {
            if (!hadStateTransition) {
                action.run();
            }
        }

        @Override
        public void close() throws Exception {
            super.close();
            cancellingStateValidator.close();
            waitingForResourcesStateValidator.close();
        }
    }

    static class CancellableExecutionGraph extends ExecutionGraph {
        private boolean cancelled = false;

        CancellableExecutionGraph() throws IOException {
            super(
                    new JobInformation(
                            new JobID(),
                            "Test Job",
                            new SerializedValue<>(new ExecutionConfig()),
                            new Configuration(),
                            Collections.emptyList(),
                            Collections.emptyList()),
                    TestingUtils.defaultExecutor(),
                    TestingUtils.defaultExecutor(),
                    AkkaUtils.getDefaultTimeout(),
                    1,
                    ExecutionGraph.class.getClassLoader(),
                    VoidBlobWriter.getInstance(),
                    PartitionReleaseStrategyFactoryLoader.loadPartitionReleaseStrategyFactory(
                            new Configuration()),
                    NettyShuffleMaster.INSTANCE,
                    NoOpJobMasterPartitionTracker.INSTANCE,
                    ScheduleMode.EAGER,
                    NoOpExecutionDeploymentListener.get(),
                    (execution, newState) -> {},
                    0L);
            setJsonPlan("");
        }

        @Override
        public void cancel() {
            cancelled = true;
        }

        public boolean isCancelled() {
            return cancelled;
        }
    }
}
