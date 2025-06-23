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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.failure.FailureEnricherUtils;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.runtime.scheduler.adaptive.allocator.VertexParallelism;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Collections.singletonMap;
import static org.apache.flink.runtime.scheduler.adaptive.WaitingForResourcesTest.assertNonNull;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link Restarting} state of the {@link AdaptiveScheduler}. */
class RestartingTest {
    private static final Logger log = LoggerFactory.getLogger(RestartingTest.class);

    @Test
    void testExecutionGraphCancellationOnEnter() throws Exception {
        try (MockRestartingContext ctx = new MockRestartingContext()) {
            StateTrackingMockExecutionGraph mockExecutionGraph =
                    new StateTrackingMockExecutionGraph();
            createRestartingState(ctx, mockExecutionGraph);

            assertThat(mockExecutionGraph.getState()).isEqualTo(JobStatus.CANCELLING);
        }
    }

    @ParameterizedTest
    @MethodSource("provideRestartWithParallelism")
    public void testTransitionToSubsequentStateWhenCancellationComplete(
            Optional<VertexParallelism> restartWithParallelism) throws Exception {
        try (MockRestartingContext ctx = new MockRestartingContext()) {
            restartWithParallelism.ifPresent(ctx::setAvailableVertexParallelism);
            Restarting restarting = createRestartingState(ctx, restartWithParallelism.orElse(null));

            if (restartWithParallelism.isPresent()) {
                ctx.setExpectCreatingExecutionGraph();
            } else {
                ctx.setExpectWaitingForResources();
            }
            restarting.onGloballyTerminalState(JobStatus.CANCELED);
        }
    }

    @Test
    public void testTransitionToSubsequentStateWhenResourceChanged() throws Exception {
        try (MockRestartingContext ctx = new MockRestartingContext()) {
            JobVertexID jobVertexId = new JobVertexID();
            VertexParallelism availableParallelism =
                    new VertexParallelism(singletonMap(jobVertexId, 1));
            VertexParallelism requiredParallelismForForcedRestart =
                    new VertexParallelism(singletonMap(jobVertexId, 2));

            ctx.setAvailableVertexParallelism(availableParallelism);
            Restarting restarting = createRestartingState(ctx, requiredParallelismForForcedRestart);
            ctx.setExpectWaitingForResources();
            restarting.onGloballyTerminalState(JobStatus.CANCELED);
        }
    }

    @Test
    void testCancel() throws Exception {
        try (MockRestartingContext ctx = new MockRestartingContext()) {
            Restarting restarting = createRestartingState(ctx);
            ctx.setExpectCancelling(assertNonNull());
            restarting.cancel();
        }
    }

    @Test
    void testSuspendWithJobInCancellingState() throws Exception {
        testSuspend(false);
    }

    @Test
    void testSuspendWithJobInCancelledState() throws Exception {
        testSuspend(true);
    }

    private void testSuspend(boolean cancellationCompleted) throws Exception {
        try (MockRestartingContext ctx = new MockRestartingContext()) {
            final StateTrackingMockExecutionGraph executionGraph =
                    new StateTrackingMockExecutionGraph();
            final Restarting restarting = createRestartingState(ctx, executionGraph);

            if (cancellationCompleted) {
                executionGraph.completeTerminationFuture(JobStatus.CANCELED);
            }

            ctx.setExpectFinished(
                    archivedExecutionGraph ->
                            assertThat(archivedExecutionGraph.getState())
                                    .isEqualTo(JobStatus.SUSPENDED));
            final Throwable cause = new RuntimeException("suspend");
            restarting.suspend(cause);
        }
    }

    @Test
    void testGlobalFailuresAreIgnored() throws Exception {
        try (MockRestartingContext ctx = new MockRestartingContext()) {
            Restarting restarting = createRestartingState(ctx);
            restarting.handleGlobalFailure(
                    new RuntimeException(), FailureEnricherUtils.EMPTY_FAILURE_LABELS);
            ctx.assertNoStateTransition();
        }
    }

    @ParameterizedTest
    @MethodSource("provideRestartWithParallelism")
    public void testStateDoesNotExposeGloballyTerminalExecutionGraph(
            Optional<VertexParallelism> restartWithParallelism) throws Exception {
        try (MockRestartingContext ctx = new MockRestartingContext()) {
            restartWithParallelism.ifPresent(ctx::setAvailableVertexParallelism);
            StateTrackingMockExecutionGraph mockExecutionGraph =
                    new StateTrackingMockExecutionGraph();
            Restarting restarting =
                    createRestartingState(
                            ctx, mockExecutionGraph, restartWithParallelism.orElse(null));

            // ideally we'd just delay the state transitions, but the context does not support that
            if (restartWithParallelism.isPresent()) {
                ctx.setExpectCreatingExecutionGraph();
            } else {
                ctx.setExpectWaitingForResources();
            }

            mockExecutionGraph.completeTerminationFuture(JobStatus.CANCELED);

            // this is just a sanity check for the test
            assertThat(restarting.getExecutionGraph().getState()).isEqualTo(JobStatus.CANCELED);

            assertThat(restarting.getJobStatus()).isEqualTo(JobStatus.RESTARTING);
            assertThat(restarting.getJob().getState()).isEqualTo(JobStatus.RESTARTING);
            assertThat(restarting.getJob().getStatusTimestamp(JobStatus.CANCELED)).isZero();
        }
    }

    public Restarting createRestartingState(
            MockRestartingContext ctx, @Nullable VertexParallelism restartWithParallelism) {
        return createRestartingState(
                ctx, new StateTrackingMockExecutionGraph(), restartWithParallelism);
    }

    public Restarting createRestartingState(
            MockRestartingContext ctx, ExecutionGraph executionGraph) {
        return createRestartingState(ctx, executionGraph, null);
    }

    public Restarting createRestartingState(
            MockRestartingContext ctx,
            ExecutionGraph executionGraph,
            @Nullable VertexParallelism restartWithParallelism) {
        final ExecutionGraphHandler executionGraphHandler =
                new ExecutionGraphHandler(
                        executionGraph,
                        log,
                        ctx.getMainThreadExecutor(),
                        ctx.getMainThreadExecutor());
        final OperatorCoordinatorHandler operatorCoordinatorHandler =
                new TestingOperatorCoordinatorHandler();
        executionGraph.transitionToRunning();
        return new Restarting(
                ctx,
                executionGraph,
                executionGraphHandler,
                operatorCoordinatorHandler,
                log,
                Duration.ZERO,
                restartWithParallelism,
                ClassLoader.getSystemClassLoader(),
                new ArrayList<>());
    }

    public Restarting createRestartingState(MockRestartingContext ctx) {
        return createRestartingState(ctx, new StateTrackingMockExecutionGraph());
    }

    private static Stream<Arguments> provideRestartWithParallelism() {
        return Stream.of(
                Arguments.of(Optional.empty()),
                Arguments.of(
                        Optional.of(new VertexParallelism(singletonMap(new JobVertexID(), 1)))));
    }
}
