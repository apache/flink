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

package org.apache.flink.runtime.scheduler.declarative;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.TestingExecutionGraphBuilder;
import org.apache.flink.runtime.jobmaster.slotpool.ResourceCounter;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.SupplierWithException;

import org.junit.Test;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests for the WaitingForResources state. */
public class WaitingForResourcesTest extends TestLogger {
    private static final ResourceCounter RESOURCE_COUNTER =
            ResourceCounter.withResource(ResourceProfile.ANY, 1);

    /** WaitingForResources is transitioning to Executing if there are enough resources. */
    @Test
    public void testTransitionToExecuting() throws Exception {
        try (MockContext ctx = new MockContext()) {
            ctx.setHasEnoughResources(() -> true);

            WaitingForResources wfr = new WaitingForResources(ctx, log, RESOURCE_COUNTER);
            ctx.setExpectExecuting(assertNonNull());
            wfr.onEnter();
        }
    }

    @Test
    public void testTransitionToFinishedOnFailure() throws Exception {
        try (MockContext ctx = new MockContext()) {
            ctx.setHasEnoughResources(() -> true);
            ctx.setCreateExecutionGraphWithAvailableResources(
                    () -> {
                        throw new RuntimeException("Test exception");
                    });

            WaitingForResources wfr = new WaitingForResources(ctx, log, RESOURCE_COUNTER);
            ctx.setExpectFinished(
                    (archivedExecutionGraph -> {
                        assertThat(archivedExecutionGraph.getState(), is(JobStatus.FAILED));
                    }));
            wfr.onEnter();
        }
    }

    @Test
    public void testNotEnoughResources() throws Exception {
        try (MockContext ctx = new MockContext()) {
            ctx.setHasEnoughResources(() -> false);
            WaitingForResources wfr = new WaitingForResources(ctx, log, RESOURCE_COUNTER);

            // we expect no state transition.
            wfr.onEnter();
            wfr.notifyNewResourcesAvailable();
        }
    }

    @Test
    public void testNotifyNewResourcesAvailable() throws Exception {
        try (MockContext ctx = new MockContext()) {
            ctx.setHasEnoughResources(() -> false); // initially, not enough resources
            WaitingForResources wfr = new WaitingForResources(ctx, log, RESOURCE_COUNTER);
            wfr.onEnter();
            ctx.setExpectExecuting(assertNonNull());
            ctx.setHasEnoughResources(() -> true); // make resources available
            wfr.notifyNewResourcesAvailable(); // .. and notify
        }
    }

    @Test
    public void testResourceTimeout() throws Exception {
        try (MockContext ctx = new MockContext()) {
            ctx.setHasEnoughResources(() -> false);
            WaitingForResources wfr = new WaitingForResources(ctx, log, RESOURCE_COUNTER);

            ctx.setExpectExecuting(assertNonNull());
            wfr.onEnter();

            // immediately execute all scheduled runnables
            assertThat(ctx.getScheduledRunnables().size(), greaterThan(0));
            for (ScheduledRunnable scheduledRunnable : ctx.getScheduledRunnables()) {
                if (scheduledRunnable.getExpectedState() == wfr) {
                    scheduledRunnable.runAction();
                }
            }
        }
    }

    @Test
    public void testTransitionToFinishedOnGlobalFailure() throws Exception {
        final String testExceptionString = "This is a test exception";
        try (MockContext ctx = new MockContext()) {
            ctx.setHasEnoughResources(() -> false);
            WaitingForResources wfr = new WaitingForResources(ctx, log, RESOURCE_COUNTER);

            ctx.setExpectFinished(
                    archivedExecutionGraph -> {
                        assertThat(archivedExecutionGraph.getState(), is(JobStatus.FAILED));
                        assertThat(archivedExecutionGraph.getFailureInfo(), notNullValue());
                        assertTrue(
                                archivedExecutionGraph
                                        .getFailureInfo()
                                        .getExceptionAsString()
                                        .contains(testExceptionString));
                    });
            wfr.onEnter();

            wfr.handleGlobalFailure(new RuntimeException(testExceptionString));
        }
    }

    @Test
    public void testCancel() throws Exception {
        try (MockContext ctx = new MockContext()) {
            ctx.setHasEnoughResources(() -> false);
            WaitingForResources wfr = new WaitingForResources(ctx, log, RESOURCE_COUNTER);

            ctx.setExpectFinished(
                    (archivedExecutionGraph -> {
                        assertThat(archivedExecutionGraph.getState(), is(JobStatus.CANCELED));
                    }));
            wfr.onEnter();
            wfr.cancel();
        }
    }

    @Test
    public void testSuspend() throws Exception {
        try (MockContext ctx = new MockContext()) {
            ctx.setHasEnoughResources(() -> false);
            WaitingForResources wfr = new WaitingForResources(ctx, log, RESOURCE_COUNTER);

            ctx.setExpectFinished(
                    (archivedExecutionGraph -> {
                        assertThat(archivedExecutionGraph.getState(), is(JobStatus.SUSPENDED));
                        assertThat(archivedExecutionGraph.getFailureInfo(), notNullValue());
                    }));
            wfr.onEnter();
            wfr.suspend(new RuntimeException("suspend"));
        }
    }

    private static class MockContext implements WaitingForResources.Context, AutoCloseable {

        private final StateValidator<ExecutionGraph> executingStateValidator =
                new StateValidator<>("executing");
        private final StateValidator<ArchivedExecutionGraph> finishingStateValidator =
                new StateValidator<>("finishing");

        private Supplier<Boolean> hasEnoughResourcesSupplier = () -> false;
        private SupplierWithException<ExecutionGraph, FlinkException>
                createExecutionGraphWithAvailableResources =
                        () -> TestingExecutionGraphBuilder.newBuilder().build();
        private final List<ScheduledRunnable> scheduledRunnables = new ArrayList<>();

        public List<ScheduledRunnable> getScheduledRunnables() {
            return scheduledRunnables;
        }

        public void setHasEnoughResources(Supplier<Boolean> sup) {
            hasEnoughResourcesSupplier = sup;
        }

        public void setCreateExecutionGraphWithAvailableResources(
                SupplierWithException<ExecutionGraph, FlinkException> sup) {
            this.createExecutionGraphWithAvailableResources = sup;
        }

        void setExpectFinished(Consumer<ArchivedExecutionGraph> asserter) {
            finishingStateValidator.activate(asserter);
        }

        void setExpectExecuting(Consumer<ExecutionGraph> asserter) {
            executingStateValidator.activate(asserter);
        }

        @Override
        public void close() throws Exception {
            executingStateValidator.close();
            finishingStateValidator.close();
        }

        @Override
        public ArchivedExecutionGraph getArchivedExecutionGraph(
                JobStatus jobStatus, @Nullable Throwable cause) {
            return new ArchivedExecutionGraphBuilder()
                    .setState(jobStatus)
                    .setFailureCause(cause == null ? null : new ErrorInfo(cause, 1337))
                    .build();
        }

        @Override
        public boolean hasEnoughResources(ResourceCounter desiredResources) {
            return hasEnoughResourcesSupplier.get();
        }

        @Override
        public ExecutionGraph createExecutionGraphWithAvailableResources() throws FlinkException {
            return createExecutionGraphWithAvailableResources.get();
        }

        @Override
        public void runIfState(State expectedState, Runnable action, Duration delay) {
            scheduledRunnables.add(new ScheduledRunnable(expectedState, action, delay));
        }

        @Override
        public void goToFinished(ArchivedExecutionGraph archivedExecutionGraph) {
            finishingStateValidator.validateInput(archivedExecutionGraph);
        }

        @Override
        public void goToExecuting(ExecutionGraph executionGraph) {
            executingStateValidator.validateInput(executionGraph);
        }
    }

    private static final class ScheduledRunnable {
        private final Runnable action;
        private final State expectedState;
        private final Duration delay;

        private ScheduledRunnable(State expectedState, Runnable action, Duration delay) {
            this.expectedState = expectedState;
            this.action = action;
            this.delay = delay;
        }

        public void runAction() {
            action.run();
        }

        public State getExpectedState() {
            return expectedState;
        }
    }

    static <T> Consumer<T> assertNonNull() {
        return (item) -> assertThat(item, notNullValue());
    }
}
