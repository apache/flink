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
import org.apache.flink.core.testutils.ScheduledTask;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests for the WaitingForResources state. */
public class WaitingForResourcesTest extends TestLogger {
    private static final ResourceCounter RESOURCE_COUNTER =
            ResourceCounter.withResource(ResourceProfile.UNKNOWN, 1);

    private static final Duration STABILIZATION_TIMEOUT = Duration.ofSeconds(1);

    /** WaitingForResources is transitioning to Executing if there are enough resources. */
    @Test
    public void testTransitionToCreatingExecutionGraph() throws Exception {
        try (MockContext ctx = new MockContext()) {
            ctx.setHasDesiredResources(() -> true);

            ctx.setExpectCreatingExecutionGraph();

            new WaitingForResources(ctx, log, RESOURCE_COUNTER, Duration.ZERO, STABILIZATION_TIMEOUT);
            // run delayed actions
            ctx.runScheduledTasks();
        }
    }

    @Test
    public void testTransitionToFinishedOnExecutionGraphInitializationFailure() throws Exception {
        try (MockContext ctx = new MockContext()) {
            ctx.setHasDesiredResources(() -> true);
            ctx.setCreateExecutionGraphWithAvailableResources(
                    () -> {
                        throw new RuntimeException("Test exception");
                    });

            ctx.setExpectFinished(
                    (archivedExecutionGraph -> {
                        assertThat(archivedExecutionGraph.getState(), is(JobStatus.FAILED));
                    }));

            new WaitingForResources(
                    ctx, log, RESOURCE_COUNTER, Duration.ZERO, STABILIZATION_TIMEOUT);

            ctx.runScheduledTasks();
        }
    }

    @Test
    public void testNotEnoughResources() throws Exception {
        try (MockContext ctx = new MockContext()) {
            ctx.setHasDesiredResources(() -> false);
            WaitingForResources wfr =
                    new WaitingForResources(
                            ctx, log, RESOURCE_COUNTER, Duration.ZERO, STABILIZATION_TIMEOUT);

            // we expect no state transition.
            wfr.notifyNewResourcesAvailable();
        }
    }

    @Test
    public void testNotifyNewResourcesAvailable() throws Exception {
        try (MockContext ctx = new MockContext()) {
            ctx.setHasDesiredResources(() -> false); // initially, not enough resources
            WaitingForResources wfr =
                    new WaitingForResources(
                            ctx, log, RESOURCE_COUNTER, Duration.ZERO, STABILIZATION_TIMEOUT);
            ctx.setHasDesiredResources(() -> true); // make resources available
            ctx.setExpectCreatingExecutionGraph();
            wfr.notifyNewResourcesAvailable(); // .. and notify
        }
    }

    @Test
    public void testSchedulingWithSufficientResourcesAndNoStabilizationTimeout() throws Exception {
        try (MockContext ctx = new MockContext()) {

            Duration noStabilizationTimeout = Duration.ofMillis(0);
            WaitingForResources wfr =
                    new WaitingForResources(
                            ctx,
                            log,
                            RESOURCE_COUNTER,
                            Duration.ofSeconds(1000),
                            noStabilizationTimeout);

            ctx.setHasDesiredResources(() -> false);
            ctx.setHasSufficientResources(() -> true);
            ctx.setExpectCreatingExecutionGraph();
            wfr.notifyNewResourcesAvailable();
        }
    }

    @Test
    public void testNoSchedulingIfStabilizationTimeoutIsConfigured() throws Exception {
        try (MockContext ctx = new MockContext()) {

            Duration stabilizationTimeout = Duration.ofMillis(50000);

            WaitingForResources wfr =
                    new WaitingForResources(
                            ctx,
                            log,
                            RESOURCE_COUNTER,
                            Duration.ofSeconds(1000),
                            stabilizationTimeout);

            ctx.setHasDesiredResources(() -> false);
            ctx.setHasSufficientResources(() -> true);
            wfr.notifyNewResourcesAvailable();
            // we are not triggering the scheduled tasks, to simulate a long stabilization timeout

            assertThat(ctx.hasStateTransition(), is(false));
        }
    }

    @Test
    public void testSchedulingIfStabilizationTimeoutIsConfigured() throws Exception {
        try (MockContext ctx = new MockContext()) {

            Duration initialResourceTimeout = Duration.ofMillis(120948);
            Duration stabilizationTimeout = Duration.ofMillis(50000);

            TestingWaitingForResources wfr =
                    new TestingWaitingForResources(
                            ctx,
                            log,
                            RESOURCE_COUNTER,
                            initialResourceTimeout,
                            stabilizationTimeout);

            // not enough resources available
            ctx.setHasDesiredResources(() -> false);
            ctx.setHasSufficientResources(() -> false);

            assertNoStateTransitionsAfterExecutingRunnables(ctx, wfr);

            // immediately execute all scheduled runnables
            ctx.runScheduledTasks();
        }
    }

    private static void assertNoStateTransitionsAfterExecutingRunnables(
            MockContext ctx, WaitingForResources wfr) {
        Iterator<ScheduledRunnable> runnableIterator = ctx.getScheduledRunnables().iterator();
        while (runnableIterator.hasNext()) {
            ScheduledRunnable scheduledRunnable = runnableIterator.next();
            if (scheduledRunnable.getExpectedState() == wfr
                    && scheduledRunnable.getDeadline().isOverdue()) {
                scheduledRunnable.runAction();
                runnableIterator.remove();
            }
        }
        assertThat(ctx.hasStateTransition, is(false));
    }

    private static class TestingWaitingForResources extends WaitingForResources {

        private Deadline testDeadline;

        TestingWaitingForResources(
                Context context,
                Logger log,
                ResourceCounter desiredResources,
                Duration initialResourceAllocationTimeout,
                Duration resourceStabilizationTimeout) {
            super(
                    context,
                    log,
                    desiredResources,
                    initialResourceAllocationTimeout,
                    resourceStabilizationTimeout);
        }

        @Override
        protected Deadline initializeOrGetResourceStabilizationDeadline() {
            return testDeadline;
        }

        public void setTestDeadline(Deadline testDeadline) {
            this.testDeadline = testDeadline;
        }
    }

    @Test
    public void testNoStateTransitionOnNoResourceTimeout() throws Exception {
        try (MockContext ctx = new MockContext()) {
            ctx.setHasDesiredResources(() -> false);
            WaitingForResources wfr =
                    new WaitingForResources(
                            ctx,
                            log,
                            RESOURCE_COUNTER,
                            Duration.ofMillis(-1),
                            STABILIZATION_TIMEOUT);

            executeAllScheduledRunnables(ctx, wfr);

            assertThat(ctx.hasStateTransition(), is(false));
        }
    }

    @Test
    public void testStateTransitionOnResourceTimeout() throws Exception {
        try (MockContext ctx = new MockContext()) {
            ctx.setHasDesiredResources(() -> false);
            WaitingForResources wfr =
                    new WaitingForResources(
                            ctx, log, RESOURCE_COUNTER, Duration.ZERO, STABILIZATION_TIMEOUT);

            ctx.setExpectExecuting(assertNonNull());

            executeAllScheduledRunnables(ctx, wfr);
        }
    }

    @Test
    public void testTransitionToFinishedOnGlobalFailure() throws Exception {
        final String testExceptionString = "This is a test exception";
        try (MockContext ctx = new MockContext()) {
            ctx.setHasDesiredResources(() -> false);
            WaitingForResources wfr =
                    new WaitingForResources(
                            ctx, log, RESOURCE_COUNTER, Duration.ZERO, STABILIZATION_TIMEOUT);

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

            wfr.handleGlobalFailure(new RuntimeException(testExceptionString));
        }
    }

    @Test
    public void testCancel() throws Exception {
        try (MockContext ctx = new MockContext()) {
            ctx.setHasDesiredResources(() -> false);
            WaitingForResources wfr =
                    new WaitingForResources(
                            ctx, log, RESOURCE_COUNTER, Duration.ZERO, STABILIZATION_TIMEOUT);

            ctx.setExpectFinished(
                    (archivedExecutionGraph -> {
                        assertThat(archivedExecutionGraph.getState(), is(JobStatus.CANCELED));
                    }));
            wfr.cancel();
        }
    }

    @Test
    public void testSuspend() throws Exception {
        try (MockContext ctx = new MockContext()) {
            ctx.setHasDesiredResources(() -> false);
            WaitingForResources wfr =
                    new WaitingForResources(
                            ctx, log, RESOURCE_COUNTER, Duration.ZERO, STABILIZATION_TIMEOUT);

            ctx.setExpectFinished(
                    (archivedExecutionGraph -> {
                        assertThat(archivedExecutionGraph.getState(), is(JobStatus.SUSPENDED));
                        assertThat(archivedExecutionGraph.getFailureInfo(), notNullValue());
                    }));

            wfr.suspend(new RuntimeException("suspend"));
        }
    }

    private void executeAllScheduledRunnables(MockContext ctx, WaitingForResources expectedState) {
        for (ScheduledRunnable scheduledRunnable : ctx.getScheduledRunnables()) {
            if (scheduledRunnable.getExpectedState() == expectedState) {
                scheduledRunnable.runAction();
            }
        }
    }

    private static class MockContext implements WaitingForResources.Context, AutoCloseable {

        private final StateValidator<Void> creatingExecutionGraphStateValidator =
                new StateValidator<>("executing");
        private final StateValidator<ArchivedExecutionGraph> finishedStateValidator =
                new StateValidator<>("finished");

        private Supplier<Boolean> hasDesiredResourcesSupplier = () -> false;
        private Supplier<Boolean> hasSufficientResourcesSupplier = () -> false;

        private final List<ScheduledTask<Void>> scheduledTasks = new ArrayList<>();
        private boolean hasStateTransition = false;

        public void setHasDesiredResources(Supplier<Boolean> sup) {
            hasDesiredResourcesSupplier = sup;
        }

        public void setHasSufficientResources(Supplier<Boolean> sup) {
            hasSufficientResourcesSupplier = sup;
        }

        void setExpectFinished(Consumer<ArchivedExecutionGraph> asserter) {
            finishedStateValidator.expectInput(asserter);
        }

        void setExpectCreatingExecutionGraph() {
            creatingExecutionGraphStateValidator.expectInput(none -> {});
        }

        void runScheduledTasks() {
            for (ScheduledTask<Void> scheduledTask : scheduledTasks) {
                scheduledTask.execute();
            }
        }

        @Override
        public void close() throws Exception {
            creatingExecutionGraphStateValidator.close();
            finishedStateValidator.close();
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
        public boolean hasDesiredResources(ResourceCounter desiredResources) {
            return hasDesiredResourcesSupplier.get();
        }

        @Override
        public boolean hasSufficientResources() {
            return hasSufficientResourcesSupplier.get();
        }

        @Override
        public ScheduledFuture<?> runIfState(State expectedState, Runnable action, Duration delay) {
            final ScheduledTask<Void> scheduledTask =
                    new ScheduledTask<>(
                            () -> {
                                if (!hasStateTransition) {
                                    action.run();
                                }

                                return null;
                            },
                            delay.toMillis());

            scheduledTasks.add(scheduledTask);

            return scheduledTask;
        }

        @Override
        public void goToFinished(ArchivedExecutionGraph archivedExecutionGraph) {
            finishedStateValidator.validateInput(archivedExecutionGraph);
            hasStateTransition = true;
        }

        @Override
        public void goToCreatingExecutionGraph() {
            creatingExecutionGraphStateValidator.validateInput(null);
            hasStateTransition = true;
        }

        public boolean hasStateTransition() {
            return hasStateTransition;
        }
    }

    private static final class ScheduledRunnable {
        private final Runnable action;
        private final State expectedState;
        private final Duration delay;
        private final Deadline deadline;

        private ScheduledRunnable(State expectedState, Runnable action, Duration delay) {
            this.expectedState = expectedState;
            this.action = action;
            this.delay = delay;
            this.deadline = Deadline.fromNow(delay);
        }

        public void runAction() {
            action.run();
        }

        public State getExpectedState() {
            return expectedState;
        }

        public Duration getDelay() {
            return delay;
        }

        public Deadline getDeadline() {
            return deadline;
        }
    }

    static <T> Consumer<T> assertNonNull() {
        return (item) -> assertThat(item, notNullValue());
    }
}
