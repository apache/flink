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
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.ManualClock;

import org.junit.Test;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
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
            ResourceCounter.withResource(ResourceProfile.UNKNOWN, 1);

    private static final Duration STABILIZATION_TIMEOUT = Duration.ofSeconds(1);

    /** WaitingForResources is transitioning to Executing if there are enough resources. */
    @Test
    public void testTransitionToCreatingExecutionGraph() throws Exception {
        try (MockContext ctx = new MockContext()) {
            ctx.setHasDesiredResources(() -> true);

            ctx.setExpectCreatingExecutionGraph();

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
    public void testSchedulingWithSufficientResourcesAfterStabilizationTimeout() throws Exception {
        try (MockContext ctx = new MockContext()) {

            Duration initialResourceTimeout = Duration.ofMillis(-1);
            Duration stabilizationTimeout = Duration.ofMillis(50_000L);

            ManualClock testingClock = new ManualClock();
            WaitingForResources wfr =
                    new WaitingForResources(
                            ctx,
                            log,
                            RESOURCE_COUNTER,
                            initialResourceTimeout,
                            stabilizationTimeout,
                            testingClock);
            // sufficient resources available
            ctx.setHasDesiredResources(() -> false);
            ctx.setHasSufficientResources(() -> true);

            // notify about sufficient resources
            wfr.notifyNewResourcesAvailable();

            ctx.setExpectCreatingExecutionGraph();

            // execute all runnables and trigger expected state transition
            final Duration afterStabilizationTimeout = stabilizationTimeout.plusMillis(1);
            testingClock.advanceTime(afterStabilizationTimeout);

            ctx.runScheduledTasks(afterStabilizationTimeout.toMillis());

            assertThat(ctx.hasStateTransition(), is(true));
        }
    }

    @Test
    public void testStabilizationTimeoutReset() throws Exception {
        try (MockContext ctx = new MockContext()) {

            Duration initialResourceTimeout = Duration.ofMillis(-1);
            Duration stabilizationTimeout = Duration.ofMillis(50L);

            ManualTestTime testTime =
                    new ManualTestTime(
                            (durationSinceTestStart) ->
                                    ctx.runScheduledTasks(durationSinceTestStart.toMillis()));

            WaitingForResources wfr =
                    new WaitingForResources(
                            ctx,
                            log,
                            RESOURCE_COUNTER,
                            initialResourceTimeout,
                            stabilizationTimeout,
                            testTime.getClock());

            ctx.setHasDesiredResources(() -> false);

            // notify about resources, trigger stabilization timeout
            ctx.setHasSufficientResources(() -> true);
            testTime.advanceMillis(40); // advance time, but don't trigger stabilizationTimeout
            wfr.notifyNewResourcesAvailable();

            // notify again, but insufficient (reset stabilization timeout)
            ctx.setHasSufficientResources(() -> false);
            testTime.advanceMillis(40);
            wfr.notifyNewResourcesAvailable();

            // notify again, but sufficient, trigger timeout
            ctx.setHasSufficientResources(() -> true);
            testTime.advanceMillis(40);
            wfr.notifyNewResourcesAvailable();

            // sanity check: no state transition has been triggered so far
            assertThat(ctx.hasStateTransition(), is(false));
            assertThat(testTime.getTestDuration(), greaterThan(stabilizationTimeout));

            ctx.setExpectCreatingExecutionGraph();

            testTime.advanceMillis(1);
            assertThat(ctx.hasStateTransition(), is(false));

            testTime.advanceMillis(stabilizationTimeout.toMillis());
            assertThat(ctx.hasStateTransition(), is(true));
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

            ctx.runScheduledTasks();
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

            ctx.setExpectCreatingExecutionGraph();

            ctx.runScheduledTasks();
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

        void runScheduledTasks(long untilDelay) {
            Iterable<ScheduledTask<Void>> copyOfScheduledTasks = new ArrayList<>(scheduledTasks);
            Iterator<ScheduledTask<Void>> scheduledTaskIterator = copyOfScheduledTasks.iterator();
            while (scheduledTaskIterator.hasNext()) {
                ScheduledTask<Void> scheduledTask = scheduledTaskIterator.next();
                if (scheduledTask.getDelay(TimeUnit.MILLISECONDS) <= untilDelay) {
                    scheduledTask.execute();
                    if (!scheduledTask.isPeriodic()) {
                        scheduledTaskIterator.remove();
                    }
                }
            }
        }

        void runScheduledTasks() {
            runScheduledTasks(Long.MAX_VALUE);
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

    private static final class ManualTestTime {
        private final ManualClock testingClock = new ManualClock();
        private final Consumer<Duration> runOnAdvance;

        private Duration durationSinceTestStart = Duration.ZERO;

        private ManualTestTime(Consumer<Duration> runOnAdvance) {
            this.runOnAdvance = runOnAdvance;
        }

        private Clock getClock() {
            return testingClock;
        }

        public void advanceMillis(long millis) {
            durationSinceTestStart = durationSinceTestStart.plusMillis(millis);
            testingClock.advanceTime(millis, TimeUnit.MILLISECONDS);
            runOnAdvance.accept(durationSinceTestStart);
        }

        public Duration getTestDuration() {
            return durationSinceTestStart;
        }
    }

    static <T> Consumer<T> assertNonNull() {
        return (item) -> assertThat(item, notNullValue());
    }
}
