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

import org.apache.flink.core.testutils.ScheduledTask;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.ManualClock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for the WaitingForResources state. */
class WaitingForResourcesTest {

    private static final Logger LOG = LoggerFactory.getLogger(WaitingForResourcesTest.class);

    private static final Duration STABILIZATION_TIMEOUT = Duration.ofSeconds(1);

    @RegisterExtension MockContext ctx = new MockContext();

    /** WaitingForResources is transitioning to Executing if there are enough resources. */
    @Test
    void testTransitionToCreatingExecutionGraph() {
        ctx.setHasDesiredResources(() -> true);

        ctx.setExpectCreatingExecutionGraph();

        new WaitingForResources(ctx, LOG, Duration.ZERO, STABILIZATION_TIMEOUT);

        ctx.runScheduledTasks();
    }

    @Test
    void testNotEnoughResources() {
        ctx.setHasDesiredResources(() -> false);
        WaitingForResources wfr =
                new WaitingForResources(ctx, LOG, Duration.ZERO, STABILIZATION_TIMEOUT);

        // we expect no state transition.
        wfr.onNewResourcesAvailable();
    }

    @Test
    void testNotifyNewResourcesAvailable() {
        ctx.setHasDesiredResources(() -> false); // initially, not enough resources
        WaitingForResources wfr =
                new WaitingForResources(ctx, LOG, Duration.ZERO, STABILIZATION_TIMEOUT);
        ctx.setHasDesiredResources(() -> true); // make resources available
        ctx.setExpectCreatingExecutionGraph();
        wfr.onNewResourcesAvailable(); // .. and notify
    }

    @Test
    void testSchedulingWithSufficientResourcesAndNoStabilizationTimeout() {
        Duration noStabilizationTimeout = Duration.ofMillis(0);
        WaitingForResources wfr =
                new WaitingForResources(ctx, LOG, Duration.ofSeconds(1000), noStabilizationTimeout);

        ctx.setHasDesiredResources(() -> false);
        ctx.setHasSufficientResources(() -> true);
        ctx.setExpectCreatingExecutionGraph();
        wfr.onNewResourcesAvailable();
    }

    @Test
    void testNoSchedulingIfStabilizationTimeoutIsConfigured() {
        Duration stabilizationTimeout = Duration.ofMillis(50000);

        WaitingForResources wfr =
                new WaitingForResources(ctx, LOG, Duration.ofSeconds(1000), stabilizationTimeout);

        ctx.setHasDesiredResources(() -> false);
        ctx.setHasSufficientResources(() -> true);
        wfr.onNewResourcesAvailable();
        // we are not triggering the scheduled tasks, to simulate a long stabilization timeout

        assertThat(ctx.hasStateTransition()).isFalse();
    }

    @Test
    void testSchedulingWithSufficientResourcesAfterStabilizationTimeout() {
        Duration initialResourceTimeout = Duration.ofMillis(-1);
        Duration stabilizationTimeout = Duration.ofMillis(50_000L);

        WaitingForResources wfr =
                new WaitingForResources(
                        ctx,
                        LOG,
                        initialResourceTimeout,
                        stabilizationTimeout,
                        ctx.getClock(),
                        null);
        // sufficient resources available
        ctx.setHasDesiredResources(() -> false);
        ctx.setHasSufficientResources(() -> true);

        // notify about sufficient resources
        wfr.onNewResourcesAvailable();

        ctx.setExpectCreatingExecutionGraph();

        // execute all runnables and trigger expected state transition
        final Duration afterStabilizationTimeout = stabilizationTimeout.plusMillis(1);
        ctx.advanceTimeByMillis(afterStabilizationTimeout.toMillis());

        ctx.runScheduledTasks(afterStabilizationTimeout.toMillis());

        assertThat(ctx.hasStateTransition()).isTrue();
    }

    @Test
    void testStabilizationTimeoutReset() {
        Duration initialResourceTimeout = Duration.ofMillis(-1);
        Duration stabilizationTimeout = Duration.ofMillis(50L);

        WaitingForResources wfr =
                new WaitingForResources(
                        ctx,
                        LOG,
                        initialResourceTimeout,
                        stabilizationTimeout,
                        ctx.getClock(),
                        null);

        ctx.setHasDesiredResources(() -> false);

        // notify about resources, trigger stabilization timeout
        ctx.setHasSufficientResources(() -> true);
        ctx.advanceTimeByMillis(40); // advance time, but don't trigger stabilizationTimeout
        wfr.onNewResourcesAvailable();

        // notify again, but insufficient (reset stabilization timeout)
        ctx.setHasSufficientResources(() -> false);
        ctx.advanceTimeByMillis(40);
        wfr.onNewResourcesAvailable();

        // notify again, but sufficient, trigger timeout
        ctx.setHasSufficientResources(() -> true);
        ctx.advanceTimeByMillis(40);
        wfr.onNewResourcesAvailable();

        // sanity check: no state transition has been triggered so far
        assertThat(ctx.hasStateTransition()).isFalse();
        assertThat(ctx.getTestDuration()).isGreaterThan(stabilizationTimeout);

        ctx.setExpectCreatingExecutionGraph();

        ctx.advanceTimeByMillis(1);
        assertThat(ctx.hasStateTransition()).isFalse();

        ctx.advanceTimeByMillis(stabilizationTimeout.toMillis());
        assertThat(ctx.hasStateTransition()).isTrue();
    }

    @Test
    void testNoStateTransitionOnNoResourceTimeout() {
        ctx.setHasDesiredResources(() -> false);
        WaitingForResources wfr =
                new WaitingForResources(ctx, LOG, Duration.ofMillis(-1), STABILIZATION_TIMEOUT);

        ctx.runScheduledTasks();
        assertThat(ctx.hasStateTransition()).isFalse();
    }

    @Test
    void testStateTransitionOnResourceTimeout() {
        ctx.setHasDesiredResources(() -> false);
        WaitingForResources wfr =
                new WaitingForResources(ctx, LOG, Duration.ZERO, STABILIZATION_TIMEOUT);

        ctx.setExpectCreatingExecutionGraph();

        ctx.runScheduledTasks();
    }

    @Test
    void testInternalRunScheduledTasks_correctExecutionOrder() {
        AtomicBoolean firstRun = new AtomicBoolean(false);
        AtomicBoolean secondRun = new AtomicBoolean(false);
        AtomicBoolean thirdRun = new AtomicBoolean(false);

        Runnable runFirstBecauseOfLowDelay = () -> firstRun.set(true);
        Runnable runSecondBecauseOfScheduleOrder =
                () -> {
                    assertThat(firstRun).as("order violated").isTrue();
                    secondRun.set(true);
                };
        Runnable runLastBecauseOfHighDelay =
                () -> {
                    assertThat(secondRun).as("order violated").isTrue();
                    thirdRun.set(true);
                };

        ctx.runIfState(
                new AdaptiveSchedulerTest.DummyState(),
                runLastBecauseOfHighDelay,
                Duration.ofMillis(999));
        ctx.runIfState(
                new AdaptiveSchedulerTest.DummyState(), runFirstBecauseOfLowDelay, Duration.ZERO);
        ctx.runIfState(
                new AdaptiveSchedulerTest.DummyState(),
                runSecondBecauseOfScheduleOrder,
                Duration.ZERO);

        ctx.runScheduledTasks();

        assertThat(thirdRun).isTrue();
    }

    @Test
    void testInternalRunScheduledTasks_tasksAreRemovedAfterExecution() {
        AtomicBoolean executed = new AtomicBoolean(false);
        Runnable executeOnce =
                () -> {
                    assertThat(executed).as("Multiple executions").isFalse();
                    executed.set(true);
                };

        ctx.runIfState(new AdaptiveSchedulerTest.DummyState(), executeOnce, Duration.ZERO);

        // execute at least twice
        ctx.runScheduledTasks();
        ctx.runScheduledTasks();
        assertThat(executed).isTrue();
    }

    @Test
    void testInternalRunScheduledTasks_upperBoundRespected() {
        Runnable executeNever = () -> fail("Not expected");

        ctx.runIfState(new AdaptiveSchedulerTest.DummyState(), executeNever, Duration.ofMillis(10));

        ctx.runScheduledTasks(4);
    }

    @Test
    void testInternalRunScheduledTasks_scheduleTaskFromRunnable() {
        final State state = new AdaptiveSchedulerTest.DummyState();

        AtomicBoolean executed = new AtomicBoolean(false);
        ctx.runIfState(
                state,
                () -> {
                    // schedule something
                    ctx.runIfState(state, () -> executed.set(true), Duration.ofMillis(4));
                },
                Duration.ZERO);

        // choose time that includes inner execution as well
        ctx.runScheduledTasks(10);
        assertThat(executed).isTrue();
    }

    private static class MockContext extends MockStateWithoutExecutionGraphContext
            implements WaitingForResources.Context {

        private static final Logger LOG = LoggerFactory.getLogger(MockContext.class);

        private final StateValidator<Void> creatingExecutionGraphStateValidator =
                new StateValidator<>("executing");

        private Supplier<Boolean> hasDesiredResourcesSupplier = () -> false;
        private Supplier<Boolean> hasSufficientResourcesSupplier = () -> false;

        private final Queue<ScheduledTask<Void>> scheduledTasks =
                new PriorityQueue<>(
                        Comparator.comparingLong(o -> o.getDelay(TimeUnit.MILLISECONDS)));

        private final ManualTestTime testTime =
                new ManualTestTime(
                        (durationSinceTestStart) ->
                                runScheduledTasks(durationSinceTestStart.toMillis()));

        public void setHasDesiredResources(Supplier<Boolean> sup) {
            hasDesiredResourcesSupplier = sup;
        }

        public void setHasSufficientResources(Supplier<Boolean> sup) {
            hasSufficientResourcesSupplier = sup;
        }

        void setExpectCreatingExecutionGraph() {
            creatingExecutionGraphStateValidator.expectInput(none -> {});
        }

        void runScheduledTasks(long untilDelay) {
            LOG.info("Running scheduled tasks with a delay between 0 and {}ms:", untilDelay);

            while (scheduledTasks.peek() != null
                    && scheduledTasks.peek().getDelay(TimeUnit.MILLISECONDS) <= untilDelay) {
                ScheduledTask<Void> scheduledTask = scheduledTasks.poll();

                LOG.info(
                        "Running task with delay {}",
                        scheduledTask.getDelay(TimeUnit.MILLISECONDS));
                scheduledTask.execute();
                if (scheduledTask.isPeriodic()) {
                    // remove non-periodic tasks
                    scheduledTasks.add(scheduledTask);
                }
            }
        }

        void runScheduledTasks() {
            runScheduledTasks(Long.MAX_VALUE);
        }

        @Override
        public void afterEach(ExtensionContext extensionContext) throws Exception {
            super.afterEach(extensionContext);
            creatingExecutionGraphStateValidator.close();
        }

        @Override
        public boolean hasDesiredResources() {
            return hasDesiredResourcesSupplier.get();
        }

        @Override
        public boolean hasSufficientResources() {
            return hasSufficientResourcesSupplier.get();
        }

        @Override
        public ScheduledFuture<?> runIfState(State expectedState, Runnable action, Duration delay) {
            LOG.info(
                    "Scheduling work with delay {} for earliest execution at {}",
                    delay.toMillis(),
                    testTime.getClock().absoluteTimeMillis() + delay.toMillis());
            final ScheduledTask<Void> scheduledTask =
                    new ScheduledTask<>(
                            () -> {
                                if (!hasStateTransition()) {
                                    action.run();
                                }

                                return null;
                            },
                            testTime.getClock().absoluteTimeMillis() + delay.toMillis());

            scheduledTasks.add(scheduledTask);

            return scheduledTask;
        }

        @Override
        public void goToCreatingExecutionGraph(@Nullable ExecutionGraph previousExecutionGraph) {
            creatingExecutionGraphStateValidator.validateInput(null);
            registerStateTransition();
        }

        public Clock getClock() {
            return testTime.getClock();
        }

        public void advanceTimeByMillis(long millis) {
            testTime.advanceMillis(millis);
        }

        public Duration getTestDuration() {
            return testTime.getTestDuration();
        }
    }

    private static final class ManualTestTime {
        private static final Logger LOG = LoggerFactory.getLogger(ManualTestTime.class);

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

            LOG.info(
                    "Advance testing time by {} ms to time {} ms",
                    millis,
                    durationSinceTestStart.toMillis());

            testingClock.advanceTime(millis, TimeUnit.MILLISECONDS);
            runOnAdvance.accept(durationSinceTestStart);
        }

        public Duration getTestDuration() {
            return durationSinceTestStart;
        }
    }

    static <T> Consumer<T> assertNonNull() {
        return (item) -> assertThat(item).isNotNull();
    }
}
