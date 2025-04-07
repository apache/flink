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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for the WaitingForResources state. */
class WaitingForResourcesTest {

    private static final Logger LOG = LoggerFactory.getLogger(WaitingForResourcesTest.class);

    private static final Duration DISABLED_RESOURCE_WAIT_TIMEOUT = Duration.ofSeconds(-1);

    @RegisterExtension private MockContext ctx = new MockContext();

    /** WaitingForResources is transitioning to Executing if there are enough resources. */
    @Test
    void testTransitionToCreatingExecutionGraph() {
        final AtomicBoolean onTriggerCalled = new AtomicBoolean();
        final Function<StateTransitionManager.Context, StateTransitionManager>
                stateTransitionManagerFactory =
                        context ->
                                new TestingStateTransitionManager(
                                        () -> {
                                            assertThat(context.hasDesiredResources()).isTrue();
                                            assertThat(context.hasSufficientResources()).isTrue();

                                            context.transitionToSubsequentState();
                                        },
                                        () -> onTriggerCalled.set(true));

        ctx.setHasDesiredResources(() -> true);
        ctx.setHasSufficientResources(() -> true);
        ctx.setExpectCreatingExecutionGraph();

        new WaitingForResources(
                ctx, LOG, DISABLED_RESOURCE_WAIT_TIMEOUT, stateTransitionManagerFactory);

        ctx.runScheduledTasks();

        assertThat(onTriggerCalled.get()).isTrue();
    }

    @Test
    void testNotEnoughResources() {
        final AtomicBoolean onChangeCalled = new AtomicBoolean();
        final AtomicBoolean onTriggerCalled = new AtomicBoolean();
        final Function<StateTransitionManager.Context, StateTransitionManager>
                stateTransitionManagerFactory =
                        context ->
                                new TestingStateTransitionManager(
                                        () -> {
                                            onChangeCalled.set(true);

                                            assertThat(context.hasDesiredResources()).isFalse();
                                            assertThat(context.hasSufficientResources()).isFalse();
                                        },
                                        () -> onTriggerCalled.set(true));

        ctx.setHasDesiredResources(() -> false);
        ctx.setHasSufficientResources(() -> false);
        WaitingForResources wfr =
                new WaitingForResources(
                        ctx, LOG, DISABLED_RESOURCE_WAIT_TIMEOUT, stateTransitionManagerFactory);

        ctx.runScheduledTasks();
        // we expect no state transition.
        assertThat(ctx.hasStateTransition()).isFalse();
        assertThat(onChangeCalled.get()).isTrue();
        assertThat(onTriggerCalled.get()).isTrue();
    }

    @Test
    void testNotifyNewResourcesAvailable() {
        final AtomicInteger callsCounter = new AtomicInteger();
        final Function<StateTransitionManager.Context, StateTransitionManager>
                stateTransitionManagerFactory =
                        context ->
                                TestingStateTransitionManager.withOnChangeEventOnly(
                                        () -> {
                                            if (callsCounter.incrementAndGet() == 0) {
                                                // initially, not enough resources
                                                assertThat(context.hasDesiredResources()).isFalse();
                                                assertThat(context.hasSufficientResources())
                                                        .isFalse();
                                            }

                                            if (context.hasDesiredResources()
                                                    && context.hasSufficientResources()) {
                                                context.transitionToSubsequentState();
                                            }
                                        });

        // initially, not enough resources
        ctx.setHasDesiredResources(() -> false);
        ctx.setHasSufficientResources(() -> false);

        WaitingForResources wfr =
                new WaitingForResources(
                        ctx, LOG, DISABLED_RESOURCE_WAIT_TIMEOUT, stateTransitionManagerFactory);
        ctx.runScheduledTasks();

        // make resources available
        ctx.setHasDesiredResources(() -> true);
        ctx.setHasSufficientResources(() -> true);
        ctx.setExpectCreatingExecutionGraph();
        wfr.onNewResourcesAvailable(); // .. and notify
    }

    @Test
    void testSchedulingWithSufficientResources() {
        final Function<StateTransitionManager.Context, StateTransitionManager>
                stateTransitionManagerFactory =
                        context ->
                                TestingStateTransitionManager.withOnChangeEventOnly(
                                        () -> {
                                            assertThat(context.hasDesiredResources()).isFalse();
                                            if (context.hasSufficientResources()) {
                                                context.transitionToSubsequentState();
                                            }
                                        });
        WaitingForResources wfr =
                new WaitingForResources(
                        ctx, LOG, DISABLED_RESOURCE_WAIT_TIMEOUT, stateTransitionManagerFactory);
        ctx.runScheduledTasks();
        // we expect no state transition.
        assertThat(ctx.hasStateTransition()).isFalse();

        ctx.setHasDesiredResources(() -> false);
        ctx.setHasSufficientResources(() -> true);
        ctx.setExpectCreatingExecutionGraph();
        wfr.onNewResourcesAvailable();
    }

    @Test
    void testNoStateTransitionOnNoResourceTimeout() {
        ctx.setHasDesiredResources(() -> false);
        ctx.setHasSufficientResources(() -> false);

        WaitingForResources wfr =
                new WaitingForResources(
                        ctx,
                        LOG,
                        DISABLED_RESOURCE_WAIT_TIMEOUT,
                        context -> TestingStateTransitionManager.withNoOp());
        ctx.runScheduledTasks();

        assertThat(ctx.hasStateTransition()).isFalse();
    }

    @Test
    void testStateTransitionOnResourceTimeout() {
        WaitingForResources wfr =
                new WaitingForResources(
                        ctx,
                        LOG,
                        Duration.ZERO,
                        context -> TestingStateTransitionManager.withNoOp());
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
                new AdaptiveSchedulerTest.DummyState(ctx),
                runLastBecauseOfHighDelay,
                Duration.ofMillis(999));
        ctx.runIfState(
                new AdaptiveSchedulerTest.DummyState(ctx),
                runFirstBecauseOfLowDelay,
                Duration.ZERO);
        ctx.runIfState(
                new AdaptiveSchedulerTest.DummyState(ctx),
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

        ctx.runIfState(new AdaptiveSchedulerTest.DummyState(ctx), executeOnce, Duration.ZERO);

        // execute at least twice
        ctx.runScheduledTasks();
        ctx.runScheduledTasks();
        assertThat(executed).isTrue();
    }

    @Test
    void testInternalRunScheduledTasks_upperBoundRespected() {
        Runnable executeNever = () -> fail("Not expected");

        ctx.runIfState(
                new AdaptiveSchedulerTest.DummyState(ctx), executeNever, Duration.ofMillis(10));

        ctx.runScheduledTasks(4);
    }

    @Test
    void testInternalRunScheduledTasks_scheduleTaskFromRunnable() {
        final State state = new AdaptiveSchedulerTest.DummyState(ctx);

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

        private final ManualClock testingClock = new ManualClock();

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
                    testingClock.absoluteTimeMillis() + delay.toMillis());
            final ScheduledTask<Void> scheduledTask =
                    new ScheduledTask<>(
                            () -> {
                                if (!hasStateTransition()) {
                                    action.run();
                                }

                                return null;
                            },
                            testingClock.absoluteTimeMillis() + delay.toMillis());

            scheduledTasks.add(scheduledTask);

            return scheduledTask;
        }

        @Override
        public void goToCreatingExecutionGraph(@Nullable ExecutionGraph previousExecutionGraph) {
            creatingExecutionGraphStateValidator.validateInput(null);
            registerStateTransition();
        }
    }

    static <T> Consumer<T> assertNonNull() {
        return (item) -> assertThat(item).isNotNull();
    }
}
