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

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.testutils.ScheduledTask;
import org.apache.flink.runtime.scheduler.adaptive.DefaultStateTransitionManager.Idling;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.apache.flink.runtime.scheduler.adaptive.DefaultStateTransitionManager.Cooldown;
import static org.apache.flink.runtime.scheduler.adaptive.DefaultStateTransitionManager.Phase;
import static org.apache.flink.runtime.scheduler.adaptive.DefaultStateTransitionManager.Stabilized;
import static org.apache.flink.runtime.scheduler.adaptive.DefaultStateTransitionManager.Stabilizing;
import static org.apache.flink.runtime.scheduler.adaptive.DefaultStateTransitionManager.Transitioning;
import static org.assertj.core.api.Assertions.assertThat;

class DefaultStateTransitionManagerTest {

    @Test
    void testTriggerWithoutChangeEventNoopInCooldownPhase() {
        final TestingStateTransitionManagerContext ctx =
                TestingStateTransitionManagerContext.stableContext().withDesiredResources();
        final DefaultStateTransitionManager testInstance = ctx.createTestInstanceInCooldownPhase();
        triggerWithoutPhaseMove(ctx, testInstance, Cooldown.class);
    }

    @Test
    void testTriggerWithoutChangeEventNoopInIdlingPhase() {
        final TestingStateTransitionManagerContext ctx =
                TestingStateTransitionManagerContext.stableContext().withDesiredResources();
        final DefaultStateTransitionManager testInstance =
                ctx.createTestInstanceThatPassedCooldownPhase();
        triggerWithoutPhaseMove(ctx, testInstance, Idling.class);
    }

    @Test
    void testTriggerWithoutChangeEventNoopInTransitioningPhase() {
        final TestingStateTransitionManagerContext ctx =
                TestingStateTransitionManagerContext.stableContext().withDesiredResources();
        final DefaultStateTransitionManager testInstance =
                ctx.createTestInstanceInTransitioningPhase();
        triggerWithoutPhaseMove(ctx, testInstance, Transitioning.class);
    }

    @Test
    void testStateTransitionRightAfterCooldown() {
        final TestingStateTransitionManagerContext ctx =
                TestingStateTransitionManagerContext.stableContext().withDesiredResources();
        final DefaultStateTransitionManager testInstance = ctx.createTestInstanceInCooldownPhase();

        changeWithoutPhaseMove(ctx, testInstance, Cooldown.class);

        triggerWithoutPhaseMove(ctx, testInstance, Cooldown.class);

        ctx.transitionToInclusiveCooldownEnd();

        assertPhaseWithoutStateTransition(ctx, testInstance, Cooldown.class);

        testInstance.onTrigger();

        ctx.passTime(Duration.ofMillis(1));

        assertPhaseWithoutStateTransition(ctx, testInstance, Stabilizing.class);

        testInstance.onTrigger();

        assertFinalStateTransitionHappened(ctx, testInstance);
    }

    @Test
    void testDesiredChangeInCooldownPhase() {
        final TestingStateTransitionManagerContext ctx =
                TestingStateTransitionManagerContext.stableContext().withDesiredResources();
        final DefaultStateTransitionManager testInstance = ctx.createTestInstanceInCooldownPhase();

        changeWithoutPhaseMove(ctx, testInstance, Cooldown.class);

        triggerWithoutPhaseMove(ctx, testInstance, Cooldown.class);

        ctx.transitionOutOfCooldownPhase();

        assertPhaseWithoutStateTransition(ctx, testInstance, Stabilizing.class);

        testInstance.onTrigger();

        assertFinalStateTransitionHappened(ctx, testInstance);
    }

    @Test
    void testDesiredChangeInIdlingPhase() {
        final TestingStateTransitionManagerContext ctx =
                TestingStateTransitionManagerContext.stableContext().withDesiredResources();
        final DefaultStateTransitionManager testInstance =
                ctx.createTestInstanceThatPassedCooldownPhase();

        assertPhaseWithoutStateTransition(ctx, testInstance, Idling.class);

        testInstance.onChange();

        assertPhaseWithoutStateTransition(ctx, testInstance, Stabilizing.class);

        testInstance.onTrigger();

        assertFinalStateTransitionHappened(ctx, testInstance);
    }

    @Test
    void testDesiredChangeInStabilizedPhase() {
        final TestingStateTransitionManagerContext ctx =
                TestingStateTransitionManagerContext.stableContext().withSufficientResources();
        final DefaultStateTransitionManager testInstance =
                ctx.createTestInstanceInStabilizedPhase();

        assertPhaseWithoutStateTransition(ctx, testInstance, Stabilized.class);

        withDesiredChange(ctx, testInstance);

        assertPhaseWithoutStateTransition(ctx, testInstance, Stabilized.class);

        testInstance.onTrigger();

        assertFinalStateTransitionHappened(ctx, testInstance);
    }

    @Test
    void testDesiredResourcesInStabilizingPhaseAfterMaxTriggerDelay() {
        final TestingStateTransitionManagerContext ctx =
                TestingStateTransitionManagerContext.stableContext()
                        .withSufficientResources()
                        .withMaxTriggerDelay(Duration.ofSeconds(10));
        final DefaultStateTransitionManager testInstance =
                ctx.createTestInstance(
                        manager -> {
                            manager.onChange();
                            ctx.transitionOutOfCooldownPhase();
                        });

        assertPhaseWithoutStateTransition(ctx, testInstance, Stabilizing.class);

        ctx.passMaxDelayTriggerTimeout();

        withDesiredChange(ctx, testInstance);

        assertPhaseWithoutStateTransition(ctx, testInstance, Stabilizing.class);

        ctx.passMaxDelayTriggerTimeout();

        assertFinalStateTransitionHappened(ctx, testInstance);
    }

    @Test
    void testNoResourcesChangeInCooldownPhase() {
        final TestingStateTransitionManagerContext ctx =
                TestingStateTransitionManagerContext.stableContext();
        final DefaultStateTransitionManager testInstance = ctx.createTestInstanceInCooldownPhase();

        changeWithoutPhaseMove(ctx, testInstance, Cooldown.class);

        triggerWithoutPhaseMove(ctx, testInstance, Cooldown.class);

        ctx.transitionOutOfCooldownPhase();

        assertPhaseWithoutStateTransition(ctx, testInstance, Idling.class);
    }

    @Test
    void testNoResourcesChangeInIdlingPhase() {
        final TestingStateTransitionManagerContext ctx =
                TestingStateTransitionManagerContext.stableContext();
        final DefaultStateTransitionManager testInstance =
                ctx.createTestInstanceThatPassedCooldownPhase();

        changeWithoutPhaseMove(ctx, testInstance, Idling.class);

        triggerWithoutPhaseMove(ctx, testInstance, Idling.class);
    }

    @Test
    void testSufficientResourcesInStabilizingPhaseAfterMaxTriggerDelay() {
        final TestingStateTransitionManagerContext ctx =
                TestingStateTransitionManagerContext.stableContext()
                        .withSufficientResources()
                        .withMaxTriggerDelay(Duration.ofSeconds(10));
        final DefaultStateTransitionManager testInstance =
                ctx.createTestInstance(
                        manager -> {
                            manager.onChange();
                            ctx.transitionOutOfCooldownPhase();
                        });

        assertPhaseWithoutStateTransition(ctx, testInstance, Stabilizing.class);

        ctx.passMaxDelayTriggerTimeout();

        assertPhaseWithoutStateTransition(ctx, testInstance, Stabilizing.class);
    }

    @Test
    void testSufficientResourcesInStabilizedPhaseAfterMaxTriggerDelay() {
        final TestingStateTransitionManagerContext ctx =
                TestingStateTransitionManagerContext.stableContext().withSufficientResources();
        final DefaultStateTransitionManager testInstance =
                ctx.createTestInstanceInStabilizedPhase();

        assertPhaseWithoutStateTransition(ctx, testInstance, Stabilized.class);

        ctx.passMaxDelayTriggerTimeout();

        assertFinalStateTransitionHappened(ctx, testInstance);
    }

    @Test
    void testSufficientChangeInCooldownPhase() {
        final TestingStateTransitionManagerContext ctx =
                TestingStateTransitionManagerContext.stableContext().withSufficientResources();
        final DefaultStateTransitionManager testInstance = ctx.createTestInstanceInCooldownPhase();

        changeWithoutPhaseMove(ctx, testInstance, Cooldown.class);

        triggerWithoutPhaseMove(ctx, testInstance, Cooldown.class);

        ctx.transitionOutOfCooldownPhase();

        triggerWithoutPhaseMove(ctx, testInstance, Stabilizing.class);

        ctx.passResourceStabilizationTimeout();

        testInstance.onTrigger();

        assertFinalStateTransitionHappened(ctx, testInstance);
    }

    @Test
    void testSufficientChangeInIdlingPhase() {
        final TestingStateTransitionManagerContext ctx =
                TestingStateTransitionManagerContext.stableContext().withSufficientResources();
        final DefaultStateTransitionManager testInstance =
                ctx.createTestInstanceThatPassedCooldownPhase();

        assertPhaseWithoutStateTransition(ctx, testInstance, Idling.class);

        testInstance.onChange();

        triggerWithoutPhaseMove(ctx, testInstance, Stabilizing.class);

        ctx.passResourceStabilizationTimeout();

        testInstance.onTrigger();

        assertFinalStateTransitionHappened(ctx, testInstance);
    }

    @Test
    void testSufficientChangeInStabilizedPhase() {
        final TestingStateTransitionManagerContext ctx =
                TestingStateTransitionManagerContext.stableContext().withSufficientResources();
        final DefaultStateTransitionManager testInstance =
                ctx.createTestInstanceInStabilizedPhase();

        assertPhaseWithoutStateTransition(ctx, testInstance, Stabilized.class);

        testInstance.onTrigger();

        assertFinalStateTransitionHappened(ctx, testInstance);
    }

    @Test
    void testSufficientChangeWithSubsequentDesiredChangeInStabilizingPhase() {
        final TestingStateTransitionManagerContext ctx =
                TestingStateTransitionManagerContext.stableContext().withSufficientResources();
        final DefaultStateTransitionManager testInstance =
                ctx.createTestInstanceThatPassedCooldownPhase();

        assertPhaseWithoutStateTransition(ctx, testInstance, Idling.class);

        testInstance.onChange();

        triggerWithoutPhaseMove(ctx, testInstance, Stabilizing.class);

        withDesiredChange(ctx, testInstance);

        assertPhaseWithoutStateTransition(ctx, testInstance, Stabilizing.class);

        testInstance.onTrigger();

        assertFinalStateTransitionHappened(ctx, testInstance);
    }

    @Test
    void testRevokedChangeInStabilizingPhaseWithSubsequentSufficientChangeInStabilizedPhase() {
        final TestingStateTransitionManagerContext ctx =
                TestingStateTransitionManagerContext.stableContext().withSufficientResources();
        final DefaultStateTransitionManager testInstance =
                ctx.createTestInstanceThatPassedCooldownPhase();

        assertPhaseWithoutStateTransition(ctx, testInstance, Idling.class);

        testInstance.onChange();

        triggerWithoutPhaseMove(ctx, testInstance, Stabilizing.class);

        ctx.withRevokeResources();

        changeWithoutPhaseMove(ctx, testInstance, Stabilizing.class);

        triggerWithoutPhaseMove(ctx, testInstance, Stabilizing.class);

        ctx.passResourceStabilizationTimeout();

        assertPhaseWithoutStateTransition(ctx, testInstance, Stabilized.class);

        withSufficientChange(ctx, testInstance);

        assertPhaseWithoutStateTransition(ctx, testInstance, Stabilized.class);

        testInstance.onTrigger();

        assertFinalStateTransitionHappened(ctx, testInstance);
    }

    @Test
    void testRevokedChangeInStabilizedPhase() {
        final TestingStateTransitionManagerContext ctx =
                TestingStateTransitionManagerContext.stableContext().withSufficientResources();
        final DefaultStateTransitionManager testInstance =
                ctx.createTestInstanceInStabilizedPhase();

        assertPhaseWithoutStateTransition(ctx, testInstance, Stabilized.class);

        ctx.withRevokeResources();

        testInstance.onTrigger();

        assertPhaseWithoutStateTransition(ctx, testInstance, Idling.class);
    }

    @Test
    void testScheduledTaskBeingIgnoredAfterStateChanged() {
        final TestingStateTransitionManagerContext ctx =
                TestingStateTransitionManagerContext.stableContext();
        final DefaultStateTransitionManager testInstance =
                ctx.createTestInstanceInStabilizedPhase();

        final AtomicBoolean callbackCalled = new AtomicBoolean();
        testInstance.scheduleFromNow(
                () -> callbackCalled.set(true), Duration.ZERO, new TestPhase());
        ctx.triggerOutdatedTasks();

        assertThat(callbackCalled).isFalse();
    }

    private static class TestPhase extends Phase {
        private TestPhase() {
            super(Instant::now, null);
        }
    }

    private static void assertPhaseWithoutStateTransition(
            TestingStateTransitionManagerContext ctx,
            DefaultStateTransitionManager testInstance,
            Class<? extends Phase> expectedPhase) {
        assertThat(ctx.stateTransitionWasTriggered()).isFalse();
        assertThat(testInstance.getPhase()).isInstanceOf(expectedPhase);
    }

    private static void assertFinalStateTransitionHappened(
            TestingStateTransitionManagerContext ctx, DefaultStateTransitionManager testInstance) {
        assertThat(ctx.stateTransitionWasTriggered()).isTrue();
        assertThat(testInstance.getPhase()).isInstanceOf(Transitioning.class);
    }

    private static void changeWithoutPhaseMove(
            TestingStateTransitionManagerContext ctx,
            DefaultStateTransitionManager testInstance,
            Class<? extends Phase> expectedPhase) {
        assertPhaseWithoutStateTransition(ctx, testInstance, expectedPhase);

        testInstance.onChange();

        assertPhaseWithoutStateTransition(ctx, testInstance, expectedPhase);
    }

    private static void triggerWithoutPhaseMove(
            TestingStateTransitionManagerContext ctx,
            DefaultStateTransitionManager testInstance,
            Class<? extends Phase> expectedPhase) {

        assertPhaseWithoutStateTransition(ctx, testInstance, expectedPhase);

        testInstance.onTrigger();

        assertPhaseWithoutStateTransition(ctx, testInstance, expectedPhase);
    }

    private static void withSufficientChange(
            TestingStateTransitionManagerContext ctx, DefaultStateTransitionManager testInstance) {

        ctx.withSufficientResources();
        testInstance.onChange();
    }

    private static void withDesiredChange(
            TestingStateTransitionManagerContext ctx, DefaultStateTransitionManager testInstance) {

        ctx.withDesiredResources();
        testInstance.onChange();
    }

    /**
     * {@code TestingStateTransitionManagerContext} provides methods for adjusting the elapsed time
     * and for adjusting the available resources for rescaling.
     */
    private static class TestingStateTransitionManagerContext
            implements StateTransitionManager.Context {

        // default configuration values to allow for easy transitioning between the phases
        private static final Duration COOLDOWN_TIMEOUT = Duration.ofHours(1);
        private static final Duration RESOURCE_STABILIZATION_TIMEOUT = Duration.ofHours(2);
        private static final Duration MAX_TRIGGER_DELAY =
                RESOURCE_STABILIZATION_TIMEOUT.plus(Duration.ofMinutes(10));

        private final Duration cooldownTimeout = COOLDOWN_TIMEOUT;
        private final Duration resourceStabilizationTimeout = RESOURCE_STABILIZATION_TIMEOUT;
        private Duration maxTriggerDelay = MAX_TRIGGER_DELAY;

        // configuration that defines what kind of rescaling would be possible
        private boolean hasSufficientResources = false;
        private boolean hasDesiredResources = false;

        // internal state used for assertions
        private final AtomicBoolean transitionTriggered = new AtomicBoolean();
        private final SortedMap<Instant, List<ScheduledTask<Object>>> scheduledTasks =
                new TreeMap<>();

        // Instant.MIN makes debugging easier because timestamps become human-readable
        private final Instant initializationTime = Instant.MIN;
        private Duration elapsedTime = Duration.ZERO;
        private final JobID jobId = new JobID();

        // ///////////////////////////////////////////////
        // Context creation
        // ///////////////////////////////////////////////

        public static TestingStateTransitionManagerContext stableContext() {
            return new TestingStateTransitionManagerContext();
        }

        private TestingStateTransitionManagerContext() {
            // no rescaling is enabled by default
            withRevokeResources();
        }

        public TestingStateTransitionManagerContext withMaxTriggerDelay(Duration maxTriggerDelay) {
            this.maxTriggerDelay = maxTriggerDelay;
            return this;
        }

        public TestingStateTransitionManagerContext withRevokeResources() {
            this.hasSufficientResources = false;
            this.hasDesiredResources = false;

            return this;
        }

        public TestingStateTransitionManagerContext withDesiredResources() {
            // having desired resources should also mean that the sufficient resources are met
            this.hasSufficientResources = true;
            this.hasDesiredResources = true;

            return this;
        }

        public TestingStateTransitionManagerContext withSufficientResources() {
            this.hasSufficientResources = true;
            this.hasDesiredResources = false;

            return this;
        }

        // ///////////////////////////////////////////////
        // StateTransitionManager.Context interface methods
        // ///////////////////////////////////////////////

        @Override
        public boolean hasSufficientResources() {
            return this.hasSufficientResources;
        }

        @Override
        public boolean hasDesiredResources() {
            return this.hasDesiredResources;
        }

        @Override
        public void transitionToSubsequentState() {
            transitionTriggered.set(true);
        }

        @Override
        public ScheduledFuture<?> scheduleOperation(Runnable callback, Duration delay) {
            final Instant triggerTime =
                    Objects.requireNonNull(initializationTime).plus(elapsedTime).plus(delay);
            if (!scheduledTasks.containsKey(triggerTime)) {
                scheduledTasks.put(triggerTime, new ArrayList<>());
            }
            ScheduledTask<Object> scheduledTask =
                    new ScheduledTask<>(Executors.callable(callback), delay.toMillis());
            scheduledTasks.get(triggerTime).add(scheduledTask);
            return scheduledTask;
        }

        @Override
        public JobID getJobId() {
            return jobId;
        }

        // ///////////////////////////////////////////////
        // Test instance creation
        // ///////////////////////////////////////////////

        /**
         * Creates the {@code DefaultStateTransitionManager} test instance and advances into a
         * period in time where the instance is in cooldown phase.
         */
        public DefaultStateTransitionManager createTestInstanceInCooldownPhase() {
            return createTestInstance(ignored -> this.transitionIntoCooldownTimeframe());
        }

        /**
         * Creates the {@code DefaultStateTransitionManager} test instance and advances into a
         * period in time where the instance is in stabilizing phase.
         */
        public DefaultStateTransitionManager createTestInstanceThatPassedCooldownPhase() {
            return createTestInstance(ignored -> this.transitionOutOfCooldownPhase());
        }

        /**
         * Creates the {@code DefaultStateTransitionManager} test instance and advances into a
         * period in time where the instance is in stabilized phase.
         */
        public DefaultStateTransitionManager createTestInstanceInStabilizedPhase() {
            return createTestInstance(
                    manager -> {
                        manager.onChange();
                        passResourceStabilizationTimeout();
                    });
        }

        /**
         * Creates the {@code DefaultStateTransitionManager} test instance in terminal transitioning
         * phase.
         */
        public DefaultStateTransitionManager createTestInstanceInTransitioningPhase() {
            return createTestInstance(
                    manager -> {
                        manager.onChange();
                        passResourceStabilizationTimeout();
                        manager.onTrigger();
                        clearStateTransition();
                    });
        }

        /**
         * Initializes the test instance with a given max trigger delay timeout and sets the
         * context's elapsed time based on the passed callback.
         */
        public DefaultStateTransitionManager createTestInstance(
                Consumer<DefaultStateTransitionManager> callback) {
            final DefaultStateTransitionManager testInstance =
                    new DefaultStateTransitionManager(
                            this,
                            // clock that returns the time based on the configured elapsedTime
                            () -> Objects.requireNonNull(initializationTime).plus(elapsedTime),
                            cooldownTimeout,
                            resourceStabilizationTimeout,
                            maxTriggerDelay) {
                        @Override
                        public void onChange() {
                            super.onChange();

                            // hack to avoid calling this method in every test method
                            // we want to trigger tasks that are meant to run right-away
                            TestingStateTransitionManagerContext.this.triggerOutdatedTasks();
                        }

                        @Override
                        public void onTrigger() {
                            super.onTrigger();

                            // hack to avoid calling this method in every test method
                            // we want to trigger tasks that are meant to run right-away
                            TestingStateTransitionManagerContext.this.triggerOutdatedTasks();
                        }
                    };

            callback.accept(testInstance);
            return testInstance;
        }

        // ///////////////////////////////////////////////
        // Time-adjustment functionality
        // ///////////////////////////////////////////////

        /**
         * Transitions the context's time to a moment that falls into the test instance's cooldown
         * phase.
         */
        public void transitionIntoCooldownTimeframe() {
            setElapsedTime(cooldownTimeout.dividedBy(2));
            this.triggerOutdatedTasks();
        }

        public void transitionOutOfCooldownPhase() {
            this.setElapsedTime(cooldownTimeout.plusMillis(1));
        }

        public void passResourceStabilizationTimeout() {
            // resource stabilization is based on the current time
            this.passTime(resourceStabilizationTimeout.plusMillis(1));
        }

        public void transitionToInclusiveCooldownEnd() {
            setElapsedTime(cooldownTimeout.minusMillis(1));
        }

        public void passMaxDelayTriggerTimeout() {
            this.passTime(maxTriggerDelay.plusMillis(1));
        }

        public void passTime(Duration elapsed) {
            setElapsedTime(this.elapsedTime.plus(elapsed));
        }

        public void setElapsedTime(Duration elapsedTime) {
            Preconditions.checkState(
                    this.elapsedTime.compareTo(elapsedTime) <= 0,
                    "The elapsed time should monotonically increase.");
            this.elapsedTime = elapsedTime;
            this.triggerOutdatedTasks();
        }

        private void triggerOutdatedTasks() {
            while (!scheduledTasks.isEmpty()) {
                final Instant timeOfExecution = scheduledTasks.firstKey();
                if (!timeOfExecution.isAfter(
                        Objects.requireNonNull(initializationTime).plus(elapsedTime))) {
                    scheduledTasks.remove(timeOfExecution).forEach(ScheduledTask::execute);
                } else {
                    break;
                }
            }
        }

        // ///////////////////////////////////////////////
        // Methods for verifying the context's state
        // ///////////////////////////////////////////////

        public boolean stateTransitionWasTriggered() {
            return transitionTriggered.get();
        }

        public void clearStateTransition() {
            transitionTriggered.set(false);
        }
    }
}
