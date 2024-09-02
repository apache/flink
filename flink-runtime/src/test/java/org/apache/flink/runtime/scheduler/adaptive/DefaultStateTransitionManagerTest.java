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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.testutils.ScheduledTask;
import org.apache.flink.runtime.scheduler.adaptive.DefaultStateTransitionManager.Idling;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

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
    void testProperConfiguration() throws ConfigurationException {
        final Duration cooldownTimeout = Duration.ofMillis(1337);
        final Duration resourceStabilizationTimeout = Duration.ofMillis(7331);
        final Duration maximumDelayForRescaleTrigger = Duration.ofMillis(4242);

        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.SCHEDULER_SCALING_INTERVAL_MIN, cooldownTimeout);
        configuration.set(
                JobManagerOptions.SCHEDULER_SCALING_INTERVAL_MAX, resourceStabilizationTimeout);
        configuration.set(
                JobManagerOptions.MAXIMUM_DELAY_FOR_SCALE_TRIGGER, maximumDelayForRescaleTrigger);

        final DefaultStateTransitionManager testInstance =
                DefaultStateTransitionManager.Factory.fromSettings(
                                AdaptiveScheduler.Settings.of(configuration))
                        .create(
                                TestingStateTransitionManagerContext.stableContext(),
                                Instant.now());
        assertThat(testInstance.cooldownTimeout).isEqualTo(cooldownTimeout);
        assertThat(testInstance.resourceStabilizationTimeout)
                .isEqualTo(resourceStabilizationTimeout);
        assertThat(testInstance.maxTriggerDelay).isEqualTo(maximumDelayForRescaleTrigger);
    }

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
                TestingStateTransitionManagerContext.stableContext().withSufficientResources();
        final DefaultStateTransitionManager testInstance =
                ctx.createTestInstanceWithoutStabilizationTimeout(
                        manager -> {
                            manager.onChange();
                            ctx.passTime(TestingStateTransitionManagerContext.COOLDOWN_TIMEOUT);
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
                TestingStateTransitionManagerContext.stableContext().withSufficientResources();
        final DefaultStateTransitionManager testInstance =
                ctx.createTestInstanceWithoutStabilizationTimeout(
                        manager -> {
                            manager.onChange();
                            ctx.passTime(TestingStateTransitionManagerContext.COOLDOWN_TIMEOUT);
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
         * Initializes the test instance with set stabilization timeout and sets the context's
         * elapsed time based on the passed callback.
         */
        public DefaultStateTransitionManager createTestInstance(
                Consumer<DefaultStateTransitionManager> callback) {
            return createTestInstance(callback, RESOURCE_STABILIZATION_TIMEOUT);
        }

        /**
         * Initializes the test instance without stabilization timeout and sets the context's
         * elapsed time based on the passed callback.
         */
        public DefaultStateTransitionManager createTestInstanceWithoutStabilizationTimeout(
                Consumer<DefaultStateTransitionManager> callback) {
            return createTestInstance(callback, null);
        }

        private DefaultStateTransitionManager createTestInstance(
                Consumer<DefaultStateTransitionManager> callback,
                @Nullable Duration resourceStabilizationTimeout) {
            final DefaultStateTransitionManager testInstance =
                    new DefaultStateTransitionManager(
                            initializationTime,
                            // clock that returns the time based on the configured elapsedTime
                            () -> Objects.requireNonNull(initializationTime).plus(elapsedTime),
                            this,
                            TestingStateTransitionManagerContext.COOLDOWN_TIMEOUT,
                            resourceStabilizationTimeout,
                            TestingStateTransitionManagerContext.MAX_TRIGGER_DELAY) {
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
            setElapsedTime(COOLDOWN_TIMEOUT.dividedBy(2));
            this.triggerOutdatedTasks();
        }

        public void transitionOutOfCooldownPhase() {
            this.setElapsedTime(COOLDOWN_TIMEOUT.plusMillis(1));
        }

        public void passResourceStabilizationTimeout() {
            // resource stabilization is based on the current time
            this.passTime(RESOURCE_STABILIZATION_TIMEOUT.plusMillis(1));
        }

        public void transitionToInclusiveCooldownEnd() {
            setElapsedTime(COOLDOWN_TIMEOUT.minusMillis(1));
        }

        public void passMaxDelayTriggerTimeout() {
            this.passTime(MAX_TRIGGER_DELAY.plusMillis(1));
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
