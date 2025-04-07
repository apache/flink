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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.time.Duration;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Supplier;

/**
 * {@code DefaultStateTransitionManager} is a state machine which manages the {@link
 * AdaptiveScheduler}'s state transitions based on the previous transition time and the available
 * resources. See {@link Phase} for details on each individual phase of this state machine. Note: We
 * use the term phase here to avoid confusion with the state used in the {@link AdaptiveScheduler}.
 *
 * <pre>
 * {@link Cooldown}
 *   |
 *   +--> {@link Idling}
 *   |      |
 *   |      V
 *   +--> {@link Stabilizing}
 *          |
 *          +--> {@link Stabilized} --> {@link Idling}
 *          |      |
 *          |      V
 *          \--> {@link Transitioning}
 * </pre>
 *
 * <p>Thread-safety: This class is not implemented in a thread-safe manner and relies on the fact
 * that any method call happens within a single thread.
 *
 * @see Executing
 */
@NotThreadSafe
public class DefaultStateTransitionManager implements StateTransitionManager {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultStateTransitionManager.class);

    private final Supplier<Temporal> clock;
    private final StateTransitionManager.Context transitionContext;
    private Phase phase;
    private final List<ScheduledFuture<?>> scheduledFutures;
    private final Duration resourceStabilizationTimeout;
    private final Duration maxTriggerDelay;

    /**
     * Creates a {@code DefaultStateTransitionManager} instance with the given parameters.
     *
     * @param transitionContext The context for the {@code StateTransitionManager}.
     * @param clock A supplier for the current time.
     * @param cooldownTimeout The timeout for the cooldown phase.
     * @param resourceStabilizationTimeout The timeout for the resource stabilization phase.
     * @param maxTriggerDelay The maximum delay for triggering a {@link AdaptiveScheduler}'s state
     *     transition if only sufficient resources are available.
     */
    DefaultStateTransitionManager(
            Context transitionContext,
            Supplier<Temporal> clock,
            Duration cooldownTimeout,
            Duration resourceStabilizationTimeout,
            Duration maxTriggerDelay) {

        this.clock = Preconditions.checkNotNull(clock);
        Preconditions.checkArgument(
                !maxTriggerDelay.isNegative(), "Max trigger delay must not be negative");
        this.maxTriggerDelay = maxTriggerDelay;
        this.resourceStabilizationTimeout =
                Preconditions.checkNotNull(resourceStabilizationTimeout);
        Preconditions.checkArgument(
                !resourceStabilizationTimeout.isNegative(),
                "Resource stabilization timeout must not be negative");
        this.transitionContext = Preconditions.checkNotNull(transitionContext);
        this.scheduledFutures = new ArrayList<>();
        this.phase =
                new Cooldown(
                        Preconditions.checkNotNull(clock.get()),
                        clock,
                        this,
                        Preconditions.checkNotNull(cooldownTimeout));
    }

    @Override
    public void onChange() {
        LOG.debug("OnChange event received in phase {} for job {}.", getPhase(), getJobId());
        phase.onChange();
    }

    @Override
    public void onTrigger() {
        LOG.debug("OnTrigger event received in phase {} for job {}.", getPhase(), getJobId());
        phase.onTrigger();
    }

    @Override
    public void close() {
        scheduledFutures.forEach(future -> future.cancel(true));
        scheduledFutures.clear();
    }

    @VisibleForTesting
    Phase getPhase() {
        return phase;
    }

    private void progressToIdling() {
        progressToPhase(new Idling(clock, this));
    }

    private void progressToStabilizing(Temporal firstChangeEventTimestamp) {
        progressToPhase(
                new Stabilizing(
                        clock,
                        this,
                        resourceStabilizationTimeout,
                        firstChangeEventTimestamp,
                        maxTriggerDelay));
    }

    private void progressToStabilized(Temporal firstChangeEventTimestamp) {
        progressToPhase(new Stabilized(clock, this, firstChangeEventTimestamp, maxTriggerDelay));
    }

    private void triggerTransitionToSubsequentState() {
        progressToPhase(new Transitioning(clock, this));
        transitionContext.transitionToSubsequentState();
    }

    private void progressToPhase(Phase newPhase) {
        Preconditions.checkState(
                !(phase instanceof Transitioning),
                "The state transition operation has already been triggered.");
        LOG.info("Transitioning from {} to {}, job {}.", phase, newPhase, getJobId());
        phase = newPhase;
    }

    @VisibleForTesting
    void scheduleFromNow(Runnable callback, Duration delay, Phase phase) {
        scheduledFutures.add(
                transitionContext.scheduleOperation(() -> runIfPhase(phase, callback), delay));
    }

    private void runIfPhase(Phase expectedPhase, Runnable callback) {
        if (getPhase() == expectedPhase) {
            callback.run();
        } else {
            LOG.debug(
                    "Ignoring scheduled action because expected phase {} is not the actual phase {}, job {}.",
                    expectedPhase,
                    getPhase(),
                    getJobId());
        }
    }

    private JobID getJobId() {
        return transitionContext.getJobId();
    }

    /**
     * A phase in the state machine of the {@link DefaultStateTransitionManager}. Each phase is
     * responsible for a specific part of the state transition process.
     */
    @VisibleForTesting
    abstract static class Phase {

        private final Supplier<Temporal> clock;
        private final DefaultStateTransitionManager context;

        @VisibleForTesting
        Phase(Supplier<Temporal> clock, DefaultStateTransitionManager context) {
            this.clock = clock;
            this.context = context;
        }

        Temporal now() {
            return clock.get();
        }

        DefaultStateTransitionManager context() {
            return context;
        }

        void scheduleRelativelyTo(Runnable callback, Temporal startOfTimeout, Duration timeout) {
            final Duration passedTimeout = Duration.between(startOfTimeout, now());
            Preconditions.checkArgument(
                    !passedTimeout.isNegative(),
                    "The startOfTimeout ({}) should be in the past but is after the current time.",
                    startOfTimeout);

            final Duration timeoutLeft = timeout.minus(passedTimeout);
            scheduleFromNow(callback, timeoutLeft.isNegative() ? Duration.ZERO : timeoutLeft);
        }

        void scheduleFromNow(Runnable callback, Duration delay) {
            context.scheduleFromNow(callback, delay, this);
        }

        boolean hasDesiredResources() {
            return context.transitionContext.hasDesiredResources();
        }

        boolean hasSufficientResources() {
            return context.transitionContext.hasSufficientResources();
        }

        void onChange() {}

        void onTrigger() {}

        @Override
        public String toString() {
            return getClass().getSimpleName();
        }

        JobID getJobId() {
            return context.getJobId();
        }
    }

    /**
     * {@link Phase} to prevent any rescaling. {@link StateTransitionManager#onChange()} events will
     * be monitored and forwarded to the next phase. {@link StateTransitionManager#onTrigger()}
     * events will be ignored.
     */
    @VisibleForTesting
    static final class Cooldown extends Phase {

        @Nullable private Temporal firstChangeEventTimestamp;

        private Cooldown(
                Temporal timeOfLastRescale,
                Supplier<Temporal> clock,
                DefaultStateTransitionManager context,
                Duration cooldownTimeout) {
            super(clock, context);

            this.scheduleRelativelyTo(this::finalizeCooldown, timeOfLastRescale, cooldownTimeout);
        }

        @Override
        void onChange() {
            if (hasSufficientResources() && firstChangeEventTimestamp == null) {
                firstChangeEventTimestamp = now();
            }
        }

        private void finalizeCooldown() {
            if (firstChangeEventTimestamp == null) {
                context().progressToIdling();
            } else {
                context().progressToStabilizing(firstChangeEventTimestamp);
            }
        }
    }

    /**
     * {@link Phase} which follows the {@link Cooldown} phase if no {@link
     * StateTransitionManager#onChange()} was observed, yet. The {@code
     * DefaultStateTransitionManager} waits for a first {@link StateTransitionManager#onChange()}
     * event. {@link StateTransitionManager#onTrigger()} events will be ignored.
     */
    @VisibleForTesting
    static final class Idling extends Phase {

        private Idling(Supplier<Temporal> clock, DefaultStateTransitionManager context) {
            super(clock, context);
        }

        @Override
        void onChange() {
            if (hasSufficientResources()) {
                context().progressToStabilizing(now());
            }
        }
    }

    /**
     * {@link Phase} that handles the resources stabilization. In this phase, {@link
     * StateTransitionManager#onTrigger()} will initiate rescaling if desired resources are met and
     * {@link StateTransitionManager#onChange()} will schedule the evaluation of the desired
     * resources.
     */
    static final class Stabilizing extends Phase {

        private Temporal onChangeEventTimestamp;
        private final Duration maxTriggerDelay;
        private boolean evaluationScheduled = false;

        private Stabilizing(
                Supplier<Temporal> clock,
                DefaultStateTransitionManager context,
                Duration resourceStabilizationTimeout,
                Temporal firstOnChangeEventTimestamp,
                Duration maxTriggerDelay) {
            super(clock, context);
            this.onChangeEventTimestamp = firstOnChangeEventTimestamp;
            this.maxTriggerDelay = maxTriggerDelay;

            scheduleRelativelyTo(
                    () -> context().progressToStabilized(firstOnChangeEventTimestamp),
                    firstOnChangeEventTimestamp,
                    resourceStabilizationTimeout);

            scheduleTransitionEvaluation();
        }

        @Override
        void onChange() {
            // schedule another desired-resource evaluation in scenarios where the previous change
            // event was already handled by a onTrigger callback with a no-op
            onChangeEventTimestamp = now();
            scheduleTransitionEvaluation();
        }

        @Override
        void onTrigger() {
            transitionToSubSequentStateForDesiredResources();
        }

        private void scheduleTransitionEvaluation() {
            if (!evaluationScheduled) {
                evaluationScheduled = true;
                this.scheduleRelativelyTo(
                        () -> {
                            evaluationScheduled = false;
                            transitionToSubSequentStateForDesiredResources();
                        },
                        onChangeEventTimestamp,
                        maxTriggerDelay);
            }
        }

        private void transitionToSubSequentStateForDesiredResources() {
            if (hasDesiredResources()) {
                LOG.info(
                        "Desired resources are met, transitioning to the subsequent state, job {}.",
                        getJobId());
                context().triggerTransitionToSubsequentState();
            } else {
                LOG.debug(
                        "Desired resources are not met, skipping the transition to the subsequent state, job {}.",
                        getJobId());
            }
        }
    }

    /**
     * {@link Phase} that handles the post-stabilization phase. A {@link
     * StateTransitionManager#onTrigger()} event initiates rescaling if sufficient resources are
     * available; otherwise transitioning to {@link Idling} will be performed.
     */
    @VisibleForTesting
    static final class Stabilized extends Phase {

        private Stabilized(
                Supplier<Temporal> clock,
                DefaultStateTransitionManager context,
                Temporal firstChangeEventTimestamp,
                Duration maxTriggerDelay) {
            super(clock, context);
            this.scheduleRelativelyTo(
                    () -> {
                        LOG.info(
                                "Scheduled onTrigger event fired in Stabilized phase, job {}.",
                                getJobId());
                        onTrigger();
                    },
                    firstChangeEventTimestamp,
                    maxTriggerDelay);
        }

        @Override
        void onTrigger() {
            if (hasSufficientResources()) {
                LOG.info(
                        "Sufficient resources are met, progressing to subsequent state, job {}.",
                        getJobId());
                context().triggerTransitionToSubsequentState();
            } else {
                LOG.debug(
                        "Sufficient resources are not met, progressing to idling, job {}.",
                        getJobId());
                context().progressToIdling();
            }
        }
    }

    /**
     * In this final {@link Phase} no additional transition is possible: {@link
     * StateTransitionManager#onChange()} and {@link StateTransitionManager#onTrigger()} events will
     * be ignored.
     */
    @VisibleForTesting
    static final class Transitioning extends Phase {
        private Transitioning(Supplier<Temporal> clock, DefaultStateTransitionManager context) {
            super(clock, context);
        }
    }
}
