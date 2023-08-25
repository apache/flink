/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator.CheckpointTriggerRequest;
import org.apache.flink.util.clock.ManualClock;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.rangeClosed;
import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.MINIMUM_TIME_BETWEEN_CHECKPOINTS;
import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.TOO_MANY_CHECKPOINT_REQUESTS;
import static org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION;
import static org.assertj.core.api.Assertions.assertThat;

/** {@link CheckpointRequestDecider} test. */
class CheckpointRequestDeciderTest {

    @Test
    void testForce() {
        CheckpointRequestDecider decider =
                decider(1, 1, Integer.MAX_VALUE, new AtomicInteger(1), new AtomicInteger(0));
        CheckpointTriggerRequest request = periodicSavepoint();
        assertThat(decider.chooseRequestToExecute(request, false, 123)).hasValue(request);
    }

    @Test
    void testEnqueueOnTooManyPending() {
        final int maxPending = 1;
        final boolean isTriggering = false;
        final AtomicInteger currentPending = new AtomicInteger(maxPending);
        CheckpointRequestDecider decider =
                decider(Integer.MAX_VALUE, maxPending, 1, currentPending, new AtomicInteger(0));

        CheckpointTriggerRequest request = regularCheckpoint();
        assertThat(decider.chooseRequestToExecute(request, isTriggering, 0)).isNotPresent();

        currentPending.set(0);
        assertThat(decider.chooseQueuedRequestToExecute(isTriggering, 0)).hasValue(request);
    }

    @Test
    void testNonForcedEnqueueOnTooManyPending() {
        final int maxPending = 1;
        final boolean isTriggering = false;
        final AtomicInteger currentPending = new AtomicInteger(maxPending);
        final AtomicInteger currentCleaning = new AtomicInteger(0);
        CheckpointRequestDecider decider =
                decider(Integer.MAX_VALUE, maxPending, 1, currentPending, currentCleaning);

        CheckpointTriggerRequest request = nonForcedSavepoint();
        assertThat(decider.chooseRequestToExecute(request, isTriggering, 0)).isNotPresent();

        currentPending.set(0);
        assertThat(decider.chooseQueuedRequestToExecute(isTriggering, 0)).hasValue(request);
    }

    @Test
    void testEnqueueOnTooManyCleaning() {
        final boolean isTriggering = false;
        int maxQueuedRequests = 10;
        int maxConcurrentCheckpointAttempts = 10;
        // too many cleaning threshold is currently maxConcurrentCheckpointAttempts
        AtomicInteger currentCleaning = new AtomicInteger(maxConcurrentCheckpointAttempts + 1);
        CheckpointRequestDecider decider =
                decider(
                        maxQueuedRequests,
                        maxConcurrentCheckpointAttempts,
                        1,
                        new AtomicInteger(0),
                        currentCleaning);

        CheckpointTriggerRequest request = regularCheckpoint();
        assertThat(decider.chooseRequestToExecute(request, isTriggering, 0)).isNotPresent();

        // a checkpoint has been cleaned
        currentCleaning.decrementAndGet();
        assertThat(decider.chooseQueuedRequestToExecute(isTriggering, 0)).hasValue(request);
    }

    @Test
    void testUserSubmittedPrioritized() {
        CheckpointTriggerRequest userSubmitted = regularSavepoint();
        CheckpointTriggerRequest periodic = periodicSavepoint();
        testRequestsOrdering(
                new CheckpointTriggerRequest[] {periodic, userSubmitted},
                new CheckpointTriggerRequest[] {userSubmitted, periodic});
    }

    @Test
    void testSavepointPrioritized() {
        CheckpointTriggerRequest savepoint = regularSavepoint();
        CheckpointTriggerRequest checkpoint = regularCheckpoint();
        testRequestsOrdering(
                new CheckpointTriggerRequest[] {checkpoint, savepoint},
                new CheckpointTriggerRequest[] {savepoint, checkpoint});
    }

    @Test
    void testNonForcedUserSubmittedPrioritized() {
        CheckpointTriggerRequest userSubmitted = nonForcedSavepoint();
        CheckpointTriggerRequest periodic = nonForcedPeriodicSavepoint();
        testRequestsOrdering(
                new CheckpointTriggerRequest[] {periodic, userSubmitted},
                new CheckpointTriggerRequest[] {userSubmitted, periodic});
    }

    @Test
    void testNonForcedSavepointPrioritized() {
        CheckpointTriggerRequest savepoint = nonForcedSavepoint();
        CheckpointTriggerRequest checkpoint = regularCheckpoint();
        testRequestsOrdering(
                new CheckpointTriggerRequest[] {checkpoint, savepoint},
                new CheckpointTriggerRequest[] {savepoint, checkpoint});
    }

    @Test
    void testQueueSizeLimit() {
        final int maxQueuedRequests = 10;
        final boolean isTriggering = true;
        CheckpointRequestDecider decider = decider(maxQueuedRequests);
        List<CheckpointTriggerRequest> requests =
                rangeClosed(0, maxQueuedRequests)
                        .mapToObj(i -> regularCheckpoint())
                        .collect(toList());
        int numAdded = 0;
        for (CheckpointTriggerRequest request : requests) {
            assertThat(decider.chooseRequestToExecute(request, isTriggering, 0)).isNotPresent();
            List<CheckpointTriggerRequest> completed =
                    requests.stream()
                            .filter(r1 -> r1.getOnCompletionFuture().isDone())
                            .collect(toList());
            completed.forEach(r -> assertFailed(r, TOO_MANY_CHECKPOINT_REQUESTS));
            assertThat(completed).hasSize(Math.max(++numAdded - maxQueuedRequests, 0));
        }
    }

    @Test
    void testQueueSizeLimitPriority() {
        final int maxQueuedRequests = 1;
        final boolean isTriggering = true;
        CheckpointRequestDecider decider = decider(maxQueuedRequests);

        CheckpointTriggerRequest checkpoint = regularCheckpoint();
        CheckpointTriggerRequest savepoint = regularSavepoint();

        decider.chooseRequestToExecute(checkpoint, isTriggering, 0);
        decider.chooseRequestToExecute(savepoint, isTriggering, 0);

        assertFailed(checkpoint, TOO_MANY_CHECKPOINT_REQUESTS);
        assertThat(savepoint.getOnCompletionFuture()).isNotDone();
    }

    @Test
    void testSavepointTiming() {
        testTiming(regularSavepoint(), TriggerExpectation.IMMEDIATELY);
        testTiming(periodicSavepoint(), TriggerExpectation.IMMEDIATELY);
        testTiming(nonForcedSavepoint(), TriggerExpectation.IMMEDIATELY);
    }

    @Test
    void testCheckpointTiming() {
        testTiming(regularCheckpoint(), TriggerExpectation.DROPPED);
        testTiming(manualCheckpoint(), TriggerExpectation.IMMEDIATELY);
    }

    private enum TriggerExpectation {
        IMMEDIATELY,
        AFTER_PAUSE,
        DROPPED
    }

    private void testTiming(CheckpointTriggerRequest request, TriggerExpectation expectation) {
        final long pause = 10;
        final ManualClock clock = new ManualClock();
        final CheckpointRequestDecider decider =
                new CheckpointRequestDecider(
                        1, NO_OP, clock, pause, () -> 0, () -> 0, Integer.MAX_VALUE);

        final long lastCompletionMs = clock.relativeTimeMillis();
        final boolean isTriggering = false;

        switch (expectation) {
            case IMMEDIATELY:
                assertThat(decider.chooseRequestToExecute(request, isTriggering, lastCompletionMs))
                        .isPresent();
                break;
            case AFTER_PAUSE:
                assertThat(decider.chooseRequestToExecute(request, isTriggering, lastCompletionMs))
                        .isNotPresent();
                clock.advanceTime(pause, MILLISECONDS);
                assertThat(decider.chooseQueuedRequestToExecute(isTriggering, lastCompletionMs))
                        .isPresent();
                break;
            case DROPPED:
                assertThat(decider.chooseRequestToExecute(request, isTriggering, lastCompletionMs))
                        .isNotPresent();
                assertFailed(request, MINIMUM_TIME_BETWEEN_CHECKPOINTS);
                break;
            default:
                throw new IllegalArgumentException("unknown expectation: " + expectation);
        }
    }

    private void testRequestsOrdering(
            CheckpointTriggerRequest[] requests,
            CheckpointTriggerRequest[] expectedExecutionOrder) {
        CheckpointRequestDecider decider = decider(10);
        for (CheckpointTriggerRequest r : requests) {
            assertThat(decider.chooseRequestToExecute(r, true, 123)).isNotPresent();
        }
        for (CheckpointTriggerRequest r : expectedExecutionOrder) {
            assertThat(decider.chooseQueuedRequestToExecute(false, 123)).hasValue(r);
        }
    }

    private void assertFailed(CheckpointTriggerRequest request, CheckpointFailureReason reason) {
        assertThat(request.getOnCompletionFuture()).isCompletedExceptionally();
        request.getOnCompletionFuture()
                .handle(
                        (checkpoint, throwable) -> {
                            assertThat(checkpoint).isNull();
                            assertThat(throwable)
                                    .isNotNull()
                                    .isInstanceOfSatisfying(
                                            CheckpointException.class,
                                            e ->
                                                    assertThat(e.getCheckpointFailureReason())
                                                            .isEqualTo(reason));
                            return null;
                        })
                .join();
    }

    public CheckpointRequestDecider decider(int maxQueuedRequests) {
        return decider(maxQueuedRequests, 1, 1, new AtomicInteger(0), new AtomicInteger(0));
    }

    private CheckpointRequestDecider decider(
            int maxQueued,
            int maxPending,
            int minPause,
            AtomicInteger currentPending,
            AtomicInteger currentCleaning) {
        ManualClock clock = new ManualClock();
        clock.advanceTime(1, TimeUnit.DAYS);
        return new CheckpointRequestDecider(
                maxPending,
                NO_OP,
                clock,
                minPause,
                currentPending::get,
                currentCleaning::get,
                maxQueued);
    }

    private static final BiConsumer<Long, Long> NO_OP = (currentTimeMillis, tillNextMillis) -> {};

    static CheckpointTriggerRequest regularCheckpoint() {
        return checkpointRequest(true);
    }

    private static CheckpointTriggerRequest manualCheckpoint() {
        return checkpointRequest(false);
    }

    private static CheckpointTriggerRequest regularSavepoint() {
        return savepointRequest(true, false);
    }

    private static CheckpointTriggerRequest periodicSavepoint() {
        return savepointRequest(true, true);
    }

    private static CheckpointTriggerRequest nonForcedPeriodicSavepoint() {
        return savepointRequest(false, true);
    }

    private static CheckpointTriggerRequest nonForcedSavepoint() {
        return savepointRequest(false, false);
    }

    private static CheckpointTriggerRequest savepointRequest(boolean force, boolean periodic) {
        return new CheckpointTriggerRequest(
                CheckpointProperties.forSavepoint(force, SavepointFormatType.CANONICAL),
                null,
                periodic);
    }

    private static CheckpointTriggerRequest checkpointRequest(boolean periodic) {
        return new CheckpointTriggerRequest(
                CheckpointProperties.forCheckpoint(NEVER_RETAIN_AFTER_TERMINATION), null, periodic);
    }
}
