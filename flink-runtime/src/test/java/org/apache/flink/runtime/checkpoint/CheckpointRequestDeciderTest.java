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

import org.apache.flink.runtime.checkpoint.CheckpointCoordinator.CheckpointTriggerRequest;
import org.apache.flink.util.clock.ManualClock;

import org.junit.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.rangeClosed;
import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.MINIMUM_TIME_BETWEEN_CHECKPOINTS;
import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.TOO_MANY_CHECKPOINT_REQUESTS;
import static org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** {@link CheckpointRequestDecider} test. */
public class CheckpointRequestDeciderTest {

    @Test
    public void testForce() {
        CheckpointRequestDecider decider =
                decider(1, 1, Integer.MAX_VALUE, new AtomicInteger(1), new AtomicInteger(0));
        CheckpointTriggerRequest request = periodicSavepoint();
        assertEquals(Optional.of(request), decider.chooseRequestToExecute(request, false, 123));
    }

    @Test
    public void testEnqueueOnTooManyPending() {
        final int maxPending = 1;
        final boolean isTriggering = false;
        final AtomicInteger currentPending = new AtomicInteger(maxPending);
        CheckpointRequestDecider decider =
                decider(Integer.MAX_VALUE, maxPending, 1, currentPending, new AtomicInteger(0));

        CheckpointTriggerRequest request = regularCheckpoint();
        assertFalse(decider.chooseRequestToExecute(request, isTriggering, 0).isPresent());

        currentPending.set(0);
        assertEquals(Optional.of(request), decider.chooseQueuedRequestToExecute(isTriggering, 0));
    }

    @Test
    public void testNonForcedEnqueueOnTooManyPending() {
        final int maxPending = 1;
        final boolean isTriggering = false;
        final AtomicInteger currentPending = new AtomicInteger(maxPending);
        final AtomicInteger currentCleaning = new AtomicInteger(0);
        CheckpointRequestDecider decider =
                decider(Integer.MAX_VALUE, maxPending, 1, currentPending, currentCleaning);

        CheckpointTriggerRequest request = nonForcedSavepoint();
        assertFalse(decider.chooseRequestToExecute(request, isTriggering, 0).isPresent());

        currentPending.set(0);
        assertEquals(Optional.of(request), decider.chooseQueuedRequestToExecute(isTriggering, 0));
    }

    @Test
    public void testEnqueueOnTooManyCleaning() {
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
        assertFalse(decider.chooseRequestToExecute(request, isTriggering, 0).isPresent());

        // a checkpoint has been cleaned
        currentCleaning.decrementAndGet();
        assertEquals(Optional.of(request), decider.chooseQueuedRequestToExecute(isTriggering, 0));
    }

    @Test
    public void testUserSubmittedPrioritized() {
        CheckpointTriggerRequest userSubmitted = regularSavepoint();
        CheckpointTriggerRequest periodic = periodicSavepoint();
        testRequestsOrdering(
                new CheckpointTriggerRequest[] {periodic, userSubmitted},
                new CheckpointTriggerRequest[] {userSubmitted, periodic});
    }

    @Test
    public void testSavepointPrioritized() {
        CheckpointTriggerRequest savepoint = regularSavepoint();
        CheckpointTriggerRequest checkpoint = regularCheckpoint();
        testRequestsOrdering(
                new CheckpointTriggerRequest[] {checkpoint, savepoint},
                new CheckpointTriggerRequest[] {savepoint, checkpoint});
    }

    @Test
    public void testNonForcedUserSubmittedPrioritized() {
        CheckpointTriggerRequest userSubmitted = nonForcedSavepoint();
        CheckpointTriggerRequest periodic = nonForcedPeriodicSavepoint();
        testRequestsOrdering(
                new CheckpointTriggerRequest[] {periodic, userSubmitted},
                new CheckpointTriggerRequest[] {userSubmitted, periodic});
    }

    @Test
    public void testNonForcedSavepointPrioritized() {
        CheckpointTriggerRequest savepoint = nonForcedSavepoint();
        CheckpointTriggerRequest checkpoint = regularCheckpoint();
        testRequestsOrdering(
                new CheckpointTriggerRequest[] {checkpoint, savepoint},
                new CheckpointTriggerRequest[] {savepoint, checkpoint});
    }

    @Test
    public void testQueueSizeLimit() {
        final int maxQueuedRequests = 10;
        final boolean isTriggering = true;
        CheckpointRequestDecider decider = decider(maxQueuedRequests);
        List<CheckpointTriggerRequest> requests =
                rangeClosed(0, maxQueuedRequests)
                        .mapToObj(i -> regularCheckpoint())
                        .collect(toList());
        int numAdded = 0;
        for (CheckpointTriggerRequest request : requests) {
            assertFalse(decider.chooseRequestToExecute(request, isTriggering, 0).isPresent());
            List<CheckpointTriggerRequest> completed =
                    requests.stream()
                            .filter(r1 -> r1.getOnCompletionFuture().isDone())
                            .collect(toList());
            completed.forEach(r -> assertFailed(r, TOO_MANY_CHECKPOINT_REQUESTS));
            assertEquals(Math.max(++numAdded - maxQueuedRequests, 0), completed.size());
        }
    }

    @Test
    public void testQueueSizeLimitPriority() {
        final int maxQueuedRequests = 1;
        final boolean isTriggering = true;
        CheckpointRequestDecider decider = decider(maxQueuedRequests);

        CheckpointTriggerRequest checkpoint = regularCheckpoint();
        CheckpointTriggerRequest savepoint = regularSavepoint();

        decider.chooseRequestToExecute(checkpoint, isTriggering, 0);
        decider.chooseRequestToExecute(savepoint, isTriggering, 0);

        assertFailed(checkpoint, TOO_MANY_CHECKPOINT_REQUESTS);
        assertFalse(savepoint.getOnCompletionFuture().isDone());
    }

    @Test
    public void testSavepointTiming() {
        testTiming(regularSavepoint(), TriggerExpectation.IMMEDIATELY);
        testTiming(periodicSavepoint(), TriggerExpectation.IMMEDIATELY);
        testTiming(nonForcedSavepoint(), TriggerExpectation.IMMEDIATELY);
    }

    @Test
    public void testCheckpointTiming() {
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
                assertTrue(
                        decider.chooseRequestToExecute(request, isTriggering, lastCompletionMs)
                                .isPresent());
                break;
            case AFTER_PAUSE:
                assertFalse(
                        decider.chooseRequestToExecute(request, isTriggering, lastCompletionMs)
                                .isPresent());
                clock.advanceTime(pause, MILLISECONDS);
                assertTrue(
                        decider.chooseQueuedRequestToExecute(isTriggering, lastCompletionMs)
                                .isPresent());
                break;
            case DROPPED:
                assertFalse(
                        decider.chooseRequestToExecute(request, isTriggering, lastCompletionMs)
                                .isPresent());
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
            assertFalse(decider.chooseRequestToExecute(r, true, 123).isPresent());
        }
        for (CheckpointTriggerRequest r : expectedExecutionOrder) {
            assertEquals(Optional.of(r), decider.chooseQueuedRequestToExecute(false, 123));
        }
    }

    private void assertFailed(CheckpointTriggerRequest request, CheckpointFailureReason reason) {
        assertTrue(request.getOnCompletionFuture().isCompletedExceptionally());
        request.getOnCompletionFuture()
                .handle(
                        (checkpoint, throwable) -> {
                            assertNull(checkpoint);
                            assertNotNull(throwable);
                            assertTrue(throwable instanceof CheckpointException);
                            assertEquals(
                                    reason,
                                    ((CheckpointException) throwable).getCheckpointFailureReason());
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

    private static final Consumer<Long> NO_OP = unused -> {};

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
                CheckpointProperties.forSavepoint(force), null, periodic);
    }

    private static CheckpointTriggerRequest checkpointRequest(boolean periodic) {
        return new CheckpointTriggerRequest(
                CheckpointProperties.forCheckpoint(NEVER_RETAIN_AFTER_TERMINATION), null, periodic);
    }
}
