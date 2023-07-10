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

package org.apache.flink.runtime.leaderelection;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.util.ExceptionUtils;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Simple {@link LeaderElectionDriver.Listener} implementation for testing purposes. */
public final class TestingLeaderElectionListener implements LeaderElectionDriver.Listener {
    private final BlockingQueue<LeaderElectionEvent> leaderElectionEvents =
            new ArrayBlockingQueue<>(10);

    @Override
    public void onGrantLeadership(UUID leaderSessionID) {
        put(new LeaderElectionEvent.IsLeaderEvent(leaderSessionID));
    }

    @Override
    public void onRevokeLeadership() {
        put(new LeaderElectionEvent.NotLeaderEvent());
    }

    @Override
    public void onLeaderInformationChange(String componentId, LeaderInformation leaderInformation) {
        put(new LeaderElectionEvent.LeaderInformationChangeEvent(componentId, leaderInformation));
    }

    @Override
    public void onLeaderInformationChange(LeaderInformationRegister leaderInformationRegister) {
        put(new LeaderElectionEvent.AllLeaderInformationChangeEvent(leaderInformationRegister));
    }

    @Override
    public void onError(Throwable t) {
        put(new LeaderElectionEvent.ErrorEvent(t));
    }

    private void put(LeaderElectionEvent leaderElectionEvent) {
        try {
            leaderElectionEvents.put(leaderElectionEvent);
        } catch (InterruptedException e) {
            ExceptionUtils.rethrow(e);
        }
    }

    public <T> T assertNextEvent(Class<T> expectedEventClass) throws InterruptedException {
        final LeaderElectionEvent leaderElectionEvent = leaderElectionEvents.take();

        assertThat(leaderElectionEvent)
                .as(
                        "The next event didn't match the expected event type %s.",
                        expectedEventClass.getSimpleName())
                .isInstanceOf(expectedEventClass);

        return leaderElectionEvent.as(expectedEventClass);
    }

    public <T> T await(Class<T> clazz) throws InterruptedException {
        while (true) {
            final LeaderElectionEvent leaderElectionEvent = leaderElectionEvents.take();

            if (clazz.isAssignableFrom(leaderElectionEvent.getClass())) {
                return clazz.cast(leaderElectionEvent);
            }
        }
    }

    public <T> Optional<T> await(Class<T> clazz, Duration timeout) throws InterruptedException {
        final Deadline deadline = Deadline.fromNow(timeout);

        while (true) {
            final Duration timeLeft = deadline.timeLeft();

            if (timeLeft.isNegative()) {
                return Optional.empty();
            } else {
                final Optional<LeaderElectionEvent> optLeaderElectionEvent =
                        Optional.ofNullable(
                                leaderElectionEvents.poll(
                                        timeLeft.toMillis(), TimeUnit.MILLISECONDS));

                if (optLeaderElectionEvent.isPresent()) {
                    final LeaderElectionEvent leaderElectionEvent = optLeaderElectionEvent.get();

                    if (clazz.isAssignableFrom(leaderElectionEvent.getClass())) {
                        return Optional.of(clazz.cast(optLeaderElectionEvent));
                    }
                } else {
                    return Optional.empty();
                }
            }
        }
    }

    /**
     * Returns the next {@link
     * org.apache.flink.runtime.leaderelection.LeaderElectionEvent.ErrorEvent} or an empty {@code
     * Optional} if no such event happened. Any other not-yet processed events that happened before
     * the error will be removed from the queue. This method doesn't wait for events but processes
     * the queue in its current state.
     */
    public Optional<LeaderElectionEvent.ErrorEvent> getNextErrorEvent() {
        while (!leaderElectionEvents.isEmpty()) {
            final LeaderElectionEvent event = leaderElectionEvents.remove();
            if (event.isErrorEvent()) {
                return Optional.of(event.as(LeaderElectionEvent.ErrorEvent.class));
            }
        }

        return Optional.empty();
    }

    /**
     * Throws an {@code AssertionError} if an {@link LeaderElectionEvent.ErrorEvent} was observed.
     * This method can be used to ensure that any error that was triggered unexpectedly is exposed
     * within the test.
     */
    public void failIfErrorEventHappened() {
        getNextErrorEvent()
                .ifPresent(
                        error -> {
                            throw new AssertionError(
                                    "An error was reported that wasn't properly handled.",
                                    error.getError());
                        });
    }
}
