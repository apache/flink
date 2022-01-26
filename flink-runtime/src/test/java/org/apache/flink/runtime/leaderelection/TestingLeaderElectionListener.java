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
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Simple {@link MultipleComponentLeaderElectionDriver.Listener} implementation for testing
 * purposes.
 */
public final class TestingLeaderElectionListener
        implements MultipleComponentLeaderElectionDriver.Listener {
    private final BlockingQueue<LeaderElectionEvent> leaderElectionEvents =
            new ArrayBlockingQueue<>(10);

    @Override
    public void isLeader() {
        put(new LeaderElectionEvent.IsLeaderEvent());
    }

    @Override
    public void notLeader() {
        put(new LeaderElectionEvent.NotLeaderEvent());
    }

    @Override
    public void notifyLeaderInformationChange(
            String componentId, LeaderInformation leaderInformation) {
        put(new LeaderElectionEvent.LeaderInformationChangeEvent(componentId, leaderInformation));
    }

    @Override
    public void notifyAllKnownLeaderInformation(
            Collection<LeaderInformationWithComponentId> leaderInformationWithComponentIds) {
        put(
                new LeaderElectionEvent.AllKnownLeaderInformationEvent(
                        leaderInformationWithComponentIds));
    }

    private void put(LeaderElectionEvent leaderElectionEvent) {
        try {
            leaderElectionEvents.put(leaderElectionEvent);
        } catch (InterruptedException e) {
            ExceptionUtils.rethrow(e);
        }
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
}
