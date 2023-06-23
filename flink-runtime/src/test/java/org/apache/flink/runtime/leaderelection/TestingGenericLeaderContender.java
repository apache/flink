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

package org.apache.flink.runtime.leaderelection;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * {@code TestingGenericLeaderContender} is a more generic testing implementation of the {@link
 * LeaderContender} interface.
 */
public class TestingGenericLeaderContender implements LeaderContender {

    private final ReentrantLock lock = new ReentrantLock();

    private final BiConsumer<ReentrantLock, UUID> grantLeadershipConsumer;
    private final Consumer<ReentrantLock> revokeLeadershipConsumer;
    private final BiConsumer<ReentrantLock, Exception> handleErrorConsumer;

    public TestingGenericLeaderContender(
            BiConsumer<ReentrantLock, UUID> grantLeadershipConsumer,
            Consumer<ReentrantLock> revokeLeadershipConsumer,
            BiConsumer<ReentrantLock, Exception> handleErrorConsumer) {
        this.grantLeadershipConsumer = grantLeadershipConsumer;
        this.revokeLeadershipConsumer = revokeLeadershipConsumer;
        this.handleErrorConsumer = handleErrorConsumer;
    }

    @Override
    public void grantLeadership(UUID leaderSessionID) {
        grantLeadershipConsumer.accept(lock, leaderSessionID);
    }

    @Override
    public void revokeLeadership() {
        revokeLeadershipConsumer.accept(lock);
    }

    @Override
    public void handleError(Exception exception) {
        handleErrorConsumer.accept(lock, exception);
    }

    public static Builder newBuilderForNoOpContender() {
        return new Builder();
    }

    public static Builder newBuilder(
            Collection<LeaderElectionEvent> leaderElectionEventQueue,
            Consumer<Throwable> errorHandler) {
        return newBuilder(
                leaderElectionEventQueue,
                NoOpLeaderElection.INSTANCE,
                "unused-address",
                errorHandler);
    }

    public static Builder newBuilder(
            LeaderElection leaderElection, String address, Consumer<Throwable> errorHandler) {
        return newBuilder(event -> {}, leaderElection, address, errorHandler);
    }

    public static Builder newBuilder(
            Collection<LeaderElectionEvent> leaderElectionEventQueue,
            LeaderElection leaderElection,
            String address,
            Consumer<Throwable> errorHandler) {
        return newBuilder(leaderElectionEventQueue::add, leaderElection, address, errorHandler);
    }

    private static Builder newBuilder(
            Consumer<LeaderElectionEvent> leaderElectionEventQueue,
            LeaderElection leaderElection,
            String address,
            Consumer<Throwable> errorHandler) {
        return newBuilderForNoOpContender()
                .setGrantLeadershipConsumer(
                        (lock, leaderSessionID) -> {
                            try {
                                lock.lock();
                                leaderElectionEventQueue.accept(
                                        new LeaderElectionEvent.IsLeaderEvent(leaderSessionID));
                                leaderElection.confirmLeadership(leaderSessionID, address);
                            } finally {
                                lock.unlock();
                            }
                        })
                .setRevokeLeadershipConsumer(
                        lock -> {
                            try {
                                lock.lock();
                                leaderElectionEventQueue.accept(
                                        new LeaderElectionEvent.NotLeaderEvent());
                            } finally {
                                lock.unlock();
                            }
                        })
                .setHandleErrorConsumer(
                        (lock, throwable) -> {
                            try {
                                lock.lock();
                                errorHandler.accept(throwable);
                            } finally {
                                lock.unlock();
                            }
                        });
    }

    /** {@code Builder} for creating {@code TestingGenericLeaderContender} instances. */
    public static class Builder {
        private BiConsumer<ReentrantLock, UUID> grantLeadershipConsumer =
                (ignoredLock, ignoredSessionID) -> {};
        private Consumer<ReentrantLock> revokeLeadershipConsumer = ignoredLock -> {};
        private BiConsumer<ReentrantLock, Exception> handleErrorConsumer =
                (lock, error) -> {
                    try {
                        lock.lock();
                        throw new AssertionError(error);
                    } finally {
                        lock.unlock();
                    }
                };

        private Builder() {}

        public Builder setGrantLeadershipConsumer(
                BiConsumer<ReentrantLock, UUID> grantLeadershipConsumer) {
            this.grantLeadershipConsumer = grantLeadershipConsumer;
            return this;
        }

        public Builder setRevokeLeadershipConsumer(
                Consumer<ReentrantLock> revokeLeadershipConsumer) {
            this.revokeLeadershipConsumer = revokeLeadershipConsumer;
            return this;
        }

        public Builder setHandleErrorConsumer(
                BiConsumer<ReentrantLock, Exception> handleErrorConsumer) {
            this.handleErrorConsumer = handleErrorConsumer;
            return this;
        }

        public TestingGenericLeaderContender build() {
            return new TestingGenericLeaderContender(
                    grantLeadershipConsumer, revokeLeadershipConsumer, handleErrorConsumer);
        }
    }
}
