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

import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.util.function.ThrowingConsumer;

import javax.annotation.Nullable;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * {@link LeaderElectionDriver} implementation which provides some convenience functions for testing
 * purposes. Please use {@link #isLeader} and {@link #notLeader()} to manually control the
 * leadership.
 */
public class TestingLeaderElectionDriver implements LeaderElectionDriver {

    private final Object lock = new Object();

    private final AtomicBoolean isLeader = new AtomicBoolean(false);
    private final LeaderElectionEventHandler leaderElectionEventHandler;
    private final FatalErrorHandler fatalErrorHandler;

    private final ThrowingConsumer<LeaderElectionEventHandler, Exception> closeRunnable;
    private final ThrowingConsumer<LeaderElectionEventHandler, Exception> beforeLockCloseRunnable;

    private final Consumer<LeaderElectionEventHandler> beforeGrantRunnable;

    // Leader information on external storage
    private LeaderInformation leaderInformation = LeaderInformation.empty();

    private TestingLeaderElectionDriver(
            LeaderElectionEventHandler leaderElectionEventHandler,
            FatalErrorHandler fatalErrorHandler,
            ThrowingConsumer<LeaderElectionEventHandler, Exception> closeRunnable,
            ThrowingConsumer<LeaderElectionEventHandler, Exception> beforeLockCloseRunnable,
            Consumer<LeaderElectionEventHandler> beforeGrantRunnable) {
        this.leaderElectionEventHandler = leaderElectionEventHandler;
        this.fatalErrorHandler = fatalErrorHandler;
        this.closeRunnable = closeRunnable;
        this.beforeLockCloseRunnable = beforeLockCloseRunnable;
        this.beforeGrantRunnable = beforeGrantRunnable;
    }

    @Override
    public void writeLeaderInformation(LeaderInformation leaderInformation) {
        this.leaderInformation = leaderInformation;
    }

    @Override
    public boolean hasLeadership() {
        return isLeader.get();
    }

    @Override
    public void close() throws Exception {
        beforeLockCloseRunnable.accept(leaderElectionEventHandler);
        synchronized (lock) {
            closeRunnable.accept(leaderElectionEventHandler);
        }
    }

    public LeaderInformation getLeaderInformation() {
        return leaderInformation;
    }

    public void isLeader(UUID newSessionID) {
        synchronized (lock) {
            isLeader.set(true);
            beforeGrantRunnable.accept(leaderElectionEventHandler);
            leaderElectionEventHandler.onGrantLeadership(newSessionID);
        }
    }

    public void isLeader() {
        isLeader(UUID.randomUUID());
    }

    public void notLeader() {
        synchronized (lock) {
            isLeader.set(false);
            leaderElectionEventHandler.onRevokeLeadership();
        }
    }

    public void leaderInformationChanged(LeaderInformation newLeader) {
        leaderInformation = newLeader;
        leaderElectionEventHandler.onLeaderInformationChange(newLeader);
    }

    public void onFatalError(Throwable throwable) {
        fatalErrorHandler.onFatalError(throwable);
    }

    /** Factory for create {@link TestingLeaderElectionDriver}. */
    public static class TestingLeaderElectionDriverFactory implements LeaderElectionDriverFactory {

        private TestingLeaderElectionDriver currentLeaderDriver;

        private final ThrowingConsumer<LeaderElectionEventHandler, Exception> closeRunnable;
        private final ThrowingConsumer<LeaderElectionEventHandler, Exception>
                beforeLockCloseRunnable;

        private final Consumer<LeaderElectionEventHandler> beforeGrantRunnable;

        public TestingLeaderElectionDriverFactory() {
            this(ignoredLeaderElectionEventHandler -> {});
        }

        public TestingLeaderElectionDriverFactory(
                ThrowingConsumer<LeaderElectionEventHandler, Exception> closeRunnable) {
            this(
                    closeRunnable,
                    ignoredLeaderElectionEventHandler -> {},
                    ignoredLeaderElectionEventHandler -> {});
        }

        public TestingLeaderElectionDriverFactory(
                ThrowingConsumer<LeaderElectionEventHandler, Exception> closeRunnable,
                ThrowingConsumer<LeaderElectionEventHandler, Exception> beforeLockCloseRunnable,
                Consumer<LeaderElectionEventHandler> beforeGrantRunnable) {
            this.closeRunnable = closeRunnable;
            this.beforeLockCloseRunnable = beforeLockCloseRunnable;
            this.beforeGrantRunnable = beforeGrantRunnable;
        }

        @Override
        public LeaderElectionDriver createLeaderElectionDriver(
                LeaderElectionEventHandler leaderEventHandler,
                FatalErrorHandler fatalErrorHandler) {
            currentLeaderDriver =
                    new TestingLeaderElectionDriver(
                            leaderEventHandler,
                            fatalErrorHandler,
                            closeRunnable,
                            beforeLockCloseRunnable,
                            beforeGrantRunnable);
            return currentLeaderDriver;
        }

        @Nullable
        public TestingLeaderElectionDriver getCurrentLeaderDriver() {
            return currentLeaderDriver;
        }
    }
}
