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

import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * {@code TestingLeaderElection} implements simple leader election for test cases where no {@code
 * LeaderElectionService} is required.
 */
public class TestingLeaderElection implements LeaderElection {

    /**
     * Is {@code null} if the {@code LeaderElection} isn't started.
     *
     * @see LeaderElection#startLeaderElection(LeaderContender)
     */
    @Nullable private LeaderContender contender = null;

    @Nullable private CompletableFuture<LeaderInformation> confirmationFuture = null;

    /**
     * Is {@code null} if the leadership wasn't acquired.
     *
     * @see TestingLeaderElection#isLeader(UUID)
     * @see TestingLeaderElection#notLeader()
     */
    @Nullable private UUID issuedLeaderSessionId = null;

    private CompletableFuture<Void> startFuture = new CompletableFuture<>();

    @Override
    public synchronized void startLeaderElection(LeaderContender contender) throws Exception {
        Preconditions.checkNotNull(contender);
        Preconditions.checkState(this.contender == null, "Only one contender is supported.");

        this.contender = contender;

        if (hasLeadership()) {
            contender.grantLeadership(issuedLeaderSessionId);
        }

        startFuture.complete(null);
    }

    @Override
    public synchronized void confirmLeadership(UUID leaderSessionID, String leaderAddress) {
        if (confirmationFuture != null && !confirmationFuture.isDone()) {
            confirmationFuture.complete(LeaderInformation.known(leaderSessionID, leaderAddress));
        }
    }

    @Override
    public synchronized boolean hasLeadership(UUID leaderSessionId) {
        return hasLeadership() && leaderSessionId.equals(issuedLeaderSessionId);
    }

    private boolean hasLeadership() {
        return issuedLeaderSessionId != null;
    }

    @Override
    public synchronized void close() {
        if (hasLeadership() && this.contender != null) {
            this.contender.revokeLeadership();
        }

        if (confirmationFuture != null) {
            // the confirmationFuture is kind of bound to the LeaderContender which response to the
            // grantLeadership call - resetting the LeaderElection should also inform the contender
            // of such a state change
            confirmationFuture.cancel(true);
            confirmationFuture = null;
        }

        this.contender = null;
        startFuture.cancel(false);
        startFuture = new CompletableFuture<>();
    }

    /**
     * Acquires the leadership with the given {@code leaderSessionID}.
     *
     * @return the contender's {@link LeaderInformation} after the leadership was confirmed. Waiting
     *     for the {@code CompletableFuture} to complete will leave the test code in a state where
     *     the {@link LeaderContender} confirmed the leadership. This simulates the information
     *     being written to the HA backend.
     */
    public synchronized CompletableFuture<LeaderInformation> isLeader(UUID leaderSessionID) {
        if (confirmationFuture != null) {
            confirmationFuture.cancel(false);
        }

        confirmationFuture = new CompletableFuture<>();
        issuedLeaderSessionId = leaderSessionID;

        if (contender != null) {
            contender.grantLeadership(leaderSessionID);
        }

        return confirmationFuture;
    }

    /** Revokes the leadership. */
    public synchronized void notLeader() {
        Preconditions.checkState(
                hasLeadership(),
                "Leadership should have been acquired before calling this method.");
        issuedLeaderSessionId = null;

        if (contender != null) {
            contender.revokeLeadership();
        }
    }

    /**
     * Returns the start future indicating whether this leader election service has been started or
     * not.
     *
     * @return Future which is completed once this service has been started.
     * @see TestingLeaderElection#startLeaderElection(LeaderContender)
     */
    public synchronized CompletableFuture<Void> getStartFuture() {
        return startFuture;
    }

    /**
     * Returns {@code true} if no contender is registered write now and the service is, therefore,
     * stopped; otherwise {@code false}.
     */
    public synchronized boolean isStopped() {
        return contender == null;
    }
}
