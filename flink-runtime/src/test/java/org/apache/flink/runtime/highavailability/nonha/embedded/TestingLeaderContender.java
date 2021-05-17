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

package org.apache.flink.runtime.highavailability.nonha.embedded;

import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.leaderelection.LeaderContender;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/** {@link LeaderContender} implementation for testing purposes. */
final class TestingLeaderContender implements LeaderContender {

    private final Object lock = new Object();

    private CompletableFuture<UUID> leaderSessionFuture;

    TestingLeaderContender() {
        leaderSessionFuture = new CompletableFuture<>();
    }

    @Override
    public void grantLeadership(UUID leaderSessionID) {
        synchronized (lock) {
            if (!leaderSessionFuture.isCompletedExceptionally()) {
                if (!leaderSessionFuture.complete(leaderSessionID)) {
                    leaderSessionFuture = CompletableFuture.completedFuture(leaderSessionID);
                }
            }
        }
    }

    @Override
    public void revokeLeadership() {
        synchronized (lock) {
            if (leaderSessionFuture.isDone() && !leaderSessionFuture.isCompletedExceptionally()) {
                leaderSessionFuture = new CompletableFuture<>();
            }
        }
    }

    @Override
    public String getDescription() {
        return "foobar";
    }

    @Override
    public void handleError(Exception exception) {
        synchronized (lock) {
            if (!(leaderSessionFuture.isCompletedExceptionally()
                    || leaderSessionFuture.completeExceptionally(exception))) {
                leaderSessionFuture = FutureUtils.completedExceptionally(exception);
            }
        }
    }

    public void tryRethrowException() {
        synchronized (lock) {
            if (leaderSessionFuture.isCompletedExceptionally()) {
                leaderSessionFuture.getNow(null);
            }
        }
    }

    CompletableFuture<UUID> getLeaderSessionFuture() {
        synchronized (lock) {
            return leaderSessionFuture;
        }
    }
}
