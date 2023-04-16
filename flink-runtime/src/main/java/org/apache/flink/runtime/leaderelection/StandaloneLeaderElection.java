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

/**
 * {@code StandaloneLeaderElection} implements {@link LeaderElection} for non-HA cases. This
 * implementation can be used for testing, and for cluster setups that do not tolerate failures of
 * the main components (e.g. ResourceManager or Dispatcher).
 */
public class StandaloneLeaderElection implements LeaderElection {

    private final Object lock = new Object();

    private final UUID sessionID;
    @Nullable private LeaderContender leaderContender;

    public StandaloneLeaderElection(UUID sessionID) {
        this.sessionID = sessionID;
    }

    @Override
    public void startLeaderElection(LeaderContender contender) throws Exception {
        synchronized (lock) {
            Preconditions.checkState(
                    leaderContender == null,
                    "No LeaderContender should have been registered with this LeaderElection, yet.");
            this.leaderContender = contender;

            this.leaderContender.grantLeadership(sessionID);
        }
    }

    @Override
    public void confirmLeadership(UUID leaderSessionID, String leaderAddress) {}

    @Override
    public boolean hasLeadership(UUID leaderSessionId) {
        synchronized (lock) {
            return this.leaderContender != null && this.sessionID.equals(leaderSessionId);
        }
    }

    @Override
    public void close() throws Exception {
        synchronized (lock) {
            if (this.leaderContender != null) {
                this.leaderContender.revokeLeadership();
                this.leaderContender = null;
            }
        }
    }
}
