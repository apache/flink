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

public abstract class AbstractLeaderElectionService implements LeaderElectionService {

    public void createLeaderElection() throws Exception {}

    @Override
    public LeaderElection createLeaderElection(String contenderID) {
        return new LeaderElectionImpl(this, contenderID);
    }

    protected abstract void register(String contenderID, LeaderContender contender)
            throws Exception;

    protected abstract void remove(String contenderID);

    protected abstract void confirmLeadership(
            String contenderID, UUID leaderSessionID, String leaderAddress);

    protected abstract boolean hasLeadership(String contenderID, UUID leaderSessionId);

    private static class LeaderElectionImpl implements LeaderElectionService.LeaderElection {

        private final AbstractLeaderElectionService parentService;
        private final String contenderID;
        @Nullable private LeaderContender leaderContender;

        public LeaderElectionImpl(AbstractLeaderElectionService parentService, String contenderID) {
            this.parentService = parentService;
            this.contenderID = contenderID;
        }

        @Override
        public void register(LeaderContender contender) {
            this.leaderContender = contender;
        }

        @Override
        public void startLeaderElection() throws Exception {
            Preconditions.checkState(
                    leaderContender != null, "No LeaderContender was registered, yet.");
            parentService.register(contenderID, leaderContender);
        }

        @Override
        public void close() throws Exception {
            parentService.remove(contenderID);
        }

        @Override
        public void confirmLeadership(UUID leaderSessionID, String leaderAddress) {
            parentService.confirmLeadership(contenderID, leaderSessionID, leaderAddress);
        }

        @Override
        public boolean hasLeadership(UUID leaderSessionId) {
            return parentService.hasLeadership(contenderID, leaderSessionId);
        }
    }
}
