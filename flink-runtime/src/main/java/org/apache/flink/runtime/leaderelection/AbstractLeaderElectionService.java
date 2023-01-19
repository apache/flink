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

import java.util.UUID;

public abstract class AbstractLeaderElectionService implements LeaderElectionService {
    @Override
    public LeaderElection createLeaderElection(LeaderContender contender) throws Exception {
        register(contender);
        return new LeaderElectionImpl(this);
    }

    protected abstract void register(LeaderContender contender) throws Exception;

    protected abstract void confirmLeadership(UUID leaderSessionID, String leaderAddress);

    protected abstract boolean hasLeadership(UUID leaderSessionId);

    private static class LeaderElectionImpl implements LeaderElectionService.LeaderElection {

        private final AbstractLeaderElectionService parentService;

        public LeaderElectionImpl(AbstractLeaderElectionService parentService) {
            this.parentService = parentService;
        }

        @Override
        public void confirmLeadership(UUID leaderSessionID, String leaderAddress) {
            parentService.confirmLeadership(leaderSessionID, leaderAddress);
        }

        @Override
        public boolean hasLeadership(UUID leaderSessionId) {
            return parentService.hasLeadership(leaderSessionId);
        }
    }
}
