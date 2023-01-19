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

import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.util.Preconditions;

import java.util.UUID;

/**
 * Standalone implementation of the {@link LeaderElectionService} interface. The standalone
 * implementation assumes that there is only a single {@link LeaderContender} and thus directly
 * grants him the leadership upon start up. Furthermore, there is no communication needed between
 * multiple standalone leader election services.
 */
public class StandaloneLeaderElectionService extends AbstractLeaderElectionService {

    private String contenderID;
    private LeaderContender contender = null;

    @Override
    public void startLeaderElectionBackend() throws Exception {}

    @Override
    protected void register(String contenderID, LeaderContender newContender) throws Exception {
        Preconditions.checkArgument(
                this.contenderID == null, "There is already a contender registered.");
        if (contender != null) {
            // Service was already started
            throw new IllegalArgumentException(
                    "Leader election service cannot be started multiple times.");
        }

        this.contenderID = contenderID;
        contender = Preconditions.checkNotNull(newContender);

        // directly grant leadership to the given contender
        contender.grantLeadership(HighAvailabilityServices.DEFAULT_LEADER_ID);
    }

    @Override
    public void close() {
        if (contender != null) {
            contender.revokeLeadership();
            contender = null;
        }
    }

    @Override
    protected void confirmLeadership(
            String contenderID, UUID leaderSessionID, String leaderAddress) {
        Preconditions.checkState(
                contenderID.equals(this.contenderID),
                "The passed contender ID {} does not match the registered contender ID {}.",
                contenderID,
                this.contenderID);
    }

    @Override
    protected boolean hasLeadership(String contenderID, UUID leaderSessionId) {
        return (contenderID.equals(this.contenderID)
                && contender != null
                && HighAvailabilityServices.DEFAULT_LEADER_ID.equals(leaderSessionId));
    }
}
