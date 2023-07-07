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

import java.util.UUID;

/**
 * {@code DefaultLeaderElection} implements the {@link LeaderElection} based on the {@link
 * ParentService}.
 */
class DefaultLeaderElection implements LeaderElection {

    private final ParentService parentService;
    private final String contenderID;

    DefaultLeaderElection(ParentService parentService, String contenderID) {
        this.parentService = parentService;
        this.contenderID = contenderID;
    }

    @Override
    public void startLeaderElection(LeaderContender contender) throws Exception {
        Preconditions.checkNotNull(contender);
        parentService.register(contenderID, contender);
    }

    @Override
    public void confirmLeadership(UUID leaderSessionID, String leaderAddress) {
        parentService.confirmLeadership(contenderID, leaderSessionID, leaderAddress);
    }

    @Override
    public boolean hasLeadership(UUID leaderSessionId) {
        return parentService.hasLeadership(contenderID, leaderSessionId);
    }

    @Override
    public void close() throws Exception {
        parentService.remove(contenderID);
    }

    /**
     * {@link ParentService} defines the protocol between any implementing class and {@code
     * DefaultLeaderElection}.
     */
    static abstract class ParentService {

        /**
         * Registers the {@link LeaderContender} under the {@code contenderID} with the underlying
         * {@code ParentService}. Leadership changes are starting to be reported to the {@code
         * LeaderContender}.
         */
        abstract void register(String contenderID, LeaderContender contender) throws Exception;

        /**
         * Removes the {@code LeaderContender} from the {@code ParentService} that is associated
         * with the {@code contenderID}.
         */
        abstract void remove(String contenderID);

        /**
         * Confirms the leadership with the {@code leaderSessionID} and {@code leaderAddress} for
         * the {@link LeaderContender} that is associated with the {@code contenderID}.
         */
        abstract void confirmLeadership(
                String contenderID, UUID leaderSessionID, String leaderAddress);

        /**
         * Checks whether the {@code ParentService} has the leadership acquired for the {@code
         * contenderID} and {@code leaderSessionID}.
         *
         * @return {@code true} if the service has leadership with the passed {@code
         *     leaderSessionID} acquired; {@code false} otherwise.
         */
        abstract boolean hasLeadership(String contenderID, UUID leaderSessionID);
    }
}
