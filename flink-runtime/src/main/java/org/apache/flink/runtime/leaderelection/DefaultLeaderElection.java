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
    private final String componentId;

    DefaultLeaderElection(ParentService parentService, String componentId) {
        this.parentService = parentService;
        this.componentId = componentId;
    }

    @Override
    public void startLeaderElection(LeaderContender contender) throws Exception {
        Preconditions.checkNotNull(contender);
        parentService.register(componentId, contender);
    }

    @Override
    public void confirmLeadership(UUID leaderSessionID, String leaderAddress) {
        parentService.confirmLeadership(componentId, leaderSessionID, leaderAddress);
    }

    @Override
    public boolean hasLeadership(UUID leaderSessionId) {
        return parentService.hasLeadership(componentId, leaderSessionId);
    }

    @Override
    public void close() throws Exception {
        parentService.remove(componentId);
    }

    /**
     * {@link ParentService} defines the protocol between any implementing class and {@code
     * DefaultLeaderElection}.
     */
    abstract static class ParentService {

        /**
         * Registers the {@link LeaderContender} under the {@code componentId} with the underlying
         * {@code ParentService}. Leadership changes are starting to be reported to the {@code
         * LeaderContender}.
         */
        abstract void register(String componentId, LeaderContender contender) throws Exception;

        /**
         * Removes the {@code LeaderContender} from the {@code ParentService} that is associated
         * with the {@code componentId}.
         */
        abstract void remove(String componentId) throws Exception;

        /**
         * Confirms the leadership with the {@code leaderSessionID} and {@code leaderAddress} for
         * the {@link LeaderContender} that is associated with the {@code componentId}.
         */
        abstract void confirmLeadership(
                String componentId, UUID leaderSessionID, String leaderAddress);

        /**
         * Checks whether the {@code ParentService} has the leadership acquired for the {@code
         * componentId} and {@code leaderSessionID}.
         *
         * @return {@code true} if the service has leadership with the passed {@code
         *     leaderSessionID} acquired; {@code false} otherwise.
         */
        abstract boolean hasLeadership(String componentId, UUID leaderSessionID);
    }
}
