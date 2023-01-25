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

/**
 * Interface for a service which allows to elect a leader among a group of contenders.
 *
 * <p>Prior to using this service, it has to be started calling the start method. The start method
 * takes the contender as a parameter. If there are multiple contenders, then each contender has to
 * instantiate its own leader election service.
 *
 * <p>Once a contender has been granted leadership he has to confirm the received leader session ID
 * by calling the method {@link LeaderElection#confirmLeadership(UUID, String)}. This will notify
 * the leader election service, that the contender has accepted the leadership specified and that
 * the leader session id as well as the leader address can now be published for leader retrieval
 * services.
 */
public interface LeaderElectionService extends AutoCloseable {

    /**
     * Initializes any resources that are necessary to make the {@code LeaderElectionService}
     * operate properly.
     *
     * @throws Exception if an error appears during the initialization.
     */
    void startLeaderElectionBackend() throws Exception;

    /**
     * Creates a {@link LeaderElection} which will have the given {@code contenderID} attached with
     * it. The {@code contenderID} serves as a unique ID among all participating {@code
     * LeaderElection} instances within a {@code LeaderElectionService}. This {@code contenderID}
     * enables the {@code LeaderElectionService} to differentiate the leader information that it
     * retrieves from the different {@code LeaderElection} instances.
     *
     * @param contenderID A unique identifier for the {@code LeaderElection} that's going to be
     *     created by this call.
     * @return A newly created {@code LeaderElection}.
     * @throws IllegalArgumentException if the passed {@code contenderID} is already used within the
     *     {@code LeaderElectionService}.
     */
    LeaderElection createLeaderElection(String contenderID) throws IllegalArgumentException;

    /**
     * Stops the leader election service. Stopping the {@code LeaderElectionService} will trigger
     * {@link LeaderContender#revokeLeadership()}.
     *
     * @throws Exception if an error occurs while stopping the {@code LeaderElectionService}.
     */
    void close() throws Exception;

    interface LeaderElection extends AutoCloseable {

        /** Registers the {@link LeaderContender} with the {@code LeaderElection}. */
        void register(LeaderContender contender);

        /** Establishes the connection with the {@link LeaderElectionService}. */
        void startLeaderElection() throws Exception;

        /**
         * Confirms that the {@link LeaderContender} has accepted the leadership identified by the
         * given leader session id. It also publishes the leader address under which the leader is
         * reachable.
         *
         * <p>The rational behind this method is to establish an order between setting the new
         * leader session ID in the {@link LeaderContender} and publishing the new leader session ID
         * as well as the leader address to the leader retrieval services.
         *
         * @param leaderSessionID The new leader session ID
         * @param leaderAddress The address of the new leader
         */
        void confirmLeadership(UUID leaderSessionID, String leaderAddress);

        /**
         * Returns true if the {@link LeaderContender} with which the service has been started owns
         * currently the leadership under the given leader session id.
         *
         * @param leaderSessionId identifying the current leader
         * @return true if the associated {@link LeaderContender} is the leader, otherwise false
         */
        boolean hasLeadership(UUID leaderSessionId);
    }
}
