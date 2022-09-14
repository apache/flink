/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.leaderelection;

import java.util.Collection;

/**
 * A leader election driver that allows to write {@link LeaderInformation} for multiple components.
 */
public interface MultipleComponentLeaderElectionDriver {

    /**
     * Closes the driver.
     *
     * @throws Exception if closing this driver fails
     */
    void close() throws Exception;

    /**
     * Returns whether the driver has currently leadership.
     *
     * @return {@code true} if the driver has leadership, otherwise {@code false}
     */
    boolean hasLeadership();

    /**
     * Publishes the leader information for the given component.
     *
     * @param componentId identifying the component for which to publish the leader information
     * @param leaderInformation leader information of the respective component
     * @throws Exception if publishing fails
     */
    void publishLeaderInformation(String componentId, LeaderInformation leaderInformation)
            throws Exception;

    /**
     * Deletes the leader information for the given component.
     *
     * @param componentId identifying the component for which to delete the leader information
     * @throws Exception if deleting fails
     */
    void deleteLeaderInformation(String componentId) throws Exception;

    /**
     * Listener interface for state changes of the {@link MultipleComponentLeaderElectionDriver}.
     */
    interface Listener {

        /** Callback that is called once the driver obtains the leadership. */
        void isLeader();

        /** Callback that is called once the driver loses the leadership. */
        void notLeader();

        /**
         * Notifies the listener about a changed leader information for the given component.
         *
         * @param componentId identifying the component whose leader information has changed
         * @param leaderInformation new leader information
         */
        void notifyLeaderInformationChange(String componentId, LeaderInformation leaderInformation);

        /**
         * Notifies the listener about all currently known leader information.
         *
         * @param leaderInformationWithComponentIds leader information with component ids
         */
        void notifyAllKnownLeaderInformation(
                Collection<LeaderInformationWithComponentId> leaderInformationWithComponentIds);
    }
}
