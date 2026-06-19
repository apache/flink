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

import java.util.UUID;

/**
 * A leader election driver that allows to write {@link LeaderInformation} for multiple components.
 */
public interface LeaderElectionDriver extends AutoCloseable {

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
     */
    void publishLeaderInformation(String componentId, LeaderInformation leaderInformation);

    /**
     * Deletes the leader information for the given component.
     *
     * @param componentId identifying the component for which to delete the leader information
     */
    void deleteLeaderInformation(String componentId);

    /** Listener interface for state changes of the {@link LeaderElectionDriver}. */
    interface Listener {

        /** Callback that is called once the driver obtains the leadership. */
        void onGrantLeadership(UUID leaderSessionID);

        /** Callback that is called once the driver loses the leadership. */
        void onRevokeLeadership();

        /**
         * Notifies the listener about a changed leader information for the given component.
         *
         * @param componentId identifying the component whose leader information has changed
         * @param leaderInformation new leader information
         */
        void onLeaderInformationChange(String componentId, LeaderInformation leaderInformation);

        /** Notifies the listener about all currently known leader information. */
        void onLeaderInformationChange(LeaderInformationRegister leaderInformationRegister);

        /** Notifies the listener if an error occurred. */
        void onError(Throwable t);
    }
}
