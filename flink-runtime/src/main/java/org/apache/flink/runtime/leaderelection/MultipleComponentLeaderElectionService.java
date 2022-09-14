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

/**
 * Leader election service that allows to register multiple {@link LeaderElectionEventHandler
 * LeaderElectionEventHandlers} that are identified by different names. For each event handler it is
 * possible to write the corresponding {@link LeaderInformation}.
 */
public interface MultipleComponentLeaderElectionService {

    /**
     * Closes this service.
     *
     * @throws Exception if the service failed to close
     */
    void close() throws Exception;

    /**
     * Creates a {@link LeaderElectionDriverFactory} for the given leader name.
     *
     * @param componentId identifying the component for which to create a leader election driver
     *     factory
     * @return Leader election driver factory
     */
    LeaderElectionDriverFactory createDriverFactory(String componentId);

    /**
     * Publishes the given leader information for the component identified by the given leader name.
     *
     * @param componentId identifying the component
     * @param leaderInformation leader information
     */
    void publishLeaderInformation(String componentId, LeaderInformation leaderInformation);

    /**
     * Registers a new leader election event handler under the given component id.
     *
     * @param componentId identifying the leader election event handler
     * @param leaderElectionEventHandler leader election event handler to register
     * @throws IllegalArgumentException if there is already a handler registered for the given
     *     component id
     */
    void registerLeaderElectionEventHandler(
            String componentId, LeaderElectionEventHandler leaderElectionEventHandler);

    /**
     * Unregisters the leader election event handler with the given component id.
     *
     * @param componentId identifying the component
     * @throws Exception if the leader election event handler could not be unregistered
     */
    void unregisterLeaderElectionEventHandler(String componentId) throws Exception;

    /**
     * Returns whether the given component has leadership.
     *
     * @param componentId identifying the component
     * @return {@code true} if the component has leadership otherwise {@code false}
     */
    boolean hasLeadership(String componentId);
}
