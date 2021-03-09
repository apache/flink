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

package org.apache.flink.runtime.leaderretrieval;

import org.apache.flink.runtime.highavailability.HighAvailabilityServices;

import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Standalone implementation of the {@link LeaderRetrievalService}. This implementation assumes that
 * there is only a single contender for leadership (e.g., a single JobManager or ResourceManager
 * process) and that this process is reachable under a constant address.
 *
 * <p>As soon as this service is started, it immediately notifies the leader listener of the leader
 * contender with the pre-configured address.
 */
public class StandaloneLeaderRetrievalService implements LeaderRetrievalService {

    private final Object startStopLock = new Object();

    /** The fix address of the leader. */
    private final String leaderAddress;

    /** The fix leader ID (leader lock fencing token). */
    private final UUID leaderId;

    /** Flag whether this service is started. */
    private boolean started;

    /**
     * Creates a StandaloneLeaderRetrievalService with the given leader address. The leaderId will
     * be null.
     *
     * @param leaderAddress The leader's pre-configured address
     * @deprecated Use {@link #StandaloneLeaderRetrievalService(String, UUID)} instead
     */
    @Deprecated
    public StandaloneLeaderRetrievalService(String leaderAddress) {
        this.leaderAddress = checkNotNull(leaderAddress);
        this.leaderId = HighAvailabilityServices.DEFAULT_LEADER_ID;
    }

    /**
     * Creates a StandaloneLeaderRetrievalService with the given leader address.
     *
     * @param leaderAddress The leader's pre-configured address
     * @param leaderId The constant leaderId.
     */
    public StandaloneLeaderRetrievalService(String leaderAddress, UUID leaderId) {
        this.leaderAddress = checkNotNull(leaderAddress);
        this.leaderId = checkNotNull(leaderId);
    }

    // ------------------------------------------------------------------------

    @Override
    public void start(LeaderRetrievalListener listener) {
        checkNotNull(listener, "Listener must not be null.");

        synchronized (startStopLock) {
            checkState(!started, "StandaloneLeaderRetrievalService can only be started once.");
            started = true;

            // directly notify the listener, because we already know the leading JobManager's
            // address
            listener.notifyLeaderAddress(leaderAddress, leaderId);
        }
    }

    @Override
    public void stop() {
        synchronized (startStopLock) {
            started = false;
        }
    }
}
