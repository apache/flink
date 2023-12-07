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

package org.apache.flink.runtime.highavailability.nonha.standalone;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.leaderelection.LeaderElection;
import org.apache.flink.runtime.leaderelection.StandaloneLeaderElection;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.StandaloneLeaderRetrievalService;
import org.apache.flink.runtime.leaderservice.LeaderServices;

import javax.annotation.concurrent.GuardedBy;

import static org.apache.flink.runtime.highavailability.HighAvailabilityServices.DEFAULT_LEADER_ID;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An implementation of the {@link org.apache.flink.runtime.leaderservice.LeaderServices} for the
 * non-high-availability case. This implementation can be used for testing, and for cluster setups
 * that do not tolerate failures of the master processes (JobManager, ResourceManager).
 *
 * <p>This implementation has no dependencies on any external services. It returns a fix
 * pre-configured ResourceManager and JobManager.
 */
public class StandaloneLeaderServices implements LeaderServices {

    /** The fix address of the ResourceManager. */
    private final String resourceManagerAddress;

    /** The fix address of the Dispatcher. */
    private final String dispatcherAddress;

    private final String clusterRestEndpointAddress;

    private final Object lock;

    private boolean closed;

    /**
     * Creates a new services class for the fix pre-defined leaders.
     *
     * @param resourceManagerAddress The fix address of the ResourceManager
     * @param clusterRestEndpointAddress
     */
    public StandaloneLeaderServices(
            String resourceManagerAddress,
            String dispatcherAddress,
            String clusterRestEndpointAddress) {
        this.resourceManagerAddress =
                checkNotNull(resourceManagerAddress, "resourceManagerAddress");
        this.dispatcherAddress = checkNotNull(dispatcherAddress, "dispatcherAddress");
        this.clusterRestEndpointAddress =
                checkNotNull(clusterRestEndpointAddress, clusterRestEndpointAddress);
        this.lock = new Object();
        this.closed = false;
    }

    @Override
    public LeaderElection getResourceManagerLeaderElection() {
        synchronized (lock) {
            checkNotClose();

            return new StandaloneLeaderElection(DEFAULT_LEADER_ID);
        }
    }

    @Override
    public LeaderRetrievalService getResourceManagerLeaderRetriever() {
        synchronized (lock) {
            checkNotClose();

            return new StandaloneLeaderRetrievalService(resourceManagerAddress, DEFAULT_LEADER_ID);
        }
    }

    @Override
    public LeaderElection getDispatcherLeaderElection() {
        synchronized (lock) {
            checkNotClose();

            return new StandaloneLeaderElection(DEFAULT_LEADER_ID);
        }
    }

    @Override
    public LeaderRetrievalService getDispatcherLeaderRetriever() {
        synchronized (lock) {
            checkNotClose();

            return new StandaloneLeaderRetrievalService(dispatcherAddress, DEFAULT_LEADER_ID);
        }
    }

    @Override
    public LeaderRetrievalService getJobMasterLeaderRetriever(
            JobID jobID, String defaultJobManagerAddress) {
        synchronized (lock) {
            checkNotClose();

            return new StandaloneLeaderRetrievalService(
                    defaultJobManagerAddress, DEFAULT_LEADER_ID);
        }
    }

    @Override
    public LeaderElection getJobMasterLeaderElection(JobID jobID) {
        synchronized (lock) {
            checkNotClose();

            return new StandaloneLeaderElection(DEFAULT_LEADER_ID);
        }
    }

    @Override
    public LeaderElection getRestEndpointLeaderElection() {
        synchronized (lock) {
            checkNotClose();

            return new StandaloneLeaderElection(DEFAULT_LEADER_ID);
        }
    }

    @Override
    public LeaderRetrievalService getRestEndpointLeaderRetriever() {
        synchronized (lock) {
            checkNotClose();

            return new StandaloneLeaderRetrievalService(
                    clusterRestEndpointAddress, DEFAULT_LEADER_ID);
        }
    }

    @GuardedBy("lock")
    protected void checkNotClose() {
        checkState(!closed, "leader services are shut down");
    }

    @Override
    public void close() throws Exception {
        synchronized (lock) {
            if (!closed) {
                closed = true;
            }
        }
    }
}
