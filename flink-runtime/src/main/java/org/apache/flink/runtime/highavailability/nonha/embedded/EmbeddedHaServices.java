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

package org.apache.flink.runtime.highavailability.nonha.embedded;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.nonha.AbstractNonHaServices;
import org.apache.flink.runtime.leaderelection.LeaderElection;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of the {@link HighAvailabilityServices} for the non-high-availability case
 * where all participants (ResourceManager, JobManagers, TaskManagers) run in the same process.
 *
 * <p>This implementation has no dependencies on any external services. It returns a fix
 * pre-configured ResourceManager, and stores checkpoints and metadata simply on the heap or on a
 * local file system and therefore in a storage without guarantees.
 */
public class EmbeddedHaServices extends AbstractNonHaServices {

    private final Executor executor;

    private final EmbeddedLeaderService clusterLeaderService;

    public EmbeddedHaServices(Executor executor) {
        this.executor = Preconditions.checkNotNull(executor);
        this.clusterLeaderService = createEmbeddedLeaderService(executor);
    }

    // ------------------------------------------------------------------------
    //  services
    // ------------------------------------------------------------------------

    @Override
    public LeaderRetrievalService getResourceManagerLeaderRetriever() {
        return clusterLeaderService.createLeaderRetrievalService("resource_manager");
    }

    @Override
    public LeaderRetrievalService getDispatcherLeaderRetriever() {
        return clusterLeaderService.createLeaderRetrievalService("dispatcher");
    }

    @Override
    public LeaderElection getResourceManagerLeaderElection() {
        return clusterLeaderService.createLeaderElectionService("resource_manager");
    }

    @Override
    public LeaderElection getDispatcherLeaderElection() {
        return clusterLeaderService.createLeaderElectionService("dispatcher");
    }

    @Override
    public LeaderRetrievalService getClusterRestEndpointLeaderRetriever() {
        return clusterLeaderService.createLeaderRetrievalService("rest_server");
    }

    @Override
    public LeaderElection getJobManagerLeaderElection(JobID jobID) {
        checkNotNull(jobID);
        return clusterLeaderService.createLeaderElectionService("job-" + jobID);
    }

    @Override
    public LeaderElection getClusterRestEndpointLeaderElection() {
        return clusterLeaderService.createLeaderElectionService("rest_server");
    }

    // ------------------------------------------------------------------------
    // internal
    // ------------------------------------------------------------------------

    EmbeddedLeaderService getClusterLeaderService() {
        return clusterLeaderService;
    }

    EmbeddedLeaderService getDispatcherLeaderService() {
        return clusterLeaderService;
    }

    EmbeddedLeaderService getJobManagerLeaderService(JobID jobId) {
        return clusterLeaderService;
    }

    EmbeddedLeaderService getResourceManagerLeaderService() {
        return clusterLeaderService;
    }

    @Nonnull
    private EmbeddedLeaderService createEmbeddedLeaderService(Executor executor) {
        return new EmbeddedLeaderService(executor);
    }

    // ------------------------------------------------------------------------
    //  shutdown
    // ------------------------------------------------------------------------

    @Override
    public void close() throws Exception {
        synchronized (lock) {
            if (!isShutDown()) {
                clusterLeaderService.shutdown();
            }

            super.close();
        }
    }
}
