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
import javax.annotation.concurrent.GuardedBy;

import java.util.HashMap;
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

    private final EmbeddedLeaderService resourceManagerLeaderService;

    private final EmbeddedLeaderService dispatcherLeaderService;

    private final HashMap<JobID, EmbeddedLeaderService> jobManagerLeaderServices;

    private final EmbeddedLeaderService clusterRestEndpointLeaderService;

    public EmbeddedHaServices(Executor executor) {
        this.executor = Preconditions.checkNotNull(executor);
        this.resourceManagerLeaderService = createEmbeddedLeaderService(executor);
        this.dispatcherLeaderService = createEmbeddedLeaderService(executor);
        this.jobManagerLeaderServices = new HashMap<>();
        this.clusterRestEndpointLeaderService = createEmbeddedLeaderService(executor);
    }

    // ------------------------------------------------------------------------
    //  services
    // ------------------------------------------------------------------------

    @Override
    public LeaderRetrievalService getResourceManagerLeaderRetriever() {
        return resourceManagerLeaderService.createLeaderRetrievalService();
    }

    @Override
    public LeaderRetrievalService getDispatcherLeaderRetriever() {
        return dispatcherLeaderService.createLeaderRetrievalService();
    }

    @Override
    public LeaderElection getResourceManagerLeaderElection() {
        return resourceManagerLeaderService.createLeaderElectionService("resource_manager");
    }

    @Override
    public LeaderElection getDispatcherLeaderElection() {
        return dispatcherLeaderService.createLeaderElectionService("dispatcher");
    }

    @Override
    public LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID) {
        checkNotNull(jobID);

        synchronized (lock) {
            checkNotShutdown();
            EmbeddedLeaderService service = getOrCreateJobManagerService(jobID);
            return service.createLeaderRetrievalService();
        }
    }

    @Override
    public LeaderRetrievalService getJobManagerLeaderRetriever(
            JobID jobID, String defaultJobManagerAddress) {
        return getJobManagerLeaderRetriever(jobID);
    }

    @Override
    public LeaderRetrievalService getClusterRestEndpointLeaderRetriever() {
        return clusterRestEndpointLeaderService.createLeaderRetrievalService();
    }

    @Override
    public LeaderElection getJobManagerLeaderElection(JobID jobID) {
        checkNotNull(jobID);

        synchronized (lock) {
            checkNotShutdown();
            EmbeddedLeaderService service = getOrCreateJobManagerService(jobID);
            return service.createLeaderElectionService("job-" + jobID);
        }
    }

    @Override
    public LeaderElection getClusterRestEndpointLeaderElection() {
        return clusterRestEndpointLeaderService.createLeaderElectionService("rest_server");
    }

    // ------------------------------------------------------------------------
    // internal
    // ------------------------------------------------------------------------

    EmbeddedLeaderService getDispatcherLeaderService() {
        return dispatcherLeaderService;
    }

    EmbeddedLeaderService getJobManagerLeaderService(JobID jobId) {
        return jobManagerLeaderServices.get(jobId);
    }

    EmbeddedLeaderService getResourceManagerLeaderService() {
        return resourceManagerLeaderService;
    }

    @Nonnull
    private EmbeddedLeaderService createEmbeddedLeaderService(Executor executor) {
        return new EmbeddedLeaderService(executor);
    }

    @GuardedBy("lock")
    private EmbeddedLeaderService getOrCreateJobManagerService(JobID jobID) {
        EmbeddedLeaderService service = jobManagerLeaderServices.get(jobID);
        if (service == null) {
            service = createEmbeddedLeaderService(executor);
            jobManagerLeaderServices.put(jobID, service);
        }
        return service;
    }

    // ------------------------------------------------------------------------
    //  shutdown
    // ------------------------------------------------------------------------

    @Override
    public void close() throws Exception {
        synchronized (lock) {
            if (!isShutDown()) {
                // stop all job manager leader services
                for (EmbeddedLeaderService service : jobManagerLeaderServices.values()) {
                    service.shutdown();
                }
                jobManagerLeaderServices.clear();

                resourceManagerLeaderService.shutdown();

                clusterRestEndpointLeaderService.shutdown();
            }

            super.close();
        }
    }
}
