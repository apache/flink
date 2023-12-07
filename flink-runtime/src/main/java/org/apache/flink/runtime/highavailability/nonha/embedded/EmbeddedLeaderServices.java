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
import org.apache.flink.runtime.leaderelection.LeaderElection;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.leaderservice.LeaderServices;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import java.util.HashMap;
import java.util.concurrent.Executor;

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
public class EmbeddedLeaderServices implements LeaderServices {
    private final EmbeddedLeaderElectionService resourceManagerLeaderService;

    private final EmbeddedLeaderElectionService dispatcherLeaderService;

    private final HashMap<JobID, EmbeddedLeaderElectionService> jobManagerLeaderServices;

    private final EmbeddedLeaderElectionService restEndpointLeaderService;

    private final Object lock;

    private final Executor executor;

    private boolean closed;

    public EmbeddedLeaderServices(Executor executor) {
        this.resourceManagerLeaderService = createEmbeddedLeaderService(executor);
        this.dispatcherLeaderService = createEmbeddedLeaderService(executor);
        this.jobManagerLeaderServices = new HashMap<>();
        this.restEndpointLeaderService = createEmbeddedLeaderService(executor);
        this.lock = new Object();
        this.executor = executor;
        this.closed = false;
    }

    @Nonnull
    private EmbeddedLeaderElectionService createEmbeddedLeaderService(Executor executor) {
        return new EmbeddedLeaderElectionService(executor);
    }

    @Override
    public LeaderElection getRestEndpointLeaderElection() {
        return restEndpointLeaderService.createLeaderElection("rest_server");
    }

    @Override
    public LeaderRetrievalService getRestEndpointLeaderRetriever() {
        return restEndpointLeaderService.createLeaderRetrievalService();
    }

    @Override
    public LeaderElection getResourceManagerLeaderElection() {
        return resourceManagerLeaderService.createLeaderElection("resource_manager");
    }

    @Override
    public LeaderRetrievalService getResourceManagerLeaderRetriever() {
        return resourceManagerLeaderService.createLeaderRetrievalService();
    }

    @Override
    public LeaderElection getDispatcherLeaderElection() {
        return dispatcherLeaderService.createLeaderElection("dispatcher");
    }

    @Override
    public LeaderRetrievalService getDispatcherLeaderRetriever() {
        return dispatcherLeaderService.createLeaderRetrievalService();
    }

    @Override
    public LeaderRetrievalService getJobMasterLeaderRetriever(
            JobID jobID, String defaultJobManagerAddress) {
        checkNotNull(jobID);

        synchronized (lock) {
            checkNotClose();
            EmbeddedLeaderElectionService service = getOrCreateJobManagerService(jobID);
            return service.createLeaderRetrievalService();
        }
    }

    @GuardedBy("lock")
    private EmbeddedLeaderElectionService getOrCreateJobManagerService(JobID jobID) {
        EmbeddedLeaderElectionService service = jobManagerLeaderServices.get(jobID);
        if (service == null) {
            service = createEmbeddedLeaderService(executor);
            jobManagerLeaderServices.put(jobID, service);
        }
        return service;
    }

    @Override
    public LeaderElection getJobMasterLeaderElection(JobID jobID) {
        checkNotNull(jobID);

        synchronized (lock) {
            checkNotClose();
            EmbeddedLeaderElectionService service = getOrCreateJobManagerService(jobID);
            return service.createLeaderElection("job-" + jobID);
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

    // ------------------------------------------------------------------------
    // internal
    // ------------------------------------------------------------------------

    public EmbeddedLeaderElectionService getDispatcherLeaderService() {
        return dispatcherLeaderService;
    }

    public EmbeddedLeaderElectionService getJobManagerLeaderService(JobID jobId) {
        return jobManagerLeaderServices.get(jobId);
    }

    public EmbeddedLeaderElectionService getResourceManagerLeaderService() {
        return resourceManagerLeaderService;
    }
}
