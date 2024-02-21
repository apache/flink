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

package org.apache.flink.runtime.highavailability;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedJobResultStore;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.jobmanager.StandaloneJobGraphStore;
import org.apache.flink.runtime.leaderelection.LeaderElection;
import org.apache.flink.runtime.leaderelection.StandaloneLeaderElection;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.StandaloneLeaderRetrievalService;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/** Builder for the {@link TestingHighAvailabilityServices}. */
public class TestingHighAvailabilityServicesBuilder {

    private LeaderRetrievalService resourceManagerLeaderRetriever =
            new StandaloneLeaderRetrievalService(
                    "localhost", HighAvailabilityServices.DEFAULT_LEADER_ID);

    private LeaderRetrievalService dispatcherLeaderRetriever =
            new StandaloneLeaderRetrievalService(
                    "localhost", HighAvailabilityServices.DEFAULT_LEADER_ID);

    private LeaderRetrievalService webMonitorEndpointLeaderRetriever =
            new StandaloneLeaderRetrievalService(
                    "localhost", HighAvailabilityServices.DEFAULT_LEADER_ID);

    private Function<JobID, LeaderRetrievalService> jobMasterLeaderRetrieverFunction =
            jobId ->
                    new StandaloneLeaderRetrievalService(
                            "localhost", HighAvailabilityServices.DEFAULT_LEADER_ID);

    private Function<JobID, LeaderElection> jobMasterLeaderElectionFunction =
            jobId -> new StandaloneLeaderElection(UUID.randomUUID());

    private LeaderElection resourceManagerLeaderElection =
            new StandaloneLeaderElection(UUID.randomUUID());

    private LeaderElection dispatcherLeaderElection =
            new StandaloneLeaderElection(UUID.randomUUID());

    private LeaderElection webMonitorEndpointLeaderElection =
            new StandaloneLeaderElection(UUID.randomUUID());

    private CheckpointRecoveryFactory checkpointRecoveryFactory =
            new StandaloneCheckpointRecoveryFactory();

    private JobGraphStore jobGraphStore = new StandaloneJobGraphStore();

    private JobResultStore jobResultStore = new EmbeddedJobResultStore();

    private CompletableFuture<Void> closeFuture = new CompletableFuture<>();

    private CompletableFuture<Void> cleanupAllDataFuture = new CompletableFuture<>();

    public TestingHighAvailabilityServices build() {
        final TestingHighAvailabilityServices testingHighAvailabilityServices =
                new TestingHighAvailabilityServices();

        testingHighAvailabilityServices.setResourceManagerLeaderRetriever(
                resourceManagerLeaderRetriever);
        testingHighAvailabilityServices.setDispatcherLeaderRetriever(dispatcherLeaderRetriever);
        testingHighAvailabilityServices.setClusterRestEndpointLeaderRetriever(
                webMonitorEndpointLeaderRetriever);

        testingHighAvailabilityServices.setJobMasterLeaderRetrieverFunction(
                jobMasterLeaderRetrieverFunction);
        testingHighAvailabilityServices.setJobMasterLeaderElectionFunction(
                jobMasterLeaderElectionFunction);

        testingHighAvailabilityServices.setResourceManagerLeaderElection(
                resourceManagerLeaderElection);
        testingHighAvailabilityServices.setDispatcherLeaderElection(dispatcherLeaderElection);
        testingHighAvailabilityServices.setClusterRestEndpointLeaderElection(
                webMonitorEndpointLeaderElection);

        testingHighAvailabilityServices.setCheckpointRecoveryFactory(checkpointRecoveryFactory);
        testingHighAvailabilityServices.setJobGraphStore(jobGraphStore);
        testingHighAvailabilityServices.setJobResultStore(jobResultStore);

        testingHighAvailabilityServices.setCloseFuture(closeFuture);
        testingHighAvailabilityServices.setCleanupAllDataFuture(cleanupAllDataFuture);

        return testingHighAvailabilityServices;
    }

    public TestingHighAvailabilityServicesBuilder setResourceManagerLeaderRetriever(
            LeaderRetrievalService resourceManagerLeaderRetriever) {
        this.resourceManagerLeaderRetriever = resourceManagerLeaderRetriever;
        return this;
    }

    public TestingHighAvailabilityServicesBuilder setDispatcherLeaderRetriever(
            LeaderRetrievalService dispatcherLeaderRetriever) {
        this.dispatcherLeaderRetriever = dispatcherLeaderRetriever;
        return this;
    }

    public TestingHighAvailabilityServicesBuilder setWebMonitorEndpointLeaderRetriever(
            LeaderRetrievalService webMonitorEndpointLeaderRetriever) {
        this.webMonitorEndpointLeaderRetriever = webMonitorEndpointLeaderRetriever;
        return this;
    }

    public TestingHighAvailabilityServicesBuilder setJobMasterLeaderRetrieverFunction(
            Function<JobID, LeaderRetrievalService> jobMasterLeaderRetrieverFunction) {
        this.jobMasterLeaderRetrieverFunction = jobMasterLeaderRetrieverFunction;
        return this;
    }

    public TestingHighAvailabilityServicesBuilder setJobMasterLeaderElectionFunction(
            Function<JobID, LeaderElection> jobMasterLeaderElectionFunction) {
        this.jobMasterLeaderElectionFunction = jobMasterLeaderElectionFunction;
        return this;
    }

    public TestingHighAvailabilityServicesBuilder setResourceManagerLeaderElection(
            LeaderElection resourceManagerLeaderElection) {
        this.resourceManagerLeaderElection = resourceManagerLeaderElection;
        return this;
    }

    public TestingHighAvailabilityServicesBuilder setDispatcherLeaderElection(
            LeaderElection dispatcherLeaderElection) {
        this.dispatcherLeaderElection = dispatcherLeaderElection;
        return this;
    }

    public TestingHighAvailabilityServicesBuilder setWebMonitorEndpointLeaderElection(
            LeaderElection webMonitorEndpointLeaderElection) {
        this.webMonitorEndpointLeaderElection = webMonitorEndpointLeaderElection;
        return this;
    }

    public TestingHighAvailabilityServicesBuilder setCheckpointRecoveryFactory(
            CheckpointRecoveryFactory checkpointRecoveryFactory) {
        this.checkpointRecoveryFactory = checkpointRecoveryFactory;
        return this;
    }

    public TestingHighAvailabilityServicesBuilder setJobGraphStore(JobGraphStore jobGraphStore) {
        this.jobGraphStore = jobGraphStore;
        return this;
    }

    public TestingHighAvailabilityServicesBuilder setJobResultStore(JobResultStore jobResultStore) {
        this.jobResultStore = jobResultStore;
        return this;
    }

    public TestingHighAvailabilityServicesBuilder setCloseFuture(
            CompletableFuture<Void> closeFuture) {
        this.closeFuture = closeFuture;
        return this;
    }

    public TestingHighAvailabilityServicesBuilder setCleanupAllDataFuture(
            CompletableFuture<Void> cleanupAllDataFuture) {
        this.cleanupAllDataFuture = cleanupAllDataFuture;
        return this;
    }
}
