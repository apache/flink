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
import org.apache.flink.runtime.blob.BlobStore;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedHaServicesWithLeadershipControl;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedJobResultStore;
import org.apache.flink.runtime.highavailability.nonha.embedded.HaLeadershipControl;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.jobmanager.StandaloneJobGraphStore;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderelection.StandaloneLeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.StandaloneLeaderRetrievalService;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.flink.shaded.guava30.com.google.common.base.MoreObjects;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * A variant of the HighAvailabilityServices for testing. Each individual service can be set to an
 * arbitrary implementation, such as a mock or default service.
 */
public abstract class TestingHighAvailabilityServices implements HighAvailabilityServices {

    public static EmptyBuilder newBuilder() {
        return new EmptyBuilder();
    }

    public static EmptyBuilder newStandaloneBuilder() {
        return new EmptyBuilder()
                .setCheckpointRecoveryFactory(new StandaloneCheckpointRecoveryFactory())
                .setJobGraphStore(new StandaloneJobGraphStore())
                .setDispatcherLeaderRetriever(
                        new StandaloneLeaderRetrievalService(
                                "localhost", HighAvailabilityServices.DEFAULT_LEADER_ID))
                .setResourceManagerLeaderRetriever(
                        new StandaloneLeaderRetrievalService(
                                "localhost", HighAvailabilityServices.DEFAULT_LEADER_ID))
                .setClusterRestEndpointLeaderRetriever(
                        new StandaloneLeaderRetrievalService(
                                "localhost", HighAvailabilityServices.DEFAULT_LEADER_ID))
                .setDispatcherLeaderElectionService(new StandaloneLeaderElectionService())
                .setResourceManagerLeaderElectionService(new StandaloneLeaderElectionService())
                .setClusterRestEndpointLeaderElectionService(new StandaloneLeaderElectionService())
                .setJobMasterLeaderRetrieverFunction(
                        jobId ->
                                new StandaloneLeaderRetrievalService(
                                        "localhost", HighAvailabilityServices.DEFAULT_LEADER_ID))
                .setJobMasterLeaderElectionServiceFunction(
                        jobId -> new StandaloneLeaderElectionService());
    }

    public static EmbeddedBuilder newEmbeddedBuilder(Executor executor) {
        return new EmbeddedBuilder(executor);
    }

    public abstract static class Builder<T extends Builder<T>> {

        protected @Nullable CheckpointRecoveryFactory checkpointRecoveryFactory;
        protected @Nullable JobGraphStore jobGraphStore;
        protected JobResultStore jobResultStore = new EmbeddedJobResultStore();
        protected CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        protected CompletableFuture<Void> closeAndCleanupAllDataFuture = new CompletableFuture<>();
        protected CompletableFuture<JobID> globalCleanupFuture = new CompletableFuture<>();

        private Builder() {}

        public T setCheckpointRecoveryFactory(CheckpointRecoveryFactory checkpointRecoveryFactory) {
            this.checkpointRecoveryFactory = checkpointRecoveryFactory;
            @SuppressWarnings("unchecked")
            final T cast = (T) this;
            return cast;
        }

        public T setJobGraphStore(JobGraphStore jobGraphStore) {
            this.jobGraphStore = jobGraphStore;
            @SuppressWarnings("unchecked")
            final T cast = (T) this;
            return cast;
        }

        public T setJobResultStore(JobResultStore jobResultStore) {
            this.jobResultStore = jobResultStore;
            @SuppressWarnings("unchecked")
            final T cast = (T) this;
            return cast;
        }

        public T setCloseFuture(CompletableFuture<Void> closeFuture) {
            this.closeFuture = closeFuture;
            @SuppressWarnings("unchecked")
            final T cast = (T) this;
            return cast;
        }

        public T setCloseAndCleanupAllDataFuture(
                CompletableFuture<Void> closeAndCleanupAllDataFuture) {
            this.closeAndCleanupAllDataFuture = closeAndCleanupAllDataFuture;
            @SuppressWarnings("unchecked")
            final T cast = (T) this;
            return cast;
        }

        public T setGlobalCleanupFuture(CompletableFuture<JobID> globalCleanupFuture) {
            this.globalCleanupFuture = globalCleanupFuture;
            @SuppressWarnings("unchecked")
            final T cast = (T) this;
            return cast;
        }

        public abstract TestingHighAvailabilityServices build();
    }

    public static class EmbeddedBuilder extends Builder<EmbeddedBuilder> {

        private final Executor executor;

        private EmbeddedBuilder(Executor executor) {
            this.executor = executor;
        }

        @Override
        public TestingHighAvailabilityServices build() {
            try {
                return new EmbeddedLeaderElection(
                        checkpointRecoveryFactory,
                        jobGraphStore,
                        jobResultStore,
                        closeFuture,
                        closeAndCleanupAllDataFuture,
                        globalCleanupFuture,
                        new EmbeddedHaServicesWithLeadershipControl(executor));
            } catch (Exception e) {
                throw new IllegalStateException("Error building embedded services.", e);
            }
        }
    }

    public static class EmptyBuilder extends Builder<EmptyBuilder> {

        private LeaderRetrievalService dispatcherLeaderRetriever;
        private LeaderRetrievalService resourceManagerLeaderRetriever;
        private LeaderRetrievalService clusterRestEndpointLeaderRetriever;

        private LeaderElectionService resourceManagerLeaderElectionService;
        private LeaderElectionService dispatcherLeaderElectionService;
        private LeaderElectionService clusterRestEndpointLeaderElectionService;

        private Function<JobID, LeaderRetrievalService> jobMasterLeaderRetrieverFunction =
                ignored -> null;
        private Function<JobID, LeaderElectionService> jobMasterLeaderElectionServiceFunction =
                ignored -> null;

        private EmptyBuilder() {}

        public TestingHighAvailabilityServices build() {
            return new ManualLeaderElection(
                    checkpointRecoveryFactory,
                    jobGraphStore,
                    jobResultStore,
                    closeFuture,
                    closeAndCleanupAllDataFuture,
                    globalCleanupFuture,
                    dispatcherLeaderRetriever,
                    resourceManagerLeaderRetriever,
                    clusterRestEndpointLeaderRetriever,
                    dispatcherLeaderElectionService,
                    resourceManagerLeaderElectionService,
                    clusterRestEndpointLeaderElectionService,
                    jobMasterLeaderRetrieverFunction,
                    jobMasterLeaderElectionServiceFunction);
        }

        public EmptyBuilder setResourceManagerLeaderRetriever(
                LeaderRetrievalService resourceManagerLeaderRetriever) {
            this.resourceManagerLeaderRetriever = resourceManagerLeaderRetriever;
            return this;
        }

        public EmptyBuilder setDispatcherLeaderRetriever(
                LeaderRetrievalService dispatcherLeaderRetriever) {
            this.dispatcherLeaderRetriever = dispatcherLeaderRetriever;
            return this;
        }

        public EmptyBuilder setClusterRestEndpointLeaderRetriever(
                final LeaderRetrievalService clusterRestEndpointLeaderRetriever) {
            this.clusterRestEndpointLeaderRetriever = clusterRestEndpointLeaderRetriever;
            return this;
        }

        public EmptyBuilder setResourceManagerLeaderElectionService(
                LeaderElectionService leaderElectionService) {
            this.resourceManagerLeaderElectionService = leaderElectionService;
            return this;
        }

        public EmptyBuilder setDispatcherLeaderElectionService(
                LeaderElectionService leaderElectionService) {
            this.dispatcherLeaderElectionService = leaderElectionService;
            return this;
        }

        public EmptyBuilder setClusterRestEndpointLeaderElectionService(
                final LeaderElectionService clusterRestEndpointLeaderElectionService) {
            this.clusterRestEndpointLeaderElectionService =
                    clusterRestEndpointLeaderElectionService;
            return this;
        }

        public EmptyBuilder setJobMasterLeaderElectionServiceFunction(
                Function<JobID, LeaderElectionService> jobMasterLeaderElectionServiceFunction) {
            this.jobMasterLeaderElectionServiceFunction = jobMasterLeaderElectionServiceFunction;
            return this;
        }

        public EmptyBuilder setJobMasterLeaderRetrieverFunction(
                Function<JobID, LeaderRetrievalService> jobMasterLeaderRetrieverFunction) {
            this.jobMasterLeaderRetrieverFunction = jobMasterLeaderRetrieverFunction;
            return this;
        }
    }

    public static class EmbeddedLeaderElection extends TestingHighAvailabilityServices {

        private final EmbeddedHaServicesWithLeadershipControl embeddedServices;

        private EmbeddedLeaderElection(
                @Nullable CheckpointRecoveryFactory checkpointRecoveryFactory,
                @Nullable JobGraphStore jobGraphStore,
                JobResultStore jobResultStore,
                CompletableFuture<Void> closeFuture,
                CompletableFuture<Void> closeAndCleanupAllDataFuture,
                CompletableFuture<JobID> globalCleanupFuture,
                EmbeddedHaServicesWithLeadershipControl embeddedServices)
                throws Exception {
            super(
                    MoreObjects.firstNonNull(
                            checkpointRecoveryFactory,
                            embeddedServices.getCheckpointRecoveryFactory()),
                    MoreObjects.firstNonNull(jobGraphStore, embeddedServices.getJobGraphStore()),
                    jobResultStore,
                    closeFuture,
                    closeAndCleanupAllDataFuture,
                    globalCleanupFuture);
            this.embeddedServices = embeddedServices;
        }

        @Override
        public LeaderRetrievalService getResourceManagerLeaderRetriever() {
            return embeddedServices.getResourceManagerLeaderRetriever();
        }

        @Override
        public LeaderRetrievalService getDispatcherLeaderRetriever() {
            return embeddedServices.getDispatcherLeaderRetriever();
        }

        @Override
        public LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobId) {
            return embeddedServices.getJobManagerLeaderRetriever(jobId);
        }

        @Override
        public LeaderRetrievalService getJobManagerLeaderRetriever(
                JobID jobId, String defaultJobManagerAddress) {
            return embeddedServices.getJobManagerLeaderRetriever(jobId, defaultJobManagerAddress);
        }

        @Override
        public LeaderElectionService getResourceManagerLeaderElectionService() {
            return embeddedServices.getResourceManagerLeaderElectionService();
        }

        @Override
        public LeaderElectionService getDispatcherLeaderElectionService() {
            return embeddedServices.getResourceManagerLeaderElectionService();
        }

        @Override
        public LeaderElectionService getJobManagerLeaderElectionService(JobID jobID) {
            return embeddedServices.getJobManagerLeaderElectionService(jobID);
        }

        @Override
        public LeaderElectionService getClusterRestEndpointLeaderElectionService() {
            return embeddedServices.getClusterRestEndpointLeaderElectionService();
        }

        @Override
        public LeaderRetrievalService getClusterRestEndpointLeaderRetriever() {
            return embeddedServices.getClusterRestEndpointLeaderRetriever();
        }

        @Override
        public HaLeadershipControl getLeadershipControl() {
            return embeddedServices;
        }
    }

    public static class ManualLeaderElection extends TestingHighAvailabilityServices {

        @Nullable private final LeaderRetrievalService dispatcherLeaderRetriever;
        @Nullable private final LeaderRetrievalService resourceManagerLeaderRetriever;
        @Nullable private final LeaderRetrievalService clusterRestEndpointLeaderRetriever;
        @Nullable private final LeaderElectionService dispatcherLeaderElectionService;
        @Nullable private final LeaderElectionService resourceManagerLeaderElectionService;
        @Nullable private final LeaderElectionService clusterRestEndpointLeaderElectionService;

        private final ConcurrentHashMap<JobID, LeaderRetrievalService> jobMasterLeaderRetrievers =
                new ConcurrentHashMap<>();
        private final ConcurrentHashMap<JobID, LeaderElectionService>
                jobManagerLeaderElectionServices = new ConcurrentHashMap<>();

        private final Function<JobID, LeaderRetrievalService> jobMasterLeaderRetrieverFunction;
        private final Function<JobID, LeaderElectionService> jobMasterLeaderElectionServiceFunction;

        private ManualLeaderElection(
                @Nullable CheckpointRecoveryFactory checkpointRecoveryFactory,
                @Nullable JobGraphStore jobGraphStore,
                JobResultStore jobResultStore,
                CompletableFuture<Void> closeFuture,
                CompletableFuture<Void> closeAndCleanupAllDataFuture,
                CompletableFuture<JobID> globalCleanupFuture,
                @Nullable LeaderRetrievalService dispatcherLeaderRetriever,
                @Nullable LeaderRetrievalService resourceManagerLeaderRetriever,
                @Nullable LeaderRetrievalService clusterRestEndpointLeaderRetriever,
                @Nullable LeaderElectionService dispatcherLeaderElectionService,
                @Nullable LeaderElectionService resourceManagerLeaderElectionService,
                @Nullable LeaderElectionService clusterRestEndpointLeaderElectionService,
                Function<JobID, LeaderRetrievalService> jobMasterLeaderRetrieverFunction,
                Function<JobID, LeaderElectionService> jobMasterLeaderElectionServiceFunction) {
            super(
                    checkpointRecoveryFactory,
                    jobGraphStore,
                    jobResultStore,
                    closeFuture,
                    closeAndCleanupAllDataFuture,
                    globalCleanupFuture);
            this.dispatcherLeaderRetriever = dispatcherLeaderRetriever;
            this.resourceManagerLeaderRetriever = resourceManagerLeaderRetriever;
            this.clusterRestEndpointLeaderRetriever = clusterRestEndpointLeaderRetriever;
            this.dispatcherLeaderElectionService = dispatcherLeaderElectionService;
            this.resourceManagerLeaderElectionService = resourceManagerLeaderElectionService;
            this.clusterRestEndpointLeaderElectionService =
                    clusterRestEndpointLeaderElectionService;
            this.jobMasterLeaderRetrieverFunction = jobMasterLeaderRetrieverFunction;
            this.jobMasterLeaderElectionServiceFunction = jobMasterLeaderElectionServiceFunction;
        }

        @Override
        public LeaderRetrievalService getResourceManagerLeaderRetriever() {
            if (resourceManagerLeaderRetriever == null) {
                throw new IllegalStateException("ResourceManagerLeaderRetriever has not been set");
            }
            return resourceManagerLeaderRetriever;
        }

        @Override
        public LeaderRetrievalService getDispatcherLeaderRetriever() {
            if (dispatcherLeaderRetriever == null) {
                throw new IllegalStateException("ResourceManagerLeaderRetriever has not been set");
            }
            return dispatcherLeaderRetriever;
        }

        @Override
        public LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID) {
            @Nullable
            final LeaderRetrievalService service =
                    jobMasterLeaderRetrievers.computeIfAbsent(
                            jobID, jobMasterLeaderRetrieverFunction);
            if (service == null) {
                throw new IllegalStateException("JobMasterLeaderRetriever has not been set");
            }
            return service;
        }

        @Override
        public LeaderRetrievalService getJobManagerLeaderRetriever(
                JobID jobID, String defaultJobManagerAddress) {
            return getJobManagerLeaderRetriever(jobID);
        }

        @Override
        public LeaderElectionService getResourceManagerLeaderElectionService() {
            if (resourceManagerLeaderElectionService == null) {
                throw new IllegalStateException(
                        "ResourceManagerLeaderElectionService has not been set");
            }
            return resourceManagerLeaderElectionService;
        }

        @Override
        public LeaderElectionService getDispatcherLeaderElectionService() {
            if (dispatcherLeaderElectionService == null) {
                throw new IllegalStateException("DispatcherLeaderElectionService has not been set");
            }
            return dispatcherLeaderElectionService;
        }

        @Override
        public LeaderElectionService getJobManagerLeaderElectionService(JobID jobID) {
            @Nullable
            final LeaderElectionService service =
                    jobManagerLeaderElectionServices.computeIfAbsent(
                            jobID, jobMasterLeaderElectionServiceFunction);
            if (service == null) {
                throw new IllegalStateException("JobMasterLeaderElectionService has not been set");
            }
            return service;
        }

        @Override
        public LeaderElectionService getClusterRestEndpointLeaderElectionService() {
            if (clusterRestEndpointLeaderElectionService == null) {
                throw new IllegalStateException(
                        "ClusterRestEndpointLeaderElectionService has not been set");
            }
            return dispatcherLeaderElectionService;
        }

        @Override
        public LeaderRetrievalService getClusterRestEndpointLeaderRetriever() {
            if (clusterRestEndpointLeaderRetriever == null) {
                throw new IllegalStateException(
                        "ClusterRestEndpointLeaderRetriever has not been set");
            }
            return dispatcherLeaderRetriever;
        }

        @Override
        public HaLeadershipControl getLeadershipControl() {
            throw new UnsupportedOperationException();
        }
    }

    @Nullable protected final CheckpointRecoveryFactory checkpointRecoveryFactory;
    @Nullable private final JobGraphStore jobGraphStore;
    private final JobResultStore jobResultStore;
    private final CompletableFuture<Void> closeFuture;
    private final CompletableFuture<Void> closeAndCleanupAllDataFuture;
    private final CompletableFuture<JobID> globalCleanupFuture;

    private TestingHighAvailabilityServices(
            @Nullable CheckpointRecoveryFactory checkpointRecoveryFactory,
            @Nullable JobGraphStore jobGraphStore,
            JobResultStore jobResultStore,
            CompletableFuture<Void> closeFuture,
            CompletableFuture<Void> closeAndCleanupAllDataFuture,
            CompletableFuture<JobID> globalCleanupFuture) {
        this.checkpointRecoveryFactory = checkpointRecoveryFactory;
        this.jobGraphStore = jobGraphStore;
        this.jobResultStore = jobResultStore;
        this.closeFuture = closeFuture;
        this.closeAndCleanupAllDataFuture = closeAndCleanupAllDataFuture;
        this.globalCleanupFuture = globalCleanupFuture;
    }

    @Override
    public CheckpointRecoveryFactory getCheckpointRecoveryFactory() {
        if (checkpointRecoveryFactory == null) {
            throw new IllegalStateException("CheckpointRecoveryFactory has not been set");
        }
        return checkpointRecoveryFactory;
    }

    @Override
    public JobGraphStore getJobGraphStore() {
        if (jobGraphStore == null) {
            throw new IllegalStateException("JobGraphStore has not been set");
        }
        return jobGraphStore;
    }

    @Override
    public JobResultStore getJobResultStore() {
        return jobResultStore;
    }

    @Override
    public BlobStore createBlobStore() throws IOException {
        return new VoidBlobStore();
    }

    // ------------------------------------------------------------------------
    //  Shutdown
    // ------------------------------------------------------------------------

    @Override
    public void close() throws Exception {
        closeFuture.complete(null);
    }

    @Override
    public void closeAndCleanupAllData() throws Exception {
        closeAndCleanupAllDataFuture.complete(null);
    }

    @Override
    public CompletableFuture<Void> globalCleanupAsync(JobID jobID, Executor executor) {
        if (globalCleanupFuture != null) {
            globalCleanupFuture.complete(jobID);
        }
        return FutureUtils.completedVoidFuture();
    }

    public abstract HaLeadershipControl getLeadershipControl();
}
