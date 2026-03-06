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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.application.AbstractApplication;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.dispatcher.cleanup.ApplicationResourceCleanerFactory;
import org.apache.flink.runtime.dispatcher.cleanup.CleanupRunnerFactory;
import org.apache.flink.runtime.dispatcher.cleanup.DispatcherApplicationResourceCleanerFactory;
import org.apache.flink.runtime.dispatcher.cleanup.DispatcherResourceCleanerFactory;
import org.apache.flink.runtime.dispatcher.cleanup.ResourceCleanerFactory;
import org.apache.flink.runtime.dispatcher.cleanup.TestingCleanupRunnerFactory;
import org.apache.flink.runtime.dispatcher.cleanup.TestingRetryStrategies;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.HeartbeatServicesImpl;
import org.apache.flink.runtime.highavailability.ApplicationResult;
import org.apache.flink.runtime.highavailability.ApplicationResultStore;
import org.apache.flink.runtime.highavailability.EmbeddedApplicationResultStore;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedJobResultStore;
import org.apache.flink.runtime.jobmanager.ApplicationWriter;
import org.apache.flink.runtime.jobmanager.ExecutionPlanWriter;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;

/** {@link Dispatcher} implementation used for testing purposes. */
class TestingDispatcher extends Dispatcher {

    private final CompletableFuture<Void> startFuture;

    private TestingDispatcher(
            RpcService rpcService,
            DispatcherId fencingToken,
            Collection<ExecutionPlan> recoveredJobs,
            Collection<JobResult> recoveredDirtyJobs,
            Collection<AbstractApplication> recoveredApplications,
            Collection<ApplicationResult> recoveredDirtyApplicationResults,
            Configuration configuration,
            HighAvailabilityServices highAvailabilityServices,
            GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
            HeartbeatServices heartbeatServices,
            BlobServer blobServer,
            FatalErrorHandler fatalErrorHandler,
            ExecutionPlanWriter executionPlanWriter,
            JobResultStore jobResultStore,
            ApplicationWriter applicationWriter,
            ApplicationResultStore applicationResultStore,
            JobManagerMetricGroup jobManagerMetricGroup,
            @Nullable String metricServiceQueryAddress,
            Executor ioExecutor,
            HistoryServerArchivist historyServerArchivist,
            ArchivedApplicationStore archivedApplicationStore,
            JobManagerRunnerFactory jobManagerRunnerFactory,
            CleanupRunnerFactory cleanupRunnerFactory,
            DispatcherBootstrapFactory dispatcherBootstrapFactory,
            DispatcherOperationCaches dispatcherOperationCaches,
            JobManagerRunnerRegistry jobManagerRunnerRegistry,
            ResourceCleanerFactory resourceCleanerFactory,
            ApplicationResourceCleanerFactory applicationResourceCleanerFactory)
            throws Exception {
        super(
                rpcService,
                fencingToken,
                recoveredJobs,
                recoveredDirtyJobs,
                recoveredApplications,
                recoveredDirtyApplicationResults,
                dispatcherBootstrapFactory,
                new DispatcherServices(
                        configuration,
                        highAvailabilityServices,
                        resourceManagerGatewayRetriever,
                        blobServer,
                        heartbeatServices,
                        archivedApplicationStore,
                        fatalErrorHandler,
                        historyServerArchivist,
                        metricServiceQueryAddress,
                        dispatcherOperationCaches,
                        jobManagerMetricGroup,
                        executionPlanWriter,
                        jobResultStore,
                        applicationWriter,
                        applicationResultStore,
                        jobManagerRunnerFactory,
                        cleanupRunnerFactory,
                        ioExecutor,
                        Collections.emptySet()),
                jobManagerRunnerRegistry,
                resourceCleanerFactory,
                applicationResourceCleanerFactory);

        this.startFuture = new CompletableFuture<>();
    }

    @Override
    public void onStart() throws Exception {
        try {
            super.onStart();
        } catch (Exception e) {
            startFuture.completeExceptionally(e);
            throw e;
        }

        startFuture.complete(null);
    }

    void completeJobExecution(ExecutionGraphInfo executionGraphInfo) {
        runAsync(
                () -> {
                    try {
                        jobReachedTerminalState(executionGraphInfo);
                    } catch (Exception e) {
                        throw new CompletionException(e);
                    }
                });
    }

    <T> CompletableFuture<T> callAsyncInMainThread(Callable<CompletableFuture<T>> callable) {
        return callAsync(callable, TestingUtils.TESTING_DURATION).thenCompose(Function.identity());
    }

    CompletableFuture<Void> getJobTerminationFuture(
            @Nonnull JobID jobId, @Nonnull Duration timeout) {
        return callAsync(() -> getJobTerminationFuture(jobId), timeout)
                .thenCompose(Function.identity());
    }

    CompletableFuture<Integer> getNumberJobs(Duration timeout) {
        return callAsync(() -> listJobs(timeout).get().size(), timeout);
    }

    void waitUntilStarted() {
        startFuture.join();
    }

    public static TestingDispatcher.Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private DispatcherId fencingToken = DispatcherId.generate();
        private Collection<ExecutionPlan> recoveredJobs = Collections.emptyList();
        @Nullable private Collection<JobResult> recoveredDirtyJobs = null;
        private Collection<AbstractApplication> recoveredApplications = Collections.emptyList();
        @Nullable private Collection<ApplicationResult> recoveredDirtyApplications = null;
        private HighAvailabilityServices highAvailabilityServices =
                new TestingHighAvailabilityServices();

        private TestingResourceManagerGateway resourceManagerGateway =
                new TestingResourceManagerGateway();
        private GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever =
                () -> CompletableFuture.completedFuture(resourceManagerGateway);
        private HeartbeatServices heartbeatServices = new HeartbeatServicesImpl(1000L, 1000L);

        private ExecutionPlanWriter executionPlanWriter = NoOpExecutionPlanWriter.INSTANCE;
        private JobResultStore jobResultStore = new EmbeddedJobResultStore();

        private ApplicationWriter applicationWriter = NoOpApplicationWriter.INSTANCE;
        private ApplicationResultStore applicationResultStore =
                new EmbeddedApplicationResultStore();

        private Configuration configuration = new Configuration();

        // even-though it's labeled as @Nullable, it's a mandatory field that needs to be set before
        // building the Dispatcher instance
        @Nullable private BlobServer blobServer = null;
        private FatalErrorHandler fatalErrorHandler = new TestingFatalErrorHandler();
        private JobManagerMetricGroup jobManagerMetricGroup =
                UnregisteredMetricGroups.createUnregisteredJobManagerMetricGroup();
        @Nullable private String metricServiceQueryAddress = null;
        private Executor ioExecutor = ForkJoinPool.commonPool();
        private HistoryServerArchivist historyServerArchivist = VoidHistoryServerArchivist.INSTANCE;
        private ArchivedApplicationStore archivedApplicationStore =
                new MemoryArchivedApplicationStore();
        private JobManagerRunnerFactory jobManagerRunnerFactory =
                new TestingJobMasterServiceLeadershipRunnerFactory();
        private CleanupRunnerFactory cleanupRunnerFactory = new TestingCleanupRunnerFactory();
        private DispatcherBootstrapFactory dispatcherBootstrapFactory =
                (dispatcher, scheduledExecutor, errorHandler) -> new NoOpDispatcherBootstrap();
        private DispatcherOperationCaches dispatcherOperationCaches =
                new DispatcherOperationCaches();
        private JobManagerRunnerRegistry jobManagerRunnerRegistry =
                new DefaultJobManagerRunnerRegistry(1);
        @Nullable private ResourceCleanerFactory resourceCleanerFactory;
        @Nullable private ApplicationResourceCleanerFactory applicationResourceCleanerFactory;

        public Builder setFencingToken(DispatcherId fencingToken) {
            this.fencingToken = fencingToken;
            return this;
        }

        public Builder setRecoveredJobs(Collection<ExecutionPlan> recoveredJobs) {
            this.recoveredJobs = recoveredJobs;
            return this;
        }

        public Builder setRecoveredDirtyJobs(@Nullable Collection<JobResult> recoveredDirtyJobs) {
            this.recoveredDirtyJobs = recoveredDirtyJobs;
            return this;
        }

        public Builder setRecoveredApplications(
                Collection<AbstractApplication> recoveredApplications) {
            this.recoveredApplications = recoveredApplications;
            return this;
        }

        public Builder setRecoveredDirtyApplications(
                @Nullable Collection<ApplicationResult> recoveredDirtyApplications) {
            this.recoveredDirtyApplications = recoveredDirtyApplications;
            return this;
        }

        public Builder setHighAvailabilityServices(
                HighAvailabilityServices highAvailabilityServices) {
            this.highAvailabilityServices = highAvailabilityServices;
            return this;
        }

        public Builder setResourceManagerGateway(
                TestingResourceManagerGateway resourceManagerGateway) {
            this.resourceManagerGateway = resourceManagerGateway;
            return this;
        }

        public Builder setResourceManagerGatewayRetriever(
                GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever) {
            this.resourceManagerGatewayRetriever = resourceManagerGatewayRetriever;
            return this;
        }

        public Builder setHeartbeatServices(HeartbeatServices heartbeatServices) {
            this.heartbeatServices = heartbeatServices;
            return this;
        }

        public Builder setExecutionPlanWriter(ExecutionPlanWriter executionPlanWriter) {
            this.executionPlanWriter = executionPlanWriter;
            return this;
        }

        public Builder setJobResultStore(JobResultStore jobResultStore) {
            this.jobResultStore = jobResultStore;
            return this;
        }

        public Builder setConfiguration(Configuration configuration) {
            this.configuration = configuration;
            return this;
        }

        public Builder setBlobServer(BlobServer blobServer) {
            this.blobServer = blobServer;
            return this;
        }

        public Builder setFatalErrorHandler(FatalErrorHandler fatalErrorHandler) {
            this.fatalErrorHandler = fatalErrorHandler;
            return this;
        }

        public Builder setJobManagerMetricGroup(JobManagerMetricGroup jobManagerMetricGroup) {
            this.jobManagerMetricGroup = jobManagerMetricGroup;
            return this;
        }

        public Builder setMetricServiceQueryAddress(@Nullable String metricServiceQueryAddress) {
            this.metricServiceQueryAddress = metricServiceQueryAddress;
            return this;
        }

        public Builder setIoExecutor(Executor ioExecutor) {
            this.ioExecutor = ioExecutor;
            return this;
        }

        public Builder setHistoryServerArchivist(HistoryServerArchivist historyServerArchivist) {
            this.historyServerArchivist = historyServerArchivist;
            return this;
        }

        public Builder setArchivedApplicationStore(
                ArchivedApplicationStore archivedApplicationStore) {
            this.archivedApplicationStore = archivedApplicationStore;
            return this;
        }

        public Builder setJobManagerRunnerFactory(JobManagerRunnerFactory jobManagerRunnerFactory) {
            this.jobManagerRunnerFactory = jobManagerRunnerFactory;
            return this;
        }

        public Builder setCleanupRunnerFactory(CleanupRunnerFactory cleanupRunnerFactory) {
            this.cleanupRunnerFactory = cleanupRunnerFactory;
            return this;
        }

        public Builder setDispatcherBootstrapFactory(
                DispatcherBootstrapFactory dispatcherBootstrapFactory) {
            this.dispatcherBootstrapFactory = dispatcherBootstrapFactory;
            return this;
        }

        public Builder setDispatcherOperationCaches(
                DispatcherOperationCaches dispatcherOperationCaches) {
            this.dispatcherOperationCaches = dispatcherOperationCaches;
            return this;
        }

        public Builder setJobManagerRunnerRegistry(
                JobManagerRunnerRegistry jobManagerRunnerRegistry) {
            this.jobManagerRunnerRegistry = jobManagerRunnerRegistry;
            return this;
        }

        public Builder setResourceCleanerFactory(ResourceCleanerFactory resourceCleanerFactory) {
            this.resourceCleanerFactory = resourceCleanerFactory;
            return this;
        }

        public Builder setApplicationResourceCleanerFactory(
                ApplicationResourceCleanerFactory applicationResourceCleanerFactory) {
            this.applicationResourceCleanerFactory = applicationResourceCleanerFactory;
            return this;
        }

        private ResourceCleanerFactory createDefaultResourceCleanerFactory() {
            return new DispatcherResourceCleanerFactory(
                    ioExecutor,
                    TestingRetryStrategies.NO_RETRY_STRATEGY,
                    jobManagerRunnerRegistry,
                    executionPlanWriter,
                    blobServer,
                    highAvailabilityServices,
                    jobManagerMetricGroup);
        }

        private ApplicationResourceCleanerFactory createDefaultApplicationResourceCleanerFactory() {
            return new DispatcherApplicationResourceCleanerFactory(
                    ioExecutor,
                    TestingRetryStrategies.NO_RETRY_STRATEGY,
                    applicationWriter,
                    blobServer);
        }

        public TestingDispatcher build(RpcService rpcService) throws Exception {
            return new TestingDispatcher(
                    rpcService,
                    fencingToken,
                    recoveredJobs,
                    recoveredDirtyJobs == null
                            ? jobResultStore.getDirtyResults()
                            : recoveredDirtyJobs,
                    recoveredApplications,
                    recoveredDirtyApplications == null
                            ? applicationResultStore.getDirtyResults()
                            : recoveredDirtyApplications,
                    configuration,
                    highAvailabilityServices,
                    resourceManagerGatewayRetriever,
                    heartbeatServices,
                    Preconditions.checkNotNull(
                            blobServer,
                            "No BlobServer is specified for building the TestingDispatcher"),
                    fatalErrorHandler,
                    executionPlanWriter,
                    jobResultStore,
                    applicationWriter,
                    applicationResultStore,
                    jobManagerMetricGroup,
                    metricServiceQueryAddress,
                    ioExecutor,
                    historyServerArchivist,
                    archivedApplicationStore,
                    jobManagerRunnerFactory,
                    cleanupRunnerFactory,
                    dispatcherBootstrapFactory,
                    dispatcherOperationCaches,
                    jobManagerRunnerRegistry,
                    resourceCleanerFactory != null
                            ? resourceCleanerFactory
                            : createDefaultResourceCleanerFactory(),
                    applicationResourceCleanerFactory != null
                            ? applicationResourceCleanerFactory
                            : createDefaultApplicationResourceCleanerFactory());
        }
    }
}
