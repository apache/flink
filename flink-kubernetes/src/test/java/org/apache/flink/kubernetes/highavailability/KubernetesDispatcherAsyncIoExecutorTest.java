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

package org.apache.flink.kubernetes.highavailability;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.core.testutils.AllCallbackWrapper;
import org.apache.flink.kubernetes.KubernetesTestBase;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherBootstrapFactory;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.dispatcher.DispatcherServices;
import org.apache.flink.runtime.dispatcher.JobManagerRunnerFactory;
import org.apache.flink.runtime.dispatcher.JobMasterServiceLeadershipRunnerFactory;
import org.apache.flink.runtime.dispatcher.NoOpDispatcherBootstrap;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServices;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServicesWithJobPersistenceComponents;
import org.apache.flink.runtime.dispatcher.StandaloneDispatcher;
import org.apache.flink.runtime.dispatcher.TestingPartialDispatcherServices;
import org.apache.flink.runtime.dispatcher.cleanup.CleanupRunnerFactory;
import org.apache.flink.runtime.dispatcher.cleanup.TestingCleanupRunnerFactory;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.HeartbeatServicesImpl;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedJobResultStore;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobmanager.DefaultJobGraphStore;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.jobmanager.NoOpJobGraphStoreWatcher;
import org.apache.flink.runtime.jobmanager.TestingJobGraphListener;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.jobmaster.factories.JobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.leaderelection.TestingLeaderElection;
import org.apache.flink.runtime.persistence.PossibleInconsistentStateException;
import org.apache.flink.runtime.persistence.RetrievableStateStorageHelper;
import org.apache.flink.runtime.persistence.filesystem.FileSystemStateStorageHelper;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.runtime.util.BlobServerExtension;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;

import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Predicate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Test for the {@link Dispatcher} component. */
public class KubernetesDispatcherAsyncIoExecutorTest extends KubernetesTestBase {

    private JobGraph jobGraph;
    private JobID jobId;
    private TestingLeaderElection jobMasterLeaderElection;
    private CountDownLatch createdJobManagerRunnerLatch;
    private TestingJobGraphListener testingJobGraphListener;
    private PartialDispatcherServices partialDispatcherServices;
    private Configuration configuration;
    private ThreadFactory threadFactory;
    private Executor ioExecutor;
    private JobResultStore jobResultStore = new EmbeddedJobResultStore();
    static final Time TIMEOUT = Time.minutes(10L);
    static String ioExecutorThreadPrefix = "ioExecutor-thread";
    static TestingRpcService rpcService;
    @TempDir static Path temporaryFolder;
    final TestingCleanupRunnerFactory cleanupRunnerFactory = new TestingCleanupRunnerFactory();
    TestingHighAvailabilityServices haServices;
    HeartbeatServices heartbeatServices;
    TestingPartialDispatcherServices.Builder builder;
    JobGraphStore jobGraphStore;
    BlobServer blobServer;
    Dispatcher dispatcher;
    String handleStoreThreadName = "";

    @RegisterExtension
    public static AllCallbackWrapper<BlobServerExtension> blobServerExtensionWrapper =
            new AllCallbackWrapper<>(new BlobServerExtension());

    @BeforeEach
    public void setUp(@TempDir File temporaryFolder) throws Exception {
        heartbeatServices = new HeartbeatServicesImpl(1000L, 10000L);
        jobGraph = JobGraphTestUtils.singleNoOpJobGraph();
        haServices = new TestingHighAvailabilityServices();
        jobId = jobGraph.getJobID();
        jobMasterLeaderElection = new TestingLeaderElection();
        String configMapName = getClusterConfigMap();
        createKubeConfigMap(configMapName);
        jobGraphStore = createKubeJobGraphStore(configMapName);
        haServices.setJobMasterLeaderElection(jobId, jobMasterLeaderElection);
        haServices.setJobGraphStore(jobGraphStore);
        createdJobManagerRunnerLatch = new CountDownLatch(2);
        blobServer = new BlobServer(configuration, temporaryFolder, new VoidBlobStore());
        rpcService = new TestingRpcService();
        builder =
                TestingPartialDispatcherServices.builder()
                        .withFatalErrorHandler(new TestingFatalErrorHandler())
                        .withHighAvailabilityServices(haServices);
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (dispatcher != null) {
            RpcUtils.terminateRpcEndpoint(dispatcher);
        }
    }

    private String getClusterConfigMap() {
        return CLUSTER_ID + Constants.NAME_SEPARATOR + "cluster-config-map";
    }

    private class TestKubernetesStateHandleStore<T extends Serializable>
            extends KubernetesStateHandleStore {
        public TestKubernetesStateHandleStore(
                FlinkKubeClient kubeClient,
                String configMapName,
                RetrievableStateStorageHelper storage,
                Predicate<String> configMapKeyFilter,
                @Nullable String lockIdentity) {
            super(kubeClient, configMapName, storage, configMapKeyFilter, lockIdentity);
        }

        @Override
        public RetrievableStateHandle<T> addAndLock(String key, Serializable state)
                throws PossibleInconsistentStateException, Exception {
            handleStoreThreadName = Thread.currentThread().getName();
            return super.addAndLock(key, state);
        }
    }

    private TestKubernetesStateHandleStore<JobGraph> createJobGraphStateHandleStore(
            Configuration configuration,
            FlinkKubeClient flinkKubeClient,
            String configMapName,
            String lockIdentity)
            throws Exception {

        final RetrievableStateStorageHelper<JobGraph> stateStorage =
                new FileSystemStateStorageHelper<>(
                        HighAvailabilityServicesUtils.getClusterHighAvailableStoragePath(
                                configuration),
                        Constants.SUBMITTED_JOBGRAPH_FILE_PREFIX);

        return new TestKubernetesStateHandleStore<>(
                flinkKubeClient,
                configMapName,
                stateStorage,
                k -> k.startsWith(Constants.JOB_GRAPH_STORE_KEY_PREFIX),
                lockIdentity);
    }

    private JobGraphStore createJobGraphStore(
            Configuration configuration,
            FlinkKubeClient flinkKubeClient,
            String configMapName,
            String lockIdentity)
            throws Exception {

        final TestKubernetesStateHandleStore<JobGraph> stateHandleStore =
                createJobGraphStateHandleStore(
                        configuration, flinkKubeClient, configMapName, lockIdentity);
        return new DefaultJobGraphStore<>(
                stateHandleStore,
                NoOpJobGraphStoreWatcher.INSTANCE,
                KubernetesJobGraphStoreUtil.INSTANCE);
    }

    private JobGraphStore createKubeJobGraphStore(String configMapName) throws Exception {
        String lockIdentity = UUID.randomUUID().toString();
        testingJobGraphListener = new TestingJobGraphListener();
        configuration = new Configuration();
        configuration.setString(KubernetesConfigOptions.CLUSTER_ID, CLUSTER_ID);
        configuration.setString(
                HighAvailabilityOptions.HA_STORAGE_PATH, temporaryFolder.toString());
        JobGraphStore jobGraphStore =
                createJobGraphStore(configuration, flinkKubeClient, configMapName, lockIdentity);
        jobGraphStore.start(testingJobGraphListener);
        return jobGraphStore;
    }

    private void createKubeConfigMap(String configMapName) {
        final KubernetesConfigMap configMap =
                new KubernetesConfigMap(
                        new ConfigMapBuilder()
                                .withNewMetadata()
                                .withName(configMapName)
                                .endMetadata()
                                .build());
        flinkKubeClient.createConfigMap(configMap).join();
    }

    private Dispatcher createDispatcher(
            RpcService rpcService,
            DispatcherId fencingToken,
            Collection<JobGraph> recoveredJobs,
            Collection<JobResult> recoveredDirtyJobResults,
            DispatcherBootstrapFactory dispatcherBootstrapFactory,
            PartialDispatcherServicesWithJobPersistenceComponents
                    partialDispatcherServicesWithJobPersistenceComponents,
            JobManagerRunnerFactory jobManagerRunnerFactory,
            CleanupRunnerFactory cleanupRunnerFactory)
            throws Exception {
        return new StandaloneDispatcher(
                rpcService,
                fencingToken,
                recoveredJobs,
                recoveredDirtyJobResults,
                dispatcherBootstrapFactory,
                DispatcherServices.from(
                        partialDispatcherServicesWithJobPersistenceComponents,
                        jobManagerRunnerFactory,
                        cleanupRunnerFactory));
    }

    private Dispatcher createAndStartDispatcher(
            HeartbeatServices heartbeatServices,
            TestingHighAvailabilityServices haServices,
            JobManagerRunnerFactory jobManagerRunnerFactory)
            throws Exception {
        final Dispatcher dispatcher =
                createDispatcher(
                        rpcService,
                        DispatcherId.generate(),
                        Collections.emptyList(),
                        haServices.getJobResultStore().getDirtyResults(),
                        (myDispatcher, scheduledExecutor, errorHandler) ->
                                new NoOpDispatcherBootstrap(),
                        PartialDispatcherServicesWithJobPersistenceComponents.from(
                                partialDispatcherServices, jobGraphStore, jobResultStore),
                        jobManagerRunnerFactory,
                        cleanupRunnerFactory);

        dispatcher.start();
        return dispatcher;
    }

    private void jobSubmit() throws Exception {
        partialDispatcherServices =
                builder.withIoExecutor(ioExecutor)
                        .build(
                                blobServerExtensionWrapper.getCustomExtension().getBlobServer(),
                                new Configuration());
        dispatcher =
                createAndStartDispatcher(
                        heartbeatServices,
                        haServices,
                        new ExpectedJobIdJobManagerRunnerFactory(jobId));
        DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);
        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();
        jobMasterLeaderElection.getStartFuture().get();
        assertTrue(
                "jobManagerRunner was not started",
                jobMasterLeaderElection.getStartFuture().isDone());
    }

    /**
     * Tests that JobGraphStateHandleStore's ThreadName starts with the IoExecutor thread prefix.
     */
    @Test
    public void testHandleStoreThreadNamePrefix() throws Exception {
        ioExecutorThreadPrefix = "ioExecutor-abc";
        threadFactory =
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat(ioExecutorThreadPrefix + "-%d")
                        .build();
        ioExecutor = Executors.newCachedThreadPool(threadFactory);
        jobSubmit();
        assertTrue(handleStoreThreadName.startsWith(ioExecutorThreadPrefix));
    }

    private static final class ExpectedJobIdJobManagerRunnerFactory
            implements JobManagerRunnerFactory {

        private final JobID expectedJobId;

        private ExpectedJobIdJobManagerRunnerFactory(JobID expectedJobId) {
            this.expectedJobId = expectedJobId;
        }

        @Override
        public JobManagerRunner createJobManagerRunner(
                JobGraph jobGraph,
                Configuration configuration,
                RpcService rpcService,
                HighAvailabilityServices highAvailabilityServices,
                HeartbeatServices heartbeatServices,
                JobManagerSharedServices jobManagerSharedServices,
                JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
                FatalErrorHandler fatalErrorHandler,
                Collection<FailureEnricher> failureEnrichers,
                long initializationTimestamp)
                throws Exception {
            assertEquals(expectedJobId, jobGraph.getJobID());

            return JobMasterServiceLeadershipRunnerFactory.INSTANCE.createJobManagerRunner(
                    jobGraph,
                    configuration,
                    rpcService,
                    highAvailabilityServices,
                    heartbeatServices,
                    jobManagerSharedServices,
                    jobManagerJobMetricGroupFactory,
                    fatalErrorHandler,
                    Collections.emptySet(),
                    initializationTimestamp);
        }
    }
}
