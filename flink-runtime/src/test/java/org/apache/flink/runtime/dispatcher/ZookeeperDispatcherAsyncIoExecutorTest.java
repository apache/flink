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
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.core.testutils.EachCallbackWrapper;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.dispatcher.cleanup.CheckpointResourcesCleanupRunnerFactory;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.HeartbeatServicesImpl;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.DefaultJobGraphStore;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.jobmanager.TestingJobGraphListener;
import org.apache.flink.runtime.jobmanager.ZooKeeperJobGraphStoreUtil;
import org.apache.flink.runtime.jobmanager.ZooKeeperJobGraphStoreWatcher;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterService;
import org.apache.flink.runtime.jobmaster.JobMasterServiceLeadershipRunner;
import org.apache.flink.runtime.jobmaster.TestingJobMasterService;
import org.apache.flink.runtime.jobmaster.factories.DefaultJobMasterServiceProcessFactory;
import org.apache.flink.runtime.jobmaster.factories.JobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.jobmaster.factories.TestingJobMasterServiceFactory;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.leaderelection.TestingLeaderElection;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.JobsOverview;
import org.apache.flink.runtime.persistence.PossibleInconsistentStateException;
import org.apache.flink.runtime.persistence.RetrievableStateStorageHelper;
import org.apache.flink.runtime.persistence.TestingLongStateHandleHelper;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.runtime.state.RetrievableStreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerExtension;
import org.apache.flink.runtime.zookeeper.ZooKeeperExtension;
import org.apache.flink.runtime.zookeeper.ZooKeeperStateHandleStore;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.cache.PathChildrenCache;

import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Test for the {@link Dispatcher} component. */
public class ZookeeperDispatcherAsyncIoExecutorTest {

    private JobGraph jobGraph;

    private JobID jobId;

    private TestingLeaderElection jobMasterLeaderElection;

    private final ZooKeeperExtension zooKeeperExtension = new ZooKeeperExtension();

    private TestingJobGraphListener testingJobGraphListener;

    static TestingRpcService rpcService;
    HeartbeatServices heartbeatServices = new HeartbeatServicesImpl(1000L, 10000L);
    TestingHighAvailabilityServices haServices;
    private Configuration configuration;
    static final Time TIMEOUT = Time.minutes(1L);
    @TempDir static Path temporaryFolder;

    @RegisterExtension
    final EachCallbackWrapper<ZooKeeperExtension> zooKeeperResource =
            new EachCallbackWrapper<>(zooKeeperExtension);

    @RegisterExtension
    final TestingFatalErrorHandlerExtension testingFatalErrorHandlerResource =
            new TestingFatalErrorHandlerExtension();

    private CuratorFramework getZooKeeperClient() {
        return zooKeeperExtension.getZooKeeperClient(
                testingFatalErrorHandlerResource.getTestingFatalErrorHandler());
    }

    private static final RetrievableStateStorageHelper<JobGraph> localStateStorage =
            jobGraph -> {
                ByteStreamStateHandle byteStreamStateHandle =
                        new ByteStreamStateHandle(
                                String.valueOf(UUID.randomUUID()),
                                InstantiationUtil.serializeObject(jobGraph));
                return new RetrievableStreamStateHandle<>(byteStreamStateHandle);
            };

    /** Instance under test. */
    private TestingDispatcher dispatcher;

    BlobServer blobServer;

    @BeforeEach
    public void setUp() throws Exception {
        configuration = new Configuration();
        TestingLongStateHandleHelper.clearGlobalState();
        testingJobGraphListener = new TestingJobGraphListener();
        jobGraph = JobGraphTestUtils.singleNoOpJobGraph();
        jobId = jobGraph.getJobID();
        jobMasterLeaderElection = new TestingLeaderElection();
        haServices = new TestingHighAvailabilityServices();
        haServices.setJobMasterLeaderElection(jobId, jobMasterLeaderElection);
        rpcService = new TestingRpcService();
        blobServer = new BlobServer(configuration, temporaryFolder.toFile(), new VoidBlobStore());
    }

    private CuratorFramework createFacade() throws Exception {

        String fullPath = "/abc";
        final CuratorFramework client = getZooKeeperClient();
        // Ensure that the job graphs path exists
        client.newNamespaceAwareEnsurePath(fullPath).ensure(client.getZookeeperClient());
        CuratorFramework facade = client.usingNamespace(client.getNamespace() + fullPath);
        return facade;
    }

    private JobGraphStore createZooKeeperJobGraphStore(
            CuratorFramework facade, ZooKeeperStateHandleStore zooKeeperStateHandleStore)
            throws Exception {
        JobGraphStore jobGraphStore =
                new DefaultJobGraphStore<>(
                        zooKeeperStateHandleStore,
                        new ZooKeeperJobGraphStoreWatcher(
                                new PathChildrenCache(facade, "/", false)),
                        ZooKeeperJobGraphStoreUtil.INSTANCE);

        jobGraphStore.start(testingJobGraphListener);
        return jobGraphStore;
    }

    class TestZooKeeperStateHandleStore<T extends Serializable> extends ZooKeeperStateHandleStore {
        public TestZooKeeperStateHandleStore(
                CuratorFramework client, RetrievableStateStorageHelper storage) {
            super(client, storage);
        }

        @Override
        public RetrievableStateHandle<T> addAndLock(String pathInZooKeeper, Serializable state)
                throws PossibleInconsistentStateException, Exception {
            Thread.sleep(Time.seconds(20).toMilliseconds());
            return super.addAndLock(pathInZooKeeper, state);
        }
    }

    @Nonnull
    private TestingDispatcher createAndStartDispatcher(TestingDispatcher.Builder builder)
            throws Exception {
        final TestingDispatcher dispatcher = builder.build(rpcService);
        return dispatcher;
    }

    public TestingDispatcher.Builder createTestingDispatcherBuilder(JobGraphStore jobGraphStore) {
        TestingDispatcher.Builder builder =
                TestingDispatcher.builder()
                        .setHighAvailabilityServices(haServices)
                        .setHeartbeatServices(heartbeatServices)
                        .setJobResultStore(haServices.getJobResultStore())
                        .setCleanupRunnerFactory(CheckpointResourcesCleanupRunnerFactory.INSTANCE)
                        .setBlobServer(blobServer)
                        .setJobManagerRunnerFactory(new ExpectedJobIdJobManagerRunnerFactory(jobId))
                        .setJobGraphWriter(jobGraphStore);
        return builder;
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (dispatcher != null) {
            RpcUtils.terminateRpcEndpoint(dispatcher);
        }
    }

    /**
     * Use TestZooKeeperStateHandleStore as the StateHandleStore for long time to store Graph. Tests
     * that we can submit a job to the Dispatcher.
     */
    @Test
    public void testLongTimeStateStore() throws Exception {
        CuratorFramework facade = createFacade();
        ZooKeeperStateHandleStore zooKeeperStateHandleStore =
                new TestZooKeeperStateHandleStore<>(facade, localStateStorage);
        JobGraphStore jobGraphStore =
                createZooKeeperJobGraphStore(facade, zooKeeperStateHandleStore);
        TestingDispatcher.Builder builder = createTestingDispatcherBuilder(jobGraphStore);
        dispatcher = createAndStartDispatcher(builder);

        dispatcher.start();
        DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);
        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();
        jobMasterLeaderElection.getStartFuture().get();
        assertTrue(
                "jobManagerRunner was not started",
                jobMasterLeaderElection.getStartFuture().isDone());
        Thread.sleep(Time.seconds(30).toMilliseconds());
        assertTrue(
                "jobManagerRunner was not started",
                jobMasterLeaderElection.getStartFuture().isDone());
    }

    private class TestExecutionGraphInfoStore implements ExecutionGraphInfoStore {

        @Override
        public int size() {
            return 0;
        }

        @Nullable
        @Override
        public ExecutionGraphInfo get(JobID jobId) {
            return null;
        }

        @Override
        public void put(ExecutionGraphInfo executionGraphInfo) throws IOException {
            try {
                Thread.sleep(Time.seconds(20).toMilliseconds());
                System.out.println("ddd");
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public JobsOverview getStoredJobsOverview() {
            return null;
        }

        @Override
        public Collection<JobDetails> getAvailableJobDetails() {
            return null;
        }

        @Nullable
        @Override
        public JobDetails getAvailableJobDetails(JobID jobId) {
            return null;
        }

        @Override
        public void close() throws IOException {}
    }

    /**
     * Use TestZooKeeperStateHandleStore as the StateHandleStore for long time to store Graph. Tests
     * that we can submit a job to the Dispatcher.
     */
    @Test
    public void testLongTimeExecutionGraphInfoStore() throws Exception {
        CuratorFramework facade = createFacade();
        ZooKeeperStateHandleStore zooKeeperStateHandleStore =
                new ZooKeeperStateHandleStore<>(facade, localStateStorage);
        JobGraphStore jobGraphStore =
                createZooKeeperJobGraphStore(facade, zooKeeperStateHandleStore);
        TestingDispatcher.Builder builder = createTestingDispatcherBuilder(jobGraphStore);
        builder.setExecutionGraphInfoStore(new TestExecutionGraphInfoStore());
        dispatcher = createAndStartDispatcher(builder);
        dispatcher.start();
        DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);
        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();
        jobMasterLeaderElection.getStartFuture().get();
        assertTrue(
                "jobManagerRunner was not started",
                jobMasterLeaderElection.getStartFuture().isDone());
        Thread.sleep(Time.seconds(30).toMilliseconds());
        assertTrue(
                "jobManagerRunner was not started",
                jobMasterLeaderElection.getStartFuture().isDone());
    }

    @Test
    public void testJobResourceRequirementsCanBeOnlyUpdatedOnInitializedJobMasters()
            throws Exception {

        CuratorFramework facade = createFacade();
        ZooKeeperStateHandleStore zooKeeperStateHandleStore =
                new TestZooKeeperStateHandleStore<>(facade, localStateStorage);
        JobGraphStore jobGraphStore =
                createZooKeeperJobGraphStore(facade, zooKeeperStateHandleStore);
        final JobManagerRunnerWithBlockingJobMasterFactory blockingJobMaster =
                new JobManagerRunnerWithBlockingJobMasterFactory(this::withRequestJobResponse);
        TestingDispatcher.Builder builder = createTestingDispatcherBuilder(jobGraphStore);
        builder.setJobManagerRunnerFactory(blockingJobMaster);
        dispatcher = createAndStartDispatcher(builder);
        dispatcher.start();
        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);
        jobMasterLeaderElection.isLeader(UUID.randomUUID());

        assertThatFuture(
                        dispatcherGateway.updateJobResourceRequirements(
                                jobId, JobResourceRequirements.empty()))
                .eventuallyFailsWith(ExecutionException.class)
                .withCauseInstanceOf(FlinkJobNotFoundException.class);

        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();

        try {
            assertThatFuture(
                            dispatcherGateway.updateJobResourceRequirements(
                                    jobId, JobResourceRequirements.empty()))
                    .eventuallyFailsWith(ExecutionException.class)
                    .withCauseInstanceOf(UnavailableDispatcherOperationException.class);
        } finally {
            // Unblocking the job master in the "finally block" prevents getting
            // stuck during the RPC system tear down in case of test failure.
            blockingJobMaster.unblockJobMasterInitialization();
        }

        // We can update the JRR once the job transitions to RUNNING.
        awaitStatus(dispatcherGateway, jobId, JobStatus.RUNNING);
        assertThatFuture(
                        dispatcherGateway.updateJobResourceRequirements(
                                jobId, getJobRequirements()))
                .eventuallySucceeds();
    }

    private TestingJobMasterGatewayBuilder withRequestJobResponse(
            TestingJobMasterGatewayBuilder builder) {
        return builder.setRequestJobSupplier(
                () ->
                        CompletableFuture.completedFuture(
                                new ExecutionGraphInfo(
                                        ArchivedExecutionGraph
                                                .createSparseArchivedExecutionGraphWithJobVertices(
                                                        jobGraph.getJobID(),
                                                        jobGraph.getName(),
                                                        JobStatus.RUNNING,
                                                        null,
                                                        null,
                                                        System.currentTimeMillis(),
                                                        jobGraph.getVertices(),
                                                        SchedulerBase.computeVertexParallelismStore(
                                                                jobGraph)))));
    }

    private static class JobManagerRunnerWithBlockingJobMasterFactory
            implements JobManagerRunnerFactory {

        private final JobMasterGateway jobMasterGateway;
        private final AtomicReference<JobStatus> currentJobStatus;
        private final BlockingQueue<CompletableFuture<JobMasterService>> jobMasterServiceFutures;
        private final OneShotLatch initLatch;

        private JobManagerRunnerWithBlockingJobMasterFactory() {
            this(Function.identity());
        }

        private JobManagerRunnerWithBlockingJobMasterFactory(
                Function<TestingJobMasterGatewayBuilder, TestingJobMasterGatewayBuilder>
                        modifyGatewayBuilder) {
            this.currentJobStatus = new AtomicReference<>(JobStatus.INITIALIZING);
            this.jobMasterServiceFutures = new ArrayBlockingQueue<>(2);
            this.initLatch = new OneShotLatch();
            final TestingJobMasterGatewayBuilder builder =
                    new TestingJobMasterGatewayBuilder()
                            .setRequestJobSupplier(
                                    () ->
                                            CompletableFuture.completedFuture(
                                                    new ExecutionGraphInfo(
                                                            new ArchivedExecutionGraphBuilder()
                                                                    .setState(
                                                                            currentJobStatus.get())
                                                                    .build())));
            this.jobMasterGateway = modifyGatewayBuilder.apply(builder).build();
        }

        @Override
        public JobManagerRunner createJobManagerRunner(
                JobGraph jobGraph,
                Configuration configuration,
                RpcService rpcService,
                HighAvailabilityServices highAvailabilityServices,
                HeartbeatServices heartbeatServices,
                JobManagerSharedServices jobManagerServices,
                JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
                FatalErrorHandler fatalErrorHandler,
                Collection<FailureEnricher> failureEnrichers,
                long initializationTimestamp)
                throws Exception {

            return new JobMasterServiceLeadershipRunner(
                    new DefaultJobMasterServiceProcessFactory(
                            jobGraph.getJobID(),
                            jobGraph.getName(),
                            jobGraph.getCheckpointingSettings(),
                            initializationTimestamp,
                            new TestingJobMasterServiceFactory(
                                    ignored -> {
                                        initLatch.trigger();
                                        final CompletableFuture<JobMasterService> result =
                                                new CompletableFuture<>();
                                        Preconditions.checkState(
                                                jobMasterServiceFutures.offer(result));
                                        return result;
                                    })),
                    highAvailabilityServices.getJobManagerLeaderElection(jobGraph.getJobID()),
                    highAvailabilityServices.getJobResultStore(),
                    jobManagerServices
                            .getLibraryCacheManager()
                            .registerClassLoaderLease(jobGraph.getJobID()),
                    fatalErrorHandler);
        }

        public void waitForBlockingInit() throws InterruptedException {
            initLatch.await();
        }

        public void unblockJobMasterInitialization() throws InterruptedException {
            final CompletableFuture<JobMasterService> future = jobMasterServiceFutures.take();
            future.complete(new TestingJobMasterService(jobMasterGateway));
            currentJobStatus.set(JobStatus.RUNNING);
        }
    }

    private JobResourceRequirements getJobRequirements() {
        JobResourceRequirements.Builder builder = JobResourceRequirements.newBuilder();

        for (JobVertex vertex : jobGraph.getVertices()) {
            builder.setParallelismForJobVertex(vertex.getID(), 1, vertex.getParallelism());
        }
        return builder.build();
    }

    static void awaitStatus(DispatcherGateway dispatcherGateway, JobID jobId, JobStatus status)
            throws Exception {
        CommonTestUtils.waitUntilCondition(
                () -> status.equals(dispatcherGateway.requestJobStatus(jobId, TIMEOUT).get()));
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
