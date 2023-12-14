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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.core.testutils.EachCallbackWrapper;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.dispatcher.cleanup.CheckpointResourcesCleanupRunnerFactory;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.HeartbeatServicesImpl;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobmanager.DefaultJobGraphStore;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.jobmanager.TestingJobGraphListener;
import org.apache.flink.runtime.jobmanager.ZooKeeperJobGraphStoreUtil;
import org.apache.flink.runtime.jobmanager.ZooKeeperJobGraphStoreWatcher;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.factories.JobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.leaderelection.TestingLeaderElection;
import org.apache.flink.runtime.persistence.PossibleInconsistentStateException;
import org.apache.flink.runtime.persistence.RetrievableStateStorageHelper;
import org.apache.flink.runtime.persistence.TestingLongStateHandleHelper;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.runtime.state.RetrievableStreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerExtension;
import org.apache.flink.runtime.zookeeper.ZooKeeperExtension;
import org.apache.flink.runtime.zookeeper.ZooKeeperStateHandleStore;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.concurrent.ScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.flink.shaded.guava31.com.google.common.base.Ticker;
import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Test for the {@link Dispatcher} component. */
public class ZookeeperDispatcherAsyncIoExecutorTest {

    private JobGraph jobGraph;
    private JobID jobId;
    private TestingLeaderElection jobMasterLeaderElection;
    private final ZooKeeperExtension zooKeeperExtension = new ZooKeeperExtension();
    private TestingJobGraphListener testingJobGraphListener;
    private Configuration configuration;
    private ThreadFactory threadFactory;
    private Executor ioExecutor;
    private TestingDispatcher dispatcher;
    static final Time TIMEOUT = Time.minutes(1L);
    static String ioExecutorThreadPrefix = "";
    static TestingRpcService rpcService;
    @TempDir static Path temporaryFolder;
    List<String> handleStoreThreadNameList = new ArrayList<String>();
    List<String> executionGraphInfoThreadNameList = new ArrayList<String>();
    HeartbeatServices heartbeatServices = new HeartbeatServicesImpl(1000L, 10000L);
    TestingHighAvailabilityServices haServices;
    TestingDispatcher.Builder builder;
    CuratorFramework facade;
    BlobServer blobServer;

    private static final RetrievableStateStorageHelper<JobGraph> localStateStorage =
            jobGraph -> {
                ByteStreamStateHandle byteStreamStateHandle =
                        new ByteStreamStateHandle(
                                String.valueOf(UUID.randomUUID()),
                                InstantiationUtil.serializeObject(jobGraph));
                return new RetrievableStreamStateHandle<>(byteStreamStateHandle);
            };

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
        facade = createFacade();
    }

    private CuratorFramework createFacade() throws Exception {
        String fullPath = "/abc";
        final CuratorFramework client = getZooKeeperClient();
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

    class TestThreadNameZooKeeperStateHandleStore<T extends Serializable>
            extends ZooKeeperStateHandleStore {
        public TestThreadNameZooKeeperStateHandleStore(
                CuratorFramework client, RetrievableStateStorageHelper storage) {
            super(client, storage);
        }

        @Override
        public RetrievableStateHandle<T> addAndLock(String pathInZooKeeper, Serializable state)
                throws PossibleInconsistentStateException, Exception {
            handleStoreThreadNameList.add(Thread.currentThread().getName());
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

    private void jobSubmit() throws Exception {
        dispatcher = createAndStartDispatcher(builder);
        dispatcher.start();
        DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);
        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();
        jobMasterLeaderElection.getStartFuture().get();
    }

    /**
     * Tests that JobGraphStateHandleStore's ThreadName starts with the IoExecutor thread prefix.
     */
    @Test
    public void testHandleStoreThreadNamePrefix() throws Exception {
        ioExecutorThreadPrefix = "ioExecutor-aaa";
        threadFactory =
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat(ioExecutorThreadPrefix + "-%d")
                        .build();
        ioExecutor = Executors.newCachedThreadPool(threadFactory);
        ZooKeeperStateHandleStore zooKeeperStateHandleStore =
                new TestThreadNameZooKeeperStateHandleStore<>(facade, localStateStorage);
        JobGraphStore jobGraphStore =
                createZooKeeperJobGraphStore(facade, zooKeeperStateHandleStore);
        builder = createTestingDispatcherBuilder(jobGraphStore);
        builder.setIoExecutor(ioExecutor);
        jobSubmit();
        handleStoreThreadNameList.stream()
                .forEach(threadName -> assertTrue(threadName.startsWith(ioExecutorThreadPrefix)));
    }

    private TestExecutionGraphInfoStore createDefaultExecutionGraphInfoStore() throws IOException {
        File storageDirectory = temporaryFolder.toFile();
        ScheduledExecutor scheduledExecutor =
                new ScheduledExecutorServiceAdapter(Executors.newSingleThreadScheduledExecutor());
        return new TestExecutionGraphInfoStore(
                storageDirectory,
                Time.hours(1L),
                Integer.MAX_VALUE,
                10000L,
                scheduledExecutor,
                Ticker.systemTicker());
    }

    private void jobSetStoreAndSutmit() throws Exception {
        CuratorFramework facade = createFacade();
        ZooKeeperStateHandleStore zooKeeperStateHandleStore =
                new TestThreadNameZooKeeperStateHandleStore<>(facade, localStateStorage);
        JobGraphStore jobGraphStore =
                createZooKeeperJobGraphStore(facade, zooKeeperStateHandleStore);
        TestingDispatcher.Builder builder = createTestingDispatcherBuilder(jobGraphStore);
        ioExecutorThreadPrefix = "ioExecutor-abc";
        threadFactory =
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat(ioExecutorThreadPrefix + "-%d")
                        .build();
        ioExecutor = Executors.newCachedThreadPool(threadFactory);
        builder.setIoExecutor(ioExecutor);
        ExecutionGraphInfoStore executionGraphInfoStore = createDefaultExecutionGraphInfoStore();
        builder.setExecutionGraphInfoStore(executionGraphInfoStore);
        dispatcher = createAndStartDispatcher(builder);
        dispatcher.start();
        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);
        jobMasterLeaderElection.isLeader(UUID.randomUUID());
        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();
    }

    @Test
    public void testCompleteJobExecutionStoreThreadNamePrefix() throws Exception {
        jobSetStoreAndSutmit();
        handleStoreThreadNameList.stream()
                .forEach(threadName -> assertTrue(threadName.startsWith(ioExecutorThreadPrefix)));
        ExecutionGraphInfo failedExecutionGraphInfo =
                new ExecutionGraphInfo(new ArchivedExecutionGraphBuilder().setJobID(jobId).build());

        dispatcher.completeJobExecution(failedExecutionGraphInfo);
        awaitThreadName(this);
        executionGraphInfoThreadNameList.stream()
                .forEach(threadName -> assertTrue(threadName.startsWith(ioExecutorThreadPrefix)));
    }

    @Test
    public void testFailedJobExecutionStoreThreadNamePrefix() throws Exception {
        jobSetStoreAndSutmit();
        dispatcher.submitFailedJob(jobId, "failedJobName", new RuntimeException("Test exception."));
        awaitThreadName(this);
        executionGraphInfoThreadNameList.stream()
                .forEach(threadName -> assertTrue(threadName.startsWith(ioExecutorThreadPrefix)));
    }

    private class TestExecutionGraphInfoStore extends FileExecutionGraphInfoStore {
        public TestExecutionGraphInfoStore(
                File rootDir,
                Time expirationTime,
                int maximumCapacity,
                long maximumCacheSizeBytes,
                ScheduledExecutor scheduledExecutor,
                Ticker ticker)
                throws IOException {
            super(
                    rootDir,
                    expirationTime,
                    maximumCapacity,
                    maximumCacheSizeBytes,
                    scheduledExecutor,
                    ticker);
        }

        @Override
        public void put(ExecutionGraphInfo executionGraphInfo) throws IOException {
            try {
                executionGraphInfoThreadNameList.add(Thread.currentThread().getName());
                super.put(executionGraphInfo);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    static void awaitThreadName(ZookeeperDispatcherAsyncIoExecutorTest test) throws Exception {
        CommonTestUtils.waitUntilCondition(() -> test.executionGraphInfoThreadNameList.size() > 0);
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
