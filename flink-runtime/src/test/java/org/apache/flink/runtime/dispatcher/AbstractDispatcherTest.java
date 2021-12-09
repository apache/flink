/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.JobGraphWriter;
import org.apache.flink.runtime.jobmanager.StandaloneJobGraphStore;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerResource;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.TimeUtils;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;

/** Abstract test for the {@link Dispatcher} component. */
public class AbstractDispatcherTest extends TestLogger {

    static TestingRpcService rpcService;

    static final Time TIMEOUT = Time.minutes(1L);

    @BeforeClass
    public static void setupClass() {
        rpcService = new TestingRpcService();
    }

    @AfterClass
    public static void teardownClass() throws Exception {
        if (rpcService != null) {
            RpcUtils.terminateRpcService(rpcService, TIMEOUT);
            rpcService = null;
        }
    }

    static void awaitStatus(DispatcherGateway dispatcherGateway, JobID jobId, JobStatus status)
            throws Exception {
        CommonTestUtils.waitUntilCondition(
                () -> status.equals(dispatcherGateway.requestJobStatus(jobId, TIMEOUT).get()),
                Deadline.fromNow(TimeUtils.toDuration(TIMEOUT)));
    }

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public final TestingFatalErrorHandlerResource testingFatalErrorHandlerResource =
            new TestingFatalErrorHandlerResource();

    @Rule public TestName name = new TestName();

    Configuration configuration;

    BlobServer blobServer;

    TestingHighAvailabilityServices haServices;

    HeartbeatServices heartbeatServices;

    @Before
    public void setUp() throws Exception {
        heartbeatServices = new HeartbeatServices(1000L, 10000L);

        haServices = new TestingHighAvailabilityServices();
        haServices.setCheckpointRecoveryFactory(new StandaloneCheckpointRecoveryFactory());
        haServices.setResourceManagerLeaderRetriever(new SettableLeaderRetrievalService());
        haServices.setJobGraphStore(new StandaloneJobGraphStore());

        configuration = new Configuration();
        configuration.setString(
                BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

        blobServer = new BlobServer(configuration, new VoidBlobStore());
    }

    @After
    public void tearDown() throws Exception {
        if (haServices != null) {
            haServices.closeAndCleanupAllData();
        }
        if (blobServer != null) {
            blobServer.close();
        }
    }

    /** A convenient builder for the {@link TestingDispatcher}. */
    public class TestingDispatcherBuilder {

        private Collection<JobGraph> initialJobGraphs = Collections.emptyList();

        private final DispatcherBootstrapFactory dispatcherBootstrapFactory =
                (dispatcher, scheduledExecutor, errorHandler) -> new NoOpDispatcherBootstrap();

        private HeartbeatServices heartbeatServices = AbstractDispatcherTest.this.heartbeatServices;

        private HighAvailabilityServices haServices = AbstractDispatcherTest.this.haServices;

        private JobManagerRunnerFactory jobManagerRunnerFactory =
                JobMasterServiceLeadershipRunnerFactory.INSTANCE;

        private JobGraphWriter jobGraphWriter = NoOpJobGraphWriter.INSTANCE;

        private FatalErrorHandler fatalErrorHandler =
                testingFatalErrorHandlerResource.getFatalErrorHandler();

        private HistoryServerArchivist historyServerArchivist = VoidHistoryServerArchivist.INSTANCE;

        TestingDispatcherBuilder setHeartbeatServices(HeartbeatServices heartbeatServices) {
            this.heartbeatServices = heartbeatServices;
            return this;
        }

        TestingDispatcherBuilder setHaServices(HighAvailabilityServices haServices) {
            this.haServices = haServices;
            return this;
        }

        TestingDispatcherBuilder setInitialJobGraphs(Collection<JobGraph> initialJobGraphs) {
            this.initialJobGraphs = initialJobGraphs;
            return this;
        }

        TestingDispatcherBuilder setJobManagerRunnerFactory(
                JobManagerRunnerFactory jobManagerRunnerFactory) {
            this.jobManagerRunnerFactory = jobManagerRunnerFactory;
            return this;
        }

        TestingDispatcherBuilder setJobGraphWriter(JobGraphWriter jobGraphWriter) {
            this.jobGraphWriter = jobGraphWriter;
            return this;
        }

        public TestingDispatcherBuilder setFatalErrorHandler(FatalErrorHandler fatalErrorHandler) {
            this.fatalErrorHandler = fatalErrorHandler;
            return this;
        }

        public TestingDispatcherBuilder setHistoryServerArchivist(
                HistoryServerArchivist historyServerArchivist) {
            this.historyServerArchivist = historyServerArchivist;
            return this;
        }

        TestingDispatcher build() throws Exception {
            TestingResourceManagerGateway resourceManagerGateway =
                    new TestingResourceManagerGateway();

            final MemoryExecutionGraphInfoStore executionGraphInfoStore =
                    new MemoryExecutionGraphInfoStore();

            return new TestingDispatcher(
                    rpcService,
                    DispatcherId.generate(),
                    initialJobGraphs,
                    dispatcherBootstrapFactory,
                    new DispatcherServices(
                            configuration,
                            haServices,
                            () -> CompletableFuture.completedFuture(resourceManagerGateway),
                            blobServer,
                            heartbeatServices,
                            executionGraphInfoStore,
                            fatalErrorHandler,
                            historyServerArchivist,
                            null,
                            UnregisteredMetricGroups.createUnregisteredJobManagerMetricGroup(),
                            jobGraphWriter,
                            jobManagerRunnerFactory,
                            ForkJoinPool.commonPool()));
        }
    }
}
