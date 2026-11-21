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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.dispatcher.cleanup.CheckpointResourcesCleanupRunnerFactory;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.HeartbeatServicesImpl;
import org.apache.flink.runtime.highavailability.EmbeddedApplicationResultStore;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedJobResultStore;
import org.apache.flink.runtime.jobmanager.StandaloneApplicationStore;
import org.apache.flink.runtime.jobmanager.StandaloneExecutionPlanStore;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;

import static org.apache.flink.testutils.junit.utils.TempDirUtils.newFolder;

/** Abstract test for the {@link Dispatcher} component. */
class AbstractDispatcherTest {

    static TestingRpcService rpcService;

    static final Duration TIMEOUT = Duration.ofMinutes(1L);

    @BeforeAll
    static void setupClass() {
        rpcService = new TestingRpcService();
    }

    @AfterAll
    static void teardownClass() throws Exception {
        if (rpcService != null) {
            RpcUtils.terminateRpcService(rpcService);
            rpcService = null;
        }
    }

    static void awaitStatus(DispatcherGateway dispatcherGateway, JobID jobId, JobStatus status)
            throws Exception {
        CommonTestUtils.waitUntilCondition(
                () -> status.equals(dispatcherGateway.requestJobStatus(jobId, TIMEOUT).get()));
    }

    @TempDir Path temporaryFolder;

    @RegisterExtension
    final TestingFatalErrorHandlerExtension testingFatalErrorHandlerExtension =
            new TestingFatalErrorHandlerExtension();

    Configuration configuration;

    BlobServer blobServer;

    TestingHighAvailabilityServices haServices;

    HeartbeatServices heartbeatServices;

    @BeforeEach
    void setUp() throws Exception {
        heartbeatServices = new HeartbeatServicesImpl(1000L, 10000L);

        haServices = new TestingHighAvailabilityServices();
        haServices.setCheckpointRecoveryFactory(new StandaloneCheckpointRecoveryFactory());
        haServices.setResourceManagerLeaderRetriever(new SettableLeaderRetrievalService());
        haServices.setExecutionPlanStore(new StandaloneExecutionPlanStore());
        haServices.setJobResultStore(new EmbeddedJobResultStore());
        haServices.setApplicationStore(new StandaloneApplicationStore());
        haServices.setApplicationResultStore(new EmbeddedApplicationResultStore());

        configuration = new Configuration();
        blobServer = new BlobServer(configuration, newFolder(temporaryFolder), new VoidBlobStore());
    }

    protected TestingDispatcher.Builder createTestingDispatcherBuilder() {
        return TestingDispatcher.builder()
                .setConfiguration(configuration)
                .setHeartbeatServices(heartbeatServices)
                .setHighAvailabilityServices(haServices)
                .setExecutionPlanWriter(haServices.getExecutionPlanStore())
                .setJobResultStore(haServices.getJobResultStore())
                .setApplicationWriter(haServices.getApplicationStore())
                .setApplicationResultStore(haServices.getApplicationResultStore())
                .setJobManagerRunnerFactory(JobMasterServiceLeadershipRunnerFactory.INSTANCE)
                .setCleanupRunnerFactory(CheckpointResourcesCleanupRunnerFactory.INSTANCE)
                .setFatalErrorHandler(
                        testingFatalErrorHandlerExtension.getTestingFatalErrorHandler())
                .setBlobServer(blobServer);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (haServices != null) {
            haServices.closeWithOptionalClean(true);
        }
        if (blobServer != null) {
            blobServer.close();
        }
    }

    protected BlobServer getBlobServer() {
        return blobServer;
    }
}
