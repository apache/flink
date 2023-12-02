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
import org.apache.flink.kubernetes.KubernetesTestBase;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.JobManagerRunnerFactory;
import org.apache.flink.runtime.dispatcher.JobMasterServiceLeadershipRunnerFactory;
import org.apache.flink.runtime.dispatcher.TestingDispatcher;
import org.apache.flink.runtime.dispatcher.cleanup.CheckpointResourcesCleanupRunnerFactory;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.HeartbeatServicesImpl;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.jobmanager.TestingJobGraphListener;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.factories.JobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;

import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import org.junit.After;
import org.junit.Rule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static org.apache.flink.kubernetes.utils.Constants.NAME_SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Test for the {@link Dispatcher} component. */
public class KubernetesDispatcherAsyncIoExecutorTest extends KubernetesTestBase {

    private JobGraph jobGraph;

    private JobID jobId;

    private TestingLeaderElectionService jobMasterLeaderElectionService;

    private CountDownLatch createdJobManagerRunnerLatch;
    static TestingRpcService rpcService;

    TestingHighAvailabilityServices haServices;
    HeartbeatServices heartbeatServices;
    static final Time TIMEOUT = Time.minutes(1L);
    private TestingJobGraphListener testingJobGraphListener;
    BlobServer blobServer;
    /** Instance under test. */
    private TestingDispatcher dispatcher;

    private Configuration configuration;

    @Rule private TemporaryFolder temporaryFolder = new TemporaryFolder();

    @BeforeEach
    public void setUp(@TempDir File temporaryFolder) throws Exception {
        heartbeatServices = new HeartbeatServicesImpl(1000L, 10000L);
        jobGraph = JobGraphTestUtils.singleNoOpJobGraph();
        haServices = new TestingHighAvailabilityServices();
        jobId = jobGraph.getJobID();
        // jobMasterLeaderElectionService = new TestingLeaderElectionService();
        // haServices.setJobMasterLeaderElectionService(jobId, jobMasterLeaderElectionService);
        String configMapName = getClusterConfigMap();
        createKubeConfigMap(configMapName);
        haServices.setJobGraphStore(createKubeJobGraphStore(configMapName));
        createdJobManagerRunnerLatch = new CountDownLatch(2);
        blobServer = new BlobServer(configuration, temporaryFolder, new VoidBlobStore());
        rpcService = new TestingRpcService();
    }

    private String getClusterConfigMap() {
        return CLUSTER_ID + NAME_SEPARATOR + "cluster-config-map";
    }

    private JobGraphStore createKubeJobGraphStore(String configMapName) throws Exception {
        String LOCK_IDENTITY = UUID.randomUUID().toString();
        testingJobGraphListener = new TestingJobGraphListener();
        configuration = new Configuration();
        configuration.setString(KubernetesConfigOptions.CLUSTER_ID, CLUSTER_ID);
        configuration.setString(
                HighAvailabilityOptions.HA_STORAGE_PATH, temporaryFolder.toString());
        JobGraphStore jobGraphStore =
                KubernetesUtils.createJobGraphStore(
                        configuration, flinkKubeClient, configMapName, LOCK_IDENTITY);

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

    private TestingDispatcher.Builder createTestingDispatcherBuilder() {
        return TestingDispatcher.builder()
                .setHighAvailabilityServices(haServices)
                .setHeartbeatServices(heartbeatServices)
                .setJobGraphWriter(haServices.getJobGraphStore())
                .setJobResultStore(haServices.getJobResultStore())
                .setJobManagerRunnerFactory(JobMasterServiceLeadershipRunnerFactory.INSTANCE)
                .setCleanupRunnerFactory(CheckpointResourcesCleanupRunnerFactory.INSTANCE)
                .setBlobServer(blobServer);
    }

    @Nonnull
    private TestingDispatcher createAndStartDispatcher(
            HeartbeatServices heartbeatServices,
            TestingHighAvailabilityServices haServices,
            JobManagerRunnerFactory jobManagerRunnerFactory)
            throws Exception {
        final TestingDispatcher dispatcher =
                createTestingDispatcherBuilder()
                        .setHighAvailabilityServices(haServices)
                        .setHeartbeatServices(heartbeatServices)
                        .setJobManagerRunnerFactory(jobManagerRunnerFactory)
                        .setJobGraphWriter(haServices.getJobGraphStore())
                        .setJobResultStore(haServices.getJobResultStore())
                        .build(rpcService);

        dispatcher.start();
        return dispatcher;
    }

    @After
    public void tearDown() throws Exception {
        if (dispatcher != null) {
            RpcUtils.terminateRpcEndpoint(dispatcher);
        }
    }

    /**
     * Tests that we can submit a job to the Dispatcher which then spawns a new JobManagerRunner.
     */
    @Test
    public void testJobSubmission() throws Exception {
        dispatcher =
                createAndStartDispatcher(
                        heartbeatServices,
                        haServices,
                        new ExpectedJobIdJobManagerRunnerFactory(
                                jobId, createdJobManagerRunnerLatch));
        DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);

        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();

        jobMasterLeaderElectionService.getStartFuture().get();

        assertTrue(
                "jobManagerRunner was not started",
                jobMasterLeaderElectionService.getStartFuture().isDone());
    }

    private static final class ExpectedJobIdJobManagerRunnerFactory
            implements JobManagerRunnerFactory {

        private final JobID expectedJobId;

        private final CountDownLatch createdJobManagerRunnerLatch;

        private ExpectedJobIdJobManagerRunnerFactory(
                JobID expectedJobId, CountDownLatch createdJobManagerRunnerLatch) {
            this.expectedJobId = expectedJobId;
            this.createdJobManagerRunnerLatch = createdJobManagerRunnerLatch;
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
                long initializationTimestamp)
                throws Exception {
            assertEquals(expectedJobId, jobGraph.getJobID());

            createdJobManagerRunnerLatch.countDown();

            return JobMasterServiceLeadershipRunnerFactory.INSTANCE.createJobManagerRunner(
                    jobGraph,
                    configuration,
                    rpcService,
                    highAvailabilityServices,
                    heartbeatServices,
                    jobManagerSharedServices,
                    jobManagerJobMetricGroupFactory,
                    fatalErrorHandler,
                    initializationTimestamp);
        }
    }
}
