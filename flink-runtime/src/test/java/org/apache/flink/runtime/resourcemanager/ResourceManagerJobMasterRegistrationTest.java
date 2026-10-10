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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.AutoCloseableRegistry;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobmaster.DefaultSlotPoolServiceSchedulerFactory;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.jobmaster.TestingJobManagerSharedServicesBuilder;
import org.apache.flink.runtime.jobmaster.slotpool.TestingSlotPoolServiceBuilder;
import org.apache.flink.runtime.jobmaster.utils.JobMasterBuilder;
import org.apache.flink.runtime.leaderelection.LeaderInformation;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.scheduler.TestingSchedulerNG;
import org.apache.flink.runtime.scheduler.TestingSchedulerNGFactory;
import org.apache.flink.runtime.security.token.NoOpDelegationTokenManager;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.FlinkException;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for automatic JobMaster registration retries with the ResourceManager. */
class ResourceManagerJobMasterRegistrationTest {

    /**
     * Verifies that the JobMaster retries automatically while preserving the job configuration and
     * existing job leader monitoring.
     *
     * <p>{@link ResourceManagerJobMasterTest} covers the failure response and single token-manager
     * invocation per registration request on the ResourceManager side.
     */
    @Test
    void testJobMasterAutomaticallyRetriesAfterDelegationTokenRegistrationFailure()
            throws Exception {
        try (AutoCloseableRegistry closeableRegistry = new AutoCloseableRegistry()) {
            final TestingRpcService rpcService = new TestingRpcService();
            closeableRegistry.registerCloseable(() -> rpcService.closeAsync().get());

            final JobManagerSharedServices sharedServices =
                    new TestingJobManagerSharedServicesBuilder().build();
            closeableRegistry.registerCloseable(sharedServices::shutdown);

            final JobGraph jobGraph = JobGraphTestUtils.singleNoOpJobGraph();
            jobGraph.getJobConfiguration()
                    .setString("test.job-configuration", "preserved-on-retry");
            final Map<String, String> jobConfiguration = jobGraph.getJobConfiguration().toMap();
            final FailingOnceDelegationTokenManager delegationTokenManager =
                    new FailingOnceDelegationTokenManager();
            final AtomicInteger jobLeaderRetrieverStarts = new AtomicInteger();
            final AtomicInteger jobLeaderRetrieverStops = new AtomicInteger();
            final SettableLeaderRetrievalService jobMasterLeaderRetriever =
                    new SettableLeaderRetrievalService() {
                        @Override
                        public synchronized void start(LeaderRetrievalListener listener)
                                throws Exception {
                            jobLeaderRetrieverStarts.incrementAndGet();
                            super.start(listener);
                        }

                        @Override
                        public void stop() throws Exception {
                            jobLeaderRetrieverStops.incrementAndGet();
                            super.stop();
                        }
                    };
            final TestingResourceManagerService resourceManagerService =
                    TestingResourceManagerService.newBuilder()
                            .setRpcService(rpcService)
                            .setDelegationTokenManager(delegationTokenManager)
                            .setJmLeaderRetrieverFunction(ignored -> jobMasterLeaderRetriever)
                            .build();
            closeableRegistry.registerCloseable(resourceManagerService::rethrowFatalErrorIfAny);
            closeableRegistry.registerCloseable(() -> resourceManagerService.closeAsync().get());

            final SettableLeaderRetrievalService resourceManagerLeaderRetriever =
                    new SettableLeaderRetrievalService();
            final TestingHighAvailabilityServices highAvailabilityServices =
                    new TestingHighAvailabilityServices();
            highAvailabilityServices.setResourceManagerLeaderRetriever(
                    resourceManagerLeaderRetriever);
            highAvailabilityServices.setCheckpointRecoveryFactory(
                    new StandaloneCheckpointRecoveryFactory());

            final Configuration configuration = new Configuration();
            // Prevent timeout-driven retries from changing the expected attempt count.
            configuration.set(ClusterOptions.INITIAL_REGISTRATION_TIMEOUT, RpcUtils.INF_TIMEOUT);
            configuration.set(ClusterOptions.MAX_REGISTRATION_TIMEOUT, RpcUtils.INF_TIMEOUT);
            configuration.set(ClusterOptions.REFUSED_REGISTRATION_DELAY, Duration.ZERO);
            final CompletableFuture<ResourceManagerGateway> connectedResourceManagerFuture =
                    new CompletableFuture<>();
            resourceManagerService
                    .getFatalErrorFuture()
                    .thenAccept(connectedResourceManagerFuture::completeExceptionally);
            final JobMasterBuilder.TestingOnCompletionActions completionActions =
                    new JobMasterBuilder.TestingOnCompletionActions();
            completionActions
                    .getJobMasterFailedFuture()
                    .thenAccept(connectedResourceManagerFuture::completeExceptionally);
            final TestingFatalErrorHandler fatalErrorHandler = new TestingFatalErrorHandler();
            fatalErrorHandler
                    .getErrorFuture()
                    .thenAccept(connectedResourceManagerFuture::completeExceptionally);
            closeableRegistry.registerCloseable(fatalErrorHandler::rethrowError);

            final JobMaster jobMaster =
                    new JobMasterBuilder(jobGraph, rpcService)
                            .withConfiguration(configuration)
                            .withHighAvailabilityServices(highAvailabilityServices)
                            .withJobManagerSharedServices(sharedServices)
                            .withOnCompletionActions(completionActions)
                            .withFatalErrorHandler(fatalErrorHandler)
                            .withSlotPoolServiceSchedulerFactory(
                                    DefaultSlotPoolServiceSchedulerFactory.create(
                                            TestingSlotPoolServiceBuilder.newBuilder()
                                                    .setConnectToResourceManagerConsumer(
                                                            connectedResourceManagerFuture
                                                                    ::complete),
                                            new TestingSchedulerNGFactory(
                                                    TestingSchedulerNG.newBuilder().build())))
                            .createJobMaster();
            closeableRegistry.registerCloseable(() -> jobMaster.closeAsync().get());

            resourceManagerService.start();
            final CompletableFuture<LeaderInformation> resourceManagerLeadershipFuture =
                    resourceManagerService.isLeader(UUID.randomUUID());
            resourceManagerService
                    .getFatalErrorFuture()
                    .thenAccept(resourceManagerLeadershipFuture::completeExceptionally);
            resourceManagerLeadershipFuture.get();
            final ResourceManagerGateway resourceManagerGateway =
                    resourceManagerService
                            .getResourceManagerGateway()
                            .orElseThrow(
                                    () -> new AssertionError("ResourceManager is not available"));

            jobMasterLeaderRetriever.notifyListener(
                    jobMaster.getAddress(), jobMaster.getFencingToken().toUUID());
            jobMaster.start();
            resourceManagerLeaderRetriever.notifyListener(
                    resourceManagerGateway.getAddress(),
                    resourceManagerGateway.getFencingToken().toUUID());

            final ResourceManagerGateway connectedResourceManager =
                    connectedResourceManagerFuture.get();
            assertThat(delegationTokenManager.registrations)
                    .containsExactly(
                            Tuple2.of(jobGraph.getJobID(), jobConfiguration),
                            Tuple2.of(jobGraph.getJobID(), jobConfiguration));
            assertThat(jobLeaderRetrieverStarts)
                    .as("the retry reuses the existing job leader monitoring")
                    .hasValue(1);
            assertThat(jobLeaderRetrieverStops)
                    .as("job leader monitoring remains active after the failed registration")
                    .hasValue(0);
            assertThatFuture(
                            connectedResourceManager.declareRequiredResources(
                                    jobMaster.getFencingToken(),
                                    ResourceRequirements.create(
                                            jobGraph.getJobID(),
                                            jobMaster.getAddress(),
                                            Collections.emptyList()),
                                    RpcUtils.INF_TIMEOUT))
                    .eventuallySucceeds();
            assertThat(completionActions.getJobMasterFailedFuture()).isNotDone();
        }
    }

    private static final class FailingOnceDelegationTokenManager
            extends NoOpDelegationTokenManager {

        private final AtomicInteger registrationAttempts = new AtomicInteger();
        private final Queue<Tuple2<JobID, Map<String, String>>> registrations =
                new ConcurrentLinkedQueue<>();

        @Override
        public void registerJob(JobID jobId, Configuration jobConfiguration) throws Exception {
            registrations.add(Tuple2.of(jobId, jobConfiguration.toMap()));
            if (registrationAttempts.incrementAndGet() == 1) {
                throw new FlinkException("First delegation token registration attempt failed");
            }
        }
    }
}
