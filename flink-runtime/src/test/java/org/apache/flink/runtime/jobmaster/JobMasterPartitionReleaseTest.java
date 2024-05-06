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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.HeartbeatServicesImpl;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.io.network.partition.AbstractPartitionTrackerTest;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.TestingJobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobmaster.utils.JobMasterBuilder;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalUnresolvedTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.testutils.TestingUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the partition release logic of the {@link JobMaster}. */
class JobMasterPartitionReleaseTest {

    @TempDir private static File temporaryFolder;

    private static final Time testingTimeout = Time.seconds(10L);

    private static TestingRpcService rpcService;

    private TestingFatalErrorHandler testingFatalErrorHandler;

    @BeforeAll
    private static void setupClass() {
        rpcService = new TestingRpcService();
    }

    @BeforeEach
    private void setup() throws IOException {
        testingFatalErrorHandler = new TestingFatalErrorHandler();
    }

    @AfterEach
    private void teardown() throws Exception {
        if (testingFatalErrorHandler != null) {
            testingFatalErrorHandler.rethrowError();
        }

        rpcService.clearGateways();
    }

    @AfterAll
    private static void teardownClass() {
        if (rpcService != null) {
            rpcService.closeAsync();
            rpcService = null;
        }
    }

    @Test
    void testPartitionTableCleanupOnDisconnect() throws Exception {
        final CompletableFuture<JobID> disconnectTaskExecutorFuture = new CompletableFuture<>();
        final TestingTaskExecutorGateway testingTaskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setDisconnectJobManagerConsumer(
                                (jobID, throwable) -> disconnectTaskExecutorFuture.complete(jobID))
                        .createTestingTaskExecutorGateway();

        try (final TestSetup testSetup =
                new TestSetup(rpcService, testingFatalErrorHandler, testingTaskExecutorGateway)) {
            final JobMasterGateway jobMasterGateway =
                    testSetup.jobMaster.getSelfGateway(JobMasterGateway.class);

            jobMasterGateway.disconnectTaskManager(
                    testSetup.getTaskExecutorResourceID(), new Exception("test"));
            disconnectTaskExecutorFuture.get();

            assertThatFuture(testSetup.getStopTrackingPartitionsTargetResourceId())
                    .eventuallySucceeds()
                    .isEqualTo(testSetup.getTaskExecutorResourceID());
        }
    }

    @Test
    void testPartitionReleaseOrPromotionOnJobSuccess() throws Exception {
        testPartitionReleaseOrPromotionOnJobTermination(
                TestSetup::getPartitionsForReleaseOrPromote, ExecutionState.FINISHED);
    }

    @Test
    void testPartitionReleaseOrPromotionOnJobFailure() throws Exception {
        testPartitionReleaseOrPromotionOnJobTermination(
                TestSetup::getPartitionsForRelease, ExecutionState.FAILED);
    }

    private void testPartitionReleaseOrPromotionOnJobTermination(
            Function<TestSetup, CompletableFuture<Collection<ResultPartitionID>>> callSelector,
            ExecutionState finalExecutionState)
            throws Exception {
        final CompletableFuture<TaskDeploymentDescriptor> taskDeploymentDescriptorFuture =
                new CompletableFuture<>();
        final TestingTaskExecutorGateway testingTaskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setSubmitTaskConsumer(
                                (tdd, ignored) -> {
                                    taskDeploymentDescriptorFuture.complete(tdd);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .createTestingTaskExecutorGateway();

        try (final TestSetup testSetup =
                new TestSetup(rpcService, testingFatalErrorHandler, testingTaskExecutorGateway)) {
            ResultPartitionID partitionID0 = new ResultPartitionID();
            ResultPartitionID partitionID1 = new ResultPartitionID();
            testSetup
                    .getPartitionTracker()
                    .setGetAllTrackedPartitionsSupplier(
                            () -> {
                                ResultPartitionDeploymentDescriptor partitionDesc0 =
                                        AbstractPartitionTrackerTest
                                                .createResultPartitionDeploymentDescriptor(
                                                        partitionID0, true);
                                ResultPartitionDeploymentDescriptor partitionDesc1 =
                                        AbstractPartitionTrackerTest
                                                .createResultPartitionDeploymentDescriptor(
                                                        partitionID1, false);
                                return Arrays.asList(partitionDesc0, partitionDesc1);
                            });

            final JobMasterGateway jobMasterGateway = testSetup.getJobMasterGateway();

            // update the execution state of the only execution to target state
            // this should trigger the job to finish
            final TaskDeploymentDescriptor taskDeploymentDescriptor =
                    taskDeploymentDescriptorFuture.get();
            jobMasterGateway.updateTaskExecutionState(
                    new TaskExecutionState(
                            taskDeploymentDescriptor.getExecutionAttemptId(), finalExecutionState));
            assertThat(callSelector.apply(testSetup).get())
                    .containsExactlyInAnyOrder(partitionID0, partitionID1);
        }
    }

    private static class TestSetup implements AutoCloseable {

        private final LocalUnresolvedTaskManagerLocation localTaskManagerUnresolvedLocation =
                new LocalUnresolvedTaskManagerLocation();

        private final CompletableFuture<ResourceID> taskExecutorIdForStopTracking =
                new CompletableFuture<>();

        private final CompletableFuture<Collection<ResultPartitionID>> partitionsForRelease =
                new CompletableFuture<>();

        private final CompletableFuture<Collection<ResultPartitionID>> clusterPartitionsForPromote =
                new CompletableFuture<>();

        private final JobMaster jobMaster;

        private final TestingJobMasterPartitionTracker partitionTracker;

        public TestSetup(
                TestingRpcService rpcService,
                FatalErrorHandler fatalErrorHandler,
                TaskExecutorGateway taskExecutorGateway)
                throws Exception {

            TestingHighAvailabilityServices haServices = new TestingHighAvailabilityServices();
            haServices.setCheckpointRecoveryFactory(new StandaloneCheckpointRecoveryFactory());

            haServices.setResourceManagerLeaderRetriever(
                    new SettableLeaderRetrievalService(null, null));

            partitionTracker = new TestingJobMasterPartitionTracker();

            partitionTracker.setStopTrackingAllPartitionsConsumer(
                    taskExecutorIdForStopTracking::complete);
            partitionTracker.setStopTrackingAndReleasePartitionsConsumer(
                    partitionsForRelease::complete);
            partitionTracker.setStopTrackingAndPromotePartitionsConsumer(
                    clusterPartitionsForPromote::complete);

            Configuration configuration = new Configuration();
            configuration.set(
                    BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.getAbsolutePath());

            HeartbeatServices heartbeatServices = new HeartbeatServicesImpl(1000L, 5_000_000L);

            final JobGraph jobGraph = JobGraphTestUtils.singleNoOpJobGraph();
            jobMaster =
                    new JobMasterBuilder(jobGraph, rpcService)
                            .withConfiguration(configuration)
                            .withHighAvailabilityServices(haServices)
                            .withFatalErrorHandler(fatalErrorHandler)
                            .withHeartbeatServices(heartbeatServices)
                            .withPartitionTrackerFactory(ignored -> partitionTracker)
                            .createJobMaster();

            jobMaster.start();

            registerTaskExecutorAtJobMaster(
                    rpcService, getJobMasterGateway(), jobGraph.getJobID(), taskExecutorGateway);
        }

        private void registerTaskExecutorAtJobMaster(
                TestingRpcService rpcService,
                JobMasterGateway jobMasterGateway,
                JobID jobId,
                TaskExecutorGateway taskExecutorGateway)
                throws ExecutionException, InterruptedException {

            rpcService.registerGateway(taskExecutorGateway.getAddress(), taskExecutorGateway);

            jobMasterGateway
                    .registerTaskManager(
                            jobId,
                            TaskManagerRegistrationInformation.create(
                                    taskExecutorGateway.getAddress(),
                                    localTaskManagerUnresolvedLocation,
                                    TestingUtils.zeroUUID()),
                            testingTimeout)
                    .get();

            Collection<SlotOffer> slotOffers =
                    Collections.singleton(
                            new SlotOffer(new AllocationID(), 0, ResourceProfile.ANY));

            jobMasterGateway
                    .offerSlots(
                            localTaskManagerUnresolvedLocation.getResourceID(),
                            slotOffers,
                            testingTimeout)
                    .get();
        }

        public TestingJobMasterPartitionTracker getPartitionTracker() {
            return partitionTracker;
        }

        public JobMasterGateway getJobMasterGateway() {
            return jobMaster.getSelfGateway(JobMasterGateway.class);
        }

        public ResourceID getTaskExecutorResourceID() {
            return localTaskManagerUnresolvedLocation.getResourceID();
        }

        public CompletableFuture<ResourceID> getStopTrackingPartitionsTargetResourceId() {
            return taskExecutorIdForStopTracking;
        }

        public CompletableFuture<Collection<ResultPartitionID>> getPartitionsForRelease() {
            return partitionsForRelease;
        }

        public CompletableFuture<Collection<ResultPartitionID>> getPartitionsForReleaseOrPromote() {
            return partitionsForRelease.thenCombine(
                    clusterPartitionsForPromote,
                    (resultPartitionIds, resultPartitionIds2) -> {
                        Set<ResultPartitionID> res = new HashSet<>();
                        res.addAll(resultPartitionIds);
                        res.addAll(resultPartitionIds2);
                        return res;
                    });
        }

        public void close() throws Exception {
            if (jobMaster != null) {
                RpcUtils.terminateRpcEndpoint(jobMaster);
            }
        }
    }
}
