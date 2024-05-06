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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.testutils.AllCallbackWrapper;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.HeartbeatServicesImpl;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmaster.utils.JobMasterBuilder;
import org.apache.flink.runtime.leaderelection.TestingLeaderElection;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.TestingRpcServiceExtension;
import org.apache.flink.runtime.taskexecutor.AccumulatorReport;
import org.apache.flink.runtime.taskexecutor.ExecutionDeploymentReport;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorToJobManagerHeartbeatPayload;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalUnresolvedTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerExtension;
import org.apache.flink.testutils.TestingUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the execution deployment-reconciliation logic in the {@link JobMaster}. */
class JobMasterExecutionDeploymentReconciliationTest {

    private static final Time testingTimeout = Time.seconds(10L);

    private final HeartbeatServices heartbeatServices =
            new HeartbeatServicesImpl(Integer.MAX_VALUE, Integer.MAX_VALUE);

    private final TestingHighAvailabilityServices haServices =
            new TestingHighAvailabilityServices();
    private final SettableLeaderRetrievalService resourceManagerLeaderRetriever =
            new SettableLeaderRetrievalService();

    public static final TestingRpcServiceExtension TESTING_RPC_SERVICE_EXTENSION =
            new TestingRpcServiceExtension();

    @RegisterExtension
    private static final AllCallbackWrapper<TestingRpcServiceExtension>
            RPC_SERVICE_EXTENSION_WRAPPER = new AllCallbackWrapper<>(TESTING_RPC_SERVICE_EXTENSION);

    @RegisterExtension
    private final TestingFatalErrorHandlerExtension testingFatalErrorHandlerExtension =
            new TestingFatalErrorHandlerExtension();

    @BeforeEach
    private void setup() {
        haServices.setResourceManagerLeaderRetriever(resourceManagerLeaderRetriever);
        haServices.setResourceManagerLeaderElection(new TestingLeaderElection());
        haServices.setCheckpointRecoveryFactory(new StandaloneCheckpointRecoveryFactory());
    }

    /** Tests how the job master handles unknown/missing executions. */
    @Test
    void testExecutionDeploymentReconciliation() throws Exception {
        JobMasterBuilder.TestingOnCompletionActions onCompletionActions =
                new JobMasterBuilder.TestingOnCompletionActions();

        TestingExecutionDeploymentTrackerWrapper deploymentTrackerWrapper =
                new TestingExecutionDeploymentTrackerWrapper();
        final JobGraph jobGraph = JobGraphTestUtils.singleNoOpJobGraph();
        try (JobMaster jobMaster =
                createAndStartJobMaster(onCompletionActions, deploymentTrackerWrapper, jobGraph)) {
            JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);
            TESTING_RPC_SERVICE_EXTENSION
                    .getTestingRpcService()
                    .registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);

            final CompletableFuture<ExecutionAttemptID> taskCancellationFuture =
                    new CompletableFuture<>();
            TaskExecutorGateway taskExecutorGateway =
                    createTaskExecutorGateway(taskCancellationFuture);
            LocalUnresolvedTaskManagerLocation localUnresolvedTaskManagerLocation =
                    new LocalUnresolvedTaskManagerLocation();

            registerTaskExecutorAndOfferSlots(
                    jobMasterGateway,
                    jobGraph.getJobID(),
                    taskExecutorGateway,
                    localUnresolvedTaskManagerLocation);

            ExecutionAttemptID deployedExecution =
                    deploymentTrackerWrapper.getTaskDeploymentFuture().get();
            assertThatFuture(taskCancellationFuture).isNotDone();

            ExecutionAttemptID unknownDeployment = createExecutionAttemptId();
            //  the deployment report is missing the just deployed task, but contains the ID of some
            // other unknown deployment
            //  the job master should cancel the unknown deployment, and fail the job
            jobMasterGateway.heartbeatFromTaskManager(
                    localUnresolvedTaskManagerLocation.getResourceID(),
                    new TaskExecutorToJobManagerHeartbeatPayload(
                            new AccumulatorReport(Collections.emptyList()),
                            new ExecutionDeploymentReport(
                                    Collections.singleton(unknownDeployment))));

            assertThatFuture(taskCancellationFuture)
                    .eventuallySucceeds()
                    .isEqualTo(unknownDeployment);
            assertThatFuture(deploymentTrackerWrapper.getStopFuture())
                    .eventuallySucceeds()
                    .isEqualTo(deployedExecution);

            assertThat(
                            onCompletionActions
                                    .getJobReachedGloballyTerminalStateFuture()
                                    .get()
                                    .getArchivedExecutionGraph()
                                    .getState())
                    .isEqualTo(JobStatus.FAILED);
        }
    }

    /**
     * Tests that the job master does not issue a cancel call if the heartbeat reports an execution
     * for which the deployment was not yet acknowledged.
     */
    @Test
    void testExecutionDeploymentReconciliationForPendingExecution() throws Exception {
        TestingExecutionDeploymentTrackerWrapper deploymentTrackerWrapper =
                new TestingExecutionDeploymentTrackerWrapper();
        final JobGraph jobGraph = JobGraphTestUtils.singleNoOpJobGraph();
        try (JobMaster jobMaster = createAndStartJobMaster(deploymentTrackerWrapper, jobGraph)) {
            JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);
            TESTING_RPC_SERVICE_EXTENSION
                    .getTestingRpcService()
                    .registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);

            final CompletableFuture<ExecutionAttemptID> taskSubmissionFuture =
                    new CompletableFuture<>();
            final CompletableFuture<ExecutionAttemptID> taskCancellationFuture =
                    new CompletableFuture<>();
            final CompletableFuture<Acknowledge> taskSubmissionAcknowledgeFuture =
                    new CompletableFuture<>();
            TaskExecutorGateway taskExecutorGateway =
                    createTaskExecutorGateway(
                            taskCancellationFuture,
                            taskSubmissionFuture,
                            taskSubmissionAcknowledgeFuture);
            LocalUnresolvedTaskManagerLocation localUnresolvedTaskManagerLocation =
                    new LocalUnresolvedTaskManagerLocation();

            registerTaskExecutorAndOfferSlots(
                    jobMasterGateway,
                    jobGraph.getJobID(),
                    taskExecutorGateway,
                    localUnresolvedTaskManagerLocation);

            ExecutionAttemptID pendingExecutionId = taskSubmissionFuture.get();

            // the execution has not been acknowledged yet by the TaskExecutor, but we already allow
            // the ID to be in the heartbeat payload
            jobMasterGateway.heartbeatFromTaskManager(
                    localUnresolvedTaskManagerLocation.getResourceID(),
                    new TaskExecutorToJobManagerHeartbeatPayload(
                            new AccumulatorReport(Collections.emptyList()),
                            new ExecutionDeploymentReport(
                                    Collections.singleton(pendingExecutionId))));

            taskSubmissionAcknowledgeFuture.complete(Acknowledge.get());

            deploymentTrackerWrapper.getTaskDeploymentFuture().get();
            assertThatFuture(taskCancellationFuture).isNotDone();
        }
    }

    private JobMaster createAndStartJobMaster(
            ExecutionDeploymentTracker executionDeploymentTracker, JobGraph jobGraph)
            throws Exception {
        return createAndStartJobMaster(
                new JobMasterBuilder.TestingOnCompletionActions(),
                executionDeploymentTracker,
                jobGraph);
    }

    private JobMaster createAndStartJobMaster(
            OnCompletionActions onCompletionActions,
            ExecutionDeploymentTracker executionDeploymentTracker,
            JobGraph jobGraph)
            throws Exception {

        JobMaster jobMaster =
                new JobMasterBuilder(jobGraph, TESTING_RPC_SERVICE_EXTENSION.getTestingRpcService())
                        .withFatalErrorHandler(
                                testingFatalErrorHandlerExtension.getTestingFatalErrorHandler())
                        .withHighAvailabilityServices(haServices)
                        .withHeartbeatServices(heartbeatServices)
                        .withExecutionDeploymentTracker(executionDeploymentTracker)
                        .withOnCompletionActions(onCompletionActions)
                        .createJobMaster();

        jobMaster.start();

        return jobMaster;
    }

    private TaskExecutorGateway createTaskExecutorGateway(
            CompletableFuture<ExecutionAttemptID> taskCancellationFuture) {
        return createTaskExecutorGateway(
                taskCancellationFuture,
                new CompletableFuture<>(),
                CompletableFuture.completedFuture(Acknowledge.get()));
    }

    private TaskExecutorGateway createTaskExecutorGateway(
            CompletableFuture<ExecutionAttemptID> taskCancellationFuture,
            CompletableFuture<ExecutionAttemptID> taskSubmissionFuture,
            CompletableFuture<Acknowledge> taskSubmissionResponse) {
        TestingTaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setAddress(UUID.randomUUID().toString())
                        .setCancelTaskFunction(
                                executionAttemptId -> {
                                    taskCancellationFuture.complete(executionAttemptId);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .setSubmitTaskConsumer(
                                (tdd, ignored) -> {
                                    taskSubmissionFuture.complete(tdd.getExecutionAttemptId());
                                    return taskSubmissionResponse;
                                })
                        .createTestingTaskExecutorGateway();

        TESTING_RPC_SERVICE_EXTENSION
                .getTestingRpcService()
                .registerGateway(taskExecutorGateway.getAddress(), taskExecutorGateway);

        return taskExecutorGateway;
    }

    private void registerTaskExecutorAndOfferSlots(
            JobMasterGateway jobMasterGateway,
            JobID jobId,
            TaskExecutorGateway taskExecutorGateway,
            UnresolvedTaskManagerLocation taskManagerLocation)
            throws ExecutionException, InterruptedException {
        jobMasterGateway
                .registerTaskManager(
                        jobId,
                        TaskManagerRegistrationInformation.create(
                                taskExecutorGateway.getAddress(),
                                taskManagerLocation,
                                TestingUtils.zeroUUID()),
                        testingTimeout)
                .get();

        Collection<SlotOffer> slotOffers =
                Collections.singleton(new SlotOffer(new AllocationID(), 0, ResourceProfile.ANY));

        jobMasterGateway
                .offerSlots(taskManagerLocation.getResourceID(), slotOffers, testingTimeout)
                .get();
    }

    private static class TestingExecutionDeploymentTrackerWrapper
            implements ExecutionDeploymentTracker {
        private final ExecutionDeploymentTracker originalTracker;
        private final CompletableFuture<ExecutionAttemptID> taskDeploymentFuture;
        private final CompletableFuture<ExecutionAttemptID> stopFuture;

        private TestingExecutionDeploymentTrackerWrapper() {
            this(new DefaultExecutionDeploymentTracker());
        }

        private TestingExecutionDeploymentTrackerWrapper(
                ExecutionDeploymentTracker originalTracker) {
            this.originalTracker = originalTracker;
            this.taskDeploymentFuture = new CompletableFuture<>();
            this.stopFuture = new CompletableFuture<>();
        }

        @Override
        public void startTrackingPendingDeploymentOf(
                ExecutionAttemptID executionAttemptId, ResourceID host) {
            originalTracker.startTrackingPendingDeploymentOf(executionAttemptId, host);
        }

        @Override
        public void completeDeploymentOf(ExecutionAttemptID executionAttemptId) {
            originalTracker.completeDeploymentOf(executionAttemptId);
            taskDeploymentFuture.complete(executionAttemptId);
        }

        @Override
        public void stopTrackingDeploymentOf(ExecutionAttemptID executionAttemptId) {
            originalTracker.stopTrackingDeploymentOf(executionAttemptId);
            stopFuture.complete(executionAttemptId);
        }

        @Override
        public Map<ExecutionAttemptID, ExecutionDeploymentState> getExecutionsOn(ResourceID host) {
            return originalTracker.getExecutionsOn(host);
        }

        public CompletableFuture<ExecutionAttemptID> getTaskDeploymentFuture() {
            return taskDeploymentFuture;
        }

        public CompletableFuture<ExecutionAttemptID> getStopFuture() {
            return stopFuture;
        }
    }
}
