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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.BlockerSync;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorBuilder;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.partition.TestingTaskExecutorPartitionTracker;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmaster.AllocatedSlotInfo;
import org.apache.flink.runtime.jobmaster.AllocatedSlotReport;
import org.apache.flink.runtime.jobmaster.JMTMRegistrationSuccess;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcServiceResource;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerResource;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.stream.StreamSupport;

import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests for the execution deployment-reconciliation logic in the {@link TaskExecutor}. */
public class TaskExecutorExecutionDeploymentReconciliationTest extends TestLogger {

    private static final Time timeout = Time.seconds(10L);

    private final TestingHighAvailabilityServices haServices =
            new TestingHighAvailabilityServices();
    private final SettableLeaderRetrievalService jobManagerLeaderRetriever =
            new SettableLeaderRetrievalService();
    private final SettableLeaderRetrievalService resourceManagerLeaderRetriever =
            new SettableLeaderRetrievalService();
    private final JobID jobId = new JobID();

    @ClassRule
    public static final TestingRpcServiceResource RPC_SERVICE_RESOURCE =
            new TestingRpcServiceResource();

    @Rule
    public final TestingFatalErrorHandlerResource testingFatalErrorHandlerResource =
            new TestingFatalErrorHandlerResource();

    @Before
    public void setup() {
        haServices.setResourceManagerLeaderRetriever(resourceManagerLeaderRetriever);
        haServices.setJobMasterLeaderRetriever(jobId, jobManagerLeaderRetriever);
    }

    @After
    public void shutdown() {
        RPC_SERVICE_RESOURCE.getTestingRpcService().clearGateways();
    }

    @Test
    public void testDeployedExecutionReporting() throws Exception {
        final OneShotLatch slotOfferLatch = new OneShotLatch();
        final BlockingQueue<Set<ExecutionAttemptID>> deployedExecutionsQueue =
                new ArrayBlockingQueue<>(3);
        final CompletableFuture<Void> taskFinishedFuture = new CompletableFuture<>();
        final ResourceID jobManagerResourceId = ResourceID.generate();
        final TestingJobMasterGateway jobMasterGateway =
                setupJobManagerGateway(
                        slotOfferLatch,
                        deployedExecutionsQueue,
                        taskFinishedFuture,
                        jobManagerResourceId);

        final CompletableFuture<SlotReport> initialSlotReportFuture = new CompletableFuture<>();
        final TestingResourceManagerGateway testingResourceManagerGateway =
                setupResourceManagerGateway(initialSlotReportFuture);

        final TaskManagerServices taskManagerServices =
                new TaskManagerServicesBuilder()
                        .setTaskSlotTable(TaskSlotUtils.createTaskSlotTable(1, timeout))
                        .setShuffleEnvironment(new NettyShuffleEnvironmentBuilder().build())
                        .build();

        final TestingTaskExecutor taskExecutor = createTestingTaskExecutor(taskManagerServices);

        try {
            taskExecutor.start();
            taskExecutor.waitUntilStarted();

            final TaskExecutorGateway taskExecutorGateway =
                    taskExecutor.getSelfGateway(TaskExecutorGateway.class);

            final TaskDeploymentDescriptor taskDeploymentDescriptor =
                    createTaskDeploymentDescriptor(jobId);

            connectComponentsAndRequestSlot(
                    jobMasterGateway,
                    testingResourceManagerGateway,
                    taskExecutorGateway,
                    taskManagerServices.getJobLeaderService(),
                    initialSlotReportFuture,
                    taskDeploymentDescriptor.getAllocationId());

            TestingInvokable.sync = new BlockerSync();

            // This ensures TM has been successfully registered to JM.
            slotOfferLatch.await();

            AllocatedSlotReport slotAllocationReport =
                    new AllocatedSlotReport(
                            jobId,
                            Collections.singleton(
                                    new AllocatedSlotInfo(
                                            0, taskDeploymentDescriptor.getAllocationId())));

            // nothing as deployed, so the deployment report should be empty
            taskExecutorGateway.heartbeatFromJobManager(jobManagerResourceId, slotAllocationReport);
            assertThat(deployedExecutionsQueue.take(), hasSize(0));

            taskExecutorGateway
                    .submitTask(
                            taskDeploymentDescriptor, jobMasterGateway.getFencingToken(), timeout)
                    .get();

            TestingInvokable.sync.awaitBlocker();

            // task is deployed, so the deployment report should contain it
            taskExecutorGateway.heartbeatFromJobManager(jobManagerResourceId, slotAllocationReport);
            assertThat(
                    deployedExecutionsQueue.take(),
                    hasItem(taskDeploymentDescriptor.getExecutionAttemptId()));

            TestingInvokable.sync.releaseBlocker();

            // task is finished ans was cleaned up, so the deployment report should be empty
            taskFinishedFuture.get();
            taskExecutorGateway.heartbeatFromJobManager(jobManagerResourceId, slotAllocationReport);
            assertThat(deployedExecutionsQueue.take(), hasSize(0));
        } finally {
            RpcUtils.terminateRpcEndpoint(taskExecutor, timeout);
        }
    }

    /** Test invokable which completes the given future when executed. */
    public static class TestingInvokable extends AbstractInvokable {

        static BlockerSync sync;

        public TestingInvokable(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() throws Exception {
            sync.block();
        }
    }

    private TestingTaskExecutor createTestingTaskExecutor(TaskManagerServices taskManagerServices)
            throws IOException {
        final Configuration configuration = new Configuration();
        return new TestingTaskExecutor(
                RPC_SERVICE_RESOURCE.getTestingRpcService(),
                TaskManagerConfiguration.fromConfiguration(
                        configuration,
                        TaskExecutorResourceUtils.resourceSpecFromConfigForLocalExecution(
                                configuration),
                        InetAddress.getLoopbackAddress().getHostAddress()),
                haServices,
                taskManagerServices,
                ExternalResourceInfoProvider.NO_EXTERNAL_RESOURCES,
                new HeartbeatServices(1_000L, 30_000L),
                UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup(),
                null,
                new BlobCacheService(configuration, new VoidBlobStore(), null),
                testingFatalErrorHandlerResource.getFatalErrorHandler(),
                new TestingTaskExecutorPartitionTracker());
    }

    private static TaskDeploymentDescriptor createTaskDeploymentDescriptor(JobID jobId)
            throws IOException {
        return TaskDeploymentDescriptorBuilder.newBuilder(jobId, TestingInvokable.class).build();
    }

    private static TestingJobMasterGateway setupJobManagerGateway(
            OneShotLatch slotOfferLatch,
            BlockingQueue<Set<ExecutionAttemptID>> deployedExecutionsFuture,
            CompletableFuture<Void> taskFinishedFuture,
            ResourceID jobManagerResourceId) {
        return new TestingJobMasterGatewayBuilder()
                .setRegisterTaskManagerFunction(
                        (s, location, ignored) ->
                                CompletableFuture.completedFuture(
                                        new JMTMRegistrationSuccess(jobManagerResourceId)))
                .setOfferSlotsFunction(
                        (resourceID, slotOffers) -> {
                            slotOfferLatch.trigger();
                            return CompletableFuture.completedFuture(slotOffers);
                        })
                .setTaskManagerHeartbeatFunction(
                        (resourceID, taskExecutorToJobManagerHeartbeatPayload) -> {
                            ExecutionDeploymentReport executionDeploymentReport =
                                    taskExecutorToJobManagerHeartbeatPayload
                                            .getExecutionDeploymentReport();
                            deployedExecutionsFuture.add(executionDeploymentReport.getExecutions());
                            return FutureUtils.completedVoidFuture();
                        })
                .setUpdateTaskExecutionStateFunction(
                        taskExecutionState -> {
                            if (taskExecutionState.getExecutionState() == ExecutionState.FINISHED) {
                                taskFinishedFuture.complete(null);
                            }
                            return CompletableFuture.completedFuture(Acknowledge.get());
                        })
                .build();
    }

    private static TestingResourceManagerGateway setupResourceManagerGateway(
            CompletableFuture<SlotReport> initialSlotReportFuture) {
        final TestingResourceManagerGateway testingResourceManagerGateway =
                new TestingResourceManagerGateway();
        testingResourceManagerGateway.setSendSlotReportFunction(
                resourceIDInstanceIDSlotReportTuple3 -> {
                    initialSlotReportFuture.complete(resourceIDInstanceIDSlotReportTuple3.f2);
                    return CompletableFuture.completedFuture(Acknowledge.get());
                });
        testingResourceManagerGateway.setRegisterTaskExecutorFunction(
                input ->
                        CompletableFuture.completedFuture(
                                new TaskExecutorRegistrationSuccess(
                                        new InstanceID(),
                                        testingResourceManagerGateway.getOwnResourceId(),
                                        new ClusterInformation("blobServerHost", 55555))));
        return testingResourceManagerGateway;
    }

    private void connectComponentsAndRequestSlot(
            JobMasterGateway jobMasterGateway,
            ResourceManagerGateway resourceManagerGateway,
            TaskExecutorGateway taskExecutorGateway,
            JobLeaderService jobLeaderService,
            CompletableFuture<SlotReport> initialSlotReportFuture,
            AllocationID allocationId)
            throws Exception {
        final String jobMasterAddress = "jm";
        RPC_SERVICE_RESOURCE
                .getTestingRpcService()
                .registerGateway(jobMasterAddress, jobMasterGateway);
        RPC_SERVICE_RESOURCE
                .getTestingRpcService()
                .registerGateway(resourceManagerGateway.getAddress(), resourceManagerGateway);

        // inform the task manager about the job leader
        jobLeaderService.addJob(jobId, jobMasterAddress);
        jobManagerLeaderRetriever.notifyListener(jobMasterAddress, UUID.randomUUID());
        resourceManagerLeaderRetriever.notifyListener(
                resourceManagerGateway.getAddress(),
                resourceManagerGateway.getFencingToken().toUUID());

        final Optional<SlotStatus> slotStatusOptional =
                StreamSupport.stream(initialSlotReportFuture.get().spliterator(), false).findAny();

        assertTrue(slotStatusOptional.isPresent());

        taskExecutorGateway
                .requestSlot(
                        slotStatusOptional.get().getSlotID(),
                        jobId,
                        allocationId,
                        ResourceProfile.ZERO,
                        jobMasterAddress,
                        resourceManagerGateway.getFencingToken(),
                        timeout)
                .get();
    }
}
