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
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blocklist.BlockedNode;
import org.apache.flink.runtime.blocklist.BlocklistHandler;
import org.apache.flink.runtime.blocklist.DefaultBlocklistHandler;
import org.apache.flink.runtime.blocklist.NoOpBlocklistHandler;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.HeartbeatServicesImpl;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.io.network.partition.NoOpResourceManagerPartitionTracker;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.leaderelection.StandaloneLeaderElection;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.slotmanager.DeclarativeSlotManagerBuilder;
import org.apache.flink.runtime.resourcemanager.slotmanager.FineGrainedSlotManagerBuilder;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.TestingSlotManagerBuilder;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerInfo;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.rpc.exceptions.RecipientUnreachableException;
import org.apache.flink.runtime.security.token.NoOpDelegationTokenManager;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorMemoryConfiguration;
import org.apache.flink.runtime.taskexecutor.TaskExecutorRegistrationSuccess;
import org.apache.flink.runtime.taskexecutor.TaskExecutorThreadInfoGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutor;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link ResourceManager}. */
class ResourceManagerTest {

    private static final Time TIMEOUT = Time.minutes(2L);

    private static final HeartbeatServices heartbeatServices =
            new HeartbeatServicesImpl(1000L, 10000L);

    private static final HeartbeatServices fastHeartbeatServices =
            new HeartbeatServicesImpl(1L, 1L);

    private static final HeartbeatServices failedRpcEnabledHeartbeatServices =
            new HeartbeatServicesImpl(1L, 10000000L, 1);

    private static final HardwareDescription hardwareDescription =
            new HardwareDescription(42, 1337L, 1337L, 0L);

    private static final int dataPort = 1234;

    private static final int jmxPort = 23456;

    private static TestingRpcService rpcService;

    private TestingHighAvailabilityServices highAvailabilityServices;

    private TestingFatalErrorHandler testingFatalErrorHandler;

    private ResourceID resourceManagerResourceId;

    private TestingResourceManager resourceManager;

    private ResourceManagerId resourceManagerId;

    @BeforeAll
    static void setupClass() {
        rpcService = new TestingRpcService();
    }

    @BeforeEach
    void setup() throws Exception {
        highAvailabilityServices = new TestingHighAvailabilityServices();
        highAvailabilityServices.setResourceManagerLeaderElection(
                new StandaloneLeaderElection(UUID.randomUUID()));
        testingFatalErrorHandler = new TestingFatalErrorHandler();
        resourceManagerResourceId = ResourceID.generate();
    }

    @AfterEach
    void after() throws Exception {
        if (resourceManager != null) {
            RpcUtils.terminateRpcEndpoint(resourceManager);
        }

        if (highAvailabilityServices != null) {
            highAvailabilityServices.closeWithOptionalClean(true);
        }

        if (testingFatalErrorHandler.hasExceptionOccurred()) {
            testingFatalErrorHandler.rethrowError();
        }

        if (rpcService != null) {
            rpcService.clearGateways();
        }
    }

    @AfterAll
    static void tearDownClass() throws Exception {
        if (rpcService != null) {
            RpcUtils.terminateRpcService(rpcService);
        }
    }

    private enum SlotManagerType {
        DECLARATIVE,
        FINE_GRAINED
    }

    private static SlotManager createSlotManager(SlotManagerType slotManagerType) {
        return createSlotManager(slotManagerType, rpcService.getScheduledExecutor());
    }

    private static SlotManager createSlotManager(
            SlotManagerType slotManagerType, ScheduledExecutor scheduledExecutor) {
        if (slotManagerType == SlotManagerType.DECLARATIVE) {
            return DeclarativeSlotManagerBuilder.newBuilder(scheduledExecutor).build();
        } else {
            return FineGrainedSlotManagerBuilder.newBuilder(scheduledExecutor).build();
        }
    }

    /**
     * Tests that we can retrieve the correct {@link TaskManagerInfo} from the {@link
     * ResourceManager}.
     */
    @ParameterizedTest
    @EnumSource(SlotManagerType.class)
    void testRequestTaskManagerInfo(SlotManagerType slotManagerType) throws Exception {
        final ResourceID taskManagerId = ResourceID.generate();
        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setAddress(UUID.randomUUID().toString())
                        .createTestingTaskExecutorGateway();
        rpcService.registerGateway(taskExecutorGateway.getAddress(), taskExecutorGateway);

        resourceManager =
                new ResourceManagerBuilder()
                        .withSlotManager(createSlotManager(slotManagerType))
                        .buildAndStart();
        final ResourceManagerGateway resourceManagerGateway =
                resourceManager.getSelfGateway(ResourceManagerGateway.class);

        registerTaskExecutor(
                resourceManagerGateway, taskManagerId, taskExecutorGateway.getAddress());

        CompletableFuture<TaskManagerInfoWithSlots> taskManagerInfoFuture =
                resourceManagerGateway.requestTaskManagerDetailsInfo(
                        taskManagerId, TestingUtils.TIMEOUT);

        TaskManagerInfoWithSlots taskManagerInfoWithSlots = taskManagerInfoFuture.get();
        TaskManagerInfo taskManagerInfo = taskManagerInfoWithSlots.getTaskManagerInfo();

        assertThat(taskManagerInfo.getResourceId()).isEqualTo(taskManagerId);
        assertThat(taskManagerInfo.getHardwareDescription()).isEqualTo(hardwareDescription);
        assertThat(taskManagerInfo.getAddress()).isEqualTo(taskExecutorGateway.getAddress());
        assertThat(taskManagerInfo.getDataPort()).isEqualTo(dataPort);
        assertThat(taskManagerInfo.getJmxPort()).isEqualTo(jmxPort);
        assertThat(taskManagerInfo.getNumberSlots()).isEqualTo(0);
        assertThat(taskManagerInfo.getNumberAvailableSlots()).isEqualTo(0);
        assertThat(taskManagerInfoWithSlots.getAllocatedSlots()).isEmpty();
    }

    /**
     * Tests that we can retrieve the correct {@link TaskExecutorGateway} from the {@link
     * ResourceManager}.
     */
    @ParameterizedTest
    @EnumSource(SlotManagerType.class)
    void testRequestTaskExecutorGateway(SlotManagerType slotManagerType) throws Exception {
        final ResourceID taskManagerId = ResourceID.generate();
        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setAddress(UUID.randomUUID().toString())
                        .createTestingTaskExecutorGateway();
        rpcService.registerGateway(taskExecutorGateway.getAddress(), taskExecutorGateway);

        resourceManager =
                new ResourceManagerBuilder()
                        .withSlotManager(createSlotManager(slotManagerType))
                        .buildAndStart();
        final ResourceManagerGateway resourceManagerGateway =
                resourceManager.getSelfGateway(ResourceManagerGateway.class);

        registerTaskExecutor(
                resourceManagerGateway, taskManagerId, taskExecutorGateway.getAddress());

        CompletableFuture<TaskExecutorThreadInfoGateway> taskExecutorGatewayFuture =
                resourceManagerGateway.requestTaskExecutorThreadInfoGateway(
                        taskManagerId, TestingUtils.TIMEOUT);

        TaskExecutorThreadInfoGateway taskExecutorGatewayResult = taskExecutorGatewayFuture.get();

        assertThat(taskExecutorGatewayResult).isEqualTo(taskExecutorGateway);
    }

    private void registerTaskExecutor(
            ResourceManagerGateway resourceManagerGateway,
            ResourceID taskExecutorId,
            String taskExecutorAddress)
            throws Exception {
        TaskExecutorRegistration taskExecutorRegistration =
                new TaskExecutorRegistration(
                        taskExecutorAddress,
                        taskExecutorId,
                        dataPort,
                        jmxPort,
                        hardwareDescription,
                        new TaskExecutorMemoryConfiguration(
                                1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L),
                        ResourceProfile.ZERO,
                        ResourceProfile.ZERO,
                        taskExecutorAddress);
        final CompletableFuture<RegistrationResponse> registrationFuture =
                resourceManagerGateway.registerTaskExecutor(
                        taskExecutorRegistration, TestingUtils.TIMEOUT);

        assertThat(registrationFuture.get()).isInstanceOf(RegistrationResponse.Success.class);
    }

    @Test
    void testDisconnectJobManagerClearsRequirements() throws Exception {
        final TestingJobMasterGateway jobMasterGateway =
                new TestingJobMasterGatewayBuilder()
                        .setAddress(UUID.randomUUID().toString())
                        .build();
        rpcService.registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);

        final JobLeaderIdService jobLeaderIdService =
                TestingJobLeaderIdService.newBuilder()
                        .setGetLeaderIdFunction(
                                jobId ->
                                        CompletableFuture.completedFuture(
                                                jobMasterGateway.getFencingToken()))
                        .build();

        final CompletableFuture<JobID> clearRequirementsFuture = new CompletableFuture<>();

        final SlotManager slotManager =
                new TestingSlotManagerBuilder()
                        .setClearRequirementsConsumer(clearRequirementsFuture::complete)
                        .createSlotManager();
        resourceManager =
                new ResourceManagerBuilder()
                        .withJobLeaderIdService(jobLeaderIdService)
                        .withSlotManager(slotManager)
                        .buildAndStart();

        final JobID jobId = JobID.generate();
        final ResourceManagerGateway resourceManagerGateway =
                resourceManager.getSelfGateway(ResourceManagerGateway.class);
        resourceManagerGateway
                .registerJobMaster(
                        jobMasterGateway.getFencingToken(),
                        ResourceID.generate(),
                        jobMasterGateway.getAddress(),
                        jobId,
                        TIMEOUT)
                .get();

        resourceManagerGateway
                .declareRequiredResources(
                        jobMasterGateway.getFencingToken(),
                        ResourceRequirements.create(
                                jobId,
                                jobMasterGateway.getAddress(),
                                Collections.singleton(
                                        ResourceRequirement.create(ResourceProfile.UNKNOWN, 1))),
                        TIMEOUT)
                .get();

        resourceManagerGateway.disconnectJobManager(
                jobId, JobStatus.FINISHED, new FlinkException("Test exception"));

        assertThat(clearRequirementsFuture.get(5, TimeUnit.SECONDS)).isEqualTo(jobId);
    }

    @Test
    void testProcessResourceRequirementsWhenRecoveryFinished() throws Exception {
        final TestingJobMasterGateway jobMasterGateway =
                new TestingJobMasterGatewayBuilder()
                        .setAddress(UUID.randomUUID().toString())
                        .build();
        rpcService.registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);

        final JobLeaderIdService jobLeaderIdService =
                TestingJobLeaderIdService.newBuilder()
                        .setGetLeaderIdFunction(
                                jobId ->
                                        CompletableFuture.completedFuture(
                                                jobMasterGateway.getFencingToken()))
                        .build();

        final CompletableFuture<Void> processRequirementsFuture = new CompletableFuture<>();
        final CompletableFuture<Void> readyToServeFuture = new CompletableFuture<>();

        final SlotManager slotManager =
                new TestingSlotManagerBuilder()
                        .setProcessRequirementsConsumer(
                                r -> processRequirementsFuture.complete(null))
                        .createSlotManager();
        resourceManager =
                new ResourceManagerBuilder()
                        .withJobLeaderIdService(jobLeaderIdService)
                        .withSlotManager(slotManager)
                        .withReadyToServeFuture(readyToServeFuture)
                        .buildAndStart();

        final JobID jobId = JobID.generate();
        final ResourceManagerGateway resourceManagerGateway =
                resourceManager.getSelfGateway(ResourceManagerGateway.class);
        resourceManagerGateway
                .registerJobMaster(
                        jobMasterGateway.getFencingToken(),
                        ResourceID.generate(),
                        jobMasterGateway.getAddress(),
                        jobId,
                        TIMEOUT)
                .get();

        resourceManagerGateway.declareRequiredResources(
                jobMasterGateway.getFencingToken(),
                ResourceRequirements.create(
                        jobId,
                        jobMasterGateway.getAddress(),
                        Collections.singleton(
                                ResourceRequirement.create(ResourceProfile.UNKNOWN, 1))),
                TIMEOUT);
        resourceManager
                .runInMainThread(
                        () -> {
                            assertThat(processRequirementsFuture.isDone()).isFalse();
                            readyToServeFuture.complete(null);
                            return null;
                        },
                        TIMEOUT)
                .get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
        processRequirementsFuture.get();
    }

    @ParameterizedTest
    @EnumSource(SlotManagerType.class)
    void testHeartbeatTimeoutWithJobMaster(SlotManagerType slotManagerType) throws Exception {
        final CompletableFuture<ResourceID> heartbeatRequestFuture = new CompletableFuture<>();
        final CompletableFuture<ResourceManagerId> disconnectFuture = new CompletableFuture<>();
        final TestingJobMasterGateway jobMasterGateway =
                new TestingJobMasterGatewayBuilder()
                        .setResourceManagerHeartbeatFunction(
                                resourceId -> {
                                    heartbeatRequestFuture.complete(resourceId);
                                    return FutureUtils.completedVoidFuture();
                                })
                        .setDisconnectResourceManagerConsumer(disconnectFuture::complete)
                        .build();
        rpcService.registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);
        final JobID jobId = new JobID();
        final ResourceID jobMasterResourceId = ResourceID.generate();
        final LeaderRetrievalService jobMasterLeaderRetrievalService =
                new SettableLeaderRetrievalService(
                        jobMasterGateway.getAddress(), jobMasterGateway.getFencingToken().toUUID());

        highAvailabilityServices.setJobMasterLeaderRetrieverFunction(
                requestedJobId -> {
                    assertThat(requestedJobId).isEqualTo(jobId);
                    return jobMasterLeaderRetrievalService;
                });

        runHeartbeatTimeoutTest(
                (ignore) -> {},
                resourceManagerGateway -> {
                    final CompletableFuture<RegistrationResponse> registrationFuture =
                            resourceManagerGateway.registerJobMaster(
                                    jobMasterGateway.getFencingToken(),
                                    jobMasterResourceId,
                                    jobMasterGateway.getAddress(),
                                    jobId,
                                    TIMEOUT);

                    assertThat(registrationFuture.get())
                            .isInstanceOf(RegistrationResponse.Success.class);
                },
                resourceManagerResourceId -> {
                    // might have been completed or not depending whether the timeout was triggered
                    // first
                    final ResourceID optionalHeartbeatRequestOrigin =
                            heartbeatRequestFuture.getNow(null);

                    assertThat(optionalHeartbeatRequestOrigin)
                            .satisfiesAnyOf(
                                    resourceID ->
                                            assertThat(resourceID)
                                                    .isEqualTo(resourceManagerResourceId),
                                    resourceID -> assertThat(resourceID).isNull());
                    assertThat(disconnectFuture.get()).isEqualTo(resourceManagerId);
                },
                slotManagerType);
    }

    @ParameterizedTest
    @EnumSource(SlotManagerType.class)
    void testJobMasterBecomesUnreachableTriggersDisconnect(SlotManagerType slotManagerType)
            throws Exception {
        final JobID jobId = new JobID();
        final ResourceID jobMasterResourceId = ResourceID.generate();
        final CompletableFuture<ResourceManagerId> disconnectFuture = new CompletableFuture<>();
        final TestingJobMasterGateway jobMasterGateway =
                new TestingJobMasterGatewayBuilder()
                        .setAddress(UUID.randomUUID().toString())
                        .setResourceManagerHeartbeatFunction(
                                resourceId ->
                                        FutureUtils.completedExceptionally(
                                                new RecipientUnreachableException(
                                                        "sender",
                                                        "recipient",
                                                        "task executor is unreachable")))
                        .setDisconnectResourceManagerConsumer(disconnectFuture::complete)
                        .build();
        rpcService.registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);

        final LeaderRetrievalService jobMasterLeaderRetrievalService =
                new SettableLeaderRetrievalService(
                        jobMasterGateway.getAddress(), jobMasterGateway.getFencingToken().toUUID());

        highAvailabilityServices.setJobMasterLeaderRetrieverFunction(
                requestedJobId -> {
                    assertThat(requestedJobId).isEqualTo(jobId);
                    return jobMasterLeaderRetrievalService;
                });

        runHeartbeatTargetBecomesUnreachableTest(
                (ignore) -> {},
                resourceManagerGateway -> {
                    final CompletableFuture<RegistrationResponse> registrationFuture =
                            resourceManagerGateway.registerJobMaster(
                                    jobMasterGateway.getFencingToken(),
                                    jobMasterResourceId,
                                    jobMasterGateway.getAddress(),
                                    jobId,
                                    TIMEOUT);

                    assertThat(registrationFuture.get())
                            .isInstanceOf(RegistrationResponse.Success.class);
                },
                resourceManagerResourceId ->
                        assertThat(disconnectFuture.get()).isEqualTo(resourceManagerId),
                slotManagerType);
    }

    @ParameterizedTest
    @EnumSource(SlotManagerType.class)
    void testHeartbeatTimeoutWithTaskExecutor(SlotManagerType slotManagerType) throws Exception {
        final ResourceID taskExecutorId = ResourceID.generate();
        final CompletableFuture<ResourceID> heartbeatRequestFuture = new CompletableFuture<>();
        final CompletableFuture<Exception> disconnectFuture = new CompletableFuture<>();
        final CompletableFuture<ResourceID> stopWorkerFuture = new CompletableFuture<>();
        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setDisconnectResourceManagerConsumer(disconnectFuture::complete)
                        .setHeartbeatResourceManagerFunction(
                                resourceId -> {
                                    heartbeatRequestFuture.complete(resourceId);
                                    return FutureUtils.completedVoidFuture();
                                })
                        .createTestingTaskExecutorGateway();
        rpcService.registerGateway(taskExecutorGateway.getAddress(), taskExecutorGateway);

        runHeartbeatTimeoutTest(
                builder -> builder.withStopWorkerConsumer(stopWorkerFuture::complete),
                resourceManagerGateway ->
                        registerTaskExecutor(
                                resourceManagerGateway,
                                taskExecutorId,
                                taskExecutorGateway.getAddress()),
                resourceManagerResourceId -> {
                    // might have been completed or not depending whether the timeout was triggered
                    // first
                    final ResourceID optionalHeartbeatRequestOrigin =
                            heartbeatRequestFuture.getNow(null);
                    assertThat(optionalHeartbeatRequestOrigin)
                            .satisfiesAnyOf(
                                    resourceID ->
                                            assertThat(resourceID)
                                                    .isEqualTo(resourceManagerResourceId),
                                    resourceID -> assertThat(resourceID).isNull());
                    assertThat(disconnectFuture.get()).isInstanceOf(TimeoutException.class);
                    assertThat(stopWorkerFuture.get()).isEqualTo(taskExecutorId);
                },
                slotManagerType);
    }

    @ParameterizedTest
    @EnumSource(SlotManagerType.class)
    void testTaskExecutorBecomesUnreachableTriggersDisconnect(SlotManagerType slotManagerType)
            throws Exception {
        final ResourceID taskExecutorId = ResourceID.generate();
        final CompletableFuture<Exception> disconnectFuture = new CompletableFuture<>();
        final CompletableFuture<ResourceID> stopWorkerFuture = new CompletableFuture<>();
        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setAddress(UUID.randomUUID().toString())
                        .setDisconnectResourceManagerConsumer(disconnectFuture::complete)
                        .setHeartbeatResourceManagerFunction(
                                resourceId ->
                                        FutureUtils.completedExceptionally(
                                                new RecipientUnreachableException(
                                                        "sender",
                                                        "recipient",
                                                        "task executor is unreachable")))
                        .createTestingTaskExecutorGateway();
        rpcService.registerGateway(taskExecutorGateway.getAddress(), taskExecutorGateway);

        runHeartbeatTargetBecomesUnreachableTest(
                builder -> builder.withStopWorkerConsumer(stopWorkerFuture::complete),
                resourceManagerGateway ->
                        registerTaskExecutor(
                                resourceManagerGateway,
                                taskExecutorId,
                                taskExecutorGateway.getAddress()),
                resourceManagerResourceId -> {
                    assertThat(disconnectFuture.get()).isInstanceOf(ResourceManagerException.class);
                    assertThat(stopWorkerFuture.get()).isEqualTo(taskExecutorId);
                },
                slotManagerType);
    }

    @ParameterizedTest
    @EnumSource(SlotManagerType.class)
    void testDisconnectJobManagerWithTerminalStatusShouldRemoveJob(SlotManagerType slotManagerType)
            throws Exception {
        testDisconnectJobManager(JobStatus.CANCELED, slotManagerType);
    }

    @ParameterizedTest
    @EnumSource(SlotManagerType.class)
    void testDisconnectJobManagerWithNonTerminalStatusShouldNotRemoveJob(
            SlotManagerType slotManagerType) throws Exception {
        testDisconnectJobManager(JobStatus.FAILING, slotManagerType);
    }

    @ParameterizedTest
    @EnumSource(SlotManagerType.class)
    void testDisconnectTaskManager(SlotManagerType slotManagerType) throws Exception {
        final ResourceID taskExecutorId = ResourceID.generate();
        final CompletableFuture<Exception> disconnectFuture = new CompletableFuture<>();
        final CompletableFuture<ResourceID> stopWorkerFuture = new CompletableFuture<>();

        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setDisconnectResourceManagerConsumer(disconnectFuture::complete)
                        .createTestingTaskExecutorGateway();
        rpcService.registerGateway(taskExecutorGateway.getAddress(), taskExecutorGateway);

        resourceManager =
                new ResourceManagerBuilder()
                        .withStopWorkerConsumer(stopWorkerFuture::complete)
                        .withSlotManager(createSlotManager(slotManagerType))
                        .buildAndStart();

        registerTaskExecutor(resourceManager, taskExecutorId, taskExecutorGateway.getAddress());
        resourceManager.disconnectTaskManager(taskExecutorId, new FlinkException("Test exception"));

        assertThat(disconnectFuture.get()).isInstanceOf(FlinkException.class);
        assertThat(stopWorkerFuture.get()).isEqualTo(taskExecutorId);
    }

    @Test
    void testUnblockResourcesWillTriggerResourceRequirementsCheck() throws Exception {

        final CompletableFuture<Void> triggerRequirementsCheckFuture = new CompletableFuture<>();

        final SlotManager slotManager =
                new TestingSlotManagerBuilder()
                        .setTriggerRequirementsCheckConsumer(
                                triggerRequirementsCheckFuture::complete)
                        .createSlotManager();
        resourceManager =
                new ResourceManagerBuilder()
                        .withSlotManager(slotManager)
                        .withBlocklistHandlerFactory(
                                new DefaultBlocklistHandler.Factory(Duration.ofMillis(100L)))
                        .buildAndStart();

        final ResourceManagerGateway resourceManagerGateway =
                resourceManager.getSelfGateway(ResourceManagerGateway.class);

        resourceManagerGateway.notifyNewBlockedNodes(
                Collections.singleton(
                        new BlockedNode("node", "Test cause", System.currentTimeMillis())));

        triggerRequirementsCheckFuture.get();
    }

    @ParameterizedTest
    @EnumSource(SlotManagerType.class)
    void testNewlyAddedBlockedNodesWillBeSynchronizedToAllRegisteredJobMasters(
            SlotManagerType slotManagerType) throws Exception {
        final JobID jobId1 = JobID.generate();
        final JobID jobId2 = JobID.generate();

        final Collection<BlockedNode> receivedBlockedNodes1 = new ArrayList<>();
        final Collection<BlockedNode> receivedBlockedNodes2 = new ArrayList<>();

        final JobMasterGateway jobMasterGateway1 = createJobMasterGateway(receivedBlockedNodes1);
        final JobMasterGateway jobMasterGateway2 = createJobMasterGateway(receivedBlockedNodes2);

        final JobLeaderIdService jobLeaderIdService =
                TestingJobLeaderIdService.newBuilder()
                        .setGetLeaderIdFunction(
                                jobId -> {
                                    JobMasterGateway leader;
                                    if (jobId.equals(jobId1)) {
                                        leader = jobMasterGateway1;
                                    } else if (jobId.equals(jobId2)) {
                                        leader = jobMasterGateway2;
                                    } else {
                                        throw new IllegalArgumentException("Unknown job");
                                    }
                                    return CompletableFuture.completedFuture(
                                            leader.getFencingToken());
                                })
                        .build();

        resourceManager =
                new ResourceManagerBuilder()
                        .withJobLeaderIdService(jobLeaderIdService)
                        .withBlocklistHandlerFactory(
                                new DefaultBlocklistHandler.Factory(Duration.ofMillis(100L)))
                        .withSlotManager(createSlotManager(slotManagerType))
                        .buildAndStart();

        final ResourceManagerGateway resourceManagerGateway =
                resourceManager.getSelfGateway(ResourceManagerGateway.class);

        // register the two job masters
        registerJobMasterToResourceManager(resourceManagerGateway, jobMasterGateway1, jobId1);
        registerJobMasterToResourceManager(resourceManagerGateway, jobMasterGateway2, jobId2);

        // add blocked node 1
        BlockedNode blockedNode1 = new BlockedNode("node1", "Test exception", Long.MAX_VALUE);
        resourceManagerGateway.notifyNewBlockedNodes(Collections.singleton(blockedNode1)).get();

        assertThat(receivedBlockedNodes1).containsExactly(blockedNode1);
        assertThat(receivedBlockedNodes2).containsExactly(blockedNode1);

        // disconnect job master 1
        resourceManagerGateway.disconnectJobManager(
                jobId1, JobStatus.FINISHED, new FlinkException("Test exception"));

        // add blocked node 2
        BlockedNode blockedNode2 = new BlockedNode("node2", "Test exception", Long.MAX_VALUE);
        resourceManagerGateway.notifyNewBlockedNodes(Collections.singleton(blockedNode2)).get();

        assertThat(receivedBlockedNodes1).containsExactly(blockedNode1);
        assertThat(receivedBlockedNodes2).containsExactlyInAnyOrder(blockedNode1, blockedNode2);
    }

    @ParameterizedTest
    @EnumSource(SlotManagerType.class)
    void testResourceOverviewWithBlockedSlots(SlotManagerType slotManagerType) throws Exception {
        ManuallyTriggeredScheduledExecutor executor = new ManuallyTriggeredScheduledExecutor();
        resourceManager =
                new ResourceManagerBuilder()
                        .withSlotManager(createSlotManager(slotManagerType, executor))
                        .withBlocklistHandlerFactory(
                                new DefaultBlocklistHandler.Factory(Duration.ofMillis(100L)))
                        .buildAndStart();

        final ResourceManagerGateway resourceManagerGateway =
                resourceManager.getSelfGateway(ResourceManagerGateway.class);

        ResourceID taskExecutor = ResourceID.generate();
        ResourceID taskExecutorToBlock = ResourceID.generate();
        registerTaskExecutorAndSlot(resourceManagerGateway, taskExecutor, 3);
        registerTaskExecutorAndSlot(resourceManagerGateway, taskExecutorToBlock, 5);
        executor.triggerAll();

        ResourceOverview overview =
                resourceManagerGateway.requestResourceOverview(Time.seconds(5)).get();
        assertThat(overview.getNumberTaskManagers()).isEqualTo(2);
        assertThat(overview.getNumberRegisteredSlots()).isEqualTo(8);
        assertThat(overview.getNumberFreeSlots()).isEqualTo(8);
        assertThat(overview.getNumberBlockedTaskManagers()).isEqualTo(0);
        assertThat(overview.getNumberBlockedFreeSlots()).isEqualTo(0);
        assertThat(overview.getTotalResource())
                .isEqualTo(ResourceProfile.fromResources(1, 1024).multiply(8));
        assertThat(overview.getFreeResource())
                .isEqualTo(ResourceProfile.fromResources(1, 1024).multiply(8));

        resourceManagerGateway.notifyNewBlockedNodes(
                Collections.singleton(
                        new BlockedNode(
                                resourceManager.getNodeIdOfTaskManager(taskExecutorToBlock),
                                "Test cause",
                                Long.MAX_VALUE)));

        ResourceOverview overviewBlocked =
                resourceManagerGateway.requestResourceOverview(Time.seconds(5)).get();
        assertThat(overviewBlocked.getNumberTaskManagers()).isEqualTo(2);
        assertThat(overviewBlocked.getNumberRegisteredSlots()).isEqualTo(8);
        assertThat(overviewBlocked.getNumberFreeSlots()).isEqualTo(3);
        assertThat(overviewBlocked.getNumberBlockedTaskManagers()).isEqualTo(1);
        assertThat(overviewBlocked.getNumberBlockedFreeSlots()).isEqualTo(5);
        assertThat(overviewBlocked.getTotalResource())
                .isEqualTo(ResourceProfile.fromResources(1, 1024).multiply(8));
        assertThat(overviewBlocked.getFreeResource())
                .isEqualTo(ResourceProfile.fromResources(1, 1024).multiply(3));
    }

    private void registerTaskExecutorAndSlot(
            ResourceManagerGateway resourceManagerGateway, ResourceID taskManagerId, int slotCount)
            throws Exception {
        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setAddress(UUID.randomUUID().toString())
                        .createTestingTaskExecutorGateway();
        rpcService.registerGateway(taskExecutorGateway.getAddress(), taskExecutorGateway);
        TaskExecutorRegistration taskExecutorRegistration =
                new TaskExecutorRegistration(
                        taskExecutorGateway.getAddress(),
                        taskManagerId,
                        dataPort,
                        jmxPort,
                        hardwareDescription,
                        new TaskExecutorMemoryConfiguration(
                                1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L),
                        ResourceProfile.fromResources(1, 1024),
                        ResourceProfile.fromResources(1, 1024).multiply(slotCount),
                        taskExecutorGateway.getAddress());
        RegistrationResponse registrationResult =
                resourceManagerGateway
                        .registerTaskExecutor(taskExecutorRegistration, TestingUtils.TIMEOUT)
                        .get();
        assertThat(registrationResult).isInstanceOf(TaskExecutorRegistrationSuccess.class);
        InstanceID instanceID =
                ((TaskExecutorRegistrationSuccess) registrationResult).getRegistrationId();
        List<SlotStatus> slots = new ArrayList<>();
        for (int i = 0; i < slotCount; i++) {
            slots.add(
                    new SlotStatus(
                            new SlotID(taskManagerId, i), ResourceProfile.fromResources(1, 1024)));
        }
        resourceManagerGateway.sendSlotReport(
                taskManagerId, instanceID, new SlotReport(slots), Time.seconds(5));
    }

    private JobMasterGateway createJobMasterGateway(Collection<BlockedNode> receivedBlockedNodes) {
        final TestingJobMasterGateway jobMasterGateway =
                new TestingJobMasterGatewayBuilder()
                        .setNotifyNewBlockedNodesFunction(
                                blockedNodes -> {
                                    receivedBlockedNodes.addAll(blockedNodes);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .setAddress(UUID.randomUUID().toString())
                        .build();
        rpcService.registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);
        return jobMasterGateway;
    }

    private static void registerJobMasterToResourceManager(
            ResourceManagerGateway resourceManagerGateway,
            JobMasterGateway jobMasterGateway,
            JobID jobId)
            throws Exception {
        resourceManagerGateway
                .registerJobMaster(
                        jobMasterGateway.getFencingToken(),
                        ResourceID.generate(),
                        jobMasterGateway.getAddress(),
                        jobId,
                        TIMEOUT)
                .get();
    }

    private void testDisconnectJobManager(JobStatus jobStatus, SlotManagerType slotManagerType)
            throws Exception {
        final TestingJobMasterGateway jobMasterGateway =
                new TestingJobMasterGatewayBuilder()
                        .setAddress(UUID.randomUUID().toString())
                        .build();
        rpcService.registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);

        final OneShotLatch jobAdded = new OneShotLatch();
        final OneShotLatch jobRemoved = new OneShotLatch();

        final JobLeaderIdService jobLeaderIdService =
                TestingJobLeaderIdService.newBuilder()
                        .setAddJobConsumer(ignored -> jobAdded.trigger())
                        .setRemoveJobConsumer(ignored -> jobRemoved.trigger())
                        .build();
        resourceManager =
                new ResourceManagerBuilder()
                        .withJobLeaderIdService(jobLeaderIdService)
                        .withSlotManager(createSlotManager(slotManagerType))
                        .buildAndStart();

        highAvailabilityServices.setJobMasterLeaderRetrieverFunction(
                requestedJobId ->
                        new SettableLeaderRetrievalService(
                                jobMasterGateway.getAddress(),
                                jobMasterGateway.getFencingToken().toUUID()));

        final JobID jobId = JobID.generate();
        final ResourceManagerGateway resourceManagerGateway =
                resourceManager.getSelfGateway(ResourceManagerGateway.class);
        resourceManagerGateway.registerJobMaster(
                jobMasterGateway.getFencingToken(),
                ResourceID.generate(),
                jobMasterGateway.getAddress(),
                jobId,
                TIMEOUT);

        jobAdded.await();

        resourceManagerGateway.disconnectJobManager(
                jobId, jobStatus, new FlinkException("Test exception"));

        if (jobStatus.isGloballyTerminalState()) {
            jobRemoved.await();
        } else {
            // job should not get removed
            assertThatThrownBy(
                            () -> jobRemoved.await(10L, TimeUnit.MILLISECONDS),
                            "We should not have removed the job.")
                    .isInstanceOf(TimeoutException.class);
        }
    }

    private void runHeartbeatTimeoutTest(
            Consumer<ResourceManagerBuilder> prepareResourceManager,
            ThrowingConsumer<ResourceManagerGateway, Exception> registerComponentAtResourceManager,
            ThrowingConsumer<ResourceID, Exception> verifyHeartbeatTimeout,
            SlotManagerType slotManagerType)
            throws Exception {
        final ResourceManagerBuilder rmBuilder = new ResourceManagerBuilder();
        prepareResourceManager.accept(rmBuilder);
        resourceManager =
                rmBuilder
                        .withHeartbeatServices(fastHeartbeatServices)
                        .withSlotManager(createSlotManager(slotManagerType))
                        .buildAndStart();
        final ResourceManagerGateway resourceManagerGateway =
                resourceManager.getSelfGateway(ResourceManagerGateway.class);

        registerComponentAtResourceManager.accept(resourceManagerGateway);
        verifyHeartbeatTimeout.accept(resourceManagerResourceId);
    }

    private void runHeartbeatTargetBecomesUnreachableTest(
            Consumer<ResourceManagerBuilder> prepareResourceManager,
            ThrowingConsumer<ResourceManagerGateway, Exception> registerComponentAtResourceManager,
            ThrowingConsumer<ResourceID, Exception> verifyHeartbeatTimeout,
            SlotManagerType slotManagerType)
            throws Exception {
        final ResourceManagerBuilder rmBuilder = new ResourceManagerBuilder();
        prepareResourceManager.accept(rmBuilder);
        resourceManager =
                rmBuilder
                        .withHeartbeatServices(failedRpcEnabledHeartbeatServices)
                        .withSlotManager(createSlotManager(slotManagerType))
                        .buildAndStart();
        final ResourceManagerGateway resourceManagerGateway =
                resourceManager.getSelfGateway(ResourceManagerGateway.class);

        registerComponentAtResourceManager.accept(resourceManagerGateway);
        verifyHeartbeatTimeout.accept(resourceManagerResourceId);
    }

    private class ResourceManagerBuilder {
        private HeartbeatServices heartbeatServices = null;
        private JobLeaderIdService jobLeaderIdService = null;
        private SlotManager slotManager = null;
        private BlocklistHandler.Factory blocklistHandlerFactory =
                new NoOpBlocklistHandler.Factory();
        private Consumer<ResourceID> stopWorkerConsumer = null;
        private CompletableFuture<Void> readyToServeFuture =
                CompletableFuture.completedFuture(null);

        private ResourceManagerBuilder withHeartbeatServices(HeartbeatServices heartbeatServices) {
            this.heartbeatServices = heartbeatServices;
            return this;
        }

        private ResourceManagerBuilder withJobLeaderIdService(
                JobLeaderIdService jobLeaderIdService) {
            this.jobLeaderIdService = jobLeaderIdService;
            return this;
        }

        private ResourceManagerBuilder withSlotManager(SlotManager slotManager) {
            this.slotManager = slotManager;
            return this;
        }

        private ResourceManagerBuilder withBlocklistHandlerFactory(
                BlocklistHandler.Factory blocklistHandlerFactory) {
            this.blocklistHandlerFactory = blocklistHandlerFactory;
            return this;
        }

        private ResourceManagerBuilder withStopWorkerConsumer(
                Consumer<ResourceID> stopWorkerConsumer) {
            this.stopWorkerConsumer = stopWorkerConsumer;
            return this;
        }

        public ResourceManagerBuilder withReadyToServeFuture(
                CompletableFuture<Void> readyToServeFuture) {
            this.readyToServeFuture = readyToServeFuture;
            return this;
        }

        private TestingResourceManager buildAndStart() throws Exception {
            if (heartbeatServices == null) {
                heartbeatServices = ResourceManagerTest.heartbeatServices;
            }

            if (jobLeaderIdService == null) {
                jobLeaderIdService =
                        new DefaultJobLeaderIdService(
                                highAvailabilityServices,
                                rpcService.getScheduledExecutor(),
                                TestingUtils.infiniteTime());
            }

            if (slotManager == null) {
                slotManager =
                        FineGrainedSlotManagerBuilder.newBuilder(rpcService.getScheduledExecutor())
                                .build();
            }

            if (stopWorkerConsumer == null) {
                stopWorkerConsumer = (ignore) -> {};
            }

            resourceManagerId = ResourceManagerId.generate();
            final TestingResourceManager resourceManager =
                    new TestingResourceManager(
                            rpcService,
                            resourceManagerId.toUUID(),
                            resourceManagerResourceId,
                            heartbeatServices,
                            new NoOpDelegationTokenManager(),
                            slotManager,
                            NoOpResourceManagerPartitionTracker::get,
                            blocklistHandlerFactory,
                            jobLeaderIdService,
                            testingFatalErrorHandler,
                            UnregisteredMetricGroups.createUnregisteredResourceManagerMetricGroup(),
                            stopWorkerConsumer,
                            readyToServeFuture);

            resourceManager.start();
            resourceManager.getStartedFuture().get(TIMEOUT.getSize(), TIMEOUT.getUnit());

            return resourceManager;
        }
    }
}
