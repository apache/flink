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
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.io.network.partition.NoOpResourceManagerPartitionTracker;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.slotmanager.DeclarativeSlotManagerBuilder;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.TestingSlotManagerBuilder;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerInfo;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.rpc.exceptions.RecipientUnreachableException;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorMemoryConfiguration;
import org.apache.flink.runtime.taskexecutor.TaskExecutorThreadInfoGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.testutils.TestingUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/** Tests for the {@link ResourceManager}. */
public class ResourceManagerTest extends TestLogger {

    private static final Time TIMEOUT = Time.minutes(2L);

    private static final HeartbeatServices heartbeatServices = new HeartbeatServices(1000L, 10000L);

    private static final HeartbeatServices fastHeartbeatServices = new HeartbeatServices(1L, 1L);

    private static final HeartbeatServices failedRpcEnabledHeartbeatServices =
            new HeartbeatServices(1L, 10000000L, 1);

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

    @BeforeClass
    public static void setupClass() {
        rpcService = new TestingRpcService();
    }

    @Before
    public void setup() throws Exception {
        highAvailabilityServices = new TestingHighAvailabilityServices();
        highAvailabilityServices.setResourceManagerLeaderElectionService(
                new TestingLeaderElectionService());
        testingFatalErrorHandler = new TestingFatalErrorHandler();
        resourceManagerResourceId = ResourceID.generate();
    }

    @After
    public void after() throws Exception {
        if (resourceManager != null) {
            RpcUtils.terminateRpcEndpoint(resourceManager, TIMEOUT);
        }

        if (highAvailabilityServices != null) {
            highAvailabilityServices.closeAndCleanupAllData();
        }

        if (testingFatalErrorHandler.hasExceptionOccurred()) {
            testingFatalErrorHandler.rethrowError();
        }

        if (rpcService != null) {
            rpcService.clearGateways();
        }
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        if (rpcService != null) {
            RpcUtils.terminateRpcServices(TIMEOUT, rpcService);
        }
    }

    /**
     * Tests that we can retrieve the correct {@link TaskManagerInfo} from the {@link
     * ResourceManager}.
     */
    @Test
    public void testRequestTaskManagerInfo() throws Exception {
        final ResourceID taskManagerId = ResourceID.generate();
        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setAddress(UUID.randomUUID().toString())
                        .createTestingTaskExecutorGateway();
        rpcService.registerGateway(taskExecutorGateway.getAddress(), taskExecutorGateway);

        resourceManager = new ResourceManagerBuilder().buildAndStart();
        final ResourceManagerGateway resourceManagerGateway =
                resourceManager.getSelfGateway(ResourceManagerGateway.class);

        registerTaskExecutor(
                resourceManagerGateway, taskManagerId, taskExecutorGateway.getAddress());

        CompletableFuture<TaskManagerInfoWithSlots> taskManagerInfoFuture =
                resourceManagerGateway.requestTaskManagerDetailsInfo(
                        taskManagerId, TestingUtils.TIMEOUT);

        TaskManagerInfoWithSlots taskManagerInfoWithSlots = taskManagerInfoFuture.get();
        TaskManagerInfo taskManagerInfo = taskManagerInfoWithSlots.getTaskManagerInfo();

        assertEquals(taskManagerId, taskManagerInfo.getResourceId());
        assertEquals(hardwareDescription, taskManagerInfo.getHardwareDescription());
        assertEquals(taskExecutorGateway.getAddress(), taskManagerInfo.getAddress());
        assertEquals(dataPort, taskManagerInfo.getDataPort());
        assertEquals(jmxPort, taskManagerInfo.getJmxPort());
        assertEquals(0, taskManagerInfo.getNumberSlots());
        assertEquals(0, taskManagerInfo.getNumberAvailableSlots());
        assertThat(taskManagerInfoWithSlots.getAllocatedSlots(), is(empty()));
    }

    /**
     * Tests that we can retrieve the correct {@link TaskExecutorGateway} from the {@link
     * ResourceManager}.
     */
    @Test
    public void testRequestTaskExecutorGateway() throws Exception {
        final ResourceID taskManagerId = ResourceID.generate();
        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setAddress(UUID.randomUUID().toString())
                        .createTestingTaskExecutorGateway();
        rpcService.registerGateway(taskExecutorGateway.getAddress(), taskExecutorGateway);

        resourceManager = new ResourceManagerBuilder().buildAndStart();
        final ResourceManagerGateway resourceManagerGateway =
                resourceManager.getSelfGateway(ResourceManagerGateway.class);

        registerTaskExecutor(
                resourceManagerGateway, taskManagerId, taskExecutorGateway.getAddress());

        CompletableFuture<TaskExecutorThreadInfoGateway> taskExecutorGatewayFuture =
                resourceManagerGateway.requestTaskExecutorThreadInfoGateway(
                        taskManagerId, TestingUtils.TIMEOUT);

        TaskExecutorThreadInfoGateway taskExecutorGatewayResult = taskExecutorGatewayFuture.get();

        assertEquals(taskExecutorGateway, taskExecutorGatewayResult);
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
                        ResourceProfile.ZERO);
        final CompletableFuture<RegistrationResponse> registrationFuture =
                resourceManagerGateway.registerTaskExecutor(
                        taskExecutorRegistration, TestingUtils.TIMEOUT);

        assertThat(registrationFuture.get(), instanceOf(RegistrationResponse.Success.class));
    }

    @Test
    public void testDisconnectJobManagerClearsRequirements() throws Exception {
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
                .registerJobManager(
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

        assertThat(clearRequirementsFuture.get(5, TimeUnit.SECONDS), is(jobId));
    }

    @Test
    public void testHeartbeatTimeoutWithJobMaster() throws Exception {
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
                    assertThat(requestedJobId, is(equalTo(jobId)));
                    return jobMasterLeaderRetrievalService;
                });

        runHeartbeatTimeoutTest(
                (ignore) -> {},
                resourceManagerGateway -> {
                    final CompletableFuture<RegistrationResponse> registrationFuture =
                            resourceManagerGateway.registerJobManager(
                                    jobMasterGateway.getFencingToken(),
                                    jobMasterResourceId,
                                    jobMasterGateway.getAddress(),
                                    jobId,
                                    TIMEOUT);

                    assertThat(
                            registrationFuture.get(),
                            instanceOf(RegistrationResponse.Success.class));
                },
                resourceManagerResourceId -> {
                    // might have been completed or not depending whether the timeout was triggered
                    // first
                    final ResourceID optionalHeartbeatRequestOrigin =
                            heartbeatRequestFuture.getNow(null);
                    assertThat(
                            optionalHeartbeatRequestOrigin,
                            anyOf(is(resourceManagerResourceId), is(nullValue())));
                    assertThat(disconnectFuture.get(), is(equalTo(resourceManagerId)));
                });
    }

    @Test
    public void testJobMasterBecomesUnreachableTriggersDisconnect() throws Exception {
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
                    assertThat(requestedJobId, is(equalTo(jobId)));
                    return jobMasterLeaderRetrievalService;
                });

        runHeartbeatTargetBecomesUnreachableTest(
                (ignore) -> {},
                resourceManagerGateway -> {
                    final CompletableFuture<RegistrationResponse> registrationFuture =
                            resourceManagerGateway.registerJobManager(
                                    jobMasterGateway.getFencingToken(),
                                    jobMasterResourceId,
                                    jobMasterGateway.getAddress(),
                                    jobId,
                                    TIMEOUT);

                    assertThat(
                            registrationFuture.get(),
                            instanceOf(RegistrationResponse.Success.class));
                },
                resourceManagerResourceId ->
                        assertThat(disconnectFuture.get(), is(equalTo(resourceManagerId))));
    }

    @Test
    public void testHeartbeatTimeoutWithTaskExecutor() throws Exception {
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
                builder ->
                        builder.withStopWorkerFunction(
                                (worker) -> {
                                    stopWorkerFuture.complete(worker);
                                    return true;
                                }),
                resourceManagerGateway -> {
                    registerTaskExecutor(
                            resourceManagerGateway,
                            taskExecutorId,
                            taskExecutorGateway.getAddress());
                },
                resourceManagerResourceId -> {
                    // might have been completed or not depending whether the timeout was triggered
                    // first
                    final ResourceID optionalHeartbeatRequestOrigin =
                            heartbeatRequestFuture.getNow(null);
                    assertThat(
                            optionalHeartbeatRequestOrigin,
                            anyOf(is(resourceManagerResourceId), is(nullValue())));
                    assertThat(disconnectFuture.get(), instanceOf(TimeoutException.class));
                    assertThat(stopWorkerFuture.get(), is(taskExecutorId));
                });
    }

    @Test
    public void testTaskExecutorBecomesUnreachableTriggersDisconnect() throws Exception {
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
                builder ->
                        builder.withStopWorkerFunction(
                                (worker) -> {
                                    stopWorkerFuture.complete(worker);
                                    return true;
                                }),
                resourceManagerGateway ->
                        registerTaskExecutor(
                                resourceManagerGateway,
                                taskExecutorId,
                                taskExecutorGateway.getAddress()),
                resourceManagerResourceId -> {
                    assertThat(disconnectFuture.get(), instanceOf(ResourceManagerException.class));
                    assertThat(stopWorkerFuture.get(), is(taskExecutorId));
                });
    }

    @Test
    public void testDisconnectJobManagerWithTerminalStatusShouldRemoveJob() throws Exception {
        testDisconnectJobManager(JobStatus.CANCELED);
    }

    @Test
    public void testDisconnectJobManagerWithNonTerminalStatusShouldNotRemoveJob() throws Exception {
        testDisconnectJobManager(JobStatus.FAILING);
    }

    @Test
    public void testDisconnectTaskManager() throws Exception {
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
                        .withStopWorkerFunction(stopWorkerFuture::complete)
                        .buildAndStart();

        registerTaskExecutor(resourceManager, taskExecutorId, taskExecutorGateway.getAddress());
        resourceManager.disconnectTaskManager(taskExecutorId, new FlinkException("Test exception"));

        assertThat(disconnectFuture.get(), instanceOf(FlinkException.class));
        assertThat(stopWorkerFuture.get(), is(taskExecutorId));
    }

    private void testDisconnectJobManager(JobStatus jobStatus) throws Exception {
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
                        .buildAndStart();

        highAvailabilityServices.setJobMasterLeaderRetrieverFunction(
                requestedJobId ->
                        new SettableLeaderRetrievalService(
                                jobMasterGateway.getAddress(),
                                jobMasterGateway.getFencingToken().toUUID()));

        final JobID jobId = JobID.generate();
        final ResourceManagerGateway resourceManagerGateway =
                resourceManager.getSelfGateway(ResourceManagerGateway.class);
        resourceManagerGateway.registerJobManager(
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
            try {
                jobRemoved.await(10L, TimeUnit.MILLISECONDS);
                fail("We should not have removed the job.");
            } catch (TimeoutException expected) {
            }
        }
    }

    private void runHeartbeatTimeoutTest(
            Consumer<ResourceManagerBuilder> prepareResourceManager,
            ThrowingConsumer<ResourceManagerGateway, Exception> registerComponentAtResourceManager,
            ThrowingConsumer<ResourceID, Exception> verifyHeartbeatTimeout)
            throws Exception {
        final ResourceManagerBuilder rmBuilder = new ResourceManagerBuilder();
        prepareResourceManager.accept(rmBuilder);
        resourceManager = rmBuilder.withHeartbeatServices(fastHeartbeatServices).buildAndStart();
        final ResourceManagerGateway resourceManagerGateway =
                resourceManager.getSelfGateway(ResourceManagerGateway.class);

        registerComponentAtResourceManager.accept(resourceManagerGateway);
        verifyHeartbeatTimeout.accept(resourceManagerResourceId);
    }

    private void runHeartbeatTargetBecomesUnreachableTest(
            Consumer<ResourceManagerBuilder> prepareResourceManager,
            ThrowingConsumer<ResourceManagerGateway, Exception> registerComponentAtResourceManager,
            ThrowingConsumer<ResourceID, Exception> verifyHeartbeatTimeout)
            throws Exception {
        final ResourceManagerBuilder rmBuilder = new ResourceManagerBuilder();
        prepareResourceManager.accept(rmBuilder);
        resourceManager =
                rmBuilder.withHeartbeatServices(failedRpcEnabledHeartbeatServices).buildAndStart();
        final ResourceManagerGateway resourceManagerGateway =
                resourceManager.getSelfGateway(ResourceManagerGateway.class);

        registerComponentAtResourceManager.accept(resourceManagerGateway);
        verifyHeartbeatTimeout.accept(resourceManagerResourceId);
    }

    private class ResourceManagerBuilder {
        private HeartbeatServices heartbeatServices = null;
        private JobLeaderIdService jobLeaderIdService = null;
        private SlotManager slotManager = null;
        private Function<ResourceID, Boolean> stopWorkerFunction = null;

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

        private ResourceManagerBuilder withStopWorkerFunction(
                Function<ResourceID, Boolean> stopWorkerFunction) {
            this.stopWorkerFunction = stopWorkerFunction;
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
                        DeclarativeSlotManagerBuilder.newBuilder()
                                .setScheduledExecutor(rpcService.getScheduledExecutor())
                                .build();
            }

            if (stopWorkerFunction == null) {
                stopWorkerFunction = (ignore) -> false;
            }

            resourceManagerId = ResourceManagerId.generate();
            final TestingResourceManager resourceManager =
                    new TestingResourceManager(
                            rpcService,
                            resourceManagerId.toUUID(),
                            resourceManagerResourceId,
                            heartbeatServices,
                            slotManager,
                            NoOpResourceManagerPartitionTracker::get,
                            jobLeaderIdService,
                            testingFatalErrorHandler,
                            UnregisteredMetricGroups.createUnregisteredResourceManagerMetricGroup(),
                            stopWorkerFunction);

            resourceManager.start();
            resourceManager.getStartedFuture().get(TIMEOUT.getSize(), TIMEOUT.getUnit());

            return resourceManager;
        }
    }
}
