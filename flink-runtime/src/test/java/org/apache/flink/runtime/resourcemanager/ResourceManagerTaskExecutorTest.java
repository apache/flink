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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.io.network.partition.NoOpResourceManagerPartitionTracker;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerBuilder;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerInfo;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.rpc.exceptions.FencingTokenException;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.runtime.taskexecutor.TaskExecutorMemoryConfiguration;
import org.apache.flink.runtime.taskexecutor.TaskExecutorRegistrationSuccess;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for the {@link ResourceManager} and {@link TaskExecutor} interaction. */
public class ResourceManagerTaskExecutorTest extends TestLogger {

    private static final Time TIMEOUT = Time.seconds(10L);

    private static final long HEARTBEAT_TIMEOUT = 5000;

    private static TestingRpcService rpcService;

    private TestingTaskExecutorGateway taskExecutorGateway;

    private int dataPort = 1234;

    private int jmxPort = 23456;

    private HardwareDescription hardwareDescription = new HardwareDescription(1, 2L, 3L, 4L);

    private ResourceID taskExecutorResourceID;

    private ResourceID resourceManagerResourceID;

    private StandaloneResourceManager resourceManager;

    private ResourceManagerGateway rmGateway;

    private ResourceManagerGateway wronglyFencedGateway;

    private TestingFatalErrorHandler testingFatalErrorHandler;

    @BeforeClass
    public static void setupClass() {
        rpcService = new TestingRpcService();
    }

    @Before
    public void setup() throws Exception {
        rpcService = new TestingRpcService();

        createAndRegisterTaskExecutorGateway();
        taskExecutorResourceID = ResourceID.generate();
        resourceManagerResourceID = ResourceID.generate();
        testingFatalErrorHandler = new TestingFatalErrorHandler();
        TestingLeaderElectionService rmLeaderElectionService = new TestingLeaderElectionService();
        resourceManager =
                createAndStartResourceManager(rmLeaderElectionService, testingFatalErrorHandler);
        rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);
        wronglyFencedGateway =
                rpcService
                        .connect(
                                resourceManager.getAddress(),
                                ResourceManagerId.generate(),
                                ResourceManagerGateway.class)
                        .get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);

        grantLeadership(rmLeaderElectionService)
                .get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
    }

    private void createAndRegisterTaskExecutorGateway() {
        taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
        rpcService.registerGateway(taskExecutorGateway.getAddress(), taskExecutorGateway);
    }

    private CompletableFuture<UUID> grantLeadership(
            TestingLeaderElectionService leaderElectionService) {
        UUID leaderSessionId = UUID.randomUUID();
        return leaderElectionService.isLeader(leaderSessionId);
    }

    private StandaloneResourceManager createAndStartResourceManager(
            LeaderElectionService rmLeaderElectionService, FatalErrorHandler fatalErrorHandler)
            throws Exception {
        TestingHighAvailabilityServices highAvailabilityServices =
                new TestingHighAvailabilityServices();
        HeartbeatServices heartbeatServices = new HeartbeatServices(1000L, HEARTBEAT_TIMEOUT);
        highAvailabilityServices.setResourceManagerLeaderElectionService(rmLeaderElectionService);

        SlotManager slotManager =
                SlotManagerBuilder.newBuilder()
                        .setScheduledExecutor(rpcService.getScheduledExecutor())
                        .build();

        JobLeaderIdService jobLeaderIdService =
                new JobLeaderIdService(
                        highAvailabilityServices,
                        rpcService.getScheduledExecutor(),
                        Time.minutes(5L));

        StandaloneResourceManager resourceManager =
                new StandaloneResourceManager(
                        rpcService,
                        resourceManagerResourceID,
                        highAvailabilityServices,
                        heartbeatServices,
                        slotManager,
                        NoOpResourceManagerPartitionTracker::get,
                        jobLeaderIdService,
                        new ClusterInformation("localhost", 1234),
                        fatalErrorHandler,
                        UnregisteredMetricGroups.createUnregisteredResourceManagerMetricGroup(),
                        Time.minutes(5L),
                        RpcUtils.INF_TIMEOUT,
                        ForkJoinPool.commonPool());

        resourceManager.start();

        return resourceManager;
    }

    @After
    public void teardown() throws Exception {
        if (resourceManager != null) {
            RpcUtils.terminateRpcEndpoint(resourceManager, TIMEOUT);
        }

        if (testingFatalErrorHandler != null && testingFatalErrorHandler.hasExceptionOccurred()) {
            testingFatalErrorHandler.rethrowError();
        }
    }

    @AfterClass
    public static void teardownClass() throws Exception {
        if (rpcService != null) {
            RpcUtils.terminateRpcService(rpcService, TIMEOUT);
        }
    }

    /**
     * Test receive normal registration from task executor and receive duplicate registration from
     * task executor.
     */
    @Test
    public void testRegisterTaskExecutor() throws Exception {
        // test response successful
        CompletableFuture<RegistrationResponse> successfulFuture =
                registerTaskExecutor(rmGateway, taskExecutorGateway.getAddress());

        RegistrationResponse response =
                successfulFuture.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
        assertTrue(response instanceof TaskExecutorRegistrationSuccess);
        final TaskManagerInfo taskManagerInfo =
                rmGateway.requestTaskManagerInfo(taskExecutorResourceID, TIMEOUT).get();
        assertThat(taskManagerInfo.getResourceId(), equalTo(taskExecutorResourceID));

        // test response successful with instanceID not equal to previous when receive duplicate
        // registration from taskExecutor
        CompletableFuture<RegistrationResponse> duplicateFuture =
                registerTaskExecutor(rmGateway, taskExecutorGateway.getAddress());

        RegistrationResponse duplicateResponse = duplicateFuture.get();
        assertTrue(duplicateResponse instanceof TaskExecutorRegistrationSuccess);
        assertNotEquals(
                ((TaskExecutorRegistrationSuccess) response).getRegistrationId(),
                ((TaskExecutorRegistrationSuccess) duplicateResponse).getRegistrationId());

        assertThat(rmGateway.requestResourceOverview(TIMEOUT).get().getNumberTaskManagers(), is(1));
    }

    /**
     * Test delayed registration of task executor where the delay is introduced during connection
     * from resource manager to the registering task executor.
     */
    @Test
    public void testDelayedRegisterTaskExecutor() throws Exception {
        final Time fastTimeout = Time.milliseconds(1L);
        try {
            final OneShotLatch startConnection = new OneShotLatch();
            final OneShotLatch finishConnection = new OneShotLatch();

            // first registration is with blocking connection
            rpcService.setRpcGatewayFutureFunction(
                    rpcGateway ->
                            CompletableFuture.supplyAsync(
                                    () -> {
                                        startConnection.trigger();
                                        try {
                                            finishConnection.await();
                                        } catch (InterruptedException ignored) {
                                        }
                                        return rpcGateway;
                                    },
                                    TestingUtils.defaultExecutor()));

            TaskExecutorRegistration taskExecutorRegistration =
                    new TaskExecutorRegistration(
                            taskExecutorGateway.getAddress(),
                            taskExecutorResourceID,
                            dataPort,
                            jmxPort,
                            hardwareDescription,
                            new TaskExecutorMemoryConfiguration(
                                    1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L),
                            ResourceProfile.ZERO,
                            ResourceProfile.ZERO);

            CompletableFuture<RegistrationResponse> firstFuture =
                    rmGateway.registerTaskExecutor(taskExecutorRegistration, fastTimeout);
            try {
                firstFuture.get();
                fail(
                        "Should have failed because connection to taskmanager is delayed beyond timeout");
            } catch (Exception e) {
                final Throwable cause = ExceptionUtils.stripExecutionException(e);
                assertThat(cause, instanceOf(TimeoutException.class));
                assertThat(
                        cause.getMessage(),
                        containsString("ResourceManagerGateway.registerTaskExecutor"));
            }

            startConnection.await();

            // second registration after timeout is with no delay, expecting it to be succeeded
            rpcService.resetRpcGatewayFutureFunction();
            CompletableFuture<RegistrationResponse> secondFuture =
                    rmGateway.registerTaskExecutor(taskExecutorRegistration, TIMEOUT);
            RegistrationResponse response = secondFuture.get();
            assertTrue(response instanceof TaskExecutorRegistrationSuccess);

            // on success, send slot report for taskmanager registration
            final SlotReport slotReport =
                    new SlotReport(
                            new SlotStatus(
                                    new SlotID(taskExecutorResourceID, 0), ResourceProfile.ANY));
            rmGateway
                    .sendSlotReport(
                            taskExecutorResourceID,
                            ((TaskExecutorRegistrationSuccess) response).getRegistrationId(),
                            slotReport,
                            TIMEOUT)
                    .get();

            // let the remaining part of the first registration proceed
            finishConnection.trigger();
            Thread.sleep(1L);

            // verify that the latest registration is valid not being unregistered by the delayed
            // one
            final TaskManagerInfo taskManagerInfo =
                    rmGateway.requestTaskManagerInfo(taskExecutorResourceID, TIMEOUT).get();
            assertThat(taskManagerInfo.getResourceId(), equalTo(taskExecutorResourceID));
            assertThat(taskManagerInfo.getNumberSlots(), equalTo(1));
        } finally {
            rpcService.resetRpcGatewayFutureFunction();
        }
    }

    /** Tests that a TaskExecutor can disconnect from the {@link ResourceManager}. */
    @Test
    public void testDisconnectTaskExecutor() throws Exception {
        final RegistrationResponse registrationResponse =
                registerTaskExecutor(rmGateway, taskExecutorGateway.getAddress()).get();
        assertThat(registrationResponse, instanceOf(TaskExecutorRegistrationSuccess.class));

        final InstanceID registrationId =
                ((TaskExecutorRegistrationSuccess) registrationResponse).getRegistrationId();
        final int numberSlots = 10;
        final Collection<SlotStatus> slots = createSlots(numberSlots);
        final SlotReport slotReport = new SlotReport(slots);
        rmGateway.sendSlotReport(taskExecutorResourceID, registrationId, slotReport, TIMEOUT).get();

        final ResourceOverview resourceOverview = rmGateway.requestResourceOverview(TIMEOUT).get();
        assertThat(resourceOverview.getNumberTaskManagers(), is(1));
        assertThat(resourceOverview.getNumberRegisteredSlots(), is(numberSlots));

        rmGateway.disconnectTaskManager(
                taskExecutorResourceID, new FlinkException("testDisconnectTaskExecutor"));

        final ResourceOverview afterDisconnectResourceOverview =
                rmGateway.requestResourceOverview(TIMEOUT).get();
        assertThat(afterDisconnectResourceOverview.getNumberTaskManagers(), is(0));
        assertThat(afterDisconnectResourceOverview.getNumberRegisteredSlots(), is(0));
    }

    private Collection<SlotStatus> createSlots(int numberSlots) {
        return IntStream.range(0, numberSlots)
                .mapToObj(
                        index ->
                                new SlotStatus(
                                        new SlotID(taskExecutorResourceID, index),
                                        ResourceProfile.ANY))
                .collect(Collectors.toList());
    }

    /** Test receive registration with unmatched leadershipId from task executor. */
    @Test
    public void testRegisterTaskExecutorWithUnmatchedLeaderSessionId() throws Exception {
        // test throw exception when receive a registration from taskExecutor which takes unmatched
        // leaderSessionId
        CompletableFuture<RegistrationResponse> unMatchedLeaderFuture =
                registerTaskExecutor(wronglyFencedGateway, taskExecutorGateway.getAddress());

        try {
            unMatchedLeaderFuture.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
            fail(
                    "Should have failed because we are using a wrongly fenced ResourceManagerGateway.");
        } catch (ExecutionException e) {
            assertTrue(ExceptionUtils.stripExecutionException(e) instanceof FencingTokenException);
        }
    }

    /** Test receive registration with invalid address from task executor. */
    @Test
    public void testRegisterTaskExecutorFromInvalidAddress() throws Exception {
        // test throw exception when receive a registration from taskExecutor which takes invalid
        // address
        String invalidAddress = "/taskExecutor2";

        CompletableFuture<RegistrationResponse> invalidAddressFuture =
                registerTaskExecutor(rmGateway, invalidAddress);
        assertTrue(
                invalidAddressFuture.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS)
                        instanceof RegistrationResponse.Failure);
    }

    private CompletableFuture<RegistrationResponse> registerTaskExecutor(
            ResourceManagerGateway resourceManagerGateway, String taskExecutorAddress) {
        return resourceManagerGateway.registerTaskExecutor(
                new TaskExecutorRegistration(
                        taskExecutorAddress,
                        taskExecutorResourceID,
                        dataPort,
                        jmxPort,
                        hardwareDescription,
                        new TaskExecutorMemoryConfiguration(
                                1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L),
                        ResourceProfile.ZERO,
                        ResourceProfile.ZERO),
                TIMEOUT);
    }
}
