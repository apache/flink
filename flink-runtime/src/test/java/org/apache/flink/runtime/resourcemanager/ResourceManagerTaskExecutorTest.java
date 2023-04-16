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
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.leaderelection.TestingLeaderElection;
import org.apache.flink.runtime.registration.RegistrationResponse;
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
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorResource;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
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

    private static final Time TIMEOUT = TestingUtils.infiniteTime();

    private static final ResourceProfile DEFAULT_SLOT_PROFILE =
            ResourceProfile.fromResources(1.0, 1234);

    @ClassRule
    public static final TestExecutorResource<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorResource();

    private static TestingRpcService rpcService;

    private TestingTaskExecutorGateway taskExecutorGateway;

    private int dataPort = 1234;

    private int jmxPort = 23456;

    private HardwareDescription hardwareDescription = new HardwareDescription(1, 2L, 3L, 4L);

    private ResourceID taskExecutorResourceID;

    private TestingResourceManagerService rmService;

    private ResourceManagerGateway rmGateway;

    private ResourceManagerGateway wronglyFencedGateway;

    @BeforeClass
    public static void setupClass() {
        rpcService = new TestingRpcService();
    }

    @Before
    public void setup() throws Exception {
        rpcService = new TestingRpcService();

        createAndRegisterTaskExecutorGateway();
        taskExecutorResourceID = ResourceID.generate();

        createAndStartResourceManager();

        wronglyFencedGateway =
                rpcService
                        .connect(
                                rmGateway.getAddress(),
                                ResourceManagerId.generate(),
                                ResourceManagerGateway.class)
                        .get();
    }

    private void createAndRegisterTaskExecutorGateway() {
        taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
        rpcService.registerGateway(taskExecutorGateway.getAddress(), taskExecutorGateway);
    }

    private void createAndStartResourceManager() throws Exception {
        final TestingLeaderElection leaderElection = new TestingLeaderElection();
        rmService =
                TestingResourceManagerService.newBuilder()
                        .setRpcService(rpcService)
                        .setRmLeaderElection(leaderElection)
                        .build();

        rmService.start();
        rmService.isLeader(UUID.randomUUID()).join();

        rmGateway =
                rmService
                        .getResourceManagerGateway()
                        .orElseThrow(
                                () ->
                                        new AssertionError(
                                                "RM not available after confirming leadership."));
    }

    @After
    public void teardown() throws Exception {
        if (rmService != null) {
            rmService.rethrowFatalErrorIfAny();
            rmService.cleanUp();
        }
    }

    @AfterClass
    public static void teardownClass() throws Exception {
        if (rpcService != null) {
            RpcUtils.terminateRpcService(rpcService);
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

        RegistrationResponse response = successfulFuture.get();
        assertTrue(response instanceof TaskExecutorRegistrationSuccess);
        final TaskManagerInfoWithSlots taskManagerInfoWithSlots =
                rmGateway.requestTaskManagerDetailsInfo(taskExecutorResourceID, TIMEOUT).get();
        assertThat(
                taskManagerInfoWithSlots.getTaskManagerInfo().getResourceId(),
                equalTo(taskExecutorResourceID));

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
                                    EXECUTOR_RESOURCE.getExecutor()));

            TaskExecutorRegistration taskExecutorRegistration =
                    new TaskExecutorRegistration(
                            taskExecutorGateway.getAddress(),
                            taskExecutorResourceID,
                            dataPort,
                            jmxPort,
                            hardwareDescription,
                            new TaskExecutorMemoryConfiguration(
                                    1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L),
                            DEFAULT_SLOT_PROFILE,
                            DEFAULT_SLOT_PROFILE,
                            taskExecutorGateway.getAddress());

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
            final TaskManagerInfoWithSlots taskManagerInfoWithSlots =
                    rmGateway.requestTaskManagerDetailsInfo(taskExecutorResourceID, TIMEOUT).get();
            assertThat(
                    taskManagerInfoWithSlots.getTaskManagerInfo().getResourceId(),
                    equalTo(taskExecutorResourceID));
            assertThat(taskManagerInfoWithSlots.getTaskManagerInfo().getNumberSlots(), equalTo(1));
        } finally {
            rpcService.resetRpcGatewayFutureFunction();
        }
    }

    /** Tests that a TaskExecutor can disconnect from the {@link ResourceManager}. */
    @Test
    public void testDisconnectTaskExecutor() throws Exception {
        final int numberSlots = 10;
        final TaskExecutorRegistration taskExecutorRegistration =
                new TaskExecutorRegistration(
                        taskExecutorGateway.getAddress(),
                        taskExecutorResourceID,
                        dataPort,
                        jmxPort,
                        hardwareDescription,
                        new TaskExecutorMemoryConfiguration(
                                1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L),
                        DEFAULT_SLOT_PROFILE,
                        DEFAULT_SLOT_PROFILE.multiply(numberSlots),
                        taskExecutorGateway.getAddress());
        final RegistrationResponse registrationResponse =
                rmGateway.registerTaskExecutor(taskExecutorRegistration, TIMEOUT).get();
        assertThat(registrationResponse, instanceOf(TaskExecutorRegistrationSuccess.class));

        final InstanceID registrationId =
                ((TaskExecutorRegistrationSuccess) registrationResponse).getRegistrationId();
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
            unMatchedLeaderFuture.get();
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
        assertTrue(invalidAddressFuture.get() instanceof RegistrationResponse.Failure);
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
                        DEFAULT_SLOT_PROFILE,
                        DEFAULT_SLOT_PROFILE,
                        taskExecutorAddress),
                TIMEOUT);
    }
}
