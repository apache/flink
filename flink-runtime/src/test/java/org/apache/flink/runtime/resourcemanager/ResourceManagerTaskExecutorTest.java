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
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.FlinkException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link ResourceManager} and {@link TaskExecutor} interaction. */
class ResourceManagerTaskExecutorTest {

    private static final Time TIMEOUT = TestingUtils.infiniteTime();

    private static final ResourceProfile DEFAULT_SLOT_PROFILE =
            ResourceProfile.fromResources(1.0, 1234);

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_EXTENSION =
            TestingUtils.defaultExecutorExtension();

    private static TestingRpcService rpcService;

    private TestingTaskExecutorGateway taskExecutorGateway;

    private final int dataPort = 1234;

    private final int jmxPort = 23456;

    private final HardwareDescription hardwareDescription = new HardwareDescription(1, 2L, 3L, 4L);

    private ResourceID taskExecutorResourceID;

    private TestingResourceManagerService rmService;

    private ResourceManagerGateway rmGateway;

    private ResourceManagerGateway wronglyFencedGateway;

    @BeforeAll
    static void setupClass() {
        rpcService = new TestingRpcService();
    }

    @BeforeEach
    void setup() throws Exception {
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

    @AfterEach
    void teardown() throws Exception {
        if (rmService != null) {
            rmService.rethrowFatalErrorIfAny();
            rmService.cleanUp();
        }
    }

    @AfterAll
    static void teardownClass() throws Exception {
        if (rpcService != null) {
            RpcUtils.terminateRpcService(rpcService);
        }
    }

    /**
     * Test receive normal registration from task executor and receive duplicate registration from
     * task executor.
     */
    @Test
    void testRegisterTaskExecutor() throws Exception {
        // test response successful
        CompletableFuture<RegistrationResponse> successfulFuture =
                registerTaskExecutor(rmGateway, taskExecutorGateway.getAddress());

        RegistrationResponse response = successfulFuture.get();
        assertThat(response).isInstanceOf(TaskExecutorRegistrationSuccess.class);
        final TaskManagerInfoWithSlots taskManagerInfoWithSlots =
                rmGateway.requestTaskManagerDetailsInfo(taskExecutorResourceID, TIMEOUT).get();
        assertThat(taskManagerInfoWithSlots.getTaskManagerInfo().getResourceId())
                .isEqualTo(taskExecutorResourceID);

        // test response successful with instanceID not equal to previous when receive duplicate
        // registration from taskExecutor
        CompletableFuture<RegistrationResponse> duplicateFuture =
                registerTaskExecutor(rmGateway, taskExecutorGateway.getAddress());

        RegistrationResponse duplicateResponse = duplicateFuture.get();
        assertThat(duplicateResponse).isInstanceOf(TaskExecutorRegistrationSuccess.class);
        assertThat(((TaskExecutorRegistrationSuccess) response).getRegistrationId())
                .isNotEqualTo(
                        ((TaskExecutorRegistrationSuccess) duplicateResponse).getRegistrationId());

        assertThat(rmGateway.requestResourceOverview(TIMEOUT).get().getNumberTaskManagers())
                .isOne();
    }

    /**
     * Test delayed registration of task executor where the delay is introduced during connection
     * from resource manager to the registering task executor.
     */
    @Test
    void testDelayedRegisterTaskExecutor() throws Exception {
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
                                    EXECUTOR_EXTENSION.getExecutor()));

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
            assertThatFuture(firstFuture)
                    .as(
                            "Should have failed because connection to taskmanager is delayed beyond timeout")
                    .eventuallyFails()
                    .withThrowableOfType(Exception.class)
                    .withCauseInstanceOf(TimeoutException.class)
                    .withMessageContaining("ResourceManagerGateway.registerTaskExecutor");

            startConnection.await();

            // second registration after timeout is with no delay, expecting it to be succeeded
            rpcService.resetRpcGatewayFutureFunction();
            CompletableFuture<RegistrationResponse> secondFuture =
                    rmGateway.registerTaskExecutor(taskExecutorRegistration, TIMEOUT);
            RegistrationResponse response = secondFuture.get();
            assertThat(response).isInstanceOf(TaskExecutorRegistrationSuccess.class);

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
            assertThat(taskManagerInfoWithSlots.getTaskManagerInfo().getResourceId())
                    .isEqualTo(taskExecutorResourceID);
            assertThat(taskManagerInfoWithSlots.getTaskManagerInfo().getNumberSlots()).isOne();
        } finally {
            rpcService.resetRpcGatewayFutureFunction();
        }
    }

    /** Tests that a TaskExecutor can disconnect from the {@link ResourceManager}. */
    @Test
    void testDisconnectTaskExecutor() throws Exception {
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
        assertThat(registrationResponse).isInstanceOf(TaskExecutorRegistrationSuccess.class);

        final InstanceID registrationId =
                ((TaskExecutorRegistrationSuccess) registrationResponse).getRegistrationId();
        final Collection<SlotStatus> slots = createSlots(numberSlots);
        final SlotReport slotReport = new SlotReport(slots);
        rmGateway.sendSlotReport(taskExecutorResourceID, registrationId, slotReport, TIMEOUT).get();

        final ResourceOverview resourceOverview = rmGateway.requestResourceOverview(TIMEOUT).get();
        assertThat(resourceOverview.getNumberTaskManagers()).isOne();
        assertThat(resourceOverview.getNumberRegisteredSlots()).isEqualTo(numberSlots);

        rmGateway.disconnectTaskManager(
                taskExecutorResourceID, new FlinkException("testDisconnectTaskExecutor"));

        final ResourceOverview afterDisconnectResourceOverview =
                rmGateway.requestResourceOverview(TIMEOUT).get();
        assertThat(afterDisconnectResourceOverview.getNumberTaskManagers()).isZero();
        assertThat(afterDisconnectResourceOverview.getNumberRegisteredSlots()).isZero();
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
    void testRegisterTaskExecutorWithUnmatchedLeaderSessionId() {
        // test throw exception when receive a registration from taskExecutor which takes unmatched
        // leaderSessionId
        CompletableFuture<RegistrationResponse> unMatchedLeaderFuture =
                registerTaskExecutor(wronglyFencedGateway, taskExecutorGateway.getAddress());

        assertThatFuture(unMatchedLeaderFuture)
                .withFailMessage(
                        "Should have failed because we are using a wrongly fenced ResourceManagerGateway.")
                .eventuallyFails()
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(FencingTokenException.class);
    }

    /** Test receive registration with invalid address from task executor. */
    @Test
    void testRegisterTaskExecutorFromInvalidAddress() {
        // test throw exception when receive a registration from taskExecutor which takes invalid
        // address
        String invalidAddress = "/taskExecutor2";

        CompletableFuture<RegistrationResponse> invalidAddressFuture =
                registerTaskExecutor(rmGateway, invalidAddress);
        assertThatFuture(invalidAddressFuture)
                .eventuallySucceeds()
                .isInstanceOf(RegistrationResponse.Failure.class);
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
