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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.JobMasterRegistrationSuccess;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.leaderelection.TestingLeaderElection;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.rpc.exceptions.FencingTokenException;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.apache.flink.runtime.resourcemanager.ResourceManagerPartitionLifecycleTest.registerTaskExecutor;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the interaction between the {@link ResourceManager} and the {@link JobMaster}. */
class ResourceManagerJobMasterTest {

    private static final Time TIMEOUT = Time.seconds(10L);

    private TestingRpcService rpcService;

    private JobID jobId;

    private ResourceID jobMasterResourceId;

    private TestingJobMasterGateway jobMasterGateway;

    private SettableLeaderRetrievalService jobMasterLeaderRetrievalService;

    private TestingResourceManagerService resourceManagerService;

    private ResourceManagerGateway resourceManagerGateway;

    @BeforeEach
    void setup() throws Exception {
        rpcService = new TestingRpcService();

        jobId = new JobID();
        jobMasterResourceId = ResourceID.generate();

        createAndRegisterJobMasterGateway();

        createAndStartResourceManagerService();
    }

    private void createAndRegisterJobMasterGateway() {
        jobMasterGateway = new TestingJobMasterGatewayBuilder().build();
        rpcService.registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);
        jobMasterLeaderRetrievalService =
                new SettableLeaderRetrievalService(
                        jobMasterGateway.getAddress(), jobMasterGateway.getFencingToken().toUUID());
    }

    private void createAndStartResourceManagerService() throws Exception {
        final TestingLeaderElection leaderElection = new TestingLeaderElection();
        resourceManagerService =
                TestingResourceManagerService.newBuilder()
                        .setRpcService(rpcService)
                        .setJmLeaderRetrieverFunction(
                                requestedJobId -> {
                                    if (requestedJobId.equals(jobId)) {
                                        return jobMasterLeaderRetrievalService;
                                    } else {
                                        throw new FlinkRuntimeException(
                                                String.format("Unknown job id %s", jobId));
                                    }
                                })
                        .setRmLeaderElection(leaderElection)
                        .build();

        resourceManagerService.start();
        resourceManagerService.isLeader(UUID.randomUUID()).join();

        resourceManagerGateway =
                resourceManagerService
                        .getResourceManagerGateway()
                        .orElseThrow(
                                () ->
                                        new AssertionError(
                                                "RM not available after confirming leadership."));
    }

    @AfterEach
    void teardown() throws Exception {
        if (resourceManagerService != null) {
            resourceManagerService.rethrowFatalErrorIfAny();
            resourceManagerService.cleanUp();
        }

        if (rpcService != null) {
            RpcUtils.terminateRpcService(rpcService);
        }
    }

    /**
     * Test receive normal registration from job master and receive duplicate registration from job
     * master.
     */
    @Test
    void testRegisterJobMaster() {
        // test response successful
        CompletableFuture<RegistrationResponse> successfulFuture =
                resourceManagerGateway.registerJobMaster(
                        jobMasterGateway.getFencingToken(),
                        jobMasterResourceId,
                        jobMasterGateway.getAddress(),
                        jobId,
                        TIMEOUT);
        assertThatFuture(successfulFuture)
                .succeedsWithin(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS)
                .isInstanceOf(JobMasterRegistrationSuccess.class);
    }

    @Test
    void testDisconnectTaskManagerInResourceManager()
            throws ExecutionException, InterruptedException, TimeoutException {
        final ResourceID taskExecutorId = ResourceID.generate();
        final CompletableFuture<Exception> disconnectRMFuture = new CompletableFuture<>();
        final CompletableFuture<ResourceID> disconnectTMFuture = new CompletableFuture<>();

        TestingJobMasterGateway jobMasterGateway =
                new TestingJobMasterGatewayBuilder()
                        .setDisconnectTaskManagerFunction(
                                resourceID -> {
                                    disconnectTMFuture.complete(resourceID);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .setAddress("pekko.tcp://flink@localhost:6130/user/jobmanager2")
                        .build();
        rpcService.registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);
        ResourceManagerGateway resourceManagerGateway =
                resourceManagerService
                        .getResourceManagerGateway()
                        .orElseThrow(
                                () ->
                                        new AssertionError(
                                                "RM not available after confirming leadership."));

        // test response successful
        CompletableFuture<RegistrationResponse> successfulFuture =
                resourceManagerGateway.registerJobMaster(
                        jobMasterGateway.getFencingToken(),
                        jobMasterResourceId,
                        jobMasterGateway.getAddress(),
                        jobId,
                        TIMEOUT);
        assertThatFuture(successfulFuture)
                .succeedsWithin(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS)
                .isInstanceOf(JobMasterRegistrationSuccess.class);

        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setAddress(UUID.randomUUID().toString())
                        .setDisconnectResourceManagerConsumer(disconnectRMFuture::complete)
                        .createTestingTaskExecutorGateway();
        rpcService.registerGateway(taskExecutorGateway.getAddress(), taskExecutorGateway);
        registerTaskExecutor(
                resourceManagerGateway, taskExecutorId, taskExecutorGateway.getAddress());

        resourceManagerGateway.disconnectTaskManager(taskExecutorId, new Exception("for test"));
        assertThatFuture(disconnectRMFuture)
                .succeedsWithin(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
        final ResourceID resourceId =
                disconnectTMFuture.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
        assertThat(resourceId).isEqualTo(taskExecutorId);
    }

    /** Test receive registration with unmatched leadershipId from job master. */
    @Test
    void testRegisterJobMasterWithUnmatchedLeaderSessionId1() throws Exception {
        final ResourceManagerGateway wronglyFencedGateway =
                rpcService
                        .connect(
                                resourceManagerGateway.getAddress(),
                                ResourceManagerId.generate(),
                                ResourceManagerGateway.class)
                        .get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);

        // test throw exception when receive a registration from job master which takes unmatched
        // leaderSessionId
        CompletableFuture<RegistrationResponse> unMatchedLeaderFuture =
                wronglyFencedGateway.registerJobMaster(
                        jobMasterGateway.getFencingToken(),
                        jobMasterResourceId,
                        jobMasterGateway.getAddress(),
                        jobId,
                        TIMEOUT);
        assertThatFuture(unMatchedLeaderFuture)
                .withFailMessage("Should fail because we are using the wrong fencing token.")
                .failsWithin(5L, TimeUnit.SECONDS)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(FencingTokenException.class);
    }

    /** Test receive registration with unmatched leadershipId from job master. */
    @Test
    void testRegisterJobMasterWithUnmatchedLeaderSessionId2() {
        // test throw exception when receive a registration from job master which takes unmatched
        // leaderSessionId
        JobMasterId differentJobMasterId = JobMasterId.generate();
        CompletableFuture<RegistrationResponse> unMatchedLeaderFuture =
                resourceManagerGateway.registerJobMaster(
                        differentJobMasterId,
                        jobMasterResourceId,
                        jobMasterGateway.getAddress(),
                        jobId,
                        TIMEOUT);
        assertThatFuture(unMatchedLeaderFuture)
                .eventuallySucceeds()
                .isInstanceOf(RegistrationResponse.Failure.class);
    }

    /** Test receive registration with invalid address from job master. */
    @Test
    void testRegisterJobMasterFromInvalidAddress() {
        // test throw exception when receive a registration from job master which takes invalid
        // address
        String invalidAddress = "/jobMasterAddress2";
        CompletableFuture<RegistrationResponse> invalidAddressFuture =
                resourceManagerGateway.registerJobMaster(
                        new JobMasterId(HighAvailabilityServices.DEFAULT_LEADER_ID),
                        jobMasterResourceId,
                        invalidAddress,
                        jobId,
                        TIMEOUT);
        assertThatFuture(invalidAddressFuture)
                .succeedsWithin(5, TimeUnit.SECONDS)
                .isOfAnyClassIn(RegistrationResponse.Failure.class);
    }

    /**
     * Check and verify return RegistrationResponse. Decline when failed to start a job master
     * Leader retrieval listener.
     */
    @Test
    void testRegisterJobMasterWithFailureLeaderListener() {
        JobID unknownJobIDToHAServices = new JobID();

        // this should fail because we try to register a job leader listener for an unknown job id
        CompletableFuture<RegistrationResponse> registrationFuture =
                resourceManagerGateway.registerJobMaster(
                        jobMasterGateway.getFencingToken(),
                        jobMasterResourceId,
                        jobMasterGateway.getAddress(),
                        unknownJobIDToHAServices,
                        TIMEOUT);

        assertThatFuture(registrationFuture)
                .as("Expected to fail with a ResourceManagerException.")
                .failsWithin(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(ResourceManagerException.class);

        // ignore the reported error
        resourceManagerService.ignoreFatalErrors();
    }
}
