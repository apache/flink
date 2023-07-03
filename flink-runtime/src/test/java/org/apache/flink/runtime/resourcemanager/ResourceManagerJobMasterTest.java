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
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.rpc.exceptions.FencingTokenException;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for the interaction between the {@link ResourceManager} and the {@link JobMaster}. */
public class ResourceManagerJobMasterTest extends TestLogger {

    private static final Time TIMEOUT = Time.seconds(10L);

    private TestingRpcService rpcService;

    private JobID jobId;

    private ResourceID jobMasterResourceId;

    private TestingJobMasterGateway jobMasterGateway;

    private SettableLeaderRetrievalService jobMasterLeaderRetrievalService;

    private TestingResourceManagerService resourceManagerService;

    private ResourceManagerGateway resourceManagerGateway;

    @Before
    public void setup() throws Exception {
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

    @After
    public void teardown() throws Exception {
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
    public void testRegisterJobMaster() throws Exception {
        // test response successful
        CompletableFuture<RegistrationResponse> successfulFuture =
                resourceManagerGateway.registerJobMaster(
                        jobMasterGateway.getFencingToken(),
                        jobMasterResourceId,
                        jobMasterGateway.getAddress(),
                        jobId,
                        TIMEOUT);
        RegistrationResponse response =
                successfulFuture.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
        assertTrue(response instanceof JobMasterRegistrationSuccess);
    }

    /** Test receive registration with unmatched leadershipId from job master. */
    @Test
    public void testRegisterJobMasterWithUnmatchedLeaderSessionId1() throws Exception {
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

        try {
            unMatchedLeaderFuture.get(5L, TimeUnit.SECONDS);
            fail("Should fail because we are using the wrong fencing token.");
        } catch (ExecutionException e) {
            assertTrue(ExceptionUtils.stripExecutionException(e) instanceof FencingTokenException);
        }
    }

    /** Test receive registration with unmatched leadershipId from job master. */
    @Test
    public void testRegisterJobMasterWithUnmatchedLeaderSessionId2() throws Exception {
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
        assertTrue(unMatchedLeaderFuture.get() instanceof RegistrationResponse.Failure);
    }

    /** Test receive registration with invalid address from job master. */
    @Test
    public void testRegisterJobMasterFromInvalidAddress() throws Exception {
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
        assertTrue(
                invalidAddressFuture.get(5, TimeUnit.SECONDS)
                        instanceof RegistrationResponse.Failure);
    }

    /**
     * Check and verify return RegistrationResponse. Decline when failed to start a job master
     * Leader retrieval listener.
     */
    @Test
    public void testRegisterJobMasterWithFailureLeaderListener() throws Exception {
        JobID unknownJobIDToHAServices = new JobID();

        // this should fail because we try to register a job leader listener for an unknown job id
        CompletableFuture<RegistrationResponse> registrationFuture =
                resourceManagerGateway.registerJobMaster(
                        jobMasterGateway.getFencingToken(),
                        jobMasterResourceId,
                        jobMasterGateway.getAddress(),
                        unknownJobIDToHAServices,
                        TIMEOUT);

        try {
            registrationFuture.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
            fail("Expected to fail with a ResourceManagerException.");
        } catch (ExecutionException e) {
            assertTrue(
                    ExceptionUtils.stripExecutionException(e) instanceof ResourceManagerException);
        }

        // ignore the reported error
        resourceManagerService.ignoreFatalErrors();
    }
}
