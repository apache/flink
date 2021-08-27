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
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.utils.JobMasterBuilder;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;
import static org.hamcrest.CoreMatchers.either;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for the queryable-state logic of the {@link JobMaster}. */
public class JobMasterQueryableStateTest extends TestLogger {

    private static final Time testingTimeout = Time.seconds(10L);

    private static TestingRpcService rpcService;

    private static final int PARALLELISM = 4;
    private static final JobVertex JOB_VERTEX_1;
    private static final JobVertex JOB_VERTEX_2;
    private static final JobGraph JOB_GRAPH;

    static {
        JOB_VERTEX_1 = new JobVertex("v1");
        JOB_VERTEX_1.setParallelism(PARALLELISM);
        JOB_VERTEX_1.setInvokableClass(AbstractInvokable.class);

        JOB_VERTEX_2 = new JobVertex("v2");
        JOB_VERTEX_2.setParallelism(PARALLELISM);
        JOB_VERTEX_2.setInvokableClass(AbstractInvokable.class);

        // explicit configuration required for #testRegisterAndUnregisterKvState
        JOB_VERTEX_1.setMaxParallelism(16);
        JOB_VERTEX_2.setMaxParallelism(16);

        final SlotSharingGroup slotSharingGroup = new SlotSharingGroup();
        JOB_VERTEX_1.setSlotSharingGroup(slotSharingGroup);
        JOB_VERTEX_2.setSlotSharingGroup(slotSharingGroup);

        JOB_GRAPH = JobGraphTestUtils.streamingJobGraph(JOB_VERTEX_1, JOB_VERTEX_2);
        JOB_GRAPH.setJobType(JobType.STREAMING);
    }

    @BeforeClass
    public static void setupClass() {
        rpcService = new TestingRpcService();
    }

    @After
    public void teardown() throws Exception {
        rpcService.clearGateways();
    }

    @AfterClass
    public static void teardownClass() {
        if (rpcService != null) {
            rpcService.stopService();
            rpcService = null;
        }
    }

    @Test
    public void testRequestKvStateWithoutRegistration() throws Exception {
        final JobMaster jobMaster = new JobMasterBuilder(JOB_GRAPH, rpcService).createJobMaster();

        jobMaster.start();

        final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

        registerSlotsRequiredForJobExecution(jobMasterGateway, JOB_GRAPH.getJobID());

        try {
            // lookup location
            try {
                jobMasterGateway.requestKvStateLocation(JOB_GRAPH.getJobID(), "unknown").get();
                fail("Expected to fail with UnknownKvStateLocation");
            } catch (Exception e) {
                assertTrue(
                        ExceptionUtils.findThrowable(e, UnknownKvStateLocation.class).isPresent());
            }
        } finally {
            RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
        }
    }

    @Test
    public void testRequestKvStateOfWrongJob() throws Exception {
        final JobMaster jobMaster = new JobMasterBuilder(JOB_GRAPH, rpcService).createJobMaster();

        jobMaster.start();

        final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

        registerSlotsRequiredForJobExecution(jobMasterGateway, JOB_GRAPH.getJobID());

        try {
            // lookup location
            try {
                jobMasterGateway.requestKvStateLocation(new JobID(), "unknown").get();
                fail("Expected to fail with FlinkJobNotFoundException");
            } catch (Exception e) {
                assertThat(e, containsCause(FlinkJobNotFoundException.class));
            }
        } finally {
            RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
        }
    }

    @Test
    public void testRequestKvStateWithIrrelevantRegistration() throws Exception {
        final JobMaster jobMaster = new JobMasterBuilder(JOB_GRAPH, rpcService).createJobMaster();

        jobMaster.start();

        final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

        registerSlotsRequiredForJobExecution(jobMasterGateway, JOB_GRAPH.getJobID());

        try {
            // register an irrelevant KvState
            try {
                registerKvState(jobMasterGateway, new JobID(), new JobVertexID(), "any-name");
                fail("Expected to fail with FlinkJobNotFoundException.");
            } catch (Exception e) {
                assertThat(e, containsCause(FlinkJobNotFoundException.class));
            }
        } finally {
            RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
        }
    }

    @Test
    public void testRegisterKvState() throws Exception {
        final JobMaster jobMaster = new JobMasterBuilder(JOB_GRAPH, rpcService).createJobMaster();

        jobMaster.start();

        final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

        registerSlotsRequiredForJobExecution(jobMasterGateway, JOB_GRAPH.getJobID());

        try {
            final String registrationName = "register-me";
            final KvStateID kvStateID = new KvStateID();
            final KeyGroupRange keyGroupRange = new KeyGroupRange(0, 0);
            final InetSocketAddress address =
                    new InetSocketAddress(InetAddress.getLocalHost(), 1029);

            jobMasterGateway
                    .notifyKvStateRegistered(
                            JOB_GRAPH.getJobID(),
                            JOB_VERTEX_1.getID(),
                            keyGroupRange,
                            registrationName,
                            kvStateID,
                            address)
                    .get();

            final KvStateLocation location =
                    jobMasterGateway
                            .requestKvStateLocation(JOB_GRAPH.getJobID(), registrationName)
                            .get();

            assertEquals(JOB_GRAPH.getJobID(), location.getJobId());
            assertEquals(JOB_VERTEX_1.getID(), location.getJobVertexId());
            assertEquals(JOB_VERTEX_1.getMaxParallelism(), location.getNumKeyGroups());
            assertEquals(1, location.getNumRegisteredKeyGroups());
            assertEquals(1, keyGroupRange.getNumberOfKeyGroups());
            assertEquals(kvStateID, location.getKvStateID(keyGroupRange.getStartKeyGroup()));
            assertEquals(
                    address, location.getKvStateServerAddress(keyGroupRange.getStartKeyGroup()));
        } finally {
            RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
        }
    }

    @Test
    public void testUnregisterKvState() throws Exception {
        final JobMaster jobMaster = new JobMasterBuilder(JOB_GRAPH, rpcService).createJobMaster();

        jobMaster.start();

        final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

        registerSlotsRequiredForJobExecution(jobMasterGateway, JOB_GRAPH.getJobID());

        try {
            final String registrationName = "register-me";
            final KvStateID kvStateID = new KvStateID();
            final KeyGroupRange keyGroupRange = new KeyGroupRange(0, 0);
            final InetSocketAddress address =
                    new InetSocketAddress(InetAddress.getLocalHost(), 1029);

            jobMasterGateway
                    .notifyKvStateRegistered(
                            JOB_GRAPH.getJobID(),
                            JOB_VERTEX_1.getID(),
                            keyGroupRange,
                            registrationName,
                            kvStateID,
                            address)
                    .get();

            jobMasterGateway
                    .notifyKvStateUnregistered(
                            JOB_GRAPH.getJobID(),
                            JOB_VERTEX_1.getID(),
                            keyGroupRange,
                            registrationName)
                    .get();

            try {
                jobMasterGateway
                        .requestKvStateLocation(JOB_GRAPH.getJobID(), registrationName)
                        .get();
                fail("Expected to fail with an UnknownKvStateLocation.");
            } catch (Exception e) {
                assertThat(e, containsCause(UnknownKvStateLocation.class));
            }
        } finally {
            RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
        }
    }

    @Test
    public void testDuplicatedKvStateRegistrationsFailTask() throws Exception {
        final JobMaster jobMaster = new JobMasterBuilder(JOB_GRAPH, rpcService).createJobMaster();

        jobMaster.start();

        final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

        registerSlotsRequiredForJobExecution(jobMasterGateway, JOB_GRAPH.getJobID());

        try {
            // duplicate registration fails task

            final String registrationName = "duplicate-me";

            registerKvState(
                    jobMasterGateway, JOB_GRAPH.getJobID(), JOB_VERTEX_1.getID(), registrationName);
            try {
                registerKvState(
                        jobMasterGateway,
                        JOB_GRAPH.getJobID(),
                        JOB_VERTEX_2.getID(),
                        registrationName);
                fail("Expected to fail because of clashing registration message.");
            } catch (Exception e) {
                assertTrue(
                        ExceptionUtils.findThrowableWithMessage(e, "Registration name clash")
                                .isPresent());

                assertThat(
                        jobMasterGateway.requestJobStatus(testingTimeout).get(),
                        either(is(JobStatus.FAILED)).or(is(JobStatus.FAILING)));
            }
        } finally {
            RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
        }
    }

    private static void registerSlotsRequiredForJobExecution(
            JobMasterGateway jobMasterGateway, JobID jobId)
            throws ExecutionException, InterruptedException {
        final OneShotLatch oneTaskSubmittedLatch = new OneShotLatch();
        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setSubmitTaskConsumer(
                                (taskDeploymentDescriptor, jobMasterId) -> {
                                    oneTaskSubmittedLatch.trigger();
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .createTestingTaskExecutorGateway();
        JobMasterTestUtils.registerTaskExecutorAndOfferSlots(
                rpcService,
                jobMasterGateway,
                jobId,
                PARALLELISM,
                taskExecutorGateway,
                testingTimeout);

        oneTaskSubmittedLatch.await();
    }

    private static void registerKvState(
            KvStateRegistryGateway stateRegistryGateway,
            JobID jobId,
            JobVertexID jobVertexId,
            String registrationName)
            throws UnknownHostException, ExecutionException, InterruptedException {
        stateRegistryGateway
                .notifyKvStateRegistered(
                        jobId,
                        jobVertexId,
                        new KeyGroupRange(0, 0),
                        registrationName,
                        new KvStateID(),
                        new InetSocketAddress(InetAddress.getLocalHost(), 1233))
                .get();
    }
}
