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
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.utils.JobMasterBuilder;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.testtasks.BlockingNoOpInvokable;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewInputChannelStateHandle;
import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewResultSubpartitionStateHandle;
import static org.apache.flink.runtime.checkpoint.StateObjectCollection.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for the queryable-state logic of the {@link JobMaster}. */
public class JobMasterQueryableStateTest extends TestLogger {

    @ClassRule public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final Time testingTimeout = Time.seconds(10L);

    private static final long heartbeatInterval = 1000L;
    private static final long heartbeatTimeout = 5_000_000L;

    private static TestingRpcService rpcService;

    private static HeartbeatServices heartbeatServices;

    private Configuration configuration;

    private TestingHighAvailabilityServices haServices;

    private TestingFatalErrorHandler testingFatalErrorHandler;

    @BeforeClass
    public static void setupClass() {
        rpcService = new TestingRpcService();

        heartbeatServices = new HeartbeatServices(heartbeatInterval, heartbeatTimeout);
    }

    @Before
    public void setup() throws IOException {
        configuration = new Configuration();
        haServices = new TestingHighAvailabilityServices();

        testingFatalErrorHandler = new TestingFatalErrorHandler();

        haServices.setCheckpointRecoveryFactory(new StandaloneCheckpointRecoveryFactory());

        SettableLeaderRetrievalService rmLeaderRetrievalService =
                new SettableLeaderRetrievalService(null, null);
        haServices.setResourceManagerLeaderRetriever(rmLeaderRetrievalService);

        configuration.setString(
                BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());
    }

    @After
    public void teardown() throws Exception {
        if (testingFatalErrorHandler != null) {
            testingFatalErrorHandler.rethrowError();
        }

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
        final JobGraph graph = createKvJobGraph();

        final JobMaster jobMaster =
                new JobMasterBuilder(graph, rpcService)
                        .withConfiguration(configuration)
                        .withHighAvailabilityServices(haServices)
                        .withHeartbeatServices(heartbeatServices)
                        .createJobMaster();

        jobMaster.start();

        final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

        try {
            // lookup location
            try {
                jobMasterGateway.requestKvStateLocation(graph.getJobID(), "unknown").get();
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
        final JobGraph graph = createKvJobGraph();

        final JobMaster jobMaster =
                new JobMasterBuilder(graph, rpcService)
                        .withConfiguration(configuration)
                        .withHighAvailabilityServices(haServices)
                        .withHeartbeatServices(heartbeatServices)
                        .createJobMaster();

        jobMaster.start();

        final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

        try {
            // lookup location
            try {
                jobMasterGateway.requestKvStateLocation(new JobID(), "unknown").get();
                fail("Expected to fail with FlinkJobNotFoundException");
            } catch (Exception e) {
                assertTrue(
                        ExceptionUtils.findThrowable(e, FlinkJobNotFoundException.class)
                                .isPresent());
            }
        } finally {
            RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
        }
    }

    @Nonnull
    public JobGraph createKvJobGraph() {
        final JobVertex vertex1 = new JobVertex("v1");
        vertex1.setParallelism(4);
        vertex1.setMaxParallelism(16);
        vertex1.setInvokableClass(BlockingNoOpInvokable.class);

        final JobVertex vertex2 = new JobVertex("v2");
        vertex2.setParallelism(4);
        vertex2.setMaxParallelism(16);
        vertex2.setInvokableClass(BlockingNoOpInvokable.class);

        return new JobGraph(vertex1, vertex2);
    }

    @Test
    public void testRequestKvStateWithIrrelevantRegistration() throws Exception {
        final JobGraph graph = createKvJobGraph();

        final JobMaster jobMaster =
                new JobMasterBuilder(graph, rpcService)
                        .withConfiguration(configuration)
                        .withHighAvailabilityServices(haServices)
                        .withHeartbeatServices(heartbeatServices)
                        .createJobMaster();

        jobMaster.start();

        final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

        try {
            // register an irrelevant KvState
            try {
                jobMasterGateway
                        .notifyKvStateRegistered(
                                new JobID(),
                                new JobVertexID(),
                                new KeyGroupRange(0, 0),
                                "any-name",
                                new KvStateID(),
                                new InetSocketAddress(InetAddress.getLocalHost(), 1233))
                        .get();
                fail("Expected to fail with FlinkJobNotFoundException.");
            } catch (Exception e) {
                assertTrue(
                        ExceptionUtils.findThrowable(e, FlinkJobNotFoundException.class)
                                .isPresent());
            }
        } finally {
            RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
        }
    }

    @Test
    public void testRegisterAndUnregisterKvState() throws Exception {
        final JobGraph graph = createKvJobGraph();
        final List<JobVertex> jobVertices = graph.getVerticesSortedTopologicallyFromSources();
        final JobVertex vertex1 = jobVertices.get(0);

        final JobMaster jobMaster =
                new JobMasterBuilder(graph, rpcService)
                        .withConfiguration(configuration)
                        .withHighAvailabilityServices(haServices)
                        .withHeartbeatServices(heartbeatServices)
                        .createJobMaster();

        jobMaster.start();

        final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

        try {
            // register a KvState
            final String registrationName = "register-me";
            final KvStateID kvStateID = new KvStateID();
            final KeyGroupRange keyGroupRange = new KeyGroupRange(0, 0);
            final InetSocketAddress address =
                    new InetSocketAddress(InetAddress.getLocalHost(), 1029);

            jobMasterGateway
                    .notifyKvStateRegistered(
                            graph.getJobID(),
                            vertex1.getID(),
                            keyGroupRange,
                            registrationName,
                            kvStateID,
                            address)
                    .get();

            final KvStateLocation location =
                    jobMasterGateway
                            .requestKvStateLocation(graph.getJobID(), registrationName)
                            .get();

            assertEquals(graph.getJobID(), location.getJobId());
            assertEquals(vertex1.getID(), location.getJobVertexId());
            assertEquals(vertex1.getMaxParallelism(), location.getNumKeyGroups());
            assertEquals(1, location.getNumRegisteredKeyGroups());
            assertEquals(1, keyGroupRange.getNumberOfKeyGroups());
            assertEquals(kvStateID, location.getKvStateID(keyGroupRange.getStartKeyGroup()));
            assertEquals(
                    address, location.getKvStateServerAddress(keyGroupRange.getStartKeyGroup()));

            // unregister the KvState
            jobMasterGateway
                    .notifyKvStateUnregistered(
                            graph.getJobID(), vertex1.getID(), keyGroupRange, registrationName)
                    .get();

            try {
                jobMasterGateway.requestKvStateLocation(graph.getJobID(), registrationName).get();
                fail("Expected to fail with an UnknownKvStateLocation.");
            } catch (Exception e) {
                assertTrue(
                        ExceptionUtils.findThrowable(e, UnknownKvStateLocation.class).isPresent());
            }
        } finally {
            RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
        }
    }

    @Test
    public void testDuplicatedKvStateRegistrationsFailTask() throws Exception {
        final JobGraph graph = createKvJobGraph();
        final List<JobVertex> jobVertices = graph.getVerticesSortedTopologicallyFromSources();
        final JobVertex vertex1 = jobVertices.get(0);
        final JobVertex vertex2 = jobVertices.get(1);

        final JobMaster jobMaster =
                new JobMasterBuilder(graph, rpcService)
                        .withConfiguration(configuration)
                        .withHighAvailabilityServices(haServices)
                        .withHeartbeatServices(heartbeatServices)
                        .createJobMaster();

        jobMaster.start();

        final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

        try {
            // duplicate registration fails task

            // register a KvState
            final String registrationName = "duplicate-me";
            final KvStateID kvStateID = new KvStateID();
            final KeyGroupRange keyGroupRange = new KeyGroupRange(0, 0);
            final InetSocketAddress address =
                    new InetSocketAddress(InetAddress.getLocalHost(), 4396);

            jobMasterGateway
                    .notifyKvStateRegistered(
                            graph.getJobID(),
                            vertex1.getID(),
                            keyGroupRange,
                            registrationName,
                            kvStateID,
                            address)
                    .get();

            try {
                jobMasterGateway
                        .notifyKvStateRegistered(
                                graph.getJobID(),
                                vertex2.getID(), // <--- different operator, but...
                                keyGroupRange,
                                registrationName, // ...same name
                                kvStateID,
                                address)
                        .get();
                fail("Expected to fail because of clashing registration message.");
            } catch (Exception e) {
                assertTrue(
                        ExceptionUtils.findThrowableWithMessage(e, "Registration name clash")
                                .isPresent());
                assertEquals(
                        JobStatus.FAILED, jobMasterGateway.requestJobStatus(testingTimeout).get());
            }
        } finally {
            RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
        }
    }

    private Collection<OperatorState> createOperatorState(OperatorID... operatorIds) {
        Random random = new Random();
        Collection<OperatorState> operatorStates = new ArrayList<>(operatorIds.length);

        for (OperatorID operatorId : operatorIds) {
            final OperatorState operatorState = new OperatorState(operatorId, 1, 42);
            final OperatorSubtaskState subtaskState =
                    OperatorSubtaskState.builder()
                            .setManagedOperatorState(
                                    new OperatorStreamStateHandle(
                                            Collections.emptyMap(),
                                            new ByteStreamStateHandle("foobar", new byte[0])))
                            .setInputChannelState(
                                    singleton(createNewInputChannelStateHandle(10, random)))
                            .setResultSubpartitionState(
                                    singleton(createNewResultSubpartitionStateHandle(10, random)))
                            .build();
            operatorState.putState(0, subtaskState);
            operatorStates.add(operatorState);
        }

        return operatorStates;
    }
}
