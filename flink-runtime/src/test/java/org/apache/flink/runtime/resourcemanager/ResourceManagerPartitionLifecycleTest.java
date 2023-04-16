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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.leaderelection.TestingLeaderElection;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorHeartbeatPayload;
import org.apache.flink.runtime.taskexecutor.TaskExecutorMemoryConfiguration;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.taskexecutor.partition.ClusterPartitionReport;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

/** Tests for the partition-lifecycle logic in the {@link ResourceManager}. */
public class ResourceManagerPartitionLifecycleTest extends TestLogger {

    private static TestingRpcService rpcService;

    private TestingResourceManagerService resourceManagerService;

    @BeforeClass
    public static void setupClass() {
        rpcService = new TestingRpcService();
    }

    @Before
    public void setup() throws Exception {}

    @After
    public void after() throws Exception {
        if (resourceManagerService != null) {
            resourceManagerService.rethrowFatalErrorIfAny();
            resourceManagerService.cleanUp();
        }
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        if (rpcService != null) {
            RpcUtils.terminateRpcService(rpcService);
        }
    }

    @Test
    public void testClusterPartitionReportHandling() throws Exception {
        final CompletableFuture<Collection<IntermediateDataSetID>> clusterPartitionReleaseFuture =
                new CompletableFuture<>();
        runTest(
                builder ->
                        builder.setReleaseClusterPartitionsConsumer(
                                clusterPartitionReleaseFuture::complete),
                (resourceManagerGateway, taskManagerId1, ignored) -> {
                    IntermediateDataSetID dataSetID = new IntermediateDataSetID();
                    ResultPartitionID resultPartitionID = new ResultPartitionID();

                    resourceManagerGateway.heartbeatFromTaskManager(
                            taskManagerId1,
                            createTaskExecutorHeartbeatPayload(
                                    dataSetID, 2, resultPartitionID, new ResultPartitionID()));

                    // send a heartbeat containing 1 partition less -> partition loss -> should
                    // result in partition release
                    resourceManagerGateway.heartbeatFromTaskManager(
                            taskManagerId1,
                            createTaskExecutorHeartbeatPayload(dataSetID, 2, resultPartitionID));

                    Collection<IntermediateDataSetID> intermediateDataSetIDS =
                            clusterPartitionReleaseFuture.get();
                    assertThat(intermediateDataSetIDS, contains(dataSetID));
                });
    }

    @Test
    public void testTaskExecutorShutdownHandling() throws Exception {
        final CompletableFuture<Collection<IntermediateDataSetID>> clusterPartitionReleaseFuture =
                new CompletableFuture<>();
        runTest(
                builder ->
                        builder.setReleaseClusterPartitionsConsumer(
                                clusterPartitionReleaseFuture::complete),
                (resourceManagerGateway, taskManagerId1, taskManagerId2) -> {
                    IntermediateDataSetID dataSetID = new IntermediateDataSetID();

                    resourceManagerGateway.heartbeatFromTaskManager(
                            taskManagerId1,
                            createTaskExecutorHeartbeatPayload(
                                    dataSetID, 2, new ResultPartitionID()));

                    // we need a partition on another task executor so that there's something to
                    // release when one task executor goes down
                    resourceManagerGateway.heartbeatFromTaskManager(
                            taskManagerId2,
                            createTaskExecutorHeartbeatPayload(
                                    dataSetID, 2, new ResultPartitionID()));

                    resourceManagerGateway.disconnectTaskManager(
                            taskManagerId2, new RuntimeException("test exception"));
                    Collection<IntermediateDataSetID> intermediateDataSetIDS =
                            clusterPartitionReleaseFuture.get();
                    assertThat(intermediateDataSetIDS, contains(dataSetID));
                });
    }

    private void runTest(TaskExecutorSetup taskExecutorBuilderSetup, TestAction testAction)
            throws Exception {
        final ResourceManagerGateway resourceManagerGateway = createAndStartResourceManager();

        TestingTaskExecutorGatewayBuilder testingTaskExecutorGateway1Builder =
                new TestingTaskExecutorGatewayBuilder();
        taskExecutorBuilderSetup.accept(testingTaskExecutorGateway1Builder);
        final TaskExecutorGateway taskExecutorGateway1 =
                testingTaskExecutorGateway1Builder
                        .setAddress(UUID.randomUUID().toString())
                        .createTestingTaskExecutorGateway();
        rpcService.registerGateway(taskExecutorGateway1.getAddress(), taskExecutorGateway1);

        final TaskExecutorGateway taskExecutorGateway2 =
                new TestingTaskExecutorGatewayBuilder()
                        .setAddress(UUID.randomUUID().toString())
                        .createTestingTaskExecutorGateway();
        rpcService.registerGateway(taskExecutorGateway2.getAddress(), taskExecutorGateway2);

        final ResourceID taskManagerId1 = ResourceID.generate();
        final ResourceID taskManagerId2 = ResourceID.generate();
        registerTaskExecutor(
                resourceManagerGateway, taskManagerId1, taskExecutorGateway1.getAddress());
        registerTaskExecutor(
                resourceManagerGateway, taskManagerId2, taskExecutorGateway2.getAddress());

        testAction.accept(resourceManagerGateway, taskManagerId1, taskManagerId2);
    }

    public static void registerTaskExecutor(
            ResourceManagerGateway resourceManagerGateway,
            ResourceID taskExecutorId,
            String taskExecutorAddress)
            throws Exception {
        final TaskExecutorRegistration taskExecutorRegistration =
                new TaskExecutorRegistration(
                        taskExecutorAddress,
                        taskExecutorId,
                        1234,
                        23456,
                        new HardwareDescription(42, 1337L, 1337L, 0L),
                        new TaskExecutorMemoryConfiguration(
                                1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L),
                        ResourceProfile.ZERO,
                        ResourceProfile.ZERO,
                        taskExecutorAddress);
        final CompletableFuture<RegistrationResponse> registrationFuture =
                resourceManagerGateway.registerTaskExecutor(
                        taskExecutorRegistration, TestingUtils.TIMEOUT);

        assertThat(registrationFuture.get(), instanceOf(RegistrationResponse.Success.class));
    }

    private ResourceManagerGateway createAndStartResourceManager() throws Exception {
        final TestingLeaderElection leaderElection = new TestingLeaderElection();

        resourceManagerService =
                TestingResourceManagerService.newBuilder()
                        .setRpcService(rpcService)
                        .setRmLeaderElection(leaderElection)
                        .build();
        resourceManagerService.start();

        // first make the ResourceManager the leader
        resourceManagerService.isLeader(UUID.randomUUID()).join();

        return resourceManagerService
                .getResourceManagerGateway()
                .orElseThrow(
                        () -> new AssertionError("RM not available after confirming leadership."));
    }

    private static TaskExecutorHeartbeatPayload createTaskExecutorHeartbeatPayload(
            IntermediateDataSetID dataSetId,
            int numTotalPartitions,
            ResultPartitionID... partitionIds) {

        final Map<ResultPartitionID, ShuffleDescriptor> shuffleDescriptors =
                Arrays.stream(partitionIds)
                        .map(TestingShuffleDescriptor::new)
                        .collect(
                                Collectors.toMap(
                                        TestingShuffleDescriptor::getResultPartitionID, d -> d));

        return new TaskExecutorHeartbeatPayload(
                new SlotReport(),
                new ClusterPartitionReport(
                        Collections.singletonList(
                                new ClusterPartitionReport.ClusterPartitionReportEntry(
                                        dataSetId, numTotalPartitions, shuffleDescriptors))));
    }

    @FunctionalInterface
    private interface TaskExecutorSetup {
        void accept(TestingTaskExecutorGatewayBuilder taskExecutorGatewayBuilder) throws Exception;
    }

    @FunctionalInterface
    private interface TestAction {
        void accept(
                ResourceManagerGateway resourceManagerGateway,
                ResourceID taskExecutorId1,
                ResourceID taskExecutorId2)
                throws Exception;
    }

    private static class TestingShuffleDescriptor implements ShuffleDescriptor {

        private final ResultPartitionID resultPartitionID;

        private TestingShuffleDescriptor(ResultPartitionID resultPartitionID) {
            this.resultPartitionID = resultPartitionID;
        }

        @Override
        public ResultPartitionID getResultPartitionID() {
            return resultPartitionID;
        }

        @Override
        public Optional<ResourceID> storesLocalResourcesOn() {
            return Optional.empty();
        }
    }
}
