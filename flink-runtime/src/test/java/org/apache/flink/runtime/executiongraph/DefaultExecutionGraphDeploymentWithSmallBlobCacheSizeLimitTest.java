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
 * limitations under the License
 */

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.blob.BlobCacheSizeTracker;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.PermanentBlobCache;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.operators.BatchTask;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.taskexecutor.NoOpShuffleDescriptorsCache;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.function.FunctionUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link ExecutionGraph} deployment when job and task information are offloaded into the BLOB
 * server and accessed from the {@link PermanentBlobCache}. {@link PermanentBlobCache} uses {@link
 * BlobCacheSizeTracker} to track the size of ShuffleDescriptor BLOBs. The deployment works well
 * even the size limit of {@link BlobCacheSizeTracker} in {@link PermanentBlobCache} is set to the
 * minimum value.
 */
class DefaultExecutionGraphDeploymentWithSmallBlobCacheSizeLimitTest
        extends DefaultExecutionGraphDeploymentWithBlobCacheTest {

    @BeforeEach
    @Override
    public void setupBlobServer() throws IOException {
        Configuration config = new Configuration();
        // Always offload the serialized JobInformation, TaskInformation and cached
        // ShuffleDescriptors
        config.setInteger(BlobServerOptions.OFFLOAD_MINSIZE, 0);
        blobServer =
                new BlobServer(
                        config, TempDirUtils.newFolder(temporaryFolder), new VoidBlobStore());
        blobServer.start();
        blobWriter = blobServer;

        InetSocketAddress serverAddress = new InetSocketAddress("localhost", blobServer.getPort());
        // Set the size limit of the blob cache to 1
        BlobCacheSizeTracker blobCacheSizeTracker = new BlobCacheSizeTracker(1L);
        blobCache =
                new PermanentBlobCache(
                        config,
                        TempDirUtils.newFolder(temporaryFolder),
                        new VoidBlobStore(),
                        serverAddress,
                        blobCacheSizeTracker);
    }

    /**
     * Test the deployment works well even the size limit of {@link BlobCacheSizeTracker} in {@link
     * PermanentBlobCache} is set to the minimum value.
     *
     * <p>In this extreme case, since the size limit is 1, every time a task is deployed, all the
     * existing **tracked** BLOBs on the cache must be untracked and deleted before the new BLOB is
     * stored onto the cache.
     *
     * <p>This extreme case covers the situation of the normal case, where the size limit is much
     * larger than 1 and the deletion won't happen so frequently.
     */
    @Test
    void testDeployMultipleTasksWithSmallBlobCacheSizeLimit() throws Exception {

        final int numberOfVertices = 4;
        final int parallelism = 10;

        final ExecutionGraph eg = createAndSetupExecutionGraph(numberOfVertices, parallelism);

        final SimpleAckingTaskManagerGateway taskManagerGateway =
                new SimpleAckingTaskManagerGateway();
        final BlockingQueue<TaskDeploymentDescriptor> tdds =
                new ArrayBlockingQueue<>(numberOfVertices * parallelism);
        taskManagerGateway.setSubmitConsumer(
                FunctionUtils.uncheckedConsumer(
                        taskDeploymentDescriptor -> {
                            taskDeploymentDescriptor.loadBigData(
                                    blobCache, NoOpShuffleDescriptorsCache.INSTANCE);
                            tdds.offer(taskDeploymentDescriptor);
                        }));

        for (ExecutionJobVertex ejv : eg.getVerticesTopologically()) {
            for (ExecutionVertex ev : ejv.getTaskVertices()) {

                assertThat(ev.getExecutionState()).isEqualTo(ExecutionState.CREATED);

                LogicalSlot slot =
                        new TestingLogicalSlotBuilder()
                                .setTaskManagerGateway(taskManagerGateway)
                                .createTestingLogicalSlot();
                final Execution execution = ev.getCurrentExecutionAttempt();
                execution.transitionState(ExecutionState.SCHEDULED);
                execution.registerProducedPartitions(slot.getTaskManagerLocation()).get();
                ev.deployToSlot(slot);
                assertThat(ev.getExecutionState()).isEqualTo(ExecutionState.DEPLOYING);

                TaskDeploymentDescriptor tdd = tdds.take();
                assertThat(tdd).isNotNull();

                List<InputGateDeploymentDescriptor> igdds = tdd.getInputGates();
                assertThat(igdds).hasSize(ev.getAllConsumedPartitionGroups().size());

                if (igdds.size() > 0) {
                    checkShuffleDescriptors(igdds.get(0), ev.getConsumedPartitionGroup(0));
                }
            }
        }
    }

    private ExecutionGraph createAndSetupExecutionGraph(int numberOfVertices, int parallelism)
            throws JobException, JobExecutionException {

        final List<JobVertex> vertices = new ArrayList<>();

        for (int i = 0; i < numberOfVertices; i++) {
            JobVertex vertex = new JobVertex(String.format("v%d", i + 1), new JobVertexID());
            vertex.setParallelism(parallelism);
            vertex.setInvokableClass(BatchTask.class);
            vertices.add(vertex);
        }

        for (int i = 1; i < numberOfVertices; i++) {
            vertices.get(i)
                    .connectNewDataSetAsInput(
                            vertices.get(i - 1),
                            DistributionPattern.POINTWISE,
                            ResultPartitionType.BLOCKING);
        }

        final JobGraph jobGraph =
                JobGraphTestUtils.batchJobGraph(vertices.toArray(new JobVertex[0]));

        final DirectScheduledExecutorService executor = new DirectScheduledExecutorService();
        final DefaultExecutionGraph eg =
                TestingDefaultExecutionGraphBuilder.newBuilder()
                        .setJobGraph(jobGraph)
                        .setBlobWriter(blobWriter)
                        .build(executor);

        eg.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());

        return eg;
    }

    private static void checkShuffleDescriptors(
            InputGateDeploymentDescriptor igdd, ConsumedPartitionGroup consumedPartitionGroup) {
        int idx = 0;
        for (IntermediateResultPartitionID consumedPartitionId : consumedPartitionGroup) {
            assertThat(igdd.getShuffleDescriptors()[idx++].getResultPartitionID().getPartitionId())
                    .isEqualTo(consumedPartitionId);
        }
    }
}
