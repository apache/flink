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

package org.apache.flink.runtime.deployment;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blob.TestingBlobWriter;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor.MaybeOffloaded;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory.ShuffleDescriptorGroup;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.executiongraph.MarkPartitionFinishedStrategy;
import org.apache.flink.runtime.executiongraph.TestingDefaultExecutionGraphBuilder;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.scheduler.ClusterDatasetCorruptedException;
import org.apache.flink.runtime.scheduler.DefaultSchedulerBuilder;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.configuration.JobManagerOptions.HybridPartitionDataConsumeConstraint.ONLY_FINISHED_PRODUCERS;
import static org.apache.flink.runtime.deployment.TaskDeploymentDescriptorTestUtils.deserializeShuffleDescriptors;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TaskDeploymentDescriptorFactory}. */
class TaskDeploymentDescriptorFactoryTest {
    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    private static final int PARALLELISM = 4;

    @Test
    void testCacheShuffleDescriptorAsNonOffloaded() throws Exception {
        final Configuration jobMasterConfig = new Configuration();
        jobMasterConfig.set(
                TaskDeploymentDescriptorFactory.OFFLOAD_SHUFFLE_DESCRIPTORS_THRESHOLD,
                Integer.MAX_VALUE);
        testCacheShuffleDescriptor(jobMasterConfig);
    }

    @Test
    void testCacheShuffleDescriptorAsOffloaded() throws Exception {
        final Configuration jobMasterConfig = new Configuration();
        jobMasterConfig.set(
                TaskDeploymentDescriptorFactory.OFFLOAD_SHUFFLE_DESCRIPTORS_THRESHOLD, 0);
        testCacheShuffleDescriptor(jobMasterConfig);
    }

    private void testCacheShuffleDescriptor(Configuration jobMasterConfig) throws Exception {
        final JobID jobId = new JobID();
        final TestingBlobWriter blobWriter = new TestingBlobWriter();

        final Tuple2<ExecutionJobVertex, ExecutionJobVertex> executionJobVertices =
                setupExecutionGraphAndGetVertices(jobId, blobWriter, jobMasterConfig);

        final ExecutionVertex ev21 = executionJobVertices.f1.getTaskVertices()[0];
        createTaskDeploymentDescriptor(ev21);

        // The ShuffleDescriptors should be cached
        final IntermediateResult consumedResult = executionJobVertices.f1.getInputs().get(0);
        final List<MaybeOffloaded<ShuffleDescriptorGroup>> serializedShuffleDescriptors =
                consumedResult
                        .getCachedShuffleDescriptors(ev21.getConsumedPartitionGroup(0))
                        .getAllSerializedShuffleDescriptorGroups();

        final ShuffleDescriptor[] cachedShuffleDescriptors =
                deserializeShuffleDescriptors(serializedShuffleDescriptors, jobId, blobWriter);

        // Check if the ShuffleDescriptors are cached correctly
        assertThat(ev21.getConsumedPartitionGroup(0)).hasSize(cachedShuffleDescriptors.length);

        int idx = 0;
        for (IntermediateResultPartitionID consumedPartitionId :
                ev21.getConsumedPartitionGroup(0)) {
            assertThat(cachedShuffleDescriptors[idx++].getResultPartitionID().getPartitionId())
                    .isEqualTo(consumedPartitionId);
        }
    }

    @Test
    void testHybridVertexFinish() throws Exception {
        final Tuple2<ExecutionJobVertex, ExecutionJobVertex> executionJobVertices =
                buildExecutionGraph();
        final ExecutionJobVertex ejv1 = executionJobVertices.f0;
        final ExecutionJobVertex ejv2 = executionJobVertices.f1;

        final ExecutionVertex ev21 = ejv2.getTaskVertices()[0];
        createTaskDeploymentDescriptor(ev21);

        final ExecutionVertex ev11 = ejv1.getTaskVertices()[0];
        final ExecutionVertex ev12 = ejv1.getTaskVertices()[1];
        ev11.finishPartitionsIfNeeded();
        ev12.finishPartitionsIfNeeded();

        final ExecutionVertex ev22 = ejv2.getTaskVertices()[1];
        createTaskDeploymentDescriptor(ev22);
        IntermediateResult consumedResult = ejv2.getInputs().get(0);
        List<MaybeOffloaded<ShuffleDescriptorGroup>> serializedShuffleDescriptors =
                consumedResult
                        .getCachedShuffleDescriptors(ev22.getConsumedPartitionGroup(0))
                        .getAllSerializedShuffleDescriptorGroups();
        assertThat(serializedShuffleDescriptors).hasSize(2);

        final ExecutionVertex ev13 = ejv1.getTaskVertices()[2];
        ev13.finishPartitionsIfNeeded();
        final ExecutionVertex ev23 = ejv2.getTaskVertices()[2];
        createTaskDeploymentDescriptor(ev23);
        consumedResult = ejv2.getInputs().get(0);
        serializedShuffleDescriptors =
                consumedResult
                        .getCachedShuffleDescriptors(ev23.getConsumedPartitionGroup(0))
                        .getAllSerializedShuffleDescriptorGroups();
        assertThat(serializedShuffleDescriptors).hasSize(3);
    }

    @Test
    void testGetOffloadedShuffleDescriptorBeforeLoading() throws Exception {
        final TestingBlobWriter blobWriter = new TestingBlobWriter(0);

        final JobID jobId = new JobID();
        final Configuration jobMasterConfig = new Configuration();
        jobMasterConfig.set(
                TaskDeploymentDescriptorFactory.OFFLOAD_SHUFFLE_DESCRIPTORS_THRESHOLD, 0);

        final Tuple2<ExecutionJobVertex, ExecutionJobVertex> executionJobVertices =
                setupExecutionGraphAndGetVertices(jobId, blobWriter, jobMasterConfig);

        final ExecutionVertex ev21 = executionJobVertices.f1.getTaskVertices()[0];
        final TaskDeploymentDescriptor tdd = createTaskDeploymentDescriptor(ev21);

        // Exception should be thrown when trying to get offloaded shuffle descriptors
        assertThatThrownBy(() -> tdd.getInputGates().get(0).getShuffleDescriptors())
                .isInstanceOf(IllegalStateException.class);
    }

    private Tuple2<ExecutionJobVertex, ExecutionJobVertex> setupExecutionGraphAndGetVertices(
            JobID jobId, BlobWriter blobWriter, Configuration jobMasterConfig) throws Exception {
        return setupExecutionGraphAndGetVertices(
                jobId,
                blobWriter,
                ResultPartitionType.BLOCKING,
                ResultPartitionType::isBlockingOrBlockingPersistentResultPartition,
                jobMasterConfig);
    }

    private Tuple2<ExecutionJobVertex, ExecutionJobVertex> setupExecutionGraphAndGetVertices(
            JobID jobId,
            BlobWriter blobWriter,
            ResultPartitionType resultPartitionType,
            MarkPartitionFinishedStrategy markPartitionFinishedStrategy,
            Configuration jobMasterConfig)
            throws Exception {
        final JobVertex v1 = createJobVertex("v1", PARALLELISM);
        final JobVertex v2 = createJobVertex("v2", PARALLELISM);

        v2.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, resultPartitionType);

        final List<JobVertex> ordered = new ArrayList<>(Arrays.asList(v1, v2));

        final ExecutionGraph executionGraph =
                createExecutionGraph(
                        jobId, ordered, blobWriter, markPartitionFinishedStrategy, jobMasterConfig);

        return Tuple2.of(
                executionGraph.getJobVertex(v1.getID()), executionGraph.getJobVertex(v2.getID()));
    }

    private Tuple2<ExecutionJobVertex, ExecutionJobVertex> buildExecutionGraph() throws Exception {
        final JobVertex producer = createJobVertex("v1", PARALLELISM);
        final JobVertex consumer = createJobVertex("v2", PARALLELISM);

        consumer.connectNewDataSetAsInput(
                producer, DistributionPattern.ALL_TO_ALL, ResultPartitionType.HYBRID_FULL);

        JobGraph jobGraph = JobGraphTestUtils.batchJobGraph(producer, consumer);
        SchedulerBase scheduler =
                new DefaultSchedulerBuilder(
                                jobGraph,
                                ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                                EXECUTOR_RESOURCE.getExecutor())
                        .setHybridPartitionDataConsumeConstraint(ONLY_FINISHED_PRODUCERS)
                        .buildAdaptiveBatchJobScheduler();
        scheduler.startScheduling();
        ExecutionGraph executionGraph = scheduler.getExecutionGraph();
        return Tuple2.of(
                executionGraph.getJobVertex(producer.getID()),
                executionGraph.getJobVertex(consumer.getID()));
    }

    // ============== Utils ==============

    private static JobVertex createJobVertex(String vertexName, int parallelism) {
        JobVertex jobVertex = new JobVertex(vertexName);
        jobVertex.setParallelism(parallelism);
        jobVertex.setInvokableClass(AbstractInvokable.class);
        return jobVertex;
    }

    private static ExecutionGraph createExecutionGraph(
            final JobID jobId,
            final List<JobVertex> jobVertices,
            final BlobWriter blobWriter,
            final MarkPartitionFinishedStrategy markPartitionFinishedStrategy,
            final Configuration jobMasterConfig)
            throws JobException, JobExecutionException {

        final JobGraph jobGraph =
                JobGraphBuilder.newBatchJobGraphBuilder()
                        .setJobId(jobId)
                        .addJobVertices(jobVertices)
                        .build();

        return TestingDefaultExecutionGraphBuilder.newBuilder()
                .setJobMasterConfig(jobMasterConfig)
                .setJobGraph(jobGraph)
                .setBlobWriter(blobWriter)
                .setMarkPartitionFinishedStrategy(markPartitionFinishedStrategy)
                .build(EXECUTOR_RESOURCE.getExecutor());
    }

    private static TaskDeploymentDescriptor createTaskDeploymentDescriptor(ExecutionVertex ev)
            throws IOException, ClusterDatasetCorruptedException {

        return ev.getExecutionGraphAccessor()
                .getTaskDeploymentDescriptorFactory()
                .createDeploymentDescriptor(
                        ev.getCurrentExecutionAttempt(),
                        new AllocationID(),
                        null,
                        Collections.emptyList());
    }
}
