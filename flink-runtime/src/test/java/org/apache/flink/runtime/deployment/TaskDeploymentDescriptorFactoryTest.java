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
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blob.TestingBlobWriter;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor.MaybeOffloaded;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor.NonOffloaded;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor.Offloaded;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory.ShuffleDescriptorAndIndex;
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
import org.apache.flink.testutils.executor.TestExecutorResource;
import org.apache.flink.util.CompressedSerializedValue;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.configuration.JobManagerOptions.HybridPartitionDataConsumeConstraint.ONLY_FINISHED_PRODUCERS;
import static org.junit.Assert.assertEquals;

/** Tests for {@link TaskDeploymentDescriptorFactory}. */
public class TaskDeploymentDescriptorFactoryTest extends TestLogger {
    @ClassRule
    public static final TestExecutorResource<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorResource();

    private static final int PARALLELISM = 4;

    @Test
    public void testCacheShuffleDescriptorAsNonOffloaded() throws Exception {
        testCacheShuffleDescriptor(new TestingBlobWriter(Integer.MAX_VALUE));
    }

    @Test
    public void testCacheShuffleDescriptorAsOffloaded() throws Exception {
        testCacheShuffleDescriptor(new TestingBlobWriter(0));
    }

    private void testCacheShuffleDescriptor(TestingBlobWriter blobWriter) throws Exception {
        final JobID jobId = new JobID();

        final Tuple2<ExecutionJobVertex, ExecutionJobVertex> executionJobVertices =
                setupExecutionGraphAndGetVertices(jobId, blobWriter);

        final ExecutionVertex ev21 = executionJobVertices.f1.getTaskVertices()[0];
        createTaskDeploymentDescriptor(ev21);

        // The ShuffleDescriptors should be cached
        final IntermediateResult consumedResult = executionJobVertices.f1.getInputs().get(0);
        final List<MaybeOffloaded<ShuffleDescriptorAndIndex[]>> maybeOffloaded =
                consumedResult
                        .getCachedShuffleDescriptors(ev21.getConsumedPartitionGroup(0))
                        .getAllSerializedShuffleDescriptors();

        final ShuffleDescriptor[] cachedShuffleDescriptors =
                deserializeShuffleDescriptors(maybeOffloaded, jobId, blobWriter);

        // Check if the ShuffleDescriptors are cached correctly
        assertEquals(ev21.getConsumedPartitionGroup(0).size(), cachedShuffleDescriptors.length);

        int idx = 0;
        for (IntermediateResultPartitionID consumedPartitionId :
                ev21.getConsumedPartitionGroup(0)) {
            assertEquals(
                    consumedPartitionId,
                    cachedShuffleDescriptors[idx++].getResultPartitionID().getPartitionId());
        }
    }

    @Test
    public void testHybridVertexFinish() throws Exception {
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
        List<MaybeOffloaded<ShuffleDescriptorAndIndex[]>> maybeOffloaded =
                consumedResult
                        .getCachedShuffleDescriptors(ev22.getConsumedPartitionGroup(0))
                        .getAllSerializedShuffleDescriptors();
        assertEquals(maybeOffloaded.size(), 2);

        final ExecutionVertex ev13 = ejv1.getTaskVertices()[2];
        ev13.finishPartitionsIfNeeded();
        final ExecutionVertex ev23 = ejv2.getTaskVertices()[2];
        createTaskDeploymentDescriptor(ev23);
        consumedResult = ejv2.getInputs().get(0);
        maybeOffloaded =
                consumedResult
                        .getCachedShuffleDescriptors(ev23.getConsumedPartitionGroup(0))
                        .getAllSerializedShuffleDescriptors();
        assertEquals(maybeOffloaded.size(), 3);
    }

    @Test(expected = IllegalStateException.class)
    public void testGetOffloadedShuffleDescriptorBeforeLoading() throws Exception {
        final TestingBlobWriter blobWriter = new TestingBlobWriter(0);

        final JobID jobId = new JobID();

        final Tuple2<ExecutionJobVertex, ExecutionJobVertex> executionJobVertices =
                setupExecutionGraphAndGetVertices(jobId, blobWriter);

        final ExecutionVertex ev21 = executionJobVertices.f1.getTaskVertices()[0];
        final TaskDeploymentDescriptor tdd = createTaskDeploymentDescriptor(ev21);

        // Exception should be thrown when trying to get offloaded shuffle descriptors
        tdd.getInputGates().get(0).getShuffleDescriptors();
    }

    private Tuple2<ExecutionJobVertex, ExecutionJobVertex> setupExecutionGraphAndGetVertices(
            JobID jobId, BlobWriter blobWriter) throws Exception {
        return setupExecutionGraphAndGetVertices(
                jobId,
                blobWriter,
                ResultPartitionType.BLOCKING,
                ResultPartitionType::isBlockingOrBlockingPersistentResultPartition);
    }

    private Tuple2<ExecutionJobVertex, ExecutionJobVertex> setupExecutionGraphAndGetVertices(
            JobID jobId,
            BlobWriter blobWriter,
            ResultPartitionType resultPartitionType,
            MarkPartitionFinishedStrategy markPartitionFinishedStrategy)
            throws Exception {
        final JobVertex v1 = createJobVertex("v1", PARALLELISM);
        final JobVertex v2 = createJobVertex("v2", PARALLELISM);

        v2.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, resultPartitionType);

        final List<JobVertex> ordered = new ArrayList<>(Arrays.asList(v1, v2));

        final ExecutionGraph executionGraph =
                createExecutionGraph(jobId, ordered, blobWriter, markPartitionFinishedStrategy);

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
            final MarkPartitionFinishedStrategy markPartitionFinishedStrategy)
            throws JobException, JobExecutionException {

        final JobGraph jobGraph =
                JobGraphBuilder.newBatchJobGraphBuilder()
                        .setJobId(jobId)
                        .addJobVertices(jobVertices)
                        .build();

        return TestingDefaultExecutionGraphBuilder.newBuilder()
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

    public static ShuffleDescriptor[] deserializeShuffleDescriptors(
            List<MaybeOffloaded<ShuffleDescriptorAndIndex[]>> maybeOffloaded,
            JobID jobId,
            TestingBlobWriter blobWriter)
            throws IOException, ClassNotFoundException {
        Map<Integer, ShuffleDescriptor> shuffleDescriptorsMap = new HashMap<>();
        int maxIndex = 0;
        for (MaybeOffloaded<ShuffleDescriptorAndIndex[]> sd : maybeOffloaded) {
            ShuffleDescriptorAndIndex[] shuffleDescriptorAndIndices;
            if (sd instanceof NonOffloaded) {
                shuffleDescriptorAndIndices =
                        ((NonOffloaded<ShuffleDescriptorAndIndex[]>) sd)
                                .serializedValue.deserializeValue(
                                        ClassLoader.getSystemClassLoader());

            } else {
                final CompressedSerializedValue<ShuffleDescriptorAndIndex[]>
                        compressedSerializedValue =
                                CompressedSerializedValue.fromBytes(
                                        blobWriter.getBlob(
                                                jobId,
                                                ((Offloaded<ShuffleDescriptorAndIndex[]>) sd)
                                                        .serializedValueKey));
                shuffleDescriptorAndIndices =
                        compressedSerializedValue.deserializeValue(
                                ClassLoader.getSystemClassLoader());
            }
            for (ShuffleDescriptorAndIndex shuffleDescriptorAndIndex :
                    shuffleDescriptorAndIndices) {
                int index = shuffleDescriptorAndIndex.getIndex();
                maxIndex = Math.max(maxIndex, shuffleDescriptorAndIndex.getIndex());
                shuffleDescriptorsMap.put(index, shuffleDescriptorAndIndex.getShuffleDescriptor());
            }
        }
        ShuffleDescriptor[] shuffleDescriptors = new ShuffleDescriptor[maxIndex + 1];
        shuffleDescriptorsMap.forEach((key, value) -> shuffleDescriptors[key] = value);
        return shuffleDescriptors;
    }
}
