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
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blob.TestingBlobWriter;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor.MaybeOffloaded;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor.NonOffloaded;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor.Offloaded;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.executiongraph.TestingDefaultExecutionGraphBuilder;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.util.CompressedSerializedValue;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/** Tests for {@link TaskDeploymentDescriptorFactory}. */
public class TaskDeploymentDescriptorFactoryTest extends TestLogger {

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

        final ExecutionJobVertex ejv = setupExecutionGraphAndGetVertex(jobId, blobWriter);

        final ExecutionVertex ev21 = ejv.getTaskVertices()[0];
        createTaskDeploymentDescriptor(ev21);

        // The ShuffleDescriptors should be cached
        final IntermediateResult consumedResult = ejv.getInputs().get(0);
        final MaybeOffloaded<ShuffleDescriptor[]> maybeOffloaded =
                consumedResult.getCachedShuffleDescriptors(ev21.getConsumedPartitionGroup(0));

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

    @Test(expected = IllegalStateException.class)
    public void testGetOffloadedShuffleDescriptorBeforeLoading() throws Exception {
        final TestingBlobWriter blobWriter = new TestingBlobWriter(0);

        final JobID jobId = new JobID();

        final ExecutionJobVertex ejv = setupExecutionGraphAndGetVertex(jobId, blobWriter);

        final ExecutionVertex ev21 = ejv.getTaskVertices()[0];
        final TaskDeploymentDescriptor tdd = createTaskDeploymentDescriptor(ev21);

        // Exception should be thrown when trying to get offloaded shuffle descriptors
        tdd.getInputGates().get(0).getShuffleDescriptors();
    }

    private ExecutionJobVertex setupExecutionGraphAndGetVertex(JobID jobId, BlobWriter blobWriter)
            throws JobException, JobExecutionException {
        final JobVertex v1 = createJobVertex("v1", PARALLELISM);
        final JobVertex v2 = createJobVertex("v2", PARALLELISM);

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

        final List<JobVertex> ordered = new ArrayList<>(Arrays.asList(v1, v2));

        final ExecutionGraph executionGraph = createExecutionGraph(jobId, ordered, blobWriter);

        return executionGraph.getJobVertex(v2.getID());
    }

    // ============== Utils ==============

    private static JobVertex createJobVertex(String vertexName, int parallelism) {
        JobVertex jobVertex = new JobVertex(vertexName);
        jobVertex.setParallelism(parallelism);
        jobVertex.setInvokableClass(AbstractInvokable.class);
        return jobVertex;
    }

    private static ExecutionGraph createExecutionGraph(
            final JobID jobId, final List<JobVertex> jobVertices, final BlobWriter blobWriter)
            throws JobException, JobExecutionException {

        final JobGraph jobGraph =
                JobGraphBuilder.newBatchJobGraphBuilder()
                        .setJobId(jobId)
                        .addJobVertices(jobVertices)
                        .build();

        return TestingDefaultExecutionGraphBuilder.newBuilder()
                .setJobGraph(jobGraph)
                .setBlobWriter(blobWriter)
                .build();
    }

    private static TaskDeploymentDescriptor createTaskDeploymentDescriptor(ExecutionVertex ev)
            throws IOException {

        return TaskDeploymentDescriptorFactory.fromExecutionVertex(ev, 0)
                .createDeploymentDescriptor(new AllocationID(), null, Collections.emptyList());
    }

    public static ShuffleDescriptor[] deserializeShuffleDescriptors(
            MaybeOffloaded<ShuffleDescriptor[]> maybeOffloaded,
            JobID jobId,
            TestingBlobWriter blobWriter)
            throws IOException, ClassNotFoundException {

        if (maybeOffloaded instanceof NonOffloaded) {
            return ((NonOffloaded<ShuffleDescriptor[]>) maybeOffloaded)
                    .serializedValue.deserializeValue(ClassLoader.getSystemClassLoader());
        } else {
            final CompressedSerializedValue<ShuffleDescriptor[]> compressedSerializedValue =
                    CompressedSerializedValue.fromBytes(
                            blobWriter.getBlob(
                                    jobId,
                                    ((Offloaded<ShuffleDescriptor[]>) maybeOffloaded)
                                            .serializedValueKey));
            return compressedSerializedValue.deserializeValue(ClassLoader.getSystemClassLoader());
        }
    }
}
