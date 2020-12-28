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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.TestingJobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertFalse;

/** Tests for the {@link ExecutionVertex}. */
public class ExecutionVertexTest extends TestLogger {

    @Test
    public void testResetForNewExecutionReleasesPartitions() throws Exception {
        final JobVertex producerJobVertex = ExecutionGraphTestUtils.createNoOpVertex(1);
        final JobVertex consumerJobVertex = ExecutionGraphTestUtils.createNoOpVertex(1);

        consumerJobVertex.connectNewDataSetAsInput(
                producerJobVertex, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);

        final CompletableFuture<Collection<ResultPartitionID>> releasePartitionsFuture =
                new CompletableFuture<>();
        final TestingJobMasterPartitionTracker partitionTracker =
                new TestingJobMasterPartitionTracker();
        partitionTracker.setStopTrackingAndReleasePartitionsConsumer(
                releasePartitionsFuture::complete);

        final JobGraph jobGraph = new JobGraph(producerJobVertex, consumerJobVertex);
        final SchedulerBase scheduler =
                SchedulerTestingUtils.newSchedulerBuilder(jobGraph)
                        .setPartitionTracker(partitionTracker)
                        .build();

        scheduler.initialize(ComponentMainThreadExecutorServiceAdapter.forMainThread());
        scheduler.startScheduling();

        final ExecutionJobVertex producerExecutionJobVertex =
                scheduler.getExecutionJobVertex(producerJobVertex.getID());

        Execution execution =
                producerExecutionJobVertex.getTaskVertices()[0].getCurrentExecutionAttempt();

        assertFalse(releasePartitionsFuture.isDone());

        execution.markFinished();

        assertFalse(releasePartitionsFuture.isDone());

        producerExecutionJobVertex.resetForNewExecution(1L, 1L);

        final IntermediateResultPartitionID intermediateResultPartitionID =
                producerExecutionJobVertex.getProducedDataSets()[0].getPartitions()[0]
                        .getPartitionId();
        final ResultPartitionID resultPartitionID =
                execution
                        .getResultPartitionDeploymentDescriptor(intermediateResultPartitionID)
                        .get()
                        .getShuffleDescriptor()
                        .getResultPartitionID();

        assertThat(releasePartitionsFuture.get(), contains(resultPartitionID));
    }
}
