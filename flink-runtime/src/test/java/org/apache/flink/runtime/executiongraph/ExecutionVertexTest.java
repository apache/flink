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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.TestingJobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.scheduler.DefaultSchedulerBuilder;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.runtime.scheduler.TestingPhysicalSlot;
import org.apache.flink.runtime.scheduler.TestingPhysicalSlotProvider;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link ExecutionVertex}. */
class ExecutionVertexTest {

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    @Test
    void testResetForNewExecutionReleasesPartitions() throws Exception {
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

        final JobGraph jobGraph =
                JobGraphTestUtils.streamingJobGraph(producerJobVertex, consumerJobVertex);
        final SchedulerBase scheduler =
                new DefaultSchedulerBuilder(
                                jobGraph,
                                ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                                EXECUTOR_RESOURCE.getExecutor())
                        .setPartitionTracker(partitionTracker)
                        .build();

        scheduler.startScheduling();

        final ExecutionJobVertex producerExecutionJobVertex =
                scheduler.getExecutionJobVertex(producerJobVertex.getID());

        Execution execution =
                producerExecutionJobVertex.getTaskVertices()[0].getCurrentExecutionAttempt();

        assertThat(releasePartitionsFuture).isNotDone();

        execution.markFinished();

        assertThat(releasePartitionsFuture).isNotDone();

        for (ExecutionVertex executionVertex : producerExecutionJobVertex.getTaskVertices()) {
            executionVertex.resetForNewExecution();
        }

        final IntermediateResultPartitionID intermediateResultPartitionID =
                producerExecutionJobVertex.getProducedDataSets()[0].getPartitions()[0]
                        .getPartitionId();
        final ResultPartitionID resultPartitionID =
                execution
                        .getResultPartitionDeploymentDescriptor(intermediateResultPartitionID)
                        .get()
                        .getShuffleDescriptor()
                        .getResultPartitionID();

        assertThat(releasePartitionsFuture.get()).contains(resultPartitionID);
    }

    @Test
    void testFindLatestAllocationIgnoresFailedAttempts() throws Exception {
        final JobVertex source = ExecutionGraphTestUtils.createNoOpVertex(1);
        final JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(source);
        final TestingPhysicalSlotProvider withLimitedAmountOfPhysicalSlots =
                TestingPhysicalSlotProvider.createWithLimitedAmountOfPhysicalSlots(1);
        final Configuration configuration = new Configuration();
        // make sure that retrieving the last (al)location is independent from the history size
        configuration.set(JobManagerOptions.MAX_ATTEMPTS_HISTORY_SIZE, 1);
        final SchedulerBase scheduler =
                new DefaultSchedulerBuilder(
                                jobGraph,
                                ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                                EXECUTOR_RESOURCE.getExecutor())
                        .setJobMasterConfiguration(configuration)
                        .setExecutionSlotAllocatorFactory(
                                SchedulerTestingUtils.newSlotSharingExecutionSlotAllocatorFactory(
                                        withLimitedAmountOfPhysicalSlots))
                        .build();

        scheduler.startScheduling();

        final ExecutionJobVertex sourceExecutionJobVertex =
                scheduler.getExecutionJobVertex(source.getID());

        final ExecutionVertex sourceExecutionVertex = sourceExecutionJobVertex.getTaskVertices()[0];
        final Execution firstExecution = sourceExecutionVertex.getCurrentExecutionAttempt();

        final TestingPhysicalSlot physicalSlot =
                withLimitedAmountOfPhysicalSlots.getFirstResponseOrFail().join();
        final AllocationID allocationId = physicalSlot.getAllocationId();
        final TaskManagerLocation taskManagerLocation = physicalSlot.getTaskManagerLocation();

        cancelExecution(firstExecution);
        sourceExecutionVertex.resetForNewExecution();

        assertThat(sourceExecutionVertex.findLastAllocation()).hasValue(allocationId);
        assertThat(sourceExecutionVertex.findLastLocation()).hasValue(taskManagerLocation);

        final Execution secondExecution = sourceExecutionVertex.getCurrentExecutionAttempt();
        cancelExecution(secondExecution);
        sourceExecutionVertex.resetForNewExecution();

        assertThat(sourceExecutionVertex.findLastAllocation()).hasValue(allocationId);
        assertThat(sourceExecutionVertex.findLastLocation()).hasValue(taskManagerLocation);
    }

    private void cancelExecution(Execution execution) {
        execution.cancel();
        execution.completeCancelling();
    }
}
