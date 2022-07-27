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

import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link EdgeManager}. */
class EdgeManagerTest {

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    @Test
    void testGetConsumedPartitionGroup() throws Exception {
        JobVertex v1 = new JobVertex("source");
        JobVertex v2 = new JobVertex("sink");

        v1.setParallelism(2);
        v2.setParallelism(2);

        v1.setInvokableClass(NoOpInvokable.class);
        v2.setInvokableClass(NoOpInvokable.class);

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

        JobGraph jobGraph = JobGraphTestUtils.batchJobGraph(v1, v2);
        SchedulerBase scheduler =
                SchedulerTestingUtils.createScheduler(
                        jobGraph,
                        ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                        EXECUTOR_RESOURCE.getExecutor());
        ExecutionGraph eg = scheduler.getExecutionGraph();

        ConsumedPartitionGroup groupRetrievedByDownstreamVertex =
                Objects.requireNonNull(eg.getJobVertex(v2.getID()))
                        .getTaskVertices()[0]
                        .getAllConsumedPartitionGroups()
                        .get(0);

        IntermediateResultPartition consumedPartition =
                Objects.requireNonNull(eg.getJobVertex(v1.getID()))
                        .getProducedDataSets()[0]
                        .getPartitions()[0];

        ConsumedPartitionGroup groupRetrievedByIntermediateResultPartition =
                consumedPartition.getConsumedPartitionGroups().get(0);

        assertThat(groupRetrievedByIntermediateResultPartition)
                .isEqualTo(groupRetrievedByDownstreamVertex);

        ConsumedPartitionGroup groupRetrievedByScheduledResultPartition =
                scheduler
                        .getExecutionGraph()
                        .getSchedulingTopology()
                        .getResultPartition(consumedPartition.getPartitionId())
                        .getConsumedPartitionGroups()
                        .get(0);

        assertThat(groupRetrievedByScheduledResultPartition)
                .isEqualTo(groupRetrievedByDownstreamVertex);
    }
}
