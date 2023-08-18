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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.jobgraph.DistributionPattern.ALL_TO_ALL;
import static org.apache.flink.runtime.jobgraph.DistributionPattern.POINTWISE;
import static org.apache.flink.util.Preconditions.checkNotNull;
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
        ExecutionGraph eg = buildExecutionGraph(v1, v2, 2, 2, ALL_TO_ALL);

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
                eg.getSchedulingTopology()
                        .getResultPartition(consumedPartition.getPartitionId())
                        .getConsumedPartitionGroups()
                        .get(0);

        assertThat(groupRetrievedByScheduledResultPartition)
                .isEqualTo(groupRetrievedByDownstreamVertex);
    }

    @Test
    void testCalculateNumberOfConsumers() throws Exception {
        testCalculateNumberOfConsumers(5, 2, ALL_TO_ALL, new int[] {2, 2});
        testCalculateNumberOfConsumers(5, 2, POINTWISE, new int[] {1, 1});
        testCalculateNumberOfConsumers(2, 5, ALL_TO_ALL, new int[] {5, 5, 5, 5, 5});
        testCalculateNumberOfConsumers(2, 5, POINTWISE, new int[] {3, 3, 3, 2, 2});
        testCalculateNumberOfConsumers(5, 5, ALL_TO_ALL, new int[] {5, 5, 5, 5, 5});
        testCalculateNumberOfConsumers(5, 5, POINTWISE, new int[] {1, 1, 1, 1, 1});
    }

    private void testCalculateNumberOfConsumers(
            int producerParallelism,
            int consumerParallelism,
            DistributionPattern distributionPattern,
            int[] expectedConsumers)
            throws Exception {
        JobVertex producer = new JobVertex("producer");
        JobVertex consumer = new JobVertex("consumer");
        ExecutionGraph eg =
                buildExecutionGraph(
                        producer,
                        consumer,
                        producerParallelism,
                        consumerParallelism,
                        distributionPattern);
        List<ConsumedPartitionGroup> partitionGroups =
                Arrays.stream(checkNotNull(eg.getJobVertex(consumer.getID())).getTaskVertices())
                        .flatMap(ev -> ev.getAllConsumedPartitionGroups().stream())
                        .collect(Collectors.toList());
        int index = 0;
        for (ConsumedPartitionGroup partitionGroup : partitionGroups) {
            assertThat(partitionGroup.getNumConsumers()).isEqualTo(expectedConsumers[index++]);
        }
    }

    private ExecutionGraph buildExecutionGraph(
            JobVertex producer,
            JobVertex consumer,
            int producerParallelism,
            int consumerParallelism,
            DistributionPattern distributionPattern)
            throws Exception {
        producer.setParallelism(producerParallelism);
        consumer.setParallelism(consumerParallelism);

        producer.setInvokableClass(NoOpInvokable.class);
        consumer.setInvokableClass(NoOpInvokable.class);

        consumer.connectNewDataSetAsInput(
                producer, distributionPattern, ResultPartitionType.BLOCKING);

        JobGraph jobGraph = JobGraphTestUtils.batchJobGraph(producer, consumer);
        SchedulerBase scheduler =
                SchedulerTestingUtils.createScheduler(
                        jobGraph,
                        ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                        EXECUTOR_RESOURCE.getExecutor());
        return scheduler.getExecutionGraph();
    }
}
