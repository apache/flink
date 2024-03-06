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

package org.apache.flink.runtime.deployment;

import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor.MaybeOffloaded;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor.NonOffloaded;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory.ShuffleDescriptorAndIndex;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory.ShuffleDescriptorGroup;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.scheduler.DefaultSchedulerBuilder;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.CompressedSerializedValue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.jobgraph.DistributionPattern.ALL_TO_ALL;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CachedShuffleDescriptors}. */
class CachedShuffleDescriptorsTest {
    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    @Test
    void testCreateAndGet() throws Exception {
        JobVertex v1 = new JobVertex("source");
        JobVertex v2 = new JobVertex("sink");
        ExecutionGraph eg = buildExecutionGraph(v1, v2, 1, 1, ALL_TO_ALL);

        ExecutionJobVertex ejv1 = eg.getJobVertex(v1.getID());
        assertThat(ejv1).isNotNull();
        ExecutionVertex ev11 = ejv1.getTaskVertices()[0];
        ExecutionJobVertex ejv2 = eg.getJobVertex(v2.getID());
        assertThat(ejv2).isNotNull();
        ExecutionVertex ev21 = ejv2.getTaskVertices()[0];

        IntermediateResultPartition intermediateResultPartition =
                ev11.getProducedPartitions().values().stream().findAny().get();

        ConsumedPartitionGroup consumedPartitionGroup = ev21.getConsumedPartitionGroup(0);

        ShuffleDescriptor shuffleDescriptor = getShuffleDescriptor(intermediateResultPartition);

        CachedShuffleDescriptors cachedShuffleDescriptors =
                new CachedShuffleDescriptors(
                        consumedPartitionGroup,
                        createSingleShuffleDescriptorAndIndex(shuffleDescriptor, 0));

        assertThat(cachedShuffleDescriptors.getAllSerializedShuffleDescriptorGroups()).isEmpty();
        cachedShuffleDescriptors.serializeShuffleDescriptors(
                new TestingShuffleDescriptorSerializer());
        assertThat(cachedShuffleDescriptors.getAllSerializedShuffleDescriptorGroups()).hasSize(1);
        MaybeOffloaded<ShuffleDescriptorGroup> maybeOffloadedShuffleDescriptor =
                cachedShuffleDescriptors.getAllSerializedShuffleDescriptorGroups().get(0);
        assertNonOffloadedShuffleDescriptorAndIndexEquals(
                maybeOffloadedShuffleDescriptor,
                Collections.singletonList(shuffleDescriptor),
                Collections.singletonList(0));
    }

    @Test
    void testMarkPartitionFinishAndSerialize() throws Exception {
        JobVertex v1 = new JobVertex("source");
        JobVertex v2 = new JobVertex("sink");
        ExecutionGraph eg = buildExecutionGraph(v1, v2, 2, 1, ALL_TO_ALL);

        ExecutionJobVertex ejv1 = eg.getJobVertex(v1.getID());
        assertThat(ejv1).isNotNull();
        ExecutionVertex ev11 = ejv1.getTaskVertices()[0];
        ExecutionVertex ev12 = ejv1.getTaskVertices()[1];
        ExecutionJobVertex ejv2 = eg.getJobVertex(v2.getID());
        assertThat(ejv2).isNotNull();
        ExecutionVertex ev21 = ejv2.getTaskVertices()[0];

        IntermediateResultPartition intermediateResultPartition1 =
                ev11.getProducedPartitions().values().stream().findAny().get();
        IntermediateResultPartition intermediateResultPartition2 =
                ev12.getProducedPartitions().values().stream().findAny().get();

        ConsumedPartitionGroup consumedPartitionGroup1 = ev21.getConsumedPartitionGroup(0);

        ShuffleDescriptor shuffleDescriptor = getShuffleDescriptor(intermediateResultPartition1);

        CachedShuffleDescriptors cachedShuffleDescriptors =
                new CachedShuffleDescriptors(
                        consumedPartitionGroup1,
                        createSingleShuffleDescriptorAndIndex(shuffleDescriptor, 0));
        TestingShuffleDescriptorSerializer testingShuffleDescriptorSerializer =
                new TestingShuffleDescriptorSerializer();
        cachedShuffleDescriptors.serializeShuffleDescriptors(testingShuffleDescriptorSerializer);

        cachedShuffleDescriptors.markPartitionFinished(intermediateResultPartition1);
        cachedShuffleDescriptors.markPartitionFinished(intermediateResultPartition2);
        cachedShuffleDescriptors.serializeShuffleDescriptors(testingShuffleDescriptorSerializer);
        assertThat(cachedShuffleDescriptors.getAllSerializedShuffleDescriptorGroups()).hasSize(2);

        MaybeOffloaded<ShuffleDescriptorGroup> maybeOffloaded =
                cachedShuffleDescriptors.getAllSerializedShuffleDescriptorGroups().get(1);
        ShuffleDescriptor expectedShuffleDescriptor1 =
                TaskDeploymentDescriptorFactory.getConsumedPartitionShuffleDescriptor(
                        intermediateResultPartition1,
                        TaskDeploymentDescriptorFactory.PartitionLocationConstraint.MUST_BE_KNOWN,
                        false);
        ShuffleDescriptor expectedShuffleDescriptor2 =
                TaskDeploymentDescriptorFactory.getConsumedPartitionShuffleDescriptor(
                        intermediateResultPartition2,
                        TaskDeploymentDescriptorFactory.PartitionLocationConstraint.MUST_BE_KNOWN,
                        false);
        assertNonOffloadedShuffleDescriptorAndIndexEquals(
                maybeOffloaded,
                Arrays.asList(expectedShuffleDescriptor1, expectedShuffleDescriptor2),
                Arrays.asList(0, 1));
    }

    private void assertNonOffloadedShuffleDescriptorAndIndexEquals(
            MaybeOffloaded<ShuffleDescriptorGroup> maybeOffloaded,
            List<ShuffleDescriptor> expectedDescriptors,
            List<Integer> expectedIndices)
            throws Exception {
        assertThat(expectedDescriptors).hasSameSizeAs(expectedIndices);
        assertThat(maybeOffloaded).isInstanceOf(NonOffloaded.class);
        NonOffloaded<ShuffleDescriptorGroup> nonOffloaded =
                (NonOffloaded<ShuffleDescriptorGroup>) maybeOffloaded;
        ShuffleDescriptorAndIndex[] shuffleDescriptorAndIndices =
                nonOffloaded
                        .serializedValue
                        .deserializeValue(getClass().getClassLoader())
                        .getShuffleDescriptors();
        assertThat(shuffleDescriptorAndIndices).hasSameSizeAs(expectedDescriptors);
        for (int i = 0; i < shuffleDescriptorAndIndices.length; i++) {
            assertThat(shuffleDescriptorAndIndices[i].getIndex()).isEqualTo(expectedIndices.get(i));
            assertThat(shuffleDescriptorAndIndices[i].getShuffleDescriptor().getResultPartitionID())
                    .isEqualTo(expectedDescriptors.get(i).getResultPartitionID());
        }
    }

    private ShuffleDescriptor getShuffleDescriptor(
            IntermediateResultPartition intermediateResultPartition) {
        return TaskDeploymentDescriptorFactory.getConsumedPartitionShuffleDescriptor(
                intermediateResultPartition,
                TaskDeploymentDescriptorFactory.PartitionLocationConstraint.CAN_BE_UNKNOWN,
                true);
    }

    private static ShuffleDescriptorAndIndex[] createSingleShuffleDescriptorAndIndex(
            ShuffleDescriptor shuffleDescriptor, int index) {
        return new ShuffleDescriptorAndIndex[] {
            new ShuffleDescriptorAndIndex(shuffleDescriptor, index)
        };
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
                producer, distributionPattern, ResultPartitionType.HYBRID_FULL);

        JobGraph jobGraph = JobGraphTestUtils.batchJobGraph(producer, consumer);
        SchedulerBase scheduler =
                new DefaultSchedulerBuilder(
                                jobGraph,
                                ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                                EXECUTOR_RESOURCE.getExecutor())
                        .build();
        scheduler.startScheduling();
        return scheduler.getExecutionGraph();
    }

    private static class TestingShuffleDescriptorSerializer
            implements TaskDeploymentDescriptorFactory.ShuffleDescriptorSerializer {

        @Override
        public MaybeOffloaded<ShuffleDescriptorGroup> serializeAndTryOffloadShuffleDescriptor(
                ShuffleDescriptorGroup shuffleDescriptorGroup, int numConsumer) throws IOException {
            return new NonOffloaded<>(CompressedSerializedValue.fromObject(shuffleDescriptorGroup));
        }
    }
}
