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

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.core.io.InputSplitSource;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.apache.flink.shaded.guava31.com.google.common.collect.Sets;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * This class contains test concerning the correct conversion from {@link JobGraph} to {@link
 * ExecutionGraph} objects. It also tests that {@link
 * VertexInputInfoComputationUtils#computeVertexInputInfoForAllToAll} builds {@link
 * DistributionPattern#ALL_TO_ALL} connections correctly.
 */
class DefaultExecutionGraphConstructionTest {
    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    private static final JobManagerJobMetricGroup JOB_MANAGER_JOB_METRIC_GROUP =
            UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup();

    private ExecutionGraph createDefaultExecutionGraph(List<JobVertex> vertices) throws Exception {
        return TestingDefaultExecutionGraphBuilder.newBuilder()
                .setVertexParallelismStore(SchedulerBase.computeVertexParallelismStore(vertices))
                .build(EXECUTOR_RESOURCE.getExecutor());
    }

    private ExecutionGraph createDynamicExecutionGraph(List<JobVertex> vertices) throws Exception {
        return TestingDefaultExecutionGraphBuilder.newBuilder()
                .setVertexParallelismStore(SchedulerBase.computeVertexParallelismStore(vertices))
                .buildDynamicGraph(EXECUTOR_RESOURCE.getExecutor());
    }

    @Test
    void testExecutionAttemptIdInTwoIdenticalJobsIsNotSame() throws Exception {
        JobVertex v1 = new JobVertex("vertex1");
        JobVertex v2 = new JobVertex("vertex2");
        JobVertex v3 = new JobVertex("vertex3");

        v1.setParallelism(5);
        v2.setParallelism(7);
        v3.setParallelism(2);

        v1.setInvokableClass(AbstractInvokable.class);
        v2.setInvokableClass(AbstractInvokable.class);
        v3.setInvokableClass(AbstractInvokable.class);

        List<JobVertex> ordered = new ArrayList<>(Arrays.asList(v1, v2, v3));

        ExecutionGraph eg1 = createDefaultExecutionGraph(ordered);
        ExecutionGraph eg2 = createDefaultExecutionGraph(ordered);
        eg1.attachJobGraph(ordered, JOB_MANAGER_JOB_METRIC_GROUP);
        eg2.attachJobGraph(ordered, JOB_MANAGER_JOB_METRIC_GROUP);

        assertThat(
                        Sets.intersection(
                                eg1.getRegisteredExecutions().keySet(),
                                eg2.getRegisteredExecutions().keySet()))
                .isEmpty();
    }

    /**
     * Creates a JobGraph of the following form.
     *
     * <pre>
     *  v1--->v2-->\
     *              \
     *               v4 --->\
     *        ----->/        \
     *  v3-->/                v5
     *       \               /
     *        ------------->/
     * </pre>
     */
    @Test
    void testCreateSimpleGraphBipartite() throws Exception {
        JobVertex v1 = new JobVertex("vertex1");
        JobVertex v2 = new JobVertex("vertex2");
        JobVertex v3 = new JobVertex("vertex3");
        JobVertex v4 = new JobVertex("vertex4");
        JobVertex v5 = new JobVertex("vertex5");

        v1.setParallelism(5);
        v2.setParallelism(7);
        v3.setParallelism(2);
        v4.setParallelism(11);
        v5.setParallelism(4);

        v1.setInvokableClass(AbstractInvokable.class);
        v2.setInvokableClass(AbstractInvokable.class);
        v3.setInvokableClass(AbstractInvokable.class);
        v4.setInvokableClass(AbstractInvokable.class);
        v5.setInvokableClass(AbstractInvokable.class);

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        v4.connectNewDataSetAsInput(
                v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        v4.connectNewDataSetAsInput(
                v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        v5.connectNewDataSetAsInput(
                v4, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        v5.connectNewDataSetAsInput(
                v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

        List<JobVertex> ordered = new ArrayList<JobVertex>(Arrays.asList(v1, v2, v3, v4, v5));

        ExecutionGraph eg = createDefaultExecutionGraph(ordered);
        eg.attachJobGraph(ordered, JOB_MANAGER_JOB_METRIC_GROUP);
        verifyTestGraph(eg, v1, v2, v3, v4, v5);
    }

    private void verifyTestGraph(
            ExecutionGraph eg,
            JobVertex v1,
            JobVertex v2,
            JobVertex v3,
            JobVertex v4,
            JobVertex v5) {
        ExecutionGraphTestUtils.verifyGeneratedExecutionJobVertex(
                eg, v1, null, Collections.singletonList(v2));
        ExecutionGraphTestUtils.verifyGeneratedExecutionJobVertex(
                eg, v2, Collections.singletonList(v1), Collections.singletonList(v4));
        ExecutionGraphTestUtils.verifyGeneratedExecutionJobVertex(
                eg, v3, null, Arrays.asList(v4, v5));
        ExecutionGraphTestUtils.verifyGeneratedExecutionJobVertex(
                eg, v4, Arrays.asList(v2, v3), Collections.singletonList(v5));
        ExecutionGraphTestUtils.verifyGeneratedExecutionJobVertex(
                eg, v5, Arrays.asList(v4, v3), null);
    }

    @Test
    void testCannotConnectWrongOrder() throws Exception {
        JobVertex v1 = new JobVertex("vertex1");
        JobVertex v2 = new JobVertex("vertex2");
        JobVertex v3 = new JobVertex("vertex3");
        JobVertex v4 = new JobVertex("vertex4");
        JobVertex v5 = new JobVertex("vertex5");

        v1.setParallelism(5);
        v2.setParallelism(7);
        v3.setParallelism(2);
        v4.setParallelism(11);
        v5.setParallelism(4);

        v1.setInvokableClass(AbstractInvokable.class);
        v2.setInvokableClass(AbstractInvokable.class);
        v3.setInvokableClass(AbstractInvokable.class);
        v4.setInvokableClass(AbstractInvokable.class);
        v5.setInvokableClass(AbstractInvokable.class);

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        v4.connectNewDataSetAsInput(
                v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        v4.connectNewDataSetAsInput(
                v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        v5.connectNewDataSetAsInput(
                v4, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        v5.connectNewDataSetAsInput(
                v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

        List<JobVertex> ordered = new ArrayList<JobVertex>(Arrays.asList(v1, v2, v3, v5, v4));

        ExecutionGraph eg = createDefaultExecutionGraph(ordered);
        assertThatThrownBy(() -> eg.attachJobGraph(ordered, JOB_MANAGER_JOB_METRIC_GROUP))
                .isInstanceOf(JobException.class);
    }

    @Test
    void testSetupInputSplits() throws Exception {
        final InputSplit[] emptySplits = new InputSplit[0];

        InputSplitAssigner assigner1 = new TestingInputSplitAssigner();
        InputSplitAssigner assigner2 = new TestingInputSplitAssigner();

        InputSplitSource<InputSplit> source1 =
                new TestingInputSplitSource<>(emptySplits, assigner1);
        InputSplitSource<InputSplit> source2 =
                new TestingInputSplitSource<>(emptySplits, assigner2);

        JobVertex v1 = new JobVertex("vertex1");
        JobVertex v2 = new JobVertex("vertex2");
        JobVertex v3 = new JobVertex("vertex3");
        JobVertex v4 = new JobVertex("vertex4");
        JobVertex v5 = new JobVertex("vertex5");

        v1.setParallelism(5);
        v2.setParallelism(7);
        v3.setParallelism(2);
        v4.setParallelism(11);
        v5.setParallelism(4);

        v1.setInvokableClass(AbstractInvokable.class);
        v2.setInvokableClass(AbstractInvokable.class);
        v3.setInvokableClass(AbstractInvokable.class);
        v4.setInvokableClass(AbstractInvokable.class);
        v5.setInvokableClass(AbstractInvokable.class);

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        v4.connectNewDataSetAsInput(
                v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        v4.connectNewDataSetAsInput(
                v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        v5.connectNewDataSetAsInput(
                v4, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        v5.connectNewDataSetAsInput(
                v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

        v3.setInputSplitSource(source1);
        v5.setInputSplitSource(source2);

        List<JobVertex> ordered = new ArrayList<>(Arrays.asList(v1, v2, v3, v4, v5));

        ExecutionGraph eg = createDefaultExecutionGraph(ordered);
        eg.attachJobGraph(ordered, JOB_MANAGER_JOB_METRIC_GROUP);

        assertThat(eg.getAllVertices().get(v3.getID()).getSplitAssigner()).isEqualTo(assigner1);
        assertThat(eg.getAllVertices().get(v5.getID()).getSplitAssigner()).isEqualTo(assigner2);
    }

    @Test
    void testMultiConsumersForOneIntermediateResult() throws Exception {
        JobVertex v1 = new JobVertex("vertex1");
        JobVertex v2 = new JobVertex("vertex2");
        JobVertex v3 = new JobVertex("vertex3");

        IntermediateDataSetID dataSetId = new IntermediateDataSetID();
        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING, dataSetId, false);
        v3.connectNewDataSetAsInput(
                v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING, dataSetId, false);

        List<JobVertex> vertices = new ArrayList<>(Arrays.asList(v1, v2, v3));
        ExecutionGraph eg = createDefaultExecutionGraph(vertices);
        eg.attachJobGraph(vertices, JOB_MANAGER_JOB_METRIC_GROUP);

        ExecutionJobVertex ejv1 = checkNotNull(eg.getJobVertex(v1.getID()));
        assertThat(ejv1.getProducedDataSets()).hasSize(1);
        assertThat(ejv1.getProducedDataSets()[0].getId()).isEqualTo(dataSetId);

        ExecutionJobVertex ejv2 = checkNotNull(eg.getJobVertex(v2.getID()));
        assertThat(ejv2.getInputs()).hasSize(1);
        assertThat(ejv2.getInputs().get(0).getId()).isEqualTo(dataSetId);

        ExecutionJobVertex ejv3 = checkNotNull(eg.getJobVertex(v3.getID()));
        assertThat(ejv3.getInputs()).hasSize(1);
        assertThat(ejv3.getInputs().get(0).getId()).isEqualTo(dataSetId);

        List<ConsumedPartitionGroup> partitionGroups1 =
                ejv2.getTaskVertices()[0].getAllConsumedPartitionGroups();
        assertThat(partitionGroups1).hasSize(1);
        assertThat(partitionGroups1.get(0).getIntermediateDataSetID()).isEqualTo(dataSetId);

        List<ConsumedPartitionGroup> partitionGroups2 =
                ejv3.getTaskVertices()[0].getAllConsumedPartitionGroups();
        assertThat(partitionGroups2).hasSize(1);
        assertThat(partitionGroups2.get(0).getIntermediateDataSetID()).isEqualTo(dataSetId);
    }

    @Test
    void testRegisterConsumedPartitionGroupToEdgeManager() throws Exception {
        JobVertex v1 = new JobVertex("source");
        JobVertex v2 = new JobVertex("sink");

        v1.setParallelism(2);
        v2.setParallelism(2);

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

        List<JobVertex> ordered = new ArrayList<>(Arrays.asList(v1, v2));
        ExecutionGraph eg = createDefaultExecutionGraph(ordered);
        eg.attachJobGraph(ordered, JOB_MANAGER_JOB_METRIC_GROUP);

        IntermediateResult result =
                Objects.requireNonNull(eg.getJobVertex(v1.getID())).getProducedDataSets()[0];

        IntermediateResultPartition partition1 = result.getPartitions()[0];
        IntermediateResultPartition partition2 = result.getPartitions()[1];

        assertThat(partition2.getConsumedPartitionGroups().get(0))
                .isEqualTo(partition1.getConsumedPartitionGroups().get(0));

        ConsumedPartitionGroup consumedPartitionGroup =
                partition1.getConsumedPartitionGroups().get(0);
        Set<IntermediateResultPartitionID> partitionIds = new HashSet<>();
        for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
            partitionIds.add(partitionId);
        }
        assertThat(partitionIds)
                .containsExactlyInAnyOrder(
                        partition1.getPartitionId(), partition2.getPartitionId());
    }

    @Test
    void testPointWiseConsumedPartitionGroupPartitionFinished() throws Exception {
        JobVertex v1 = new JobVertex("source");
        JobVertex v2 = new JobVertex("sink");

        v1.setParallelism(4);
        v2.setParallelism(2);

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);

        List<JobVertex> ordered = new ArrayList<>(Arrays.asList(v1, v2));
        ExecutionGraph eg = createDefaultExecutionGraph(ordered);
        eg.attachJobGraph(ordered, JOB_MANAGER_JOB_METRIC_GROUP);

        IntermediateResult result =
                Objects.requireNonNull(eg.getJobVertex(v1.getID())).getProducedDataSets()[0];

        IntermediateResultPartition partition1 = result.getPartitions()[0];
        IntermediateResultPartition partition2 = result.getPartitions()[1];
        IntermediateResultPartition partition3 = result.getPartitions()[2];
        IntermediateResultPartition partition4 = result.getPartitions()[3];

        ConsumedPartitionGroup consumedPartitionGroup1 =
                partition1.getConsumedPartitionGroups().get(0);

        ConsumedPartitionGroup consumedPartitionGroup2 =
                partition4.getConsumedPartitionGroups().get(0);

        assertThat(consumedPartitionGroup1.getNumberOfUnfinishedPartitions()).isEqualTo(2);
        assertThat(consumedPartitionGroup2.getNumberOfUnfinishedPartitions()).isEqualTo(2);
        partition1.markFinished();
        partition2.markFinished();
        assertThat(consumedPartitionGroup1.getNumberOfUnfinishedPartitions()).isZero();
        partition3.markFinished();
        partition4.markFinished();
        assertThat(consumedPartitionGroup2.getNumberOfUnfinishedPartitions()).isZero();
    }

    @Test
    void testAllToAllConsumedPartitionGroupPartitionFinished() throws Exception {
        JobVertex v1 = new JobVertex("source");
        JobVertex v2 = new JobVertex("sink");

        v1.setParallelism(2);
        v2.setParallelism(2);

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

        List<JobVertex> ordered = new ArrayList<>(Arrays.asList(v1, v2));
        ExecutionGraph eg = createDefaultExecutionGraph(ordered);
        eg.attachJobGraph(ordered, JOB_MANAGER_JOB_METRIC_GROUP);

        IntermediateResult result =
                Objects.requireNonNull(eg.getJobVertex(v1.getID())).getProducedDataSets()[0];

        IntermediateResultPartition partition1 = result.getPartitions()[0];
        IntermediateResultPartition partition2 = result.getPartitions()[1];

        ConsumedPartitionGroup consumedPartitionGroup =
                partition1.getConsumedPartitionGroups().get(0);

        assertThat(consumedPartitionGroup.getNumberOfUnfinishedPartitions()).isEqualTo(2);
        partition1.markFinished();
        assertThat(consumedPartitionGroup.getNumberOfUnfinishedPartitions()).isOne();
        partition2.markFinished();
        assertThat(consumedPartitionGroup.getNumberOfUnfinishedPartitions()).isZero();
    }

    @Test
    void testDynamicGraphAllToAllConsumedPartitionGroupPartitionFinished() throws Exception {
        JobVertex v1 = new JobVertex("source");
        JobVertex v2 = new JobVertex("sink");

        v1.setParallelism(2);
        v2.setParallelism(2);

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

        List<JobVertex> ordered = new ArrayList<>(Arrays.asList(v1, v2));
        ExecutionGraph eg = createDynamicExecutionGraph(ordered);
        eg.attachJobGraph(ordered, JOB_MANAGER_JOB_METRIC_GROUP);

        ExecutionJobVertex ejv1 = eg.getJobVertex(v1.getID());
        eg.initializeJobVertex(ejv1, 0L, JOB_MANAGER_JOB_METRIC_GROUP);

        IntermediateResult result =
                Objects.requireNonNull(eg.getJobVertex(v1.getID())).getProducedDataSets()[0];

        IntermediateResultPartition partition1 = result.getPartitions()[0];
        IntermediateResultPartition partition2 = result.getPartitions()[1];

        partition1.markFinished();
        partition2.markFinished();

        assertThat(partition1.getConsumedPartitionGroups()).isEmpty();

        ExecutionJobVertex ejv2 = eg.getJobVertex(v2.getID());
        eg.initializeJobVertex(ejv2, 0L, JOB_MANAGER_JOB_METRIC_GROUP);

        ConsumedPartitionGroup consumedPartitionGroup =
                partition1.getConsumedPartitionGroups().get(0);

        assertThat(consumedPartitionGroup.getNumberOfUnfinishedPartitions()).isZero();
    }

    @Test
    void testDynamicGraphPointWiseConsumedPartitionGroupPartitionFinished() throws Exception {
        JobVertex v1 = new JobVertex("source");
        JobVertex v2 = new JobVertex("sink");

        v1.setParallelism(4);
        v2.setParallelism(2);

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);

        List<JobVertex> ordered = new ArrayList<>(Arrays.asList(v1, v2));
        ExecutionGraph eg = createDynamicExecutionGraph(ordered);
        eg.attachJobGraph(ordered, JOB_MANAGER_JOB_METRIC_GROUP);

        ExecutionJobVertex ejv1 = eg.getJobVertex(v1.getID());
        eg.initializeJobVertex(ejv1, 0L, JOB_MANAGER_JOB_METRIC_GROUP);

        IntermediateResult result =
                Objects.requireNonNull(eg.getJobVertex(v1.getID())).getProducedDataSets()[0];

        IntermediateResultPartition partition1 = result.getPartitions()[0];
        IntermediateResultPartition partition2 = result.getPartitions()[1];
        IntermediateResultPartition partition3 = result.getPartitions()[2];
        IntermediateResultPartition partition4 = result.getPartitions()[3];

        partition1.markFinished();
        partition2.markFinished();
        partition3.markFinished();
        partition4.markFinished();

        assertThat(partition1.getConsumedPartitionGroups()).isEmpty();
        assertThat(partition4.getConsumedPartitionGroups()).isEmpty();

        ExecutionJobVertex ejv2 = eg.getJobVertex(v2.getID());
        eg.initializeJobVertex(ejv2, 0L, JOB_MANAGER_JOB_METRIC_GROUP);

        ConsumedPartitionGroup consumedPartitionGroup1 =
                partition1.getConsumedPartitionGroups().get(0);
        assertThat(consumedPartitionGroup1.getNumberOfUnfinishedPartitions()).isZero();
        ConsumedPartitionGroup consumedPartitionGroup2 =
                partition4.getConsumedPartitionGroups().get(0);
        assertThat(consumedPartitionGroup2.getNumberOfUnfinishedPartitions()).isZero();
    }

    @Test
    void testAttachToDynamicGraph() throws Exception {
        JobVertex v1 = new JobVertex("source");
        JobVertex v2 = new JobVertex("sink");

        v1.setParallelism(2);
        v2.setParallelism(2);

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

        List<JobVertex> ordered = new ArrayList<>(Arrays.asList(v1, v2));
        ExecutionGraph eg = createDynamicExecutionGraph(ordered);
        eg.attachJobGraph(ordered, JOB_MANAGER_JOB_METRIC_GROUP);

        assertThat(eg.getAllVertices()).hasSize(2);
        Iterator<ExecutionJobVertex> jobVertices = eg.getVerticesTopologically().iterator();
        assertThat(jobVertices.next().isInitialized()).isFalse();
        assertThat(jobVertices.next().isInitialized()).isFalse();
    }

    private static final class TestingInputSplitAssigner implements InputSplitAssigner {

        @Override
        public InputSplit getNextInputSplit(String host, int taskId) {
            return null;
        }

        @Override
        public void returnInputSplit(List<InputSplit> splits, int taskId) {}
    }

    private static final class TestingInputSplitSource<T extends InputSplit>
            implements InputSplitSource<T> {

        private final T[] inputSplits;
        private final InputSplitAssigner assigner;

        private TestingInputSplitSource(T[] inputSplits, InputSplitAssigner assigner) {
            this.inputSplits = inputSplits;
            this.assigner = assigner;
        }

        @Override
        public T[] createInputSplits(int minNumSplits) throws Exception {
            return inputSplits;
        }

        @Override
        public InputSplitAssigner getInputSplitAssigner(T[] inputSplits) {
            return assigner;
        }
    }
}
