/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.taskexecutor.partition.ClusterPartitionReport;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for the {@link ResourceManagerPartitionTrackerImpl}. */
class ResourceManagerPartitionTrackerImplTest {

    private static final ClusterPartitionReport EMPTY_PARTITION_REPORT =
            new ClusterPartitionReport(Collections.emptySet());

    private static final ResourceID TASK_EXECUTOR_ID_1 = ResourceID.generate();
    private static final ResourceID TASK_EXECUTOR_ID_2 = ResourceID.generate();
    private static final IntermediateDataSetID DATA_SET_ID = new IntermediateDataSetID();
    private static final ResultPartitionID PARTITION_ID_1 = new ResultPartitionID();
    private static final ResultPartitionID PARTITION_ID_2 = new ResultPartitionID();

    @Test
    void testProcessEmptyClusterPartitionReport() {
        TestClusterPartitionReleaser partitionReleaser = new TestClusterPartitionReleaser();
        final ResourceManagerPartitionTrackerImpl tracker =
                new ResourceManagerPartitionTrackerImpl(partitionReleaser);

        reportEmpty(tracker, TASK_EXECUTOR_ID_1);
        assertThat(partitionReleaser.releaseCalls).isEmpty();
        assertThat(tracker.areAllMapsEmpty()).isTrue();
    }

    /**
     * Verifies that a task executor hosting multiple partitions of a data set receives a release
     * call if a subset of its partitions is lost.
     */
    @Test
    void testReportProcessingWithPartitionLossOnSameTaskExecutor() {
        TestClusterPartitionReleaser partitionReleaser = new TestClusterPartitionReleaser();
        final ResourceManagerPartitionTracker tracker =
                new ResourceManagerPartitionTrackerImpl(partitionReleaser);

        report(tracker, TASK_EXECUTOR_ID_1, DATA_SET_ID, 2, PARTITION_ID_1, PARTITION_ID_2);
        report(tracker, TASK_EXECUTOR_ID_1, DATA_SET_ID, 2, PARTITION_ID_2);

        assertThat(partitionReleaser.releaseCalls)
                .contains(Tuple2.of(TASK_EXECUTOR_ID_1, Collections.singleton(DATA_SET_ID)));
    }

    /**
     * Verifies that a task executor hosting partitions of a data set receives a release call if a
     * partition of the data set is lost on another task executor.
     */
    @Test
    void testReportProcessingWithPartitionLossOnOtherTaskExecutor() {
        TestClusterPartitionReleaser partitionReleaser = new TestClusterPartitionReleaser();
        final ResourceManagerPartitionTracker tracker =
                new ResourceManagerPartitionTrackerImpl(partitionReleaser);

        report(tracker, TASK_EXECUTOR_ID_1, DATA_SET_ID, 2, PARTITION_ID_1);
        report(tracker, TASK_EXECUTOR_ID_2, DATA_SET_ID, 2, PARTITION_ID_2);

        reportEmpty(tracker, TASK_EXECUTOR_ID_1);

        assertThat(partitionReleaser.releaseCalls)
                .contains(Tuple2.of(TASK_EXECUTOR_ID_2, Collections.singleton(DATA_SET_ID)));
    }

    @Test
    void testListDataSetsBasics() {
        final ResourceManagerPartitionTrackerImpl tracker =
                new ResourceManagerPartitionTrackerImpl(new TestClusterPartitionReleaser());

        assertThat(tracker.listDataSets()).isEmpty();

        report(tracker, TASK_EXECUTOR_ID_1, DATA_SET_ID, 2, PARTITION_ID_1);
        checkListedDataSets(tracker, 1, 2);

        report(tracker, TASK_EXECUTOR_ID_2, DATA_SET_ID, 2, PARTITION_ID_2);
        checkListedDataSets(tracker, 2, 2);

        reportEmpty(tracker, TASK_EXECUTOR_ID_1);
        checkListedDataSets(tracker, 1, 2);

        reportEmpty(tracker, TASK_EXECUTOR_ID_2);
        assertThat(tracker.listDataSets()).isEmpty();

        assertThat(tracker.areAllMapsEmpty()).isTrue();
    }

    private static void checkListedDataSets(
            ResourceManagerPartitionTracker tracker, int expectedRegistered, int expectedTotal) {
        final Map<IntermediateDataSetID, DataSetMetaInfo> listing = tracker.listDataSets();
        assertThat(listing).containsKey(DATA_SET_ID);
        DataSetMetaInfo metaInfo = listing.get(DATA_SET_ID);
        assertThat(metaInfo.getNumRegisteredPartitions().orElse(-1)).isEqualTo(expectedRegistered);
        assertThat(metaInfo.getNumTotalPartitions()).isEqualTo(expectedTotal);
    }

    @Test
    void testReleasePartition() {
        TestClusterPartitionReleaser partitionReleaser = new TestClusterPartitionReleaser();
        final ResourceManagerPartitionTrackerImpl tracker =
                new ResourceManagerPartitionTrackerImpl(partitionReleaser);

        report(tracker, TASK_EXECUTOR_ID_1, DATA_SET_ID, 2, PARTITION_ID_1);
        report(tracker, TASK_EXECUTOR_ID_2, DATA_SET_ID, 2, PARTITION_ID_2);

        final CompletableFuture<Void> partitionReleaseFuture =
                tracker.releaseClusterPartitions(DATA_SET_ID);

        assertThat(partitionReleaser.releaseCalls)
                .containsExactlyInAnyOrder(
                        Tuple2.of(TASK_EXECUTOR_ID_1, Collections.singleton(DATA_SET_ID)),
                        Tuple2.of(TASK_EXECUTOR_ID_2, Collections.singleton(DATA_SET_ID)));

        // the data set should still be tracked, since the partition release was not confirmed yet
        // by the task executors
        assertThat(tracker.listDataSets().keySet()).contains(DATA_SET_ID);

        // ack the partition release
        reportEmpty(tracker, TASK_EXECUTOR_ID_1, TASK_EXECUTOR_ID_2);

        assertThat(partitionReleaseFuture).isDone();
        assertThat(tracker.areAllMapsEmpty()).isTrue();
    }

    @Test
    void testShutdownProcessing() {
        TestClusterPartitionReleaser partitionReleaser = new TestClusterPartitionReleaser();
        final ResourceManagerPartitionTrackerImpl tracker =
                new ResourceManagerPartitionTrackerImpl(partitionReleaser);

        tracker.processTaskExecutorShutdown(TASK_EXECUTOR_ID_1);
        assertThat(partitionReleaser.releaseCalls).isEmpty();

        report(tracker, TASK_EXECUTOR_ID_1, DATA_SET_ID, 3, PARTITION_ID_1, PARTITION_ID_2);
        report(tracker, TASK_EXECUTOR_ID_2, DATA_SET_ID, 3, new ResultPartitionID());

        tracker.processTaskExecutorShutdown(TASK_EXECUTOR_ID_1);

        assertThat(partitionReleaser.releaseCalls)
                .contains(Tuple2.of(TASK_EXECUTOR_ID_2, Collections.singleton(DATA_SET_ID)));

        assertThat(tracker.areAllMapsEmpty()).isFalse();

        tracker.processTaskExecutorShutdown(TASK_EXECUTOR_ID_2);

        assertThat(tracker.areAllMapsEmpty()).isTrue();
    }

    @Test
    void testGetClusterPartitionShuffleDescriptors() {
        final ResourceManagerPartitionTrackerImpl tracker =
                new ResourceManagerPartitionTrackerImpl(new TestClusterPartitionReleaser());

        assertThat(tracker.listDataSets()).isEmpty();

        List<ResultPartitionID> resultPartitionIDS = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            resultPartitionIDS.add(
                    new ResultPartitionID(
                            new IntermediateResultPartitionID(DATA_SET_ID, i),
                            ExecutionAttemptID.randomId()));
        }

        for (ResultPartitionID resultPartitionID : resultPartitionIDS) {
            report(tracker, TASK_EXECUTOR_ID_1, DATA_SET_ID, 100, resultPartitionID);
        }

        final List<ShuffleDescriptor> shuffleDescriptors =
                tracker.getClusterPartitionShuffleDescriptors(DATA_SET_ID);
        assertThat(shuffleDescriptors).hasSize(100);
        assertThat(
                        shuffleDescriptors.stream()
                                .map(ShuffleDescriptor::getResultPartitionID)
                                .collect(Collectors.toList()))
                .containsExactlyElementsOf(resultPartitionIDS);

        reportEmpty(tracker, TASK_EXECUTOR_ID_1);
        reportEmpty(tracker, TASK_EXECUTOR_ID_2);
        assertThat(tracker.areAllMapsEmpty()).isTrue();
    }

    private static void reportEmpty(
            ResourceManagerPartitionTracker tracker, ResourceID... taskExecutorIds) {
        for (ResourceID taskExecutorId : taskExecutorIds) {
            tracker.processTaskExecutorClusterPartitionReport(
                    taskExecutorId, EMPTY_PARTITION_REPORT);
        }
    }

    private static void report(
            ResourceManagerPartitionTracker tracker,
            ResourceID taskExecutorId,
            IntermediateDataSetID dataSetId,
            int numTotalPartitions,
            ResultPartitionID... partitionIds) {
        tracker.processTaskExecutorClusterPartitionReport(
                taskExecutorId,
                createClusterPartitionReport(dataSetId, numTotalPartitions, partitionIds));
    }

    private static ClusterPartitionReport createClusterPartitionReport(
            IntermediateDataSetID dataSetId,
            int numTotalPartitions,
            ResultPartitionID... partitionId) {
        final Map<ResultPartitionID, ShuffleDescriptor> shuffleDescriptors =
                Arrays.stream(partitionId)
                        .map(TestShuffleDescriptor::new)
                        .collect(
                                Collectors.toMap(
                                        TestShuffleDescriptor::getResultPartitionID, d -> d));
        return new ClusterPartitionReport(
                Collections.singletonList(
                        new ClusterPartitionReport.ClusterPartitionReportEntry(
                                dataSetId, numTotalPartitions, shuffleDescriptors)));
    }

    private static class TestShuffleDescriptor implements ShuffleDescriptor {
        private final ResultPartitionID resultPartitionID;

        TestShuffleDescriptor(ResultPartitionID resultPartitionID) {
            this.resultPartitionID = resultPartitionID;
        }

        @Override
        public ResultPartitionID getResultPartitionID() {
            return resultPartitionID;
        }

        @Override
        public Optional<ResourceID> storesLocalResourcesOn() {
            return Optional.empty();
        }
    }

    private static class TestClusterPartitionReleaser
            implements TaskExecutorClusterPartitionReleaser {

        final List<Tuple2<ResourceID, Set<IntermediateDataSetID>>> releaseCalls = new ArrayList<>();

        @Override
        public void releaseClusterPartitions(
                ResourceID taskExecutorId, Set<IntermediateDataSetID> dataSetsToRelease) {
            releaseCalls.add(Tuple2.of(taskExecutorId, dataSetsToRelease));
        }
    }
}
