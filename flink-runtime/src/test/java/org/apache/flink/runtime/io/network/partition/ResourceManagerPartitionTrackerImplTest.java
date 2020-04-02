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
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.taskexecutor.partition.ClusterPartitionReport;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;

/**
 * Test for the {@link ResourceManagerPartitionTrackerImpl}.
 */
public class ResourceManagerPartitionTrackerImplTest extends TestLogger {

	private static final ClusterPartitionReport EMPTY_PARTITION_REPORT = new ClusterPartitionReport(Collections.emptySet());

	private static final ResourceID TASK_EXECUTOR_ID_1 = ResourceID.generate();
	private static final ResourceID TASK_EXECUTOR_ID_2 = ResourceID.generate();
	private static final IntermediateDataSetID DATA_SET_ID = new IntermediateDataSetID();
	private static final ResultPartitionID PARTITION_ID_1 = new ResultPartitionID();
	private static final ResultPartitionID PARTITION_ID_2 = new ResultPartitionID();

	@Test
	public void testProcessEmptyClusterPartitionReport() {
		TestClusterPartitionReleaser partitionReleaser = new TestClusterPartitionReleaser();
		final ResourceManagerPartitionTrackerImpl tracker = new ResourceManagerPartitionTrackerImpl(partitionReleaser);

		reportEmpty(tracker, TASK_EXECUTOR_ID_1);
		assertThat(partitionReleaser.releaseCalls, empty());
		assertThat(tracker.areAllMapsEmpty(), is(true));
	}

	/**
	 * Verifies that a task executor hosting multiple partitions of a data set receives a release call if a subset of
	 * its partitions is lost.
	 */
	@Test
	public void testReportProcessingWithPartitionLossOnSameTaskExecutor() {
		TestClusterPartitionReleaser partitionReleaser = new TestClusterPartitionReleaser();
		final ResourceManagerPartitionTracker tracker = new ResourceManagerPartitionTrackerImpl(partitionReleaser);

		report(tracker, TASK_EXECUTOR_ID_1, DATA_SET_ID, 2, PARTITION_ID_1, PARTITION_ID_2);
		report(tracker, TASK_EXECUTOR_ID_1, DATA_SET_ID, 2, PARTITION_ID_2);

		assertThat(partitionReleaser.releaseCalls, contains(Tuple2.of(TASK_EXECUTOR_ID_1, Collections.singleton(DATA_SET_ID))));
	}

	/**
	 * Verifies that a task executor hosting partitions of a data set receives a release call if a partition of the
	 * data set is lost on another task executor.
	 */
	@Test
	public void testReportProcessingWithPartitionLossOnOtherTaskExecutor() {
		TestClusterPartitionReleaser partitionReleaser = new TestClusterPartitionReleaser();
		final ResourceManagerPartitionTracker tracker = new ResourceManagerPartitionTrackerImpl(partitionReleaser);

		report(tracker, TASK_EXECUTOR_ID_1, DATA_SET_ID, 2, PARTITION_ID_1);
		report(tracker, TASK_EXECUTOR_ID_2, DATA_SET_ID, 2, PARTITION_ID_2);

		reportEmpty(tracker, TASK_EXECUTOR_ID_1);

		assertThat(partitionReleaser.releaseCalls, contains(Tuple2.of(TASK_EXECUTOR_ID_2, Collections.singleton(DATA_SET_ID))));
	}

	@Test
	public void testListDataSetsBasics() {
		final ResourceManagerPartitionTrackerImpl tracker = new ResourceManagerPartitionTrackerImpl(new TestClusterPartitionReleaser());

		assertThat(tracker.listDataSets().size(), is(0));

		report(tracker, TASK_EXECUTOR_ID_1, DATA_SET_ID, 2, PARTITION_ID_1);
		checkListedDataSets(tracker, 1, 2);

		report(tracker, TASK_EXECUTOR_ID_2, DATA_SET_ID, 2, PARTITION_ID_2);
		checkListedDataSets(tracker, 2, 2);

		reportEmpty(tracker, TASK_EXECUTOR_ID_1);
		checkListedDataSets(tracker, 1, 2);

		reportEmpty(tracker, TASK_EXECUTOR_ID_2);
		assertThat(tracker.listDataSets().size(), is(0));

		assertThat(tracker.areAllMapsEmpty(), is(true));
	}

	private static void checkListedDataSets(ResourceManagerPartitionTracker tracker, int expectedRegistered, int expectedTotal) {
		final Map<IntermediateDataSetID, DataSetMetaInfo> listing = tracker.listDataSets();
		assertThat(listing, hasKey(DATA_SET_ID));
		DataSetMetaInfo metaInfo = listing.get(DATA_SET_ID);
		assertThat(metaInfo.getNumRegisteredPartitions().orElse(-1), is(expectedRegistered));
		assertThat(metaInfo.getNumTotalPartitions(), is(expectedTotal));
	}

	@Test
	public void testReleasePartition() {
		TestClusterPartitionReleaser partitionReleaser = new TestClusterPartitionReleaser();
		final ResourceManagerPartitionTrackerImpl tracker = new ResourceManagerPartitionTrackerImpl(partitionReleaser);

		report(tracker, TASK_EXECUTOR_ID_1, DATA_SET_ID, 2, PARTITION_ID_1);
		report(tracker, TASK_EXECUTOR_ID_2, DATA_SET_ID, 2, PARTITION_ID_2);

		final CompletableFuture<Void> partitionReleaseFuture = tracker.releaseClusterPartitions(DATA_SET_ID);

		assertThat(partitionReleaser.releaseCalls, containsInAnyOrder(
			Tuple2.of(TASK_EXECUTOR_ID_1, Collections.singleton(DATA_SET_ID)),
			Tuple2.of(TASK_EXECUTOR_ID_2, Collections.singleton(DATA_SET_ID))));

		// the data set should still be tracked, since the partition release was not confirmed yet by the task executors
		assertThat(tracker.listDataSets().keySet(), contains(DATA_SET_ID));

		// ack the partition release
		reportEmpty(tracker, TASK_EXECUTOR_ID_1, TASK_EXECUTOR_ID_2);

		assertThat(partitionReleaseFuture.isDone(), is(true));
		assertThat(tracker.areAllMapsEmpty(), is(true));
	}

	@Test
	public void testShutdownProcessing() {
		TestClusterPartitionReleaser partitionReleaser = new TestClusterPartitionReleaser();
		final ResourceManagerPartitionTrackerImpl tracker = new ResourceManagerPartitionTrackerImpl(partitionReleaser);

		tracker.processTaskExecutorShutdown(TASK_EXECUTOR_ID_1);
		assertThat(partitionReleaser.releaseCalls, empty());

		report(tracker, TASK_EXECUTOR_ID_1, DATA_SET_ID, 3, PARTITION_ID_1, PARTITION_ID_2);
		report(tracker, TASK_EXECUTOR_ID_2, DATA_SET_ID, 3, new ResultPartitionID());

		tracker.processTaskExecutorShutdown(TASK_EXECUTOR_ID_1);

		assertThat(partitionReleaser.releaseCalls, contains(Tuple2.of(TASK_EXECUTOR_ID_2, Collections.singleton(DATA_SET_ID))));

		assertThat(tracker.areAllMapsEmpty(), is(false));

		tracker.processTaskExecutorShutdown(TASK_EXECUTOR_ID_2);

		assertThat(tracker.areAllMapsEmpty(), is(true));
	}

	private static void reportEmpty(ResourceManagerPartitionTracker tracker, ResourceID... taskExecutorIds) {
		for (ResourceID taskExecutorId : taskExecutorIds) {
			tracker.processTaskExecutorClusterPartitionReport(
				taskExecutorId,
				EMPTY_PARTITION_REPORT);
		}
	}

	private static void report(ResourceManagerPartitionTracker tracker, ResourceID taskExecutorId, IntermediateDataSetID dataSetId, int numTotalPartitions, ResultPartitionID... partitionIds) {
		tracker.processTaskExecutorClusterPartitionReport(
			taskExecutorId,
			createClusterPartitionReport(dataSetId, numTotalPartitions, partitionIds));
	}

	private static ClusterPartitionReport createClusterPartitionReport(IntermediateDataSetID dataSetId, int numTotalPartitions, ResultPartitionID... partitionId) {
		return new ClusterPartitionReport(Collections.singletonList(
			new ClusterPartitionReport.ClusterPartitionReportEntry(
				dataSetId,
				new HashSet<>(Arrays.asList(partitionId)),
				numTotalPartitions)));
	}

	private static class TestClusterPartitionReleaser implements TaskExecutorClusterPartitionReleaser {

		final List<Tuple2<ResourceID, Set<IntermediateDataSetID>>> releaseCalls = new ArrayList<>();

		@Override
		public void releaseClusterPartitions(ResourceID taskExecutorId, Set<IntermediateDataSetID> dataSetsToRelease) {
			releaseCalls.add(Tuple2.of(taskExecutorId, dataSetsToRelease));
		}
	}
}
