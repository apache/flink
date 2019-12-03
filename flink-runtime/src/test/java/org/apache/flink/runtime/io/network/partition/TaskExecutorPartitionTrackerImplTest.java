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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.taskexecutor.partition.ClusterPartitionReport;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import org.junit.Test;

import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;

/**
 * Tests for the {@link TaskExecutorPartitionTrackerImpl}.
 */
public class TaskExecutorPartitionTrackerImplTest extends TestLogger {

	@Test
	public void createClusterPartitionReport() {
		final TaskExecutorPartitionTrackerImpl partitionTracker = new TaskExecutorPartitionTrackerImpl(new NettyShuffleEnvironmentBuilder().build());

		assertThat(partitionTracker.createClusterPartitionReport().getEntries(), is(empty()));

		final IntermediateDataSetID dataSetId = new IntermediateDataSetID();
		final JobID jobId = new JobID();
		final ResultPartitionID clusterPartitionId = new ResultPartitionID();
		final ResultPartitionID jobPartitionId = new ResultPartitionID();
		final int numberOfPartitions = 1;

		partitionTracker.startTrackingPartition(jobId, new TaskExecutorPartitionInfo(clusterPartitionId, dataSetId, numberOfPartitions));
		partitionTracker.startTrackingPartition(jobId, new TaskExecutorPartitionInfo(jobPartitionId, dataSetId, numberOfPartitions + 1));

		partitionTracker.promoteJobPartitions(Collections.singleton(clusterPartitionId));

		final ClusterPartitionReport clusterPartitionReport = partitionTracker.createClusterPartitionReport();

		final ClusterPartitionReport.ClusterPartitionReportEntry reportEntry = Iterables.getOnlyElement(clusterPartitionReport.getEntries());
		assertThat(reportEntry.getDataSetId(), is(dataSetId));
		assertThat(reportEntry.getNumTotalPartitions(), is(numberOfPartitions));
		assertThat(reportEntry.getHostedPartitions(), hasItems(clusterPartitionId));
	}
}
