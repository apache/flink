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

package org.apache.flink.runtime.taskexecutor.partition;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Collection;
import java.util.Collections;

import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertFalse;

/**
 * Tests for the {@link PartitionTable}.
 */
public class PartitionTableTest extends TestLogger {

	private static final JobID JOB_ID = new JobID();
	private static final ResultPartitionID PARTITION_ID = new ResultPartitionID();

	@Test
	public void testEmptyTable() {
		final PartitionTable<JobID> table = new PartitionTable<>();

		// an empty table should always return an empty collection
		Collection<ResultPartitionID> partitionsForNonExistingJob = table.stopTrackingPartitions(JOB_ID);
		assertNotNull(partitionsForNonExistingJob);
		assertThat(partitionsForNonExistingJob, empty());

		assertFalse(table.hasTrackedPartitions(JOB_ID));
	}

	@Test
	public void testStartTrackingPartition() {
		final PartitionTable<JobID> table = new PartitionTable<>();

		table.startTrackingPartitions(JOB_ID, Collections.singletonList(PARTITION_ID));

		assertTrue(table.hasTrackedPartitions(JOB_ID));
	}

	@Test
	public void testStartTrackingZeroPartitionDoesNotMutateState() {
		final PartitionTable<JobID> table = new PartitionTable<>();

		table.startTrackingPartitions(JOB_ID, Collections.emptyList());

		assertFalse(table.hasTrackedPartitions(JOB_ID));
	}

	@Test
	public void testStopTrackingAllPartitions() {
		final PartitionTable<JobID> table = new PartitionTable<>();

		table.startTrackingPartitions(JOB_ID, Collections.singletonList(PARTITION_ID));

		Collection<ResultPartitionID> storedPartitions = table.stopTrackingPartitions(JOB_ID);
		assertThat(storedPartitions, contains(PARTITION_ID));
		assertFalse(table.hasTrackedPartitions(JOB_ID));
	}

	@Test
	public void testStopTrackingPartitions() {
		final ResultPartitionID partitionId2 = new ResultPartitionID();
		final PartitionTable<JobID> table = new PartitionTable<>();

		table.startTrackingPartitions(JOB_ID, Collections.singletonList(PARTITION_ID));
		table.startTrackingPartitions(JOB_ID, Collections.singletonList(partitionId2));

		table.stopTrackingPartitions(JOB_ID, Collections.singletonList(partitionId2));
		assertTrue(table.hasTrackedPartitions(JOB_ID));

		Collection<ResultPartitionID> storedPartitions = table.stopTrackingPartitions(JOB_ID);
		assertThat(storedPartitions, contains(PARTITION_ID));
		assertFalse(table.hasTrackedPartitions(JOB_ID));
	}
}
