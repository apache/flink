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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CompletedCheckpointStatsSummaryTest {

	/**
	 * Tests simple updates of the completed checkpoint stats.
	 */
	@Test
	public void testSimpleUpdates() throws Exception {
		long triggerTimestamp = 123123L;
		long ackTimestamp = 123123 + 1212312399L;
		long stateSize = Integer.MAX_VALUE + 17787L;
		long processedData = Integer.MAX_VALUE + 123123L;
		long persistedData = Integer.MAX_VALUE + 42L;

		CompletedCheckpointStatsSummary summary = new CompletedCheckpointStatsSummary();
		assertEquals(0, summary.getStateSizeStats().getCount());
		assertEquals(0, summary.getEndToEndDurationStats().getCount());
		assertEquals(0, summary.getProcessedDataStats().getCount());
		assertEquals(0, summary.getPersistedDataStats().getCount());

		int numCheckpoints = 10;

		for (int i = 0; i < numCheckpoints; i++) {
			CompletedCheckpointStats completed = createCompletedCheckpoint(
				i,
				triggerTimestamp,
				ackTimestamp + i,
				stateSize + i,
				processedData + i,
				persistedData + i);

			summary.updateSummary(completed);

			assertEquals(i + 1, summary.getStateSizeStats().getCount());
			assertEquals(i + 1, summary.getEndToEndDurationStats().getCount());
			assertEquals(i + 1, summary.getProcessedDataStats().getCount());
			assertEquals(i + 1, summary.getPersistedDataStats().getCount());
		}

		MinMaxAvgStats stateSizeStats = summary.getStateSizeStats();
		assertEquals(stateSize, stateSizeStats.getMinimum());
		assertEquals(stateSize + numCheckpoints - 1, stateSizeStats.getMaximum());

		MinMaxAvgStats durationStats = summary.getEndToEndDurationStats();
		assertEquals(ackTimestamp - triggerTimestamp, durationStats.getMinimum());
		assertEquals(ackTimestamp - triggerTimestamp + numCheckpoints - 1, durationStats.getMaximum());

		MinMaxAvgStats processedDataStats = summary.getProcessedDataStats();
		assertEquals(processedData, processedDataStats.getMinimum());
		assertEquals(processedData + numCheckpoints - 1, processedDataStats.getMaximum());

		MinMaxAvgStats persistedDataStats = summary.getPersistedDataStats();
		assertEquals(persistedData, persistedDataStats.getMinimum());
		assertEquals(persistedData + numCheckpoints - 1, persistedDataStats.getMaximum());
	}

	private CompletedCheckpointStats createCompletedCheckpoint(
		long checkpointId,
		long triggerTimestamp,
		long ackTimestamp,
		long stateSize,
		long processedData,
		long persistedData) {

		SubtaskStateStats latest = mock(SubtaskStateStats.class);
		when(latest.getAckTimestamp()).thenReturn(ackTimestamp);

		Map<JobVertexID, TaskStateStats> taskStats = new HashMap<>();
		JobVertexID jobVertexId = new JobVertexID();
		taskStats.put(jobVertexId, new TaskStateStats(jobVertexId, 1));

		return new CompletedCheckpointStats(
			checkpointId,
			triggerTimestamp,
			CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
			1,
			taskStats,
			1,
			stateSize,
			processedData,
			persistedData,
			latest,
			null);
	}
}
