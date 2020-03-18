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

import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TaskStateStatsTest {

	private final ThreadLocalRandom rand = ThreadLocalRandom.current();

	/**
	 * Tests that subtask stats are correctly collected.
	 */
	@Test
	public void testHandInSubtasks() throws Exception {
		JobVertexID jobVertexId = new JobVertexID();
		SubtaskStateStats[] subtasks = new SubtaskStateStats[7];

		TaskStateStats taskStats = new TaskStateStats(jobVertexId, subtasks.length);

		assertEquals(jobVertexId, taskStats.getJobVertexId());
		assertEquals(subtasks.length, taskStats.getNumberOfSubtasks());
		assertEquals(0, taskStats.getNumberOfAcknowledgedSubtasks());
		assertNull(taskStats.getLatestAcknowledgedSubtaskStats());
		assertEquals(-1, taskStats.getLatestAckTimestamp());
		assertArrayEquals(subtasks, taskStats.getSubtaskStats());

		long stateSize = 0;

		// Hand in some subtasks
		for (int i = 0; i < subtasks.length; i++) {
			subtasks[i] = new SubtaskStateStats(
				i,
				rand.nextInt(128),
				rand.nextInt(128),
				rand.nextInt(128),
				rand.nextInt(128),
				rand.nextInt(128),
				rand.nextInt(128));

			stateSize += subtasks[i].getStateSize();

			assertTrue(taskStats.reportSubtaskStats(subtasks[i]));
			assertEquals(i + 1, taskStats.getNumberOfAcknowledgedSubtasks());
			assertEquals(subtasks[i], taskStats.getLatestAcknowledgedSubtaskStats());
			assertEquals(subtasks[i].getAckTimestamp(), taskStats.getLatestAckTimestamp());
			int duration = rand.nextInt(128);
			assertEquals(duration, taskStats.getEndToEndDuration(subtasks[i].getAckTimestamp() - duration));
			assertEquals(stateSize, taskStats.getStateSize());
		}

		assertFalse(taskStats.reportSubtaskStats(new SubtaskStateStats(0, 0, 0, 0, 0, 0, 0)));

		// Test that all subtasks are taken into the account for the summary.
		// The correctness of the actual results is checked in the test of the
		// MinMaxAvgStats.
		TaskStateStats.TaskStateStatsSummary summary = taskStats.getSummaryStats();
		assertEquals(subtasks.length, summary.getStateSizeStats().getCount());
		assertEquals(subtasks.length, summary.getAckTimestampStats().getCount());
		assertEquals(subtasks.length, summary.getSyncCheckpointDurationStats().getCount());
		assertEquals(subtasks.length, summary.getAsyncCheckpointDurationStats().getCount());
		assertEquals(subtasks.length, summary.getAlignmentDurationStats().getCount());
	}

	@Test
	public void testIsJavaSerializable() throws Exception {
		JobVertexID jobVertexId = new JobVertexID();
		SubtaskStateStats[] subtasks = new SubtaskStateStats[7];

		TaskStateStats taskStats = new TaskStateStats(jobVertexId, subtasks.length);

		long stateSize = 0;

		for (int i = 0; i < subtasks.length; i++) {
			subtasks[i] = new SubtaskStateStats(
				i,
				rand.nextInt(128),
				rand.nextInt(128),
				rand.nextInt(128),
				rand.nextInt(128),
				rand.nextInt(128),
				rand.nextInt(128));

			stateSize += subtasks[i].getStateSize();

			taskStats.reportSubtaskStats(subtasks[i]);
		}

		TaskStateStats copy = CommonTestUtils.createCopySerializable(taskStats);

		assertEquals(stateSize, copy.getStateSize());

		TaskStateStats.TaskStateStatsSummary summary = copy.getSummaryStats();
		assertEquals(subtasks.length, summary.getStateSizeStats().getCount());
		assertEquals(subtasks.length, summary.getAckTimestampStats().getCount());
		assertEquals(subtasks.length, summary.getSyncCheckpointDurationStats().getCount());
		assertEquals(subtasks.length, summary.getAsyncCheckpointDurationStats().getCount());
		assertEquals(subtasks.length, summary.getAlignmentDurationStats().getCount());
	}

}
