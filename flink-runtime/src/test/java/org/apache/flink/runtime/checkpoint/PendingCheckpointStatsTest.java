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
import org.mockito.ArgumentCaptor;

import java.util.HashMap;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class PendingCheckpointStatsTest {

	/**
	 * Tests reporting of subtask stats.
	 */
	@Test
	public void testReportSubtaskStats() throws Exception {
		long checkpointId = Integer.MAX_VALUE + 1222L;
		long triggerTimestamp = Integer.MAX_VALUE - 1239L;
		CheckpointProperties props = CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION);
		TaskStateStats task1 = new TaskStateStats(new JobVertexID(), 3);
		TaskStateStats task2 = new TaskStateStats(new JobVertexID(), 4);
		int totalSubtaskCount = task1.getNumberOfSubtasks() + task2.getNumberOfSubtasks();

		HashMap<JobVertexID, TaskStateStats> taskStats = new HashMap<>();
		taskStats.put(task1.getJobVertexId(), task1);
		taskStats.put(task2.getJobVertexId(), task2);

		CheckpointStatsTracker.PendingCheckpointStatsCallback callback = mock(
			CheckpointStatsTracker.PendingCheckpointStatsCallback.class);

		PendingCheckpointStats pending = new PendingCheckpointStats(
				checkpointId,
				triggerTimestamp,
				props,
				totalSubtaskCount,
				taskStats,
				callback);

		// Check initial state
		assertEquals(checkpointId, pending.getCheckpointId());
		assertEquals(triggerTimestamp, pending.getTriggerTimestamp());
		assertEquals(props, pending.getProperties());
		assertEquals(CheckpointStatsStatus.IN_PROGRESS, pending.getStatus());
		assertEquals(0, pending.getNumberOfAcknowledgedSubtasks());
		assertEquals(0, pending.getStateSize());
		assertEquals(totalSubtaskCount, pending.getNumberOfSubtasks());
		assertNull(pending.getLatestAcknowledgedSubtaskStats());
		assertEquals(-1, pending.getLatestAckTimestamp());
		assertEquals(-1, pending.getEndToEndDuration());
		assertEquals(task1, pending.getTaskStateStats(task1.getJobVertexId()));
		assertEquals(task2, pending.getTaskStateStats(task2.getJobVertexId()));
		assertNull(pending.getTaskStateStats(new JobVertexID()));

		// Report subtasks and check getters
		assertFalse(pending.reportSubtaskStats(new JobVertexID(), createSubtaskStats(0)));

		long stateSize = 0;

		// Report 1st task
		for (int i = 0; i < task1.getNumberOfSubtasks(); i++) {
			SubtaskStateStats subtask = createSubtaskStats(i);
			stateSize += subtask.getStateSize();

			pending.reportSubtaskStats(task1.getJobVertexId(), subtask);

			assertEquals(subtask, pending.getLatestAcknowledgedSubtaskStats());
			assertEquals(subtask.getAckTimestamp(), pending.getLatestAckTimestamp());
			assertEquals(subtask.getAckTimestamp() - triggerTimestamp, pending.getEndToEndDuration());
			assertEquals(stateSize, pending.getStateSize());
		}

		// Don't allow overwrite
		assertFalse(pending.reportSubtaskStats(task1.getJobVertexId(), task1.getSubtaskStats()[0]));

		// Report 2nd task
		for (int i = 0; i < task2.getNumberOfSubtasks(); i++) {
			SubtaskStateStats subtask = createSubtaskStats(i);
			stateSize += subtask.getStateSize();

			pending.reportSubtaskStats(task2.getJobVertexId(), subtask);

			assertEquals(subtask, pending.getLatestAcknowledgedSubtaskStats());
			assertEquals(subtask.getAckTimestamp(), pending.getLatestAckTimestamp());
			assertEquals(subtask.getAckTimestamp() - triggerTimestamp, pending.getEndToEndDuration());
			assertEquals(stateSize, pending.getStateSize());
		}

		assertEquals(task1.getNumberOfSubtasks(), task1.getNumberOfAcknowledgedSubtasks());
		assertEquals(task2.getNumberOfSubtasks(), task2.getNumberOfAcknowledgedSubtasks());
	}

	/**
	 * Test reporting of a completed checkpoint.
	 */
	@Test
	public void testReportCompletedCheckpoint() throws Exception {
		TaskStateStats task1 = new TaskStateStats(new JobVertexID(), 3);
		TaskStateStats task2 = new TaskStateStats(new JobVertexID(), 4);

		HashMap<JobVertexID, TaskStateStats> taskStats = new HashMap<>();
		taskStats.put(task1.getJobVertexId(), task1);
		taskStats.put(task2.getJobVertexId(), task2);

		CheckpointStatsTracker.PendingCheckpointStatsCallback callback = mock(
			CheckpointStatsTracker.PendingCheckpointStatsCallback.class);

		PendingCheckpointStats pending = new PendingCheckpointStats(
			0,
			1,
			CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
			task1.getNumberOfSubtasks() + task2.getNumberOfSubtasks(),
			taskStats,
			callback);

		// Report subtasks
		for (int i = 0; i < task1.getNumberOfSubtasks(); i++) {
			pending.reportSubtaskStats(task1.getJobVertexId(), createSubtaskStats(i));
		}

		for (int i = 0; i < task2.getNumberOfSubtasks(); i++) {
			pending.reportSubtaskStats(task2.getJobVertexId(), createSubtaskStats(i));
		}

		// Report completed
		String externalPath = "asdjkasdjkasd";

		CompletedCheckpointStats.DiscardCallback discardCallback = pending.reportCompletedCheckpoint(externalPath);

		ArgumentCaptor<CompletedCheckpointStats> args = ArgumentCaptor.forClass(CompletedCheckpointStats.class);
		verify(callback).reportCompletedCheckpoint(args.capture());

		CompletedCheckpointStats completed = args.getValue();

		assertNotNull(completed);
		assertEquals(CheckpointStatsStatus.COMPLETED, completed.getStatus());
		assertFalse(completed.isDiscarded());
		discardCallback.notifyDiscardedCheckpoint();
		assertTrue(completed.isDiscarded());
		assertEquals(externalPath, completed.getExternalPath());

		assertEquals(pending.getCheckpointId(), completed.getCheckpointId());
		assertEquals(pending.getNumberOfAcknowledgedSubtasks(), completed.getNumberOfAcknowledgedSubtasks());
		assertEquals(pending.getLatestAcknowledgedSubtaskStats(), completed.getLatestAcknowledgedSubtaskStats());
		assertEquals(pending.getLatestAckTimestamp(), completed.getLatestAckTimestamp());
		assertEquals(pending.getEndToEndDuration(), completed.getEndToEndDuration());
		assertEquals(pending.getStateSize(), completed.getStateSize());
		assertEquals(task1, completed.getTaskStateStats(task1.getJobVertexId()));
		assertEquals(task2, completed.getTaskStateStats(task2.getJobVertexId()));
	}

	/**
	 * Test reporting of a failed checkpoint.
	 */
	@Test
	public void testReportFailedCheckpoint() throws Exception {
		TaskStateStats task1 = new TaskStateStats(new JobVertexID(), 3);
		TaskStateStats task2 = new TaskStateStats(new JobVertexID(), 4);

		HashMap<JobVertexID, TaskStateStats> taskStats = new HashMap<>();
		taskStats.put(task1.getJobVertexId(), task1);
		taskStats.put(task2.getJobVertexId(), task2);

		CheckpointStatsTracker.PendingCheckpointStatsCallback callback = mock(
			CheckpointStatsTracker.PendingCheckpointStatsCallback.class);

		long triggerTimestamp = 123123;
		PendingCheckpointStats pending = new PendingCheckpointStats(
			0,
			triggerTimestamp,
			CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
			task1.getNumberOfSubtasks() + task2.getNumberOfSubtasks(),
			taskStats,
			callback);

		// Report subtasks
		for (int i = 0; i < task1.getNumberOfSubtasks(); i++) {
			pending.reportSubtaskStats(task1.getJobVertexId(), createSubtaskStats(i));
		}

		for (int i = 0; i < task2.getNumberOfSubtasks(); i++) {
			pending.reportSubtaskStats(task2.getJobVertexId(), createSubtaskStats(i));
		}

		// Report failed
		Exception cause = new Exception("test exception");
		long failureTimestamp = 112211137;
		pending.reportFailedCheckpoint(failureTimestamp, cause);

		ArgumentCaptor<FailedCheckpointStats> args = ArgumentCaptor.forClass(FailedCheckpointStats.class);
		verify(callback).reportFailedCheckpoint(args.capture());

		FailedCheckpointStats failed = args.getValue();

		assertNotNull(failed);
		assertEquals(CheckpointStatsStatus.FAILED, failed.getStatus());
		assertEquals(failureTimestamp, failed.getFailureTimestamp());
		assertEquals(cause.getMessage(), failed.getFailureMessage());

		assertEquals(pending.getCheckpointId(), failed.getCheckpointId());
		assertEquals(pending.getNumberOfAcknowledgedSubtasks(), failed.getNumberOfAcknowledgedSubtasks());
		assertEquals(pending.getLatestAcknowledgedSubtaskStats(), failed.getLatestAcknowledgedSubtaskStats());
		assertEquals(pending.getLatestAckTimestamp(), failed.getLatestAckTimestamp());
		assertEquals(failureTimestamp - triggerTimestamp, failed.getEndToEndDuration());
		assertEquals(pending.getStateSize(), failed.getStateSize());
		assertEquals(task1, failed.getTaskStateStats(task1.getJobVertexId()));
		assertEquals(task2, failed.getTaskStateStats(task2.getJobVertexId()));
	}

	@Test
	public void testIsJavaSerializable() throws Exception {
		TaskStateStats task1 = new TaskStateStats(new JobVertexID(), 3);
		TaskStateStats task2 = new TaskStateStats(new JobVertexID(), 4);

		HashMap<JobVertexID, TaskStateStats> taskStats = new HashMap<>();
		taskStats.put(task1.getJobVertexId(), task1);
		taskStats.put(task2.getJobVertexId(), task2);

		PendingCheckpointStats pending = new PendingCheckpointStats(
			123123123L,
			10123L,
			CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
			1337,
			taskStats,
			mock(CheckpointStatsTracker.PendingCheckpointStatsCallback.class));

		PendingCheckpointStats copy = CommonTestUtils.createCopySerializable(pending);

		assertEquals(pending.getCheckpointId(), copy.getCheckpointId());
		assertEquals(pending.getTriggerTimestamp(), copy.getTriggerTimestamp());
		assertEquals(pending.getProperties(), copy.getProperties());
		assertEquals(pending.getNumberOfSubtasks(), copy.getNumberOfSubtasks());
		assertEquals(pending.getNumberOfAcknowledgedSubtasks(), copy.getNumberOfAcknowledgedSubtasks());
		assertEquals(pending.getEndToEndDuration(), copy.getEndToEndDuration());
		assertEquals(pending.getStateSize(), copy.getStateSize());
		assertEquals(pending.getLatestAcknowledgedSubtaskStats(), copy.getLatestAcknowledgedSubtaskStats());
		assertEquals(pending.getStatus(), copy.getStatus());
	}

	// ------------------------------------------------------------------------

	private SubtaskStateStats createSubtaskStats(int index) {
		return new SubtaskStateStats(
			index,
			Integer.MAX_VALUE + (long) index,
			Integer.MAX_VALUE + (long) index,
			Integer.MAX_VALUE + (long) index,
			Integer.MAX_VALUE + (long) index,
			Integer.MAX_VALUE + (long) index,
			Integer.MAX_VALUE + (long) index);
	}
}
