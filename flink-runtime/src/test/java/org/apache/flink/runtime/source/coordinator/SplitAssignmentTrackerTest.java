/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.flink.runtime.source.coordinator;

import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSourceSplitSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.source.coordinator.CoordinatorTestUtils.getSplitsAssignment;
import static org.apache.flink.runtime.source.coordinator.CoordinatorTestUtils.verifyAssignment;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for @link {@link SplitAssignmentTracker}.
 */
public class SplitAssignmentTrackerTest {

	@Test
	public void testRecordIncrementalSplitAssignment() {
		SplitAssignmentTracker<MockSourceSplit> tracker = new SplitAssignmentTracker<>();
		tracker.recordSplitAssignment(getSplitsAssignment(3, 0));
		tracker.recordSplitAssignment(getSplitsAssignment(2, 6));

		verifyAssignment(Arrays.asList("0", "6"), tracker.uncheckpointedAssignments().get(0));
		verifyAssignment(Arrays.asList("1", "2", "7", "8"), tracker.uncheckpointedAssignments().get(1));
		verifyAssignment(Arrays.asList("3", "4", "5"), tracker.uncheckpointedAssignments().get(2));
	}

	@Test
	public void testTakeSnapshot() throws Exception {
		final long checkpointId = 123L;
		SplitAssignmentTracker<MockSourceSplit> tracker = new SplitAssignmentTracker<>();
		tracker.recordSplitAssignment(getSplitsAssignment(3, 0));

		// Serialize
		takeSnapshot(tracker, checkpointId);

		// Verify the uncheckpointed assignments.
		assertTrue(tracker.uncheckpointedAssignments().isEmpty());

		// verify assignments put into the checkpoints.
		Map<Long, Map<Integer, LinkedHashSet<MockSourceSplit>>> assignmentsByCheckpoints =
				tracker.assignmentsByCheckpointId();
		assertEquals(1, assignmentsByCheckpoints.size());

		Map<Integer, LinkedHashSet<MockSourceSplit>> assignmentForCheckpoint = assignmentsByCheckpoints.get(checkpointId);
		assertNotNull(assignmentForCheckpoint);

		verifyAssignment(Arrays.asList("0"), assignmentForCheckpoint.get(0));
		verifyAssignment(Arrays.asList("1", "2"), assignmentForCheckpoint.get(1));
		verifyAssignment(Arrays.asList("3", "4", "5"), assignmentForCheckpoint.get(2));
	}

	@Test
	public void testRestore() throws Exception {
		final long checkpointId = 123L;
		SplitAssignmentTracker<MockSourceSplit> tracker = new SplitAssignmentTracker<>();
		tracker.recordSplitAssignment(getSplitsAssignment(1, 0));

		// Serialize
		byte[] bytes = takeSnapshot(tracker, checkpointId);

		// Deserialize
		SplitAssignmentTracker<MockSourceSplit> deserializedTracker = restoreSnapshot(bytes);
		// Verify the restore was successful.
		assertEquals(deserializedTracker.assignmentsByCheckpointId(), tracker.assignmentsByCheckpointId());
		assertEquals(deserializedTracker.uncheckpointedAssignments(), tracker.uncheckpointedAssignments());
	}

	@Test
	public void testOnCheckpointComplete() throws Exception {
		final long checkpointId1 = 100L;
		final long checkpointId2 = 101L;
		SplitAssignmentTracker<MockSourceSplit> tracker = new SplitAssignmentTracker<>();

		// Assign some splits to subtask 0 and 1.
		tracker.recordSplitAssignment(getSplitsAssignment(2, 0));

		// Take the first snapshot.
		takeSnapshot(tracker, checkpointId1);
		verifyAssignment(Arrays.asList("0"), tracker.assignmentsByCheckpointId(checkpointId1).get(0));
		verifyAssignment(Arrays.asList("1", "2"), tracker.assignmentsByCheckpointId(checkpointId1).get(1));

		// Assign additional splits to subtask 0 and 1.
		tracker.recordSplitAssignment(getSplitsAssignment(2, 3));

		// Take the second snapshot.
		takeSnapshot(tracker, checkpointId2);
		verifyAssignment(Arrays.asList("0"), tracker.assignmentsByCheckpointId(checkpointId1).get(0));
		verifyAssignment(Arrays.asList("1", "2"), tracker.assignmentsByCheckpointId(checkpointId1).get(1));
		verifyAssignment(Arrays.asList("3"), tracker.assignmentsByCheckpointId(checkpointId2).get(0));
		verifyAssignment(Arrays.asList("4", "5"), tracker.assignmentsByCheckpointId(checkpointId2).get(1));

		// Complete the first checkpoint.
		tracker.onCheckpointComplete(checkpointId1);
		assertNull(tracker.assignmentsByCheckpointId(checkpointId1));
		verifyAssignment(Arrays.asList("3"), tracker.assignmentsByCheckpointId(checkpointId2).get(0));
		verifyAssignment(Arrays.asList("4", "5"), tracker.assignmentsByCheckpointId(checkpointId2).get(1));
	}

	@Test
	public void testGetAndRemoveUncheckpointedAssignment() throws Exception {
		final long checkpointId1 = 100L;
		final long checkpointId2 = 101L;
		SplitAssignmentTracker<MockSourceSplit> tracker = new SplitAssignmentTracker<>();

		// Assign some splits and take snapshot 1.
		tracker.recordSplitAssignment(getSplitsAssignment(2, 0));
		takeSnapshot(tracker, checkpointId1);

		// Assign some more splits and take snapshot 2.
		tracker.recordSplitAssignment(getSplitsAssignment(2, 3));
		takeSnapshot(tracker, checkpointId2);

		// Now assume subtask 0 has failed.
		List<MockSourceSplit> splitsToPutBack = tracker.getAndRemoveUncheckpointedAssignment(0, checkpointId1 - 1);
		verifyAssignment(Arrays.asList("0", "3"), splitsToPutBack);
	}

	@Test
	public void testGetAndRemoveSplitsAfterSomeCheckpoint() throws Exception {
		final long checkpointId1 = 100L;
		final long checkpointId2 = 101L;
		SplitAssignmentTracker<MockSourceSplit> tracker = new SplitAssignmentTracker<>();

		// Assign some splits and take snapshot 1.
		tracker.recordSplitAssignment(getSplitsAssignment(2, 0));
		takeSnapshot(tracker, checkpointId1);

		// Assign some more splits and take snapshot 2.
		tracker.recordSplitAssignment(getSplitsAssignment(2, 3));
		takeSnapshot(tracker, checkpointId2);

		// Now assume subtask 0 has failed.
		List<MockSourceSplit> splitsToPutBack = tracker.getAndRemoveUncheckpointedAssignment(0, checkpointId1);
		verifyAssignment(Collections.singletonList("3"), splitsToPutBack);
	}

	// ---------------------

	private byte[] takeSnapshot(SplitAssignmentTracker<MockSourceSplit> tracker, long checkpointId) throws Exception {
		byte[] bytes;
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
				DataOutputStream out = new DataOutputViewStreamWrapper(baos)) {
			tracker.snapshotState(checkpointId, new MockSourceSplitSerializer(), out);
			out.flush();
			bytes = baos.toByteArray();
		}
		return bytes;
	}

	private SplitAssignmentTracker<MockSourceSplit> restoreSnapshot(byte[] bytes) throws Exception {
		SplitAssignmentTracker<MockSourceSplit> deserializedTracker;
		try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
				DataInputStream in = new DataInputViewStreamWrapper(bais)) {
			deserializedTracker = new SplitAssignmentTracker<>();
			deserializedTracker.restoreState(new MockSourceSplitSerializer(), in);
		}
		return deserializedTracker;
	}
}
