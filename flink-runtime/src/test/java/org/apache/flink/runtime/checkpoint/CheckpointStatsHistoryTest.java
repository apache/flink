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

import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CheckpointStatsHistoryTest {

	/**
	 * Tests a checkpoint history with allowed size 0.
	 */
	@Test
	public void testZeroMaxSizeHistory() throws Exception {
		CheckpointStatsHistory history = new CheckpointStatsHistory(0);

		history.addInProgressCheckpoint(createPendingCheckpointStats(0));
		assertFalse(history.replacePendingCheckpointById(createCompletedCheckpointStats(0)));

		CheckpointStatsHistory snapshot = history.createSnapshot();

		int counter = 0;
		for (AbstractCheckpointStats ignored : snapshot.getCheckpoints()) {
			counter++;
		}

		assertEquals(0, counter);
		assertNotNull(snapshot.getCheckpointById(0));
	}

	/**
	 * Tests a checkpoint history with allowed size 1.
	 */
	@Test
	public void testSizeOneHistory() throws Exception {
		CheckpointStatsHistory history = new CheckpointStatsHistory(1);

		history.addInProgressCheckpoint(createPendingCheckpointStats(0));
		history.addInProgressCheckpoint(createPendingCheckpointStats(1));

		assertFalse(history.replacePendingCheckpointById(createCompletedCheckpointStats(0)));
		assertTrue(history.replacePendingCheckpointById(createCompletedCheckpointStats(1)));

		CheckpointStatsHistory snapshot = history.createSnapshot();

		for (AbstractCheckpointStats stats : snapshot.getCheckpoints()) {
			assertEquals(1, stats.getCheckpointId());
			assertTrue(stats.getStatus().isCompleted());
		}
	}

	/**
	 * Tests the checkpoint history with multiple checkpoints.
	 */
	@Test
	public void testCheckpointHistory() throws Exception {
		CheckpointStatsHistory history = new CheckpointStatsHistory(3);

		history.addInProgressCheckpoint(createPendingCheckpointStats(0));

		CheckpointStatsHistory snapshot = history.createSnapshot();
		for (AbstractCheckpointStats stats : snapshot.getCheckpoints()) {
			assertEquals(0, stats.getCheckpointId());
			assertTrue(stats.getStatus().isInProgress());
		}

		history.addInProgressCheckpoint(createPendingCheckpointStats(1));
		history.addInProgressCheckpoint(createPendingCheckpointStats(2));
		history.addInProgressCheckpoint(createPendingCheckpointStats(3));

		snapshot = history.createSnapshot();

		// Check in progress stats.
		Iterator<AbstractCheckpointStats> it = snapshot.getCheckpoints().iterator();
		for (int i = 3; i > 0; i--) {
			assertTrue(it.hasNext());
			AbstractCheckpointStats stats = it.next();
			assertEquals(i, stats.getCheckpointId());
			assertTrue(stats.getStatus().isInProgress());
		}
		assertFalse(it.hasNext());

		// Update checkpoints
		history.replacePendingCheckpointById(createFailedCheckpointStats(1));
		history.replacePendingCheckpointById(createCompletedCheckpointStats(3));
		history.replacePendingCheckpointById(createFailedCheckpointStats(2));

		snapshot = history.createSnapshot();
		it = snapshot.getCheckpoints().iterator();

		assertTrue(it.hasNext());
		AbstractCheckpointStats stats = it.next();
		assertEquals(3, stats.getCheckpointId());
		assertNotNull(snapshot.getCheckpointById(3));
		assertTrue(stats.getStatus().isCompleted());
		assertTrue(snapshot.getCheckpointById(3).getStatus().isCompleted());

		assertTrue(it.hasNext());
		stats = it.next();
		assertEquals(2, stats.getCheckpointId());
		assertNotNull(snapshot.getCheckpointById(2));
		assertTrue(stats.getStatus().isFailed());
		assertTrue(snapshot.getCheckpointById(2).getStatus().isFailed());

		assertTrue(it.hasNext());
		stats = it.next();
		assertEquals(1, stats.getCheckpointId());
		assertNotNull(snapshot.getCheckpointById(1));
		assertTrue(stats.getStatus().isFailed());
		assertTrue(snapshot.getCheckpointById(1).getStatus().isFailed());

		assertFalse(it.hasNext());
	}

	/**
	 * Tests that a snapshot cannot be modified or copied.
	 */
	@Test
	public void testModifySnapshot() throws Exception {
		CheckpointStatsHistory history = new CheckpointStatsHistory(3);

		history.addInProgressCheckpoint(createPendingCheckpointStats(0));
		history.addInProgressCheckpoint(createPendingCheckpointStats(1));
		history.addInProgressCheckpoint(createPendingCheckpointStats(2));

		CheckpointStatsHistory snapshot = history.createSnapshot();

		try {
			snapshot.addInProgressCheckpoint(createPendingCheckpointStats(4));
			fail("Did not throw expected Exception");
		} catch (UnsupportedOperationException ignored) {
		}

		try {
			snapshot.replacePendingCheckpointById(createCompletedCheckpointStats(2));
			fail("Did not throw expected Exception");
		} catch (UnsupportedOperationException ignored) {
		}

		try {
			snapshot.createSnapshot();
			fail("Did not throw expected Exception");
		} catch (UnsupportedOperationException ignored) {
		}
	}

	// ------------------------------------------------------------------------

	private PendingCheckpointStats createPendingCheckpointStats(long checkpointId) {
		PendingCheckpointStats pending = mock(PendingCheckpointStats.class);
		when(pending.getStatus()).thenReturn(CheckpointStatsStatus.IN_PROGRESS);
		when(pending.getCheckpointId()).thenReturn(checkpointId);
		when(pending.getProperties()).thenReturn(CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION));
		return pending;
	}

	private CompletedCheckpointStats createCompletedCheckpointStats(long checkpointId) {
		CompletedCheckpointStats completed = mock(CompletedCheckpointStats.class);
		when(completed.getStatus()).thenReturn(CheckpointStatsStatus.COMPLETED);
		when(completed.getProperties()).thenReturn(CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION));
		when(completed.getCheckpointId()).thenReturn(checkpointId);
		when(completed.getProperties()).thenReturn(CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION));
		return completed;
	}

	private FailedCheckpointStats createFailedCheckpointStats(long checkpointId) {
		FailedCheckpointStats failed = mock(FailedCheckpointStats.class);
		when(failed.getStatus()).thenReturn(CheckpointStatsStatus.FAILED);
		when(failed.getProperties()).thenReturn(CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION));
		when(failed.getCheckpointId()).thenReturn(checkpointId);
		return failed;
	}

}
