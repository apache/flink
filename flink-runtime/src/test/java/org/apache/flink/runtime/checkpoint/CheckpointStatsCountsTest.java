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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Test checkpoint statistics counters. */
public class CheckpointStatsCountsTest {

	/**
	 * Tests that counts are reported correctly.
	 */
	@Test
	public void testCounts() {
		CheckpointStatsCounts counts = new CheckpointStatsCounts();
		assertEquals(0, counts.getNumberOfRestoredCheckpoints());
		assertEquals(0, counts.getTotalNumberOfCheckpoints());
		assertEquals(0, counts.getNumberOfInProgressCheckpoints());
		assertEquals(0, counts.getNumberOfCompletedCheckpoints());
		assertEquals(0, counts.getNumberOfFailedCheckpoints());

		counts.incrementRestoredCheckpoints();
		assertEquals(1, counts.getNumberOfRestoredCheckpoints());
		assertEquals(0, counts.getTotalNumberOfCheckpoints());
		assertEquals(0, counts.getNumberOfInProgressCheckpoints());
		assertEquals(0, counts.getNumberOfCompletedCheckpoints());
		assertEquals(0, counts.getNumberOfFailedCheckpoints());

		// 1st checkpoint
		counts.incrementInProgressCheckpoints();
		assertEquals(1, counts.getNumberOfRestoredCheckpoints());
		assertEquals(1, counts.getTotalNumberOfCheckpoints());
		assertEquals(1, counts.getNumberOfInProgressCheckpoints());
		assertEquals(0, counts.getNumberOfCompletedCheckpoints());
		assertEquals(0, counts.getNumberOfFailedCheckpoints());

		counts.incrementCompletedCheckpoints();
		assertEquals(1, counts.getNumberOfRestoredCheckpoints());
		assertEquals(1, counts.getTotalNumberOfCheckpoints());
		assertEquals(0, counts.getNumberOfInProgressCheckpoints());
		assertEquals(1, counts.getNumberOfCompletedCheckpoints());
		assertEquals(0, counts.getNumberOfFailedCheckpoints());

		// 2nd checkpoint
		counts.incrementInProgressCheckpoints();
		assertEquals(1, counts.getNumberOfRestoredCheckpoints());
		assertEquals(2, counts.getTotalNumberOfCheckpoints());
		assertEquals(1, counts.getNumberOfInProgressCheckpoints());
		assertEquals(1, counts.getNumberOfCompletedCheckpoints());
		assertEquals(0, counts.getNumberOfFailedCheckpoints());

		counts.incrementFailedCheckpoints();
		assertEquals(1, counts.getNumberOfRestoredCheckpoints());
		assertEquals(2, counts.getTotalNumberOfCheckpoints());
		assertEquals(0, counts.getNumberOfInProgressCheckpoints());
		assertEquals(1, counts.getNumberOfCompletedCheckpoints());
		assertEquals(1, counts.getNumberOfFailedCheckpoints());
	}

	/**
	 * Tests that increment the completed or failed number of checkpoints without
	 * incrementing the in progress checkpoints before throws an Exception.
	 */
	@Test
	public void testCompleteOrFailWithoutInProgressCheckpoint() {
		CheckpointStatsCounts counts = new CheckpointStatsCounts();
		counts.incrementCompletedCheckpoints();
		assertTrue("Number of checkpoints in progress should never be negative",
			counts.getNumberOfInProgressCheckpoints() >= 0);

		counts.incrementFailedCheckpoints();
		assertTrue("Number of checkpoints in progress should never be negative",
			counts.getNumberOfInProgressCheckpoints() >= 0);
	}

	/**
	 * Tests that that taking snapshots of the state are independent from the
	 * parent.
	 */
	@Test
	public void testCreateSnapshot() {
		CheckpointStatsCounts counts = new CheckpointStatsCounts();
		counts.incrementRestoredCheckpoints();
		counts.incrementRestoredCheckpoints();
		counts.incrementRestoredCheckpoints();

		counts.incrementInProgressCheckpoints();
		counts.incrementCompletedCheckpoints();

		counts.incrementInProgressCheckpoints();
		counts.incrementCompletedCheckpoints();

		counts.incrementInProgressCheckpoints();
		counts.incrementCompletedCheckpoints();

		counts.incrementInProgressCheckpoints();
		counts.incrementCompletedCheckpoints();

		counts.incrementInProgressCheckpoints();
		counts.incrementFailedCheckpoints();

		long restored = counts.getNumberOfRestoredCheckpoints();
		long total = counts.getTotalNumberOfCheckpoints();
		long inProgress = counts.getNumberOfInProgressCheckpoints();
		long completed = counts.getNumberOfCompletedCheckpoints();
		long failed = counts.getNumberOfFailedCheckpoints();

		CheckpointStatsCounts snapshot = counts.createSnapshot();
		assertEquals(restored, snapshot.getNumberOfRestoredCheckpoints());
		assertEquals(total, snapshot.getTotalNumberOfCheckpoints());
		assertEquals(inProgress, snapshot.getNumberOfInProgressCheckpoints());
		assertEquals(completed, snapshot.getNumberOfCompletedCheckpoints());
		assertEquals(failed, snapshot.getNumberOfFailedCheckpoints());

		// Update the original
		counts.incrementRestoredCheckpoints();
		counts.incrementRestoredCheckpoints();

		counts.incrementInProgressCheckpoints();
		counts.incrementCompletedCheckpoints();

		counts.incrementInProgressCheckpoints();
		counts.incrementFailedCheckpoints();

		assertEquals(restored, snapshot.getNumberOfRestoredCheckpoints());
		assertEquals(total, snapshot.getTotalNumberOfCheckpoints());
		assertEquals(inProgress, snapshot.getNumberOfInProgressCheckpoints());
		assertEquals(completed, snapshot.getNumberOfCompletedCheckpoints());
		assertEquals(failed, snapshot.getNumberOfFailedCheckpoints());
	}
}
