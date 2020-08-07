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
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SubtaskStateStatsTest {

	/**
	 * Tests simple access via the getters.
	 */
	@Test
	public void testSimpleAccess() throws Exception {
		SubtaskStateStats stats = new SubtaskStateStats(
			0,
			Integer.MAX_VALUE + 1L,
			Integer.MAX_VALUE + 2L,
			Integer.MAX_VALUE + 3L,
			Integer.MAX_VALUE + 4L,
			Integer.MAX_VALUE + 6L,
			Integer.MAX_VALUE + 7L);

		assertEquals(0, stats.getSubtaskIndex());
		assertEquals(Integer.MAX_VALUE + 1L, stats.getAckTimestamp());
		assertEquals(Integer.MAX_VALUE + 2L, stats.getStateSize());
		assertEquals(Integer.MAX_VALUE + 3L, stats.getSyncCheckpointDuration());
		assertEquals(Integer.MAX_VALUE + 4L, stats.getAsyncCheckpointDuration());
		assertEquals(Integer.MAX_VALUE + 6L, stats.getAlignmentDuration());
		assertEquals(Integer.MAX_VALUE + 7L, stats.getCheckpointStartDelay());

		// Check duration helper
		long ackTimestamp = stats.getAckTimestamp();
		long triggerTimestamp = ackTimestamp - 10123;
		assertEquals(10123, stats.getEndToEndDuration(triggerTimestamp));

		// Trigger timestamp < ack timestamp
		assertEquals(0, stats.getEndToEndDuration(ackTimestamp + 1));
	}

	/**
	 * Tests that the snapshot is actually serializable.
	 */
	@Test
	public void testIsJavaSerializable() throws Exception {
		SubtaskStateStats stats = new SubtaskStateStats(
			0,
			Integer.MAX_VALUE + 1L,
			Integer.MAX_VALUE + 2L,
			Integer.MAX_VALUE + 3L,
			Integer.MAX_VALUE + 4L,
			Integer.MAX_VALUE + 6L,
			Integer.MAX_VALUE + 7L);


		SubtaskStateStats copy = CommonTestUtils.createCopySerializable(stats);

		assertEquals(0, copy.getSubtaskIndex());
		assertEquals(Integer.MAX_VALUE + 1L, copy.getAckTimestamp());
		assertEquals(Integer.MAX_VALUE + 2L, copy.getStateSize());
		assertEquals(Integer.MAX_VALUE + 3L, copy.getSyncCheckpointDuration());
		assertEquals(Integer.MAX_VALUE + 4L, copy.getAsyncCheckpointDuration());
		assertEquals(Integer.MAX_VALUE + 6L, copy.getAlignmentDuration());
		assertEquals(Integer.MAX_VALUE + 7L, stats.getCheckpointStartDelay());

		// Check duration helper
		long ackTimestamp = copy.getAckTimestamp();
		long triggerTimestamp = ackTimestamp - 10123;
		assertEquals(10123, copy.getEndToEndDuration(triggerTimestamp));

		// Trigger timestamp < ack timestamp
		assertEquals(0, copy.getEndToEndDuration(ackTimestamp + 1));

	}

}
