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
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;

import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link CheckpointOptions} class.
 */
public class CheckpointOptionsTest {

	@Test
	public void testDefaultCheckpoint() throws Exception {
		final CheckpointOptions options = CheckpointOptions.forCheckpointWithDefaultLocation();
		assertEquals(CheckpointType.CHECKPOINT, options.getCheckpointType());
		assertTrue(options.getTargetLocation().isDefaultReference());

		final CheckpointOptions copy = CommonTestUtils.createCopySerializable(options);
		assertEquals(CheckpointType.CHECKPOINT, copy.getCheckpointType());
		assertTrue(copy.getTargetLocation().isDefaultReference());
	}

	@Test
	public void testSavepoint() throws Exception {
		final Random rnd = new Random();
		final byte[] locationBytes = new byte[rnd.nextInt(41) + 1];
		rnd.nextBytes(locationBytes);

		final CheckpointOptions options = new CheckpointOptions(
				CheckpointType.values()[rnd.nextInt(CheckpointType.values().length)],
				new CheckpointStorageLocationReference(locationBytes));

		final CheckpointOptions copy = CommonTestUtils.createCopySerializable(options);
		assertEquals(options.getCheckpointType(), copy.getCheckpointType());
		assertArrayEquals(locationBytes, copy.getTargetLocation().getReferenceBytes());
	}
}
