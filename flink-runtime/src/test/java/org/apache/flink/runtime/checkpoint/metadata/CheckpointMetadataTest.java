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

package org.apache.flink.runtime.checkpoint.metadata;

import org.apache.flink.runtime.checkpoint.MasterState;
import org.apache.flink.runtime.checkpoint.OperatorState;

import org.junit.Test;

import java.util.Collection;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Simple tests for the {@link CheckpointMetadata} data holder class.
 */
public class CheckpointMetadataTest {

	@Test
	public void testConstructAndDispose() throws Exception {
		final Random rnd = new Random();

		final long checkpointId = rnd.nextInt(Integer.MAX_VALUE) + 1;
		final int numTaskStates = 4;
		final int numSubtaskStates = 16;
		final int numMasterStates = 7;

		Collection<OperatorState> taskStates =
				CheckpointTestUtils.createOperatorStates(rnd, null, numTaskStates, numSubtaskStates);

		Collection<MasterState> masterStates =
				CheckpointTestUtils.createRandomMasterStates(rnd, numMasterStates);

		CheckpointMetadata checkpoint = new CheckpointMetadata(checkpointId, taskStates, masterStates);

		assertEquals(checkpointId, checkpoint.getCheckpointId());
		assertEquals(taskStates, checkpoint.getOperatorStates());
		assertEquals(masterStates, checkpoint.getMasterStates());

		assertFalse(checkpoint.getOperatorStates().isEmpty());
		assertFalse(checkpoint.getMasterStates().isEmpty());

		checkpoint.dispose();

		assertTrue(checkpoint.getOperatorStates().isEmpty());
		assertTrue(checkpoint.getMasterStates().isEmpty());
	}
}
