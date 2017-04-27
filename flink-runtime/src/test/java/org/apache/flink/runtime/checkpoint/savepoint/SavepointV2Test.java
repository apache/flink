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

package org.apache.flink.runtime.checkpoint.savepoint;

import org.apache.flink.runtime.checkpoint.MasterState;
import org.apache.flink.runtime.checkpoint.TaskState;

import org.junit.Test;

import java.util.Collection;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SavepointV2Test {

	/**
	 * Simple test of savepoint methods.
	 */
	@Test
	public void testSavepointV1() throws Exception {
		final Random rnd = new Random();

		final long checkpointId = rnd.nextInt(Integer.MAX_VALUE) + 1;
		final int numTaskStates = 4;
		final int numSubtaskStates = 16;
		final int numMasterStates = 7;

		Collection<TaskState> taskStates =
				CheckpointTestUtils.createTaskStates(rnd, numTaskStates, numSubtaskStates);

		Collection<MasterState> masterStates =
				CheckpointTestUtils.createRandomMasterStates(rnd, numMasterStates);

		SavepointV2 checkpoint = new SavepointV2(checkpointId, taskStates, masterStates);

		assertEquals(2, checkpoint.getVersion());
		assertEquals(checkpointId, checkpoint.getCheckpointId());
		assertEquals(taskStates, checkpoint.getTaskStates());
		assertEquals(masterStates, checkpoint.getMasterStates());

		assertFalse(checkpoint.getTaskStates().isEmpty());
		assertFalse(checkpoint.getMasterStates().isEmpty());

		checkpoint.dispose();

		assertTrue(checkpoint.getTaskStates().isEmpty());
		assertTrue(checkpoint.getMasterStates().isEmpty());
	}
}
