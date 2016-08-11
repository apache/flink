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

import org.apache.flink.runtime.checkpoint.SubtaskState;
import org.apache.flink.runtime.checkpoint.TaskState;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SavepointV1Test {

	/**
	 * Simple test of savepoint methods.
	 */
	@Test
	public void testSavepointV1() throws Exception {
		long checkpointId = ThreadLocalRandom.current().nextLong(Integer.MAX_VALUE);
		int numTaskStates = 4;
		int numSubtaskStates = 16;

		Collection<TaskState> expected = createTaskStates(numTaskStates, numSubtaskStates);

		SavepointV1 savepoint = new SavepointV1(checkpointId, expected);

		assertEquals(SavepointV1.VERSION, savepoint.getVersion());
		assertEquals(checkpointId, savepoint.getCheckpointId());
		assertEquals(expected, savepoint.getTaskStates());

		assertFalse(savepoint.getTaskStates().isEmpty());
		savepoint.dispose();
		assertTrue(savepoint.getTaskStates().isEmpty());
	}

	static Collection<TaskState> createTaskStates(int numTaskStates, int numSubtaskStates) throws IOException {
		List<TaskState> taskStates = new ArrayList<>(numTaskStates);

		for (int i = 0; i < numTaskStates; i++) {
			TaskState taskState = new TaskState(new JobVertexID(), numSubtaskStates, numSubtaskStates);
			for (int j = 0; j < numSubtaskStates; j++) {
				StreamStateHandle stateHandle = new ByteStreamStateHandle("Hello".getBytes());
				taskState.putState(i, new SubtaskState(
						new ChainedStateHandle<>(Collections.singletonList(stateHandle)), 0));
			}

			taskState.putKeyedState(
					0,
					new KeyGroupsStateHandle(
							new KeyGroupRangeOffsets(1,1, new long[] {42}), new ByteStreamStateHandle("Hello".getBytes())));

			taskStates.add(taskState);
		}

		return taskStates;
	}

}
