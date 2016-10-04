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
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.util.TestByteStreamStateHandleDeepCompare;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
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

	static Collection<TaskState> createTaskStates(int numTaskStates, int numSubtasksPerTask) throws IOException {

		Random random = new Random(numTaskStates * 31 + numSubtasksPerTask);

		List<TaskState> taskStates = new ArrayList<>(numTaskStates);

		for (int stateIdx = 0; stateIdx < numTaskStates; ++stateIdx) {

			int chainLength = 1 + random.nextInt(8);

			TaskState taskState = new TaskState(new JobVertexID(), numSubtasksPerTask, 128, chainLength);

			int noNonPartitionableStateAtIndex = random.nextInt(chainLength);
			int noOperatorStateBackendAtIndex = random.nextInt(chainLength);
			int noOperatorStateStreamAtIndex = random.nextInt(chainLength);

			boolean hasKeyedBackend = random.nextInt(4) != 0;
			boolean hasKeyedStream = random.nextInt(4) != 0;

			for (int subtaskIdx = 0; subtaskIdx < numSubtasksPerTask; subtaskIdx++) {

				List<StreamStateHandle> nonPartitionableStates = new ArrayList<>(chainLength);
				List<OperatorStateHandle> operatorStatesBackend = new ArrayList<>(chainLength);
				List<OperatorStateHandle> operatorStatesStream = new ArrayList<>(chainLength);

				for (int chainIdx = 0; chainIdx < chainLength; ++chainIdx) {

					StreamStateHandle nonPartitionableState =
							new TestByteStreamStateHandleDeepCompare("a-" + chainIdx, ("Hi-" + chainIdx).getBytes());
					StreamStateHandle operatorStateBackend =
							new TestByteStreamStateHandleDeepCompare("b-" + chainIdx, ("Beautiful-" + chainIdx).getBytes());
					StreamStateHandle operatorStateStream =
							new TestByteStreamStateHandleDeepCompare("b-" + chainIdx, ("Beautiful-" + chainIdx).getBytes());
					Map<String, long[]> offsetsMap = new HashMap<>();
					offsetsMap.put("A", new long[]{0, 10, 20});
					offsetsMap.put("B", new long[]{30, 40, 50});

					if (chainIdx != noNonPartitionableStateAtIndex) {
						nonPartitionableStates.add(nonPartitionableState);
					}

					if (chainIdx != noOperatorStateBackendAtIndex) {
						OperatorStateHandle operatorStateHandleBackend =
								new OperatorStateHandle(offsetsMap, operatorStateBackend);
						operatorStatesBackend.add(operatorStateHandleBackend);
					}

					if (chainIdx != noOperatorStateStreamAtIndex) {
						OperatorStateHandle operatorStateHandleStream =
								new OperatorStateHandle(offsetsMap, operatorStateStream);
						operatorStatesStream.add(operatorStateHandleStream);
					}
				}

				KeyGroupsStateHandle keyedStateBackend = null;
				KeyGroupsStateHandle keyedStateStream = null;

				if (hasKeyedBackend) {
					keyedStateBackend = new KeyGroupsStateHandle(
							new KeyGroupRangeOffsets(1, 1, new long[]{42}),
							new TestByteStreamStateHandleDeepCompare("c", "Hello".getBytes()));
				}

				if (hasKeyedStream) {
					keyedStateStream = new KeyGroupsStateHandle(
							new KeyGroupRangeOffsets(1, 1, new long[]{23}),
							new TestByteStreamStateHandleDeepCompare("d", "World".getBytes()));
				}

				taskState.putState(subtaskIdx, new SubtaskState(
						new ChainedStateHandle<>(nonPartitionableStates),
						new ChainedStateHandle<>(operatorStatesBackend),
						new ChainedStateHandle<>(operatorStatesStream),
						keyedStateStream,
						keyedStateBackend,
						subtaskIdx * 10L));
			}

			taskStates.add(taskState);
		}

		return taskStates;
	}

}
