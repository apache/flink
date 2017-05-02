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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.runtime.checkpoint.MasterState;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SubtaskState;
import org.apache.flink.runtime.checkpoint.TaskState;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle.StateMetaInfo;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.util.TestByteStreamStateHandleDeepCompare;
import org.apache.flink.util.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * A collection of utility methods for testing the (de)serialization of
 * checkpoint metadata for persistence.
 */
public class CheckpointTestUtils {

	/**
	 * Creates a random collection of OperatorState objects containing various types of state handles.
	 */
	public static Collection<OperatorState> createOperatorStates(int numTaskStates, int numSubtasksPerTask) {
		return createOperatorStates(new Random(), numTaskStates, numSubtasksPerTask);
	}

	/**
	 * Creates a random collection of OperatorState objects containing various types of state handles.
	 */
	public static Collection<OperatorState> createOperatorStates(
			Random random,
			int numTaskStates,
			int numSubtasksPerTask) {

		List<OperatorState> taskStates = new ArrayList<>(numTaskStates);

		for (int stateIdx = 0; stateIdx < numTaskStates; ++stateIdx) {

			OperatorState taskState = new OperatorState(new OperatorID(), numSubtasksPerTask, 128);

			boolean hasNonPartitionableState = random.nextBoolean();
			boolean hasOperatorStateBackend = random.nextBoolean();
			boolean hasOperatorStateStream = random.nextBoolean();

			boolean hasKeyedBackend = random.nextInt(4) != 0;
			boolean hasKeyedStream = random.nextInt(4) != 0;

			for (int subtaskIdx = 0; subtaskIdx < numSubtasksPerTask; subtaskIdx++) {

				StreamStateHandle nonPartitionableState = null;
				StreamStateHandle operatorStateBackend =
					new TestByteStreamStateHandleDeepCompare("b", ("Beautiful").getBytes(ConfigConstants.DEFAULT_CHARSET));
				StreamStateHandle operatorStateStream =
					new TestByteStreamStateHandleDeepCompare("b", ("Beautiful").getBytes(ConfigConstants.DEFAULT_CHARSET));

				OperatorStateHandle operatorStateHandleBackend = null;
				OperatorStateHandle operatorStateHandleStream = null;
				
				Map<String, StateMetaInfo> offsetsMap = new HashMap<>();
				offsetsMap.put("A", new OperatorStateHandle.StateMetaInfo(new long[]{0, 10, 20}, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE));
				offsetsMap.put("B", new OperatorStateHandle.StateMetaInfo(new long[]{30, 40, 50}, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE));
				offsetsMap.put("C", new OperatorStateHandle.StateMetaInfo(new long[]{60, 70, 80}, OperatorStateHandle.Mode.BROADCAST));

				if (hasNonPartitionableState) {
					nonPartitionableState =
						new TestByteStreamStateHandleDeepCompare("a", ("Hi").getBytes(ConfigConstants.DEFAULT_CHARSET));
				}

				if (hasOperatorStateBackend) {
					operatorStateHandleBackend = new OperatorStateHandle(offsetsMap, operatorStateBackend);
				}

				if (hasOperatorStateStream) {
					operatorStateHandleStream = new OperatorStateHandle(offsetsMap, operatorStateStream);
				}

				KeyGroupsStateHandle keyedStateBackend = null;
				KeyGroupsStateHandle keyedStateStream = null;

				if (hasKeyedBackend) {
					keyedStateBackend = new KeyGroupsStateHandle(
							new KeyGroupRangeOffsets(1, 1, new long[]{42}),
							new TestByteStreamStateHandleDeepCompare("c", "Hello"
									.getBytes(ConfigConstants.DEFAULT_CHARSET)));
				}

				if (hasKeyedStream) {
					keyedStateStream = new KeyGroupsStateHandle(
							new KeyGroupRangeOffsets(1, 1, new long[]{23}),
							new TestByteStreamStateHandleDeepCompare("d", "World"
									.getBytes(ConfigConstants.DEFAULT_CHARSET)));
				}

				taskState.putState(subtaskIdx, new OperatorSubtaskState(
						nonPartitionableState,
						operatorStateHandleBackend,
						operatorStateHandleStream,
						keyedStateStream,
						keyedStateBackend));
			}

			taskStates.add(taskState);
		}

		return taskStates;
	}

	/**
	 * Creates a random collection of TaskState objects containing various types of state handles.
	 */
	public static Collection<TaskState> createTaskStates(int numTaskStates, int numSubtasksPerTask) {
		return createTaskStates(new Random(), numTaskStates, numSubtasksPerTask);
	}

	/**
	 * Creates a random collection of TaskState objects containing various types of state handles.
	 */
	public static Collection<TaskState> createTaskStates(
			Random random,
			int numTaskStates,
			int numSubtasksPerTask) {

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
							new TestByteStreamStateHandleDeepCompare("a-" + chainIdx, ("Hi-" + chainIdx).getBytes(
								ConfigConstants.DEFAULT_CHARSET));
					StreamStateHandle operatorStateBackend =
							new TestByteStreamStateHandleDeepCompare("b-" + chainIdx, ("Beautiful-" + chainIdx).getBytes(ConfigConstants.DEFAULT_CHARSET));
					StreamStateHandle operatorStateStream =
							new TestByteStreamStateHandleDeepCompare("b-" + chainIdx, ("Beautiful-" + chainIdx).getBytes(ConfigConstants.DEFAULT_CHARSET));
					Map<String, StateMetaInfo> offsetsMap = new HashMap<>();
					offsetsMap.put("A", new OperatorStateHandle.StateMetaInfo(new long[]{0, 10, 20}, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE));
					offsetsMap.put("B", new OperatorStateHandle.StateMetaInfo(new long[]{30, 40, 50}, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE));
					offsetsMap.put("C", new OperatorStateHandle.StateMetaInfo(new long[]{60, 70, 80}, OperatorStateHandle.Mode.BROADCAST));

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
							new TestByteStreamStateHandleDeepCompare("c", "Hello"
								.getBytes(ConfigConstants.DEFAULT_CHARSET)));
				}

				if (hasKeyedStream) {
					keyedStateStream = new KeyGroupsStateHandle(
							new KeyGroupRangeOffsets(1, 1, new long[]{23}),
							new TestByteStreamStateHandleDeepCompare("d", "World"
								.getBytes(ConfigConstants.DEFAULT_CHARSET)));
				}

				taskState.putState(subtaskIdx, new SubtaskState(
						new ChainedStateHandle<>(nonPartitionableStates),
						new ChainedStateHandle<>(operatorStatesBackend),
						new ChainedStateHandle<>(operatorStatesStream),
						keyedStateStream,
						keyedStateBackend));
			}

			taskStates.add(taskState);
		}

		return taskStates;
	}

	/**
	 * Creates a bunch of random master states.
	 */
	public static Collection<MasterState> createRandomMasterStates(Random random, int num) {
		final ArrayList<MasterState> states = new ArrayList<>(num);

		for (int i = 0; i < num; i++) {
			int version = random.nextInt(10);
			String name = StringUtils.getRandomString(random, 5, 500);
			byte[] bytes = new byte[random.nextInt(5000) + 1];
			random.nextBytes(bytes);

			states.add(new MasterState(name, bytes, version));
		}

		return states;
	}

	/**
	 * Asserts that two MasterStates are equal.
	 * 
	 * <p>The MasterState avoids overriding {@code equals()} on purpose, because equality is not well
	 * defined in the raw contents.
	 */
	public static void assertMasterStateEquality(MasterState a, MasterState b) {
		assertEquals(a.version(), b.version());
		assertEquals(a.name(), b.name());
		assertArrayEquals(a.bytes(), b.bytes());

	}

	// ------------------------------------------------------------------------

	/** utility class, not meant to be instantiated */
	private CheckpointTestUtils() {}
}
