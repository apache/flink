/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.OperatorInstanceID;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests to verify state assignment operation.
 */
public class StateAssignmentOperationTest extends TestLogger {

	@Test
	public void testRepartitionSplitDistributeStates() {
		OperatorID operatorID = new OperatorID();
		OperatorState operatorState = new OperatorState(operatorID, 2, 4);

		Map<String, OperatorStateHandle.StateMetaInfo> metaInfoMap1 = new HashMap<>(1);
		metaInfoMap1.put("t-1", new OperatorStateHandle.StateMetaInfo(new long[]{0, 10}, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE));
		OperatorStateHandle osh1 = new OperatorStreamStateHandle(metaInfoMap1, new ByteStreamStateHandle("test1", new byte[30]));
		operatorState.putState(0, new OperatorSubtaskState(osh1, null, null, null));

		Map<String, OperatorStateHandle.StateMetaInfo> metaInfoMap2 = new HashMap<>(1);
		metaInfoMap2.put("t-2", new OperatorStateHandle.StateMetaInfo(new long[]{0, 15}, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE));
		OperatorStateHandle osh2 = new OperatorStreamStateHandle(metaInfoMap2, new ByteStreamStateHandle("test2", new byte[40]));
		operatorState.putState(1, new OperatorSubtaskState(osh2, null, null, null));

		verifyOneKindPartitionableStateRescale(operatorState, operatorID);
	}

	@Test
	public void testRepartitionUnionState() {
		OperatorID operatorID = new OperatorID();
		OperatorState operatorState = new OperatorState(operatorID, 2, 4);

		Map<String, OperatorStateHandle.StateMetaInfo> metaInfoMap1 = new HashMap<>(2);
		metaInfoMap1.put("t-3", new OperatorStateHandle.StateMetaInfo(new long[]{0}, OperatorStateHandle.Mode.UNION));
		metaInfoMap1.put("t-4", new OperatorStateHandle.StateMetaInfo(new long[]{22, 44}, OperatorStateHandle.Mode.UNION));
		OperatorStateHandle osh1 = new OperatorStreamStateHandle(metaInfoMap1, new ByteStreamStateHandle("test1", new byte[50]));
		operatorState.putState(0, new OperatorSubtaskState(osh1, null, null, null));

		Map<String, OperatorStateHandle.StateMetaInfo> metaInfoMap2 = new HashMap<>(1);
		metaInfoMap2.put("t-3", new OperatorStateHandle.StateMetaInfo(new long[]{0}, OperatorStateHandle.Mode.UNION));
		OperatorStateHandle osh2 = new OperatorStreamStateHandle(metaInfoMap2, new ByteStreamStateHandle("test2", new byte[20]));
		operatorState.putState(1, new OperatorSubtaskState(osh2, null, null, null));

		verifyOneKindPartitionableStateRescale(operatorState, operatorID);
	}

	@Test
	public void testRepartitionBroadcastState() {
		OperatorID operatorID = new OperatorID();
		OperatorState operatorState = new OperatorState(operatorID, 2, 4);

		Map<String, OperatorStateHandle.StateMetaInfo> metaInfoMap1 = new HashMap<>(2);
		metaInfoMap1.put("t-5", new OperatorStateHandle.StateMetaInfo(new long[]{0, 10, 20}, OperatorStateHandle.Mode.BROADCAST));
		metaInfoMap1.put("t-6", new OperatorStateHandle.StateMetaInfo(new long[]{30, 40, 50}, OperatorStateHandle.Mode.BROADCAST));
		OperatorStateHandle osh1 = new OperatorStreamStateHandle(metaInfoMap1, new ByteStreamStateHandle("test1", new byte[60]));
		operatorState.putState(0, new OperatorSubtaskState(osh1, null, null, null));

		Map<String, OperatorStateHandle.StateMetaInfo> metaInfoMap2 = new HashMap<>(2);
		metaInfoMap2.put("t-5", new OperatorStateHandle.StateMetaInfo(new long[]{0, 10, 20}, OperatorStateHandle.Mode.BROADCAST));
		metaInfoMap2.put("t-6", new OperatorStateHandle.StateMetaInfo(new long[]{30, 40, 50}, OperatorStateHandle.Mode.BROADCAST));
		OperatorStateHandle osh2 = new OperatorStreamStateHandle(metaInfoMap2, new ByteStreamStateHandle("test2", new byte[60]));
		operatorState.putState(1, new OperatorSubtaskState(osh2, null, null, null));

		verifyOneKindPartitionableStateRescale(operatorState, operatorID);
	}

	/**
	 * Verify repartition logic on partitionable states with all modes.
	 */
	@Test
	public void testReDistributeCombinedPartitionableStates() {
		OperatorID operatorID = new OperatorID();
		OperatorState operatorState = new OperatorState(operatorID, 2, 4);

		Map<String, OperatorStateHandle.StateMetaInfo> metaInfoMap1 = new HashMap<>(6);
		metaInfoMap1.put("t-1", new OperatorStateHandle.StateMetaInfo(new long[]{0}, OperatorStateHandle.Mode.UNION));
		metaInfoMap1.put("t-2", new OperatorStateHandle.StateMetaInfo(new long[]{22, 44}, OperatorStateHandle.Mode.UNION));
		metaInfoMap1.put("t-3", new OperatorStateHandle.StateMetaInfo(new long[]{52, 63}, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE));
		metaInfoMap1.put("t-4", new OperatorStateHandle.StateMetaInfo(new long[]{67, 74, 75}, OperatorStateHandle.Mode.BROADCAST));
		metaInfoMap1.put("t-5", new OperatorStateHandle.StateMetaInfo(new long[]{77, 88, 92}, OperatorStateHandle.Mode.BROADCAST));
		metaInfoMap1.put("t-6", new OperatorStateHandle.StateMetaInfo(new long[]{101, 123, 127}, OperatorStateHandle.Mode.BROADCAST));

		OperatorStateHandle osh1 = new OperatorStreamStateHandle(metaInfoMap1, new ByteStreamStateHandle("test1", new byte[130]));
		operatorState.putState(0, new OperatorSubtaskState(osh1, null, null, null));

		Map<String, OperatorStateHandle.StateMetaInfo> metaInfoMap2 = new HashMap<>(3);
		metaInfoMap2.put("t-1", new OperatorStateHandle.StateMetaInfo(new long[]{0}, OperatorStateHandle.Mode.UNION));
		metaInfoMap2.put("t-4", new OperatorStateHandle.StateMetaInfo(new long[]{20, 27, 28}, OperatorStateHandle.Mode.BROADCAST));
		metaInfoMap2.put("t-5", new OperatorStateHandle.StateMetaInfo(new long[]{30, 44, 48}, OperatorStateHandle.Mode.BROADCAST));
		metaInfoMap2.put("t-6", new OperatorStateHandle.StateMetaInfo(new long[]{57, 79, 83}, OperatorStateHandle.Mode.BROADCAST));

		OperatorStateHandle osh2 = new OperatorStreamStateHandle(metaInfoMap2, new ByteStreamStateHandle("test2", new byte[86]));
		operatorState.putState(1, new OperatorSubtaskState(osh2, null, null, null));

		// rescale up case, parallelism 2 --> 3
		verifyCombinedPartitionableStateRescale(operatorState, operatorID, 2, 3);

		// rescale down case, parallelism 2 --> 1
		verifyCombinedPartitionableStateRescale(operatorState, operatorID, 2, 1);

		// not rescale
		verifyCombinedPartitionableStateRescale(operatorState, operatorID, 2, 2);
	}

	// ------------------------------------------------------------------------

	/**
	 * Verify that after repartition states, state of different modes works as expected and collect the information of
	 * state-name -> how many operator stat handles would be used for new sub-tasks to initialize in total.
	 */
	private void verifyAndCollectStateInfo(
		OperatorState operatorState,
		OperatorID operatorID,
		int oldParallelism,
		int newParallelism,
		Map<String, Integer> stateInfoCounts
	) {
		final Map<OperatorInstanceID, List<OperatorStateHandle>> newManagedOperatorStates =
			StateAssignmentOperation.reDistributePartitionableStates(
				Collections.singletonList(operatorState),
				newParallelism,
				Collections.singletonList(operatorID),
				OperatorSubtaskState::getManagedOperatorState,
				RoundRobinOperatorStateRepartitioner.INSTANCE
			);

		// Verify the repartitioned managed operator states per sub-task.
		for (List<OperatorStateHandle> operatorStateHandles : newManagedOperatorStates.values()) {

			final EnumMap<OperatorStateHandle.Mode, Map<String, Integer>> stateModeOffsets =
				new EnumMap<>(OperatorStateHandle.Mode.class);
			for (OperatorStateHandle.Mode mode : OperatorStateHandle.Mode.values()) {
				stateModeOffsets.put(
					mode,
					new HashMap<>());
			}

			for (OperatorStateHandle operatorStateHandle : operatorStateHandles) {
				for (Map.Entry<String, OperatorStateHandle.StateMetaInfo> stateNameToMetaInfo :
					operatorStateHandle.getStateNameToPartitionOffsets().entrySet()) {

					String stateName = stateNameToMetaInfo.getKey();
					stateInfoCounts.merge(stateName, 1, (count, inc) -> count + inc);

					OperatorStateHandle.StateMetaInfo stateMetaInfo = stateNameToMetaInfo.getValue();

					stateModeOffsets.get(stateMetaInfo.getDistributionMode()).merge(
						stateName, stateMetaInfo.getOffsets().length, (count, inc) -> count + inc);
				}
			}

			for (Map.Entry<OperatorStateHandle.Mode, Map<String, Integer>> modeMapEntry : stateModeOffsets.entrySet()) {

				OperatorStateHandle.Mode mode = modeMapEntry.getKey();
				Map<String, Integer> stateOffsets = modeMapEntry.getValue();
				if (OperatorStateHandle.Mode.SPLIT_DISTRIBUTE.equals(mode)) {
					if (oldParallelism < newParallelism) {
						// SPLIT_DISTRIBUTE: when rescale up, split the state and re-distribute it -> each one will go to one task
						stateOffsets.values().forEach(length -> Assert.assertEquals(1, (int) length));
					} else {
						// SPLIT_DISTRIBUTE: when rescale down to 1 or not rescale, not re-distribute them.
						stateOffsets.values().forEach(length -> Assert.assertEquals(2, (int) length));
					}
				} else if (OperatorStateHandle.Mode.UNION.equals(mode)) {
					// UNION: all to all
					stateOffsets.values().forEach(length -> Assert.assertEquals(2, (int) length));
				} else {
					// BROADCAST: so all to all
					stateOffsets.values().forEach(length -> Assert.assertEquals(3, (int) length));
				}
			}
		}
	}

	private void verifyOneKindPartitionableStateRescale(OperatorState operatorState, OperatorID operatorID) {
		// rescale up case, parallelism 2 --> 3
		verifyOneKindPartitionableStateRescale(operatorState, operatorID, 2, 3);

		// rescale down case, parallelism 2 --> 1
		verifyOneKindPartitionableStateRescale(operatorState, operatorID, 2, 1);

		// not rescale
		verifyOneKindPartitionableStateRescale(operatorState, operatorID, 2, 2);
	}

	private void verifyOneKindPartitionableStateRescale(
		OperatorState operatorState,
		OperatorID operatorID,
		int oldParallelism,
		int newParallelism) {

		final Map<String, Integer> stateInfoCounts = new HashMap<>();

		verifyAndCollectStateInfo(operatorState, operatorID, oldParallelism, newParallelism, stateInfoCounts);

		Assert.assertEquals(2, stateInfoCounts.size());

		// t-1 and t-2 are SPLIT_DISTRIBUTE state, when rescale up, they will be split to re-distribute.
		if (stateInfoCounts.containsKey("t-1")) {
			if (oldParallelism < newParallelism) {
				Assert.assertEquals(2, stateInfoCounts.get("t-1").intValue());
				Assert.assertEquals(2, stateInfoCounts.get("t-2").intValue());
			} else {
				Assert.assertEquals(1, stateInfoCounts.get("t-1").intValue());
				Assert.assertEquals(1, stateInfoCounts.get("t-2").intValue());
			}
		}

		// t-3 and t-4 are UNION state.
		if (stateInfoCounts.containsKey("t-3")) {
			// original two sub-tasks both contain one "t-3" state
			Assert.assertEquals(2 * newParallelism, stateInfoCounts.get("t-3").intValue());
			// only one original sub-task contains one "t-4" state
			Assert.assertEquals(newParallelism, stateInfoCounts.get("t-4").intValue());
		}

		// t-5 and t-6 are BROADCAST state.
		if (stateInfoCounts.containsKey("t-5")) {
			Assert.assertEquals(newParallelism, stateInfoCounts.get("t-5").intValue());
			Assert.assertEquals(newParallelism, stateInfoCounts.get("t-6").intValue());
		}
	}

	private void verifyCombinedPartitionableStateRescale(
		OperatorState operatorState,
		OperatorID operatorID,
		int oldParallelism,
		int newParallelism) {

		final Map<String, Integer> stateInfoCounts = new HashMap<>();

		verifyAndCollectStateInfo(operatorState, operatorID, oldParallelism, newParallelism, stateInfoCounts);

		Assert.assertEquals(6, stateInfoCounts.size());
		// t-1 is UNION state and original two sub-tasks both contains one.
		Assert.assertEquals(2 * newParallelism, stateInfoCounts.get("t-1").intValue());
		Assert.assertEquals(newParallelism, stateInfoCounts.get("t-2").intValue());

		// t-3 is SPLIT_DISTRIBUTE state, when rescale up, they will be split to re-distribute.
		if (oldParallelism < newParallelism) {
			Assert.assertEquals(2, stateInfoCounts.get("t-3").intValue());
		} else {
			Assert.assertEquals(1, stateInfoCounts.get("t-3").intValue());
		}
		Assert.assertEquals(newParallelism, stateInfoCounts.get("t-4").intValue());
		Assert.assertEquals(newParallelism, stateInfoCounts.get("t-5").intValue());
		Assert.assertEquals(newParallelism, stateInfoCounts.get("t-6").intValue());
	}

}
