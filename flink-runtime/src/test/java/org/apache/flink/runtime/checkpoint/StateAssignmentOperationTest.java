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
	public void testReDistributePartitionableStates() {
		Map<String, OperatorStateHandle.StateMetaInfo> metaInfoMap1 = new HashMap<>(6);
		metaInfoMap1.put("t-1", new OperatorStateHandle.StateMetaInfo(new long[]{0}, OperatorStateHandle.Mode.UNION));
		metaInfoMap1.put("t-2", new OperatorStateHandle.StateMetaInfo(new long[]{22, 44}, OperatorStateHandle.Mode.UNION));
		metaInfoMap1.put("t-3", new OperatorStateHandle.StateMetaInfo(new long[]{52, 63}, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE));
		metaInfoMap1.put("t-4", new OperatorStateHandle.StateMetaInfo(new long[]{67, 74, 75}, OperatorStateHandle.Mode.BROADCAST));
		metaInfoMap1.put("t-5", new OperatorStateHandle.StateMetaInfo(new long[]{77, 88, 92}, OperatorStateHandle.Mode.BROADCAST));
		metaInfoMap1.put("t-6", new OperatorStateHandle.StateMetaInfo(new long[]{101, 123, 127}, OperatorStateHandle.Mode.BROADCAST));

		OperatorStateHandle osh1 = new OperatorStreamStateHandle(metaInfoMap1, new ByteStreamStateHandle("test", new byte[130]));
		OperatorID operatorID = new OperatorID();
		OperatorState operatorState = new OperatorState(operatorID, 2, 4);
		operatorState.putState(0, new OperatorSubtaskState(osh1, null, null, null));

		Map<String, OperatorStateHandle.StateMetaInfo> metaInfoMap2 = new HashMap<>(3);
		metaInfoMap2.put("t-1", new OperatorStateHandle.StateMetaInfo(new long[]{0}, OperatorStateHandle.Mode.UNION));
		metaInfoMap2.put("t-4", new OperatorStateHandle.StateMetaInfo(new long[]{20, 27, 28}, OperatorStateHandle.Mode.BROADCAST));
		metaInfoMap2.put("t-5", new OperatorStateHandle.StateMetaInfo(new long[]{30, 44, 48}, OperatorStateHandle.Mode.BROADCAST));
		metaInfoMap2.put("t-6", new OperatorStateHandle.StateMetaInfo(new long[]{57, 79, 83}, OperatorStateHandle.Mode.BROADCAST));

		OperatorStateHandle osh2 = new OperatorStreamStateHandle(metaInfoMap2, new ByteStreamStateHandle("test2", new byte[86]));
		operatorState.putState(1, new OperatorSubtaskState(osh2, null, null, null));

		// rescale up case, parallelism 2 --> 3
		verifyPartitionableStateRescale(operatorState, operatorID, 2, 3);

		// rescale down case, parallelism 2 --> 1
		verifyPartitionableStateRescale(operatorState, operatorID, 2, 1);

		// not rescale
		verifyPartitionableStateRescale(operatorState, operatorID, 2, 2);

	}

	private void verifyPartitionableStateRescale(
		OperatorState operatorState,
		OperatorID operatorID,
		int oldParallelism,
		int newParallelism) {

		Map<OperatorInstanceID, List<OperatorStateHandle>> newManagedOperatorStates =
			new HashMap<>(newParallelism);
		Map<OperatorInstanceID, List<OperatorStateHandle>> newRawOperatorStates =
			new HashMap<>(newParallelism);

		StateAssignmentOperation.reDistributePartitionableStates(
			Collections.singletonList(operatorState),
			newParallelism,
			Collections.singletonList(operatorID),
			newManagedOperatorStates,
			newRawOperatorStates
		);

		Map<String, Integer> checkCounts = new HashMap<>(6);

		for (List<OperatorStateHandle> operatorStateHandles : newManagedOperatorStates.values()) {

			final EnumMap<OperatorStateHandle.Mode, Map<String, Integer>> stateModeOffsets = new EnumMap<>(OperatorStateHandle.Mode.class);
			for (OperatorStateHandle.Mode mode : OperatorStateHandle.Mode.values()) {
				stateModeOffsets.put(
					mode,
					new HashMap<>(6));
			}

			for (OperatorStateHandle operatorStateHandle : operatorStateHandles) {
				for (Map.Entry<String, OperatorStateHandle.StateMetaInfo> stateNameToMetaInfo :
					operatorStateHandle.getStateNameToPartitionOffsets().entrySet()) {

					String stateName = stateNameToMetaInfo.getKey();
					checkCounts.merge(stateName, 1, (count, inc) -> count + inc);

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

		Assert.assertEquals(6, checkCounts.size());
		// t-1 is UNION state and original two sub-tasks both contains one.
		Assert.assertEquals(2 * newParallelism, checkCounts.get("t-1").intValue());
		Assert.assertEquals(newParallelism, checkCounts.get("t-2").intValue());

		// t-3 is SPLIT_DISTRIBUTE state, when rescale up, they will be split to re-distribute.
		if (oldParallelism < newParallelism) {
			Assert.assertEquals(2, checkCounts.get("t-3").intValue());
		} else {
			Assert.assertEquals(1, checkCounts.get("t-3").intValue());
		}
		Assert.assertEquals(newParallelism, checkCounts.get("t-4").intValue());
		Assert.assertEquals(newParallelism, checkCounts.get("t-5").intValue());
		Assert.assertEquals(newParallelism, checkCounts.get("t-6").intValue());
	}
}
