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

package org.apache.flink.runtime.state.generic;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.KeyGroupAssigner;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.HashKeyGroupAssigner;
import org.apache.flink.runtime.state.PartitionedStateSnapshot;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GenericKeyGroupStateBackendTest extends TestLogger {

	private GenericKeyGroupStateBackend<Integer> createStateBackend(KeyGroupAssigner<Integer> keyGroupAssigner) {
		return new GenericKeyGroupStateBackend<>(
			new MemoryStateBackend(),
			IntSerializer.INSTANCE,
			keyGroupAssigner);
	}

	/**
	 * Tests that the GenericKeyGroupStateBackend creates, snapshots and restores key groups
	 * properly for {@link ValueState}.
	 *
	 * @throws Exception
	 */
	@Test
	public void testValueState() throws Exception {
		HashKeyGroupAssigner<Integer> keyGroupAssigner = new HashKeyGroupAssigner<>(42);

		try (GenericKeyGroupStateBackend<Integer> backend = createStateBackend(keyGroupAssigner);
			 GenericKeyGroupStateBackend<Integer> backend2 = createStateBackend(keyGroupAssigner)) {
			ValueStateDescriptor<String> valueSD = new ValueStateDescriptor<String>("value", String.class, "foobar");
			int numberElements = 1001;
			Set<Tuple2<Integer, String>> elements = new HashSet<>();
			Set<Integer> keyGroups = new HashSet<>();

			long checkpointId = 1L;
			long timestamp = 1L;
			long recoveryTimestamp = 2L;

			for (int i = 0; i < numberElements; i++) {
				int keyGroupIndex = keyGroupAssigner.getKeyGroupIndex(i);

				keyGroups.add(keyGroupIndex);
				elements.add(Tuple2.of(i, "value: " + i));
			}

			ValueState<String> valueState = backend.getPartitionedState(null, VoidSerializer.INSTANCE, valueSD);

			for (Tuple2<Integer, String> element : elements) {
				backend.setCurrentKey(element.f0);
				valueState.update(element.f1);
			}

			Map<Integer, PartitionedStateSnapshot> snapshot = backend.snapshotPartitionedState(checkpointId, timestamp);

			// verify original state
			for (Tuple2<Integer, String> element : elements) {
				backend.setCurrentKey(element.f0);

				assertEquals(element.f1, valueState.value());
			}

			// verify that we have for each key group a snapshot
			assertEquals(keyGroups.size(), snapshot.size());

			for (Integer key : snapshot.keySet()) {
				assertTrue(keyGroups.contains(key));
			}

			backend2.restorePartitionedState(snapshot, recoveryTimestamp);

			// discard old state
			for (PartitionedStateSnapshot partitionedStateSnapshot : snapshot.values()) {
				partitionedStateSnapshot.discardState();
			}

			ValueState<String> restoredValueState = backend2.getPartitionedState(null, VoidSerializer.INSTANCE, valueSD);

			// check that we have properly restored the old state for each key group
			for (Tuple2<Integer, String> element : elements) {
				backend2.setCurrentKey(element.f0);

				assertEquals(element.f1, restoredValueState.value());
			}
		}
	}

	/**
	 * Tests that the GenericKeyGroupStateBackend creates, snapshots and restores key groups
	 * properly for {@link ListState}.
	 *
	 * @throws Exception
	 */
	@Test
	public void testListState() throws Exception {
		HashKeyGroupAssigner<Integer> keyGroupAssigner = new HashKeyGroupAssigner<>(330);

		try (GenericKeyGroupStateBackend<Integer> backend = createStateBackend(keyGroupAssigner);
			 GenericKeyGroupStateBackend<Integer> backend2 = createStateBackend(keyGroupAssigner)) {

			int numberElements = 1001;
			ListStateDescriptor<Integer> listSD = new ListStateDescriptor<Integer>("list", Integer.class);
			List<Tuple2<Integer, Integer>> elements = new ArrayList<>(numberElements);
			List<Tuple2<Integer, Integer>> elements2 = new ArrayList<>(numberElements);
			Set<Integer> keyGroups = new HashSet<>();
			Map<Integer, List<Integer>> expected = new HashMap<>();

			int distinctGroups = 303;
			int distinctGroups2 = 442;

			long checkpointId = 1L;
			long timestamp = 1L;
			long recoveryTimestamp = 2L;
			long checkpointId2 = 3L;
			long timestamp2 = 3L;

			for (int element = 0; element < numberElements; element++) {
				int distinctGroup = element % distinctGroups;

				int keyGroupIndex = keyGroupAssigner.getKeyGroupIndex(distinctGroup);

				keyGroups.add(keyGroupIndex);

				elements.add(Tuple2.of(distinctGroup, element));

				List<Integer> list;

				if (expected.containsKey(distinctGroup)) {
					list = expected.get(distinctGroup);
				} else {
					list = new ArrayList<>();
					expected.put(distinctGroup, list);
				}

				list.add(element);
			}

			ListState<Integer> listState = backend.getPartitionedState(null, VoidSerializer.INSTANCE, listSD);

			for (Tuple2<Integer, Integer> element : elements) {
				backend.setCurrentKey(element.f0);
				listState.add(element.f1);
			}

			Map<Integer, PartitionedStateSnapshot> snapshot = backend.snapshotPartitionedState(checkpointId, timestamp);

			// verify original state
			for (Map.Entry<Integer, List<Integer>> entry : expected.entrySet()) {
				backend.setCurrentKey(entry.getKey());
				assertEquals(entry.getValue(), listState.get());
			}

			// verify snapshot
			assertEquals(keyGroups.size(), snapshot.size());

			for (Integer key : snapshot.keySet()) {
				assertTrue(keyGroups.contains(key));
			}

			backend2.restorePartitionedState(snapshot, recoveryTimestamp);

			// discard the state
			for (PartitionedStateSnapshot partitionedStateSnapshot : snapshot.values()) {
				partitionedStateSnapshot.discardState();
			}

			for (int element = 0; element < numberElements; element++) {
				int distinctGroup = element % distinctGroups2;

				int keyGroupIndex = keyGroupAssigner.getKeyGroupIndex(distinctGroup);

				keyGroups.add(keyGroupIndex);

				elements2.add(Tuple2.of(distinctGroup, element));

				List<Integer> list;

				if (expected.containsKey(distinctGroup)) {
					list = expected.get(distinctGroup);
				} else {
					list = new ArrayList<>();
					expected.put(distinctGroup, list);
				}

				list.add(element);
			}

			ListState<Integer> restoredListState = backend2.getPartitionedState(null, VoidSerializer.INSTANCE, listSD);

			for (Tuple2<Integer, Integer> element : elements2) {
				backend2.setCurrentKey(element.f0);
				restoredListState.add(element.f1);
			}

			Map<Integer, PartitionedStateSnapshot> snapshot2 = backend2.snapshotPartitionedState(checkpointId2, timestamp2);

			// verify original state
			for (Map.Entry<Integer, List<Integer>> entry : expected.entrySet()) {
				backend2.setCurrentKey(entry.getKey());
				assertEquals(entry.getValue(), restoredListState.get());
			}

			// verify snapshot
			assertEquals(keyGroups.size(), snapshot2.size());

			for (Integer key : snapshot2.keySet()) {
				assertTrue(keyGroups.contains(key));
			}
		}
	}

	/**
	 * Tests that the GenericKeyGroupStateBackend creates, snapshots and restores key groups
	 * properly for {@link org.apache.flink.api.common.state.ReducingState}.
	 *
	 * @throws Exception
	 */
	@Test
	public void testReducingState() throws Exception {
		HashKeyGroupAssigner<Integer> keyGroupAssigner = new HashKeyGroupAssigner<>(330);

		try (GenericKeyGroupStateBackend<Integer> backend = createStateBackend(keyGroupAssigner);
			GenericKeyGroupStateBackend<Integer> backend2 = createStateBackend(keyGroupAssigner)){

			int numberElements = 1001;
			int distinctGroups = 303;
			int distinctGroups2 = 442;

			ReducingStateDescriptor<String> reducingSD = new ReducingStateDescriptor<String>(
				"reduction",
				new AppendingReduce(),
				String.class);

			List<Tuple2<Integer, String>> elements = new ArrayList<>(numberElements);
			List<Tuple2<Integer, String>> elements2 = new ArrayList<>(numberElements);
			Set<Integer> keyGroups = new HashSet<>();
			Map<Integer, String> expected = new HashMap<>();

			long checkpointId = 1L;
			long timestamp = 1L;
			long recoveryTimestamp = 2L;
			long checkpointId2 = 3L;
			long timestamp2 = 3L;

			for (int element = 0; element < numberElements; element++) {
				int distinctGroup = element % distinctGroups;

				int keyGroupIndex = keyGroupAssigner.getKeyGroupIndex(distinctGroup);

				keyGroups.add(keyGroupIndex);

				elements.add(Tuple2.of(distinctGroup, "" + element));

				String reductionResult;

				if (expected.containsKey(distinctGroup)) {
					reductionResult = expected.get(distinctGroup) + "," + element;
					expected.put(distinctGroup, reductionResult);
				} else {
					reductionResult = "" + element;
					expected.put(distinctGroup, reductionResult);
				}
			}

			ReducingState<String> reducingState = backend.getPartitionedState(null, VoidSerializer.INSTANCE, reducingSD);

			for (Tuple2<Integer, String> element : elements) {
				backend.setCurrentKey(element.f0);
				reducingState.add(element.f1);
			}

			Map<Integer, PartitionedStateSnapshot> snapshot = backend.snapshotPartitionedState(checkpointId, timestamp);

			// verify original state
			for (Map.Entry<Integer, String> entry : expected.entrySet()) {
				backend.setCurrentKey(entry.getKey());
				assertEquals(entry.getValue(), reducingState.get());
			}

			// verify snapshot
			assertEquals(keyGroups.size(), snapshot.size());

			for (Integer key : snapshot.keySet()) {
				assertTrue(keyGroups.contains(key));
			}

			backend2.restorePartitionedState(snapshot, recoveryTimestamp);

			// discard the state
			for (PartitionedStateSnapshot partitionedStateSnapshot : snapshot.values()) {
				partitionedStateSnapshot.discardState();
			}

			for (int element = 0; element < numberElements; element++) {
				int distinctGroup = element % distinctGroups2;

				int keyGroupIndex = keyGroupAssigner.getKeyGroupIndex(distinctGroup);

				keyGroups.add(keyGroupIndex);

				elements2.add(Tuple2.of(distinctGroup, "" + element));

				String reductionResult;

				if (expected.containsKey(distinctGroup)) {
					reductionResult = expected.get(distinctGroup) + "," + element;
					expected.put(distinctGroup, reductionResult);
				} else {
					reductionResult = "" + element;
					expected.put(distinctGroup, reductionResult);
				}
			}

			ReducingState<String> restoredListState = backend2.getPartitionedState(null, VoidSerializer.INSTANCE, reducingSD);

			for (Tuple2<Integer, String> element : elements2) {
				backend2.setCurrentKey(element.f0);
				restoredListState.add(element.f1);
			}

			Map<Integer, PartitionedStateSnapshot> snapshot2 = backend2.snapshotPartitionedState(checkpointId2, timestamp2);

			// verify original state
			for (Map.Entry<Integer, String> entry : expected.entrySet()) {
				backend2.setCurrentKey(entry.getKey());
				assertEquals(entry.getValue(), restoredListState.get());
			}

			// verify snapshot
			assertEquals(keyGroups.size(), snapshot2.size());

			for (Integer key : snapshot2.keySet()) {
				assertTrue(keyGroups.contains(key));
			}
		}
	}

	/**
	 * Tests that the GenericKeyGroupStateBackend creates, snapshots and restores key groups
	 * properly for {@link org.apache.flink.api.common.state.FoldingState}.
	 *
	 * @throws Exception
	 */
	@Test
	public void testFoldingState() throws Exception {
		KeyGroupAssigner<Integer> keyGroupAssigner = new HashKeyGroupAssigner<>(42);

		try (GenericKeyGroupStateBackend<Integer> backend = createStateBackend(keyGroupAssigner);
			GenericKeyGroupStateBackend<Integer> backend2= createStateBackend(keyGroupAssigner)) {

			int numberElements = 1001;
			int distinctGroups = 303;
			int distinctGroups2 = 442;

			FoldingStateDescriptor<Integer, String> foldingSD = new FoldingStateDescriptor<Integer, String>(
				"reduction",
				"initialValue:",
				new AppendingFold(),
				String.class);

			List<Tuple2<Integer, Integer>> elements = new ArrayList<>(numberElements);
			List<Tuple2<Integer, Integer>> elements2 = new ArrayList<>(numberElements);
			Set<Integer> keyGroups = new HashSet<>();
			Map<Integer, String> expected = new HashMap<>();

			long checkpointId = 1L;
			long timestamp = 1L;
			long recoveryTimestamp = 2L;
			long checkpointId2 = 3L;
			long timestamp2 = 3L;

			for (int element = 0; element < numberElements; element++) {
				int distinctGroup = element % distinctGroups;

				int keyGroupIndex = keyGroupAssigner.getKeyGroupIndex(distinctGroup);

				keyGroups.add(keyGroupIndex);

				elements.add(Tuple2.of(distinctGroup, element));

				String reductionResult;

				if (expected.containsKey(distinctGroup)) {
					reductionResult = expected.get(distinctGroup) + "," + element;
					expected.put(distinctGroup, reductionResult);
				} else {
					reductionResult = "initialValue:," + element;
					expected.put(distinctGroup, reductionResult);
				}
			}

			FoldingState<Integer, String> reducingState = backend.getPartitionedState(null, VoidSerializer.INSTANCE, foldingSD);

			for (Tuple2<Integer, Integer> element : elements) {
				backend.setCurrentKey(element.f0);
				reducingState.add(element.f1);
			}

			Map<Integer, PartitionedStateSnapshot> snapshot = backend.snapshotPartitionedState(checkpointId, timestamp);

			// verify original state
			for (Map.Entry<Integer, String> entry : expected.entrySet()) {
				backend.setCurrentKey(entry.getKey());
				assertEquals(entry.getValue(), reducingState.get());
			}

			// verify snapshot
			assertEquals(keyGroups.size(), snapshot.size());

			for (Integer key : snapshot.keySet()) {
				assertTrue(keyGroups.contains(key));
			}

			backend2.restorePartitionedState(snapshot, recoveryTimestamp);

			// discard the state
			for (PartitionedStateSnapshot partitionedStateSnapshot : snapshot.values()) {
				partitionedStateSnapshot.discardState();
			}

			for (int element = 0; element < numberElements; element++) {
				int distinctGroup = element % distinctGroups2;

				int keyGroupIndex = keyGroupAssigner.getKeyGroupIndex(distinctGroup);

				keyGroups.add(keyGroupIndex);

				elements2.add(Tuple2.of(distinctGroup, element));

				String reductionResult;

				if (expected.containsKey(distinctGroup)) {
					reductionResult = expected.get(distinctGroup) + "," + element;
					expected.put(distinctGroup, reductionResult);
				} else {
					reductionResult = "initialValue:," + element;
					expected.put(distinctGroup, reductionResult);
				}
			}

			FoldingState<Integer, String> restoredListState = backend2.getPartitionedState(null, VoidSerializer.INSTANCE, foldingSD);

			for (Tuple2<Integer, Integer> element : elements2) {
				backend2.setCurrentKey(element.f0);
				restoredListState.add(element.f1);
			}

			Map<Integer, PartitionedStateSnapshot> snapshot2 = backend2.snapshotPartitionedState(checkpointId2, timestamp2);

			// verify original state
			for (Map.Entry<Integer, String> entry : expected.entrySet()) {
				backend2.setCurrentKey(entry.getKey());
				assertEquals(entry.getValue(), restoredListState.get());
			}

			// verify snapshot
			assertEquals(keyGroups.size(), snapshot2.size());

			for (Integer key : snapshot2.keySet()) {
				assertTrue(keyGroups.contains(key));
			}
		}
	}

	private static class AppendingReduce implements ReduceFunction<String> {
		private static final long serialVersionUID = 6860242820044380228L;

		@Override
		public String reduce(String value1, String value2) throws Exception {
			return value1 + "," + value2;
		}
	}

	private static class AppendingFold implements FoldFunction<Integer, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String fold(String acc, Integer value) throws Exception {
			return acc + "," + value;
		}
	}
}
