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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.state.keyed.KeyedMapState;
import org.apache.flink.runtime.state.keyed.KeyedMapStateDescriptor;
import org.apache.flink.runtime.state.keyed.KeyedValueState;
import org.apache.flink.runtime.state.keyed.KeyedValueStateDescriptor;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.util.FutureUtil;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.RunnableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * The base unit tests to validate that states can be correctly saved
 * and restored in the stateBackend.
 */
public abstract class InternalStateCheckpointTestBase extends TestLogger {

	@Rule
	public TemporaryFolder tmpFolder = new TemporaryFolder();

	private CheckpointOptions checkpointOptions;

	protected InternalStateBackend stateBackend;

	protected CheckpointStreamFactory checkpointStreamFactory;

	protected static int maxParallelism;

	protected int initParallelism;

	protected int initSubtaskIndex;

	protected ClassLoader classLoader;

	protected LocalRecoveryConfig localRecoveryConfig;

	protected ExecutionConfig executionConfig;

	/**
	 * Creates a new state stateBackend for testing.
	 *
	 * @return A new state stateBackend for testing.
	 */
	protected abstract InternalStateBackend createStateBackend(
		int numberOfGroups,
		KeyGroupRange keyGroupRange,
		ClassLoader userClassLoader,
		LocalRecoveryConfig localRecoveryConfig,
		ExecutionConfig executionConfig) throws Exception;

	protected abstract CheckpointType getCheckpointType();

	@Before
	public void open() throws Exception {
		checkpointOptions = getCheckpointType().equals(CheckpointType.CHECKPOINT) ?
			CheckpointOptions.forCheckpointWithDefaultLocation() :
			new CheckpointOptions(CheckpointType.SAVEPOINT, new CheckpointStorageLocationReference(tmpFolder.newFolder().toURI().toString().getBytes(Charset.defaultCharset())));

		checkpointStreamFactory = new MemCheckpointStreamFactory(4 * 1024 * 1024);

		maxParallelism = 10;
		initParallelism = 1;
		initSubtaskIndex = 0;
		classLoader = ClassLoader.getSystemClassLoader();
		localRecoveryConfig = TestLocalRecoveryConfig.disabled();
		executionConfig = new ExecutionConfig();
		executionConfig.setUseSnapshotCompression(true);

		stateBackend = createStateBackend(
			maxParallelism,
			getGroupsForSubtask(maxParallelism, initParallelism, initSubtaskIndex),
			classLoader,
			localRecoveryConfig,
			executionConfig);
		stateBackend.restore(null);
	}

	@After
	public void dispose() {
		if (stateBackend != null) {
			stateBackend.dispose();
		}
	}

	@Test
	public void testCheckpointWithEmptyStateBackend() throws Exception {

		RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotFuture =
			stateBackend.snapshot(0, 0, checkpointStreamFactory, checkpointOptions);
		assertNotNull(snapshotFuture);

		KeyedStateHandle snapshot = runSnapshot(snapshotFuture);
		assertNull(snapshot);
		stateBackend.dispose();

		stateBackend = createStateBackend(
			maxParallelism,
			getGroupsForSubtask(maxParallelism, initParallelism, initSubtaskIndex),
			classLoader,
			localRecoveryConfig,
			executionConfig);
		stateBackend.restore(null);

		assertTrue(stateBackend.getKeyedStates().isEmpty());

		assertTrue(stateBackend.getSubKeyedStates().isEmpty());
	}

	@Test
	public void testCheckpointWithEmptyState() throws Exception {
		// test empty backend with empty state.
		KeyedValueStateDescriptor<Integer, Float> descriptor =
			new KeyedValueStateDescriptor<>(
				"state1",
				IntSerializer.INSTANCE,
				FloatSerializer.INSTANCE
			);
		stateBackend.getKeyedState(descriptor);

		assertEquals(1, stateBackend.getKeyedStates().size());

		RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotFuture =
			stateBackend.snapshot(0, 0, checkpointStreamFactory, checkpointOptions);
		assertNotNull(snapshotFuture);

		KeyedStateHandle snapshot = runSnapshot(snapshotFuture);
		assertNotNull(snapshot);
		stateBackend.dispose();

		stateBackend = createStateBackend(
			maxParallelism,
			getGroupsForSubtask(maxParallelism, initParallelism, initSubtaskIndex),
			classLoader,
			localRecoveryConfig,
			executionConfig);
		stateBackend.restore(Collections.singleton(snapshot));

		assertEquals(1, stateBackend.getStateStorages().size());
		assertEquals(0, stateBackend.getKeyedStates().size());

		KeyedValueState<Integer, Float> keyedState = stateBackend.getKeyedState(descriptor);
		assertFalse(keyedState.keys().iterator().hasNext());
	}

	@Test
	public void testCheckpointWithoutParallelismChange() throws Exception {
		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
		Random random = new Random(System.currentTimeMillis());

		KeyedValueStateDescriptor<Integer, Float> descriptor1 =
			new KeyedValueStateDescriptor<>(
				"state1",
				IntSerializer.INSTANCE,
				FloatSerializer.INSTANCE
			);
		KeyedValueState<Integer, Float> valueState = stateBackend.getKeyedState(descriptor1);

		Map<Integer, Float> valueStateMap = new HashMap<>();
		for (int i = 0; i < 1000; i++) {
			float value = random.nextFloat();
			valueState.put(i, value);
			valueStateMap.put(i, value);
		}

		KeyedMapStateDescriptor<Integer, Integer, Float> descriptor2 =
			new KeyedMapStateDescriptor<>(
				"state2",
				IntSerializer.INSTANCE,
				IntSerializer.INSTANCE,
				FloatSerializer.INSTANCE
			);
		KeyedMapState<Integer, Integer, Float> mapState = stateBackend.getKeyedState(descriptor2);

		Map<Integer, Map<Integer, Float>> mapStateMap = new HashMap<>();
		for (int i = 0; i < 1000; i++) {
			int key = i % 10;
			int mkey = i / 10;
			float value = random.nextFloat();
			mapState.add(key, mkey, value);
			Map<Integer, Float> map = mapStateMap.computeIfAbsent(key, K -> new HashMap<>());
			map.put(mkey, value);
		}

		KeyedValueStateDescriptor<Integer, Float> descriptor3 =
			new KeyedValueStateDescriptor<>(
				"state3",
				IntSerializer.INSTANCE,
				FloatSerializer.INSTANCE
			);
		KeyedValueState<Integer, Float> emptyState = stateBackend.getKeyedState(descriptor3);

		// Takes a snapshot of the states
		KeyedStateHandle snapshot1 =
			runSnapshot(stateBackend, 0, 0, checkpointStreamFactory, checkpointOptions, sharedStateRegistry);

		// Does some updates to the states
		int index = 0;
		for (int i = 0; i < 1000; ++i) {
			if (index % 3 == 0) {
				valueState.put(i, random.nextFloat());
				mapState.add(i % 10, i / 10, random.nextFloat());
			}
			index++;
		}

		stateBackend.dispose();

		// Restores the stateBackend from the snapshot
		executionConfig.setUseSnapshotCompression(false);
		stateBackend = createStateBackend(
			maxParallelism,
			getGroupsForSubtask(maxParallelism, initParallelism, initSubtaskIndex),
			classLoader,
			localRecoveryConfig,
			executionConfig);
		stateBackend.restore(Collections.singleton(snapshot1));

		valueState = stateBackend.getKeyedState(descriptor1);
		assertNotNull(valueState);

		mapState = stateBackend.getKeyedState(descriptor2);
		assertNotNull(mapState);

		emptyState = stateBackend.getKeyedState(descriptor3);
		assertNotNull(emptyState);

		// Validates that the states are correctly restored.
		validateValueStateData(valueStateMap, valueState);
		validateMapStateData(mapStateMap, mapState);
		validateValueStateData(Collections.emptyMap(), emptyState);

		// Does some updates to the states

		index = 0;
		for (int i = 200; i < 1200; ++i) {
			if (index % 4 == 0) {
				float value1 = random.nextFloat();
				valueState.put(i, value1);
				valueStateMap.put(i, value1);

				int key2 = i / 10;
				int mkey = i % 10;
				float value2 = random.nextFloat();
				mapState.add(key2, mkey, value2);
				mapStateMap.computeIfAbsent(key2, K -> new HashMap<>()).put(mkey, value2);
			}

			index++;
		}

		// Takes a snapshot of the states
		KeyedStateHandle snapshot2 =
			runSnapshot(stateBackend, 1, 1, checkpointStreamFactory, checkpointOptions, sharedStateRegistry);

		index = 0;
		for (int i = 300; i < 1300; ++i) {
			if (index % 5 == 0) {
				valueState.remove(i);

				mapState.remove(i / 10, i % 10);
			}

			index++;
		}

		stateBackend.dispose();

		// Restores the stateBackend from the snapshot
		executionConfig.setUseSnapshotCompression(true);
		stateBackend = createStateBackend(
			maxParallelism,
			getGroupsForSubtask(maxParallelism, initParallelism, initSubtaskIndex),
			classLoader,
			localRecoveryConfig,
			executionConfig);

		stateBackend.restore(Collections.singleton(snapshot2));

		// Validates that the states are correctly restored.
		valueState = stateBackend.getKeyedState(descriptor1);
		assertNotNull(valueState);

		mapState = stateBackend.getKeyedState(descriptor2);
		assertNotNull(mapState);

		emptyState = stateBackend.getKeyedState(descriptor3);
		assertNotNull(emptyState);

		// Validates that the states are correctly restored.
		validateValueStateData(valueStateMap, valueState);
		validateMapStateData(mapStateMap, mapState);
		validateValueStateData(Collections.emptyMap(), emptyState);
	}

	/**
	 * Test checkpoint and restore when parallelism changed, from 1 -> 3 -> 2.
	 */
	@Test
	public void testCheckpointWithParallelismChange() throws Exception {
		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
		Random random = new Random(System.currentTimeMillis());

		KeyedValueStateDescriptor<Integer, Float> descriptor1 =
			new KeyedValueStateDescriptor<>(
				"state1",
				IntSerializer.INSTANCE,
				FloatSerializer.INSTANCE
			);
		KeyedValueState<Integer, Float> valueState = stateBackend.getKeyedState(descriptor1);

		Map<Integer, Map<Integer, Float>> groupValueStateMap = new HashMap<>();
		for (int i = 0; i < 1000; i++) {
			float value = random.nextFloat();
			valueState.put(i, value);
			int group = getGroupForKey(i);
			Map<Integer, Float> valueStateMap = groupValueStateMap.computeIfAbsent(group, K -> new HashMap<>());
			valueStateMap.put(i, value);
		}

		KeyedMapStateDescriptor<Integer, Integer, Float> descriptor2 =
			new KeyedMapStateDescriptor<>(
				"state2",
				IntSerializer.INSTANCE,
				IntSerializer.INSTANCE,
				FloatSerializer.INSTANCE
			);
		KeyedMapState<Integer, Integer, Float> mapState = stateBackend.getKeyedState(descriptor2);

		Map<Integer, Map<Integer, Map<Integer, Float>>> groupMapStateMap = new HashMap<>();
		for (int i = 0; i < 1000; i++) {
			int key = i % 10;
			int mkey = i / 10;
			float value = random.nextFloat();
			mapState.add(key, mkey, value);
			int group = KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism);
			Map<Integer, Map<Integer, Float>> mapStateMap = groupMapStateMap.computeIfAbsent(group, K -> new HashMap<>());
			Map<Integer, Float> map = mapStateMap.computeIfAbsent(key, K -> new HashMap<>());
			map.put(mkey, value);
		}

		KeyedValueStateDescriptor<Integer, Float> descriptor3 =
			new KeyedValueStateDescriptor<>(
				"state3",
				IntSerializer.INSTANCE,
				FloatSerializer.INSTANCE
			);
		KeyedValueState<Integer, Float> emptyState = stateBackend.getKeyedState(descriptor3);

		// Takes a snapshot of the states
		KeyedStateHandle snapshot1 =
			runSnapshot(stateBackend, 0, 0, checkpointStreamFactory, checkpointOptions, sharedStateRegistry);

		// Does some updates to the states
		int index = 0;
		for (int i = 0; i < 1000; ++i) {
			if (index % 3 == 0) {
				valueState.put(i, random.nextFloat());
				mapState.add(i % 10, i / 10, random.nextFloat());
			}
			index++;
		}

		stateBackend.dispose();

		// Restores the stateBackend from a subset of the snapshot
		KeyGroupRange firstGroups1 = getGroupsForSubtask(maxParallelism, 3, 0);
		KeyedStateHandle firstSnapshot1 = snapshot1.getIntersection(firstGroups1);

		// Restores the stateBackend from the snapshot
		executionConfig.setUseSnapshotCompression(false);
		stateBackend = createStateBackend(maxParallelism, firstGroups1, classLoader, localRecoveryConfig, executionConfig);
		stateBackend.restore(Collections.singleton(firstSnapshot1));

		// Validates that the states are correctly restored.
		valueState = stateBackend.getKeyedState(descriptor1);
		assertNotNull(valueState);

		mapState = stateBackend.getKeyedState(descriptor2);
		assertNotNull(mapState);

		emptyState = stateBackend.getKeyedState(descriptor3);
		assertNotNull(emptyState);

		Map<Integer, Float> expectedFirstGroup1ValueStateData = new HashMap<>();
		getDataWithGroupSet(groupValueStateMap, firstGroups1, expectedFirstGroup1ValueStateData);
		validateValueStateData(expectedFirstGroup1ValueStateData, valueState);

		Map<Integer, Map<Integer, Float>> expectedFirstGroup1MapStateData = new HashMap<>();
		getDataWithGroupSet(groupMapStateMap, firstGroups1, expectedFirstGroup1MapStateData);
		validateMapStateData(expectedFirstGroup1MapStateData, mapState);

		// Does some updates to the stateBackend.
		index = 0;
		for (int i = 200; i < 1200; ++i) {
			if (index % 2 == 0) {
				int key1 = i;
				if (isGroupContainsKey(firstGroups1, key1)) {
					float value1 = random.nextFloat();
					valueState.put(key1, value1);
					int group = getGroupForKey(key1);
					Map<Integer, Float> map = groupValueStateMap.computeIfAbsent(group, K -> new HashMap<>());
					map.put(key1, value1);
				}

				int key2 = i % 10;
				if (isGroupContainsKey(firstGroups1, key2)) {
					int mkey = i / 10;
					float value2 = random.nextFloat();
					mapState.add(key2, mkey, value2);
					int group = getGroupForKey(key2);
					Map<Integer, Map<Integer, Float>> mapStateMap = groupMapStateMap.computeIfAbsent(group, K -> new HashMap<>());
					Map<Integer, Float> map = mapStateMap.computeIfAbsent(key2, K -> new HashMap<>());
					map.put(mkey, value2);
				}
			}

			index++;
		}

		// Takes a snapshot of the stateBackend
		KeyedStateHandle firstSnapshot2 =
			runSnapshot(stateBackend, 1, 1, checkpointStreamFactory, checkpointOptions, sharedStateRegistry);

		stateBackend.dispose();

		// Restores the stateBackend from a subset of the snapshot
		KeyGroupRange secondGroups1 = getGroupsForSubtask(maxParallelism, 3, 1);
		KeyedStateHandle secondSnapshot1 = snapshot1.getIntersection(secondGroups1);

		// Restores the stateBackend from the snapshot
		stateBackend = createStateBackend(maxParallelism, secondGroups1, classLoader, localRecoveryConfig, executionConfig);
		stateBackend.restore(Collections.singleton(secondSnapshot1));

		// Validates that the states are correctly restored.
		valueState = stateBackend.getKeyedState(descriptor1);
		assertNotNull(valueState);

		mapState = stateBackend.getKeyedState(descriptor2);
		assertNotNull(mapState);

		emptyState = stateBackend.getKeyedState(descriptor3);
		assertNotNull(emptyState);

		Map<Integer, Float> expectedSecondGroup1ValueStateData = new HashMap<>();
		getDataWithGroupSet(groupValueStateMap, secondGroups1, expectedSecondGroup1ValueStateData);
		validateValueStateData(expectedSecondGroup1ValueStateData, valueState);

		Map<Integer, Map<Integer, Float>> expectedSecondGroup1MapStateData = new HashMap<>();
		getDataWithGroupSet(groupMapStateMap, secondGroups1, expectedSecondGroup1MapStateData);
		validateMapStateData(expectedSecondGroup1MapStateData, mapState);

		// Does some updates to the stateBackend.
		index = 0;
		for (int i = 200; i < 1200; ++i) {
			if (index % 3 == 0) {
				int key1 = i;
				if (isGroupContainsKey(secondGroups1, key1)) {
					float value1 = random.nextFloat();
					valueState.put(key1, value1);
					int group = getGroupForKey(key1);
					Map<Integer, Float> map = groupValueStateMap.computeIfAbsent(group, K -> new HashMap<>());
					map.put(key1, value1);
				}

				int key2 = i % 10;
				if (isGroupContainsKey(secondGroups1, key2)) {
					int mkey = i / 10;
					float value2 = random.nextFloat();
					mapState.add(key2, mkey, value2);
					int group = getGroupForKey(key2);
					Map<Integer, Map<Integer, Float>> mapStateMap = groupMapStateMap.computeIfAbsent(group, K -> new HashMap<>());
					Map<Integer, Float> map = mapStateMap.computeIfAbsent(key2, K -> new HashMap<>());
					map.put(mkey, value2);
				}
			}

			index++;
		}

		// Takes a snapshot of the stateBackend
		KeyedStateHandle secondSnapshot2 =
			runSnapshot(stateBackend, 1, 1, checkpointStreamFactory, checkpointOptions, sharedStateRegistry);

		stateBackend.dispose();

		// Restores the stateBackend from a subset of the snapshot
		KeyGroupRange thirdGroups1 = getGroupsForSubtask(maxParallelism, 3, 2);
		KeyedStateHandle thirdSnapshot1 = snapshot1.getIntersection(thirdGroups1);

		// Restores the stateBackend from the snapshot
		executionConfig.setUseSnapshotCompression(true);
		stateBackend = createStateBackend(maxParallelism, thirdGroups1, classLoader, localRecoveryConfig, executionConfig);
		stateBackend.restore(Collections.singleton(thirdSnapshot1));

		// Validates that the states are correctly restored.
		valueState = stateBackend.getKeyedState(descriptor1);
		assertNotNull(valueState);

		mapState = stateBackend.getKeyedState(descriptor2);
		assertNotNull(mapState);

		emptyState = stateBackend.getKeyedState(descriptor3);
		assertNotNull(emptyState);

		Map<Integer, Float> expectedThirdGroup1ValueStateData = new HashMap<>();
		getDataWithGroupSet(groupValueStateMap, thirdGroups1, expectedThirdGroup1ValueStateData);
		validateValueStateData(expectedThirdGroup1ValueStateData, valueState);

		Map<Integer, Map<Integer, Float>> expectedThirdGroup1MapStateData = new HashMap<>();
		getDataWithGroupSet(groupMapStateMap, thirdGroups1, expectedThirdGroup1MapStateData);
		validateMapStateData(expectedThirdGroup1MapStateData, mapState);

		// Does some updates to the stateBackend.
		index = 0;
		for (int i = 200; i < 1200; ++i) {
			if (index % 4 == 0) {
				int key1 = i;
				if (isGroupContainsKey(thirdGroups1, key1)) {
					float value1 = random.nextFloat();
					valueState.put(key1, value1);
					int group = getGroupForKey(key1);
					Map<Integer, Float> map = groupValueStateMap.computeIfAbsent(group, K -> new HashMap<>());
					map.put(key1, value1);
				}

				int key2 = i % 10;
				if (isGroupContainsKey(thirdGroups1, key2)) {
					int mkey = i / 10;
					float value2 = random.nextFloat();
					mapState.add(key2, mkey, value2);
					int group = getGroupForKey(key2);
					Map<Integer, Map<Integer, Float>> mapStateMap = groupMapStateMap.computeIfAbsent(group, K -> new HashMap<>());
					Map<Integer, Float> map = mapStateMap.computeIfAbsent(key2, K -> new HashMap<>());
					map.put(mkey, value2);
				}
			}

			index++;
		}

		// Takes a snapshot of the stateBackend
		KeyedStateHandle thirdSnapshot2 =
			runSnapshot(stateBackend, 1, 1, checkpointStreamFactory, checkpointOptions, sharedStateRegistry);

		stateBackend.dispose();

		// Merge the local states
		KeyGroupRange leftGroups3 = getGroupsForSubtask(maxParallelism, 2, 0);
		KeyGroupRange rightGroups3 = getGroupsForSubtask(maxParallelism, 2, 1);
		executionConfig.setUseSnapshotCompression(false);
		InternalStateBackend newLeftBackend = createStateBackend(maxParallelism, leftGroups3, classLoader, localRecoveryConfig, executionConfig);
		InternalStateBackend newRightBackend = createStateBackend(maxParallelism, rightGroups3, classLoader, localRecoveryConfig, executionConfig);

		try {
			KeyedStateHandle firstSnapshot3 = firstSnapshot2.getIntersection(leftGroups3);
			KeyedStateHandle secondSnapshot3ForLeft = secondSnapshot2.getIntersection(leftGroups3);
			KeyedStateHandle secondSnapshot3ForRight = secondSnapshot2.getIntersection(rightGroups3);
			KeyedStateHandle thirdSnapshot3 = thirdSnapshot2.getIntersection(rightGroups3);

			newLeftBackend.restore(Arrays.asList(firstSnapshot3, secondSnapshot3ForLeft));
			newRightBackend.restore(Arrays.asList(secondSnapshot3ForRight, thirdSnapshot3));

			// Validates that the states are correctly restored.
			KeyedValueState<Integer, Float> leftValueState1 = newLeftBackend.getKeyedState(descriptor1);
			assertNotNull(leftValueState1);

			KeyedValueState<Integer, Float> rightValueState1 = newRightBackend.getKeyedState(descriptor1);
			assertNotNull(rightValueState1);

			KeyedMapState<Integer, Integer, Float> leftMapState2 = newLeftBackend.getKeyedState(descriptor2);
			assertNotNull(leftMapState2);

			KeyedMapState<Integer, Integer, Float> rightMapState2 = newRightBackend.getKeyedState(descriptor2);
			assertNotNull(rightMapState2);

			emptyState = newLeftBackend.getKeyedState(descriptor3);
			assertNotNull(emptyState);

			emptyState = newRightBackend.getKeyedState(descriptor3);
			assertNotNull(emptyState);

			Map<Integer, Float> expectedLeftValueStateData = new HashMap<>();
			getDataWithGroupSet(groupValueStateMap, leftGroups3, expectedLeftValueStateData);
			validateValueStateData(expectedLeftValueStateData, leftValueState1);

			Map<Integer, Map<Integer, Float>> expectedLeftMapStateData = new HashMap<>();
			getDataWithGroupSet(groupMapStateMap, leftGroups3, expectedLeftMapStateData);
			validateMapStateData(expectedLeftMapStateData, leftMapState2);

			Map<Integer, Float> expectedRightValueStateData = new HashMap<>();
			getDataWithGroupSet(groupValueStateMap, rightGroups3, expectedRightValueStateData);
			validateValueStateData(expectedRightValueStateData, rightValueState1);

			Map<Integer, Map<Integer, Float>> expectedRightMapStateData = new HashMap<>();
			getDataWithGroupSet(groupMapStateMap, rightGroups3, expectedRightMapStateData);
			validateMapStateData(expectedRightMapStateData, rightMapState2);

		} finally {
			newLeftBackend.dispose();
			newRightBackend.dispose();
		}
	}

	//--------------------------------------------------------------------------

	private boolean isGroupContainsKey(KeyGroupRange groups, int key) {
		return groups.contains(KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism));
	}

	private static int getGroupForKey(int key) {
		return KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism);
	}

	private KeyGroupRange getGroupsForSubtask(int maxParallelism, int parallelism, int subtaskIndex) {
		return KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(maxParallelism, parallelism, subtaskIndex);
	}

	private static void validateValueStateData(Map<Integer, Float> expectedData, KeyedValueState<Integer, Float> valueState) {
		Map<Integer, Float> actualData = valueState.getAll();

		if (expectedData == null || expectedData.isEmpty()) {
			assertTrue(actualData.isEmpty());
			return;
		}

		assertEquals(expectedData.size(), actualData.size());
		for (Map.Entry<Integer, Float> entry : actualData.entrySet()) {
			assertEquals(expectedData.get(entry.getKey()), entry.getValue());
		}
	}

	private static void validateMapStateData(Map<Integer, Map<Integer, Float>> expectedData, KeyedMapState<Integer, Integer, Float> mapState) {
		Map<Integer, Map<Integer, Float>> actualData = mapState.getAll();

		if (expectedData == null || expectedData.isEmpty()) {
			assertTrue(actualData.isEmpty());
			return;
		}

		assertEquals(expectedData.size(), actualData.size());
		for (Map.Entry<Integer, Map<Integer, Float>> entry : actualData.entrySet()) {
			int key = entry.getKey();
			Map<Integer, Float> actualMap = entry.getValue();
			Map<Integer, Float> expectedMap = expectedData.get(key);
			if (expectedMap == null || expectedMap.isEmpty()) {
				assertTrue(actualMap.isEmpty());
			} else {
				assertEquals(expectedMap.size(), actualMap.size());
				for (Map.Entry<Integer, Float> e : actualMap.entrySet()) {
					assertEquals(expectedMap.get(e.getKey()), e.getValue());
				}
			}
		}
	}

	private static<K, V> void getDataWithGroupSet(
		Map<Integer, Map<K, V>> groupMaps,
		KeyGroupRange keyGroupRange,
		Map<K, V> returnedMap
	) {
		for (Integer group : keyGroupRange) {
			Map<K, V> data = groupMaps.get(group);
			if (data != null) {
				returnedMap.putAll(data);
			}
		}
	}

	private static KeyedStateHandle runSnapshot(
		InternalStateBackend stateBackend,
		long checkpointId,
		long checkpointTimestamp,
		CheckpointStreamFactory checkpointStreamFactory,
		CheckpointOptions checkpointOptions,
		SharedStateRegistry sharedStateRegistry
	) throws Exception {

		RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotFuture =
			stateBackend.snapshot(checkpointId, checkpointTimestamp, checkpointStreamFactory, checkpointOptions);

		KeyedStateHandle keyedStateHandle = runSnapshot(snapshotFuture);

		// Register the snapshot at the registry to replace the place holders with actual handles.
		if (checkpointOptions.getCheckpointType().equals(CheckpointType.CHECKPOINT)) {
			keyedStateHandle.registerSharedStates(sharedStateRegistry);
		}

		return keyedStateHandle;
	}

	private static KeyedStateHandle runSnapshot(
		RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotRunnableFuture) throws Exception {
		SnapshotResult<KeyedStateHandle> snapshotResult =
			FutureUtil.runIfNotDoneAndGet(snapshotRunnableFuture);
		return snapshotResult == null ? null : snapshotResult.getJobManagerOwnedSnapshot();
	}

}
