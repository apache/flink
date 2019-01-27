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

package org.apache.flink.runtime.state.keyed;

import org.apache.flink.api.common.functions.NaturalComparator;
import org.apache.flink.api.common.typeutils.BytewiseComparator;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.StateAccessException;
import org.apache.flink.runtime.state.TestLocalRecoveryConfig;
import org.apache.flink.runtime.state.AbstractInternalStateBackend;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link KeyedSortedMapState}.
 */
public abstract class KeyedSortedMapStateTestBase {

	protected AbstractInternalStateBackend backend;

	/**
	 * Creates a new state backend for testing.
	 *
	 * @return A new state backend for testing.
	 */
	protected abstract AbstractInternalStateBackend createStateBackend(
		int numberOfGroups,
		KeyGroupRange keyGroupRange,
		ClassLoader userClassLoader,
		LocalRecoveryConfig localRecoveryConfig) throws Exception;

	@Before
	public void openStateBackend() throws Exception {
		backend = createStateBackend(
			10,
			getGroupsForSubtask(10, 1, 0),
			ClassLoader.getSystemClassLoader(),
			TestLocalRecoveryConfig.disabled()
		);

		backend.restore(null);
	}

	@After
	public void closeStateBackend() {
		if (backend != null) {
			backend.dispose();
		}
	}

	@Test
	public void testKeyAccess() throws Exception {
		KeyedSortedMapStateDescriptor<Integer, Integer, Float> descriptor =
			new KeyedSortedMapStateDescriptor<>("keyAccess", IntSerializer.INSTANCE,
				new NaturalComparator<>(), IntSerializer.INSTANCE, FloatSerializer.INSTANCE);

		KeyedSortedMapState<Integer, Integer, Float> state = backend.getKeyedState(descriptor);
		assertNotNull(state);

		Random random = new Random();
		Map<Integer, SortedMap<Integer, Float>> pairs = new HashMap<>();
		for (int i = 0; i < 10; ++i) {
			int numMappings = random.nextInt(19) + 1;

			SortedMap<Integer, Float> mappings = new TreeMap<>();
			for (int j = 0; j < numMappings; j++) {
				Float mapValue = random.nextFloat();
				mappings.put(j, mapValue);
			}

			pairs.put(i, mappings);
		}

		// Validates that no map exists in the state when the state is empty.
		for (Integer key : pairs.keySet()) {
			assertFalse(state.contains(key));
			assertNull(state.get(key));
			assertEquals(Collections.emptySortedMap(), state.getOrDefault(key, Collections.emptySortedMap()));
		}

		// Adds some lists into the state and validates that they can be
		// correctly retrieved.
		for (Map.Entry<Integer, SortedMap<Integer, Float>> pair : pairs.entrySet()) {
			Integer key = pair.getKey();
			Map<Integer, Float> mappings = pair.getValue();
			state.addAll(key, mappings);
		}

		for (Map.Entry<Integer, SortedMap<Integer, Float>> pair : pairs.entrySet()) {
			assertTrue(state.contains(pair.getKey()));

			Map<Integer, Float> expectedValue = pair.getValue();
			assertEquals(expectedValue, state.get(pair.getKey()));
			assertEquals(expectedValue, state.getOrDefault(pair.getKey(), Collections.emptySortedMap()));
		}

		assertFalse(state.contains(null));
		assertNull(state.get(null));

		// Removes some maps from the state and validates that they do not
		// exist in the state any more.

		Set<Integer> removedKeys = new HashSet<>();

		int index = 0;
		for (Integer key : pairs.keySet()) {
			if (key == 0 || index % key == 0) {
				removedKeys.add(key);
			}

			index++;
		}

		removedKeys.add(null);
		removedKeys.add(11111);

		for (Integer removedKey : removedKeys) {
			state.remove(removedKey);
		}

		for (Map.Entry<Integer, SortedMap<Integer, Float>> pair : pairs.entrySet()) {
			Map<Integer, Float> value = state.get(pair.getKey());
			if (removedKeys.contains(pair.getKey())) {
				assertNull(value);
			} else {
				assertEquals(pair.getValue(), value);
			}
		}

		pairs.keySet().removeAll(removedKeys);

		// Adds more pairs into the state and validates that the values of the
		// pairs can be correctly retrieved.

		Map<Integer, SortedMap<Integer, Float>> addedPairs = new HashMap<>();
		for (int i = 5; i < 15; ++i) {
			int numMappings = random.nextInt(19) + 1;
			SortedMap<Integer, Float> addedMappings = new TreeMap<>();
			for (int j = 0; j < numMappings; ++j) {
				addedMappings.put(j, random.nextFloat());
			}

			addedPairs.put(i, addedMappings);

			Map<Integer, Float> mappings = pairs.get(i);
			if (mappings != null) {
				mappings.putAll(addedMappings);
			} else {
				pairs.put(i, addedMappings);
			}
		}

		state.addAll(addedPairs);

		for (Map.Entry<Integer, SortedMap<Integer, Float>> pair : pairs.entrySet()) {
			Map<Integer, Float> expectedValue = pair.getValue();
			Map<Integer, Float> actualValue = state.get(pair.getKey());
			assertEquals(expectedValue, actualValue);
		}

		// Retrieves the values of some pairs and validates the correctness of
		// these values.
		Set<Integer> retrievedKeys = null;
		Map<Integer, SortedMap<Integer, Float>> retrievedPairs = state.getAll(retrievedKeys);
		assertNotNull(retrievedPairs);
		assertTrue(retrievedPairs.isEmpty());

		retrievedKeys = new HashSet<>();
		retrievedKeys.add(null);
		assertNotNull(retrievedPairs);
		assertTrue(retrievedPairs.isEmpty());

		retrievedKeys.add(11111);
		retrievedPairs = state.getAll(retrievedKeys);
		assertNotNull(retrievedPairs);
		assertTrue(retrievedPairs.isEmpty());

		index = 0;
		for (Integer key : pairs.keySet()) {
			if (index % 4 == 0) {
				retrievedKeys.add(key);
			}
			index++;
		}
		retrievedPairs = state.getAll(retrievedKeys);
		for (Integer retrievedKey : retrievedKeys) {
			Map<Integer, Float> expectedValue = pairs.get(retrievedKey);
			Map<Integer, Float> actualValue = retrievedPairs.get(retrievedKey);
			assertEquals(expectedValue, actualValue);
		}

		// Removes some pairs from the state and validates that they are
		// correctly removed.
		removedKeys.clear();

		index = 0;
		for (Integer key : pairs.keySet()) {
			if (index % 5 == 0) {
				removedKeys.add(key);
			}
			index++;
		}

		removedKeys.add(null);
		removedKeys.add(11111);

		state.removeAll(removedKeys);
		for (Integer removedKey : removedKeys) {
			assertFalse(state.contains(removedKey));
		}

		// Removes all pairs from the state and validates that no pair exists
		// in the state.
		state.removeAll(pairs.keySet());
		for (Integer key : pairs.keySet()) {
			assertFalse(state.contains(key));
		}
	}

	@Test
	public void testMapKeyAccess() throws Exception {
		KeyedSortedMapStateDescriptor<Integer, Integer, Float> descriptor =
			new KeyedSortedMapStateDescriptor<>("mapKeyAccess", IntSerializer.INSTANCE,
				new NaturalComparator<>(), IntSerializer.INSTANCE, FloatSerializer.INSTANCE);

		KeyedSortedMapState<Integer, Integer, Float> state = backend.getKeyedState(descriptor);
		assertNotNull(state);

		// Adds some mappings into the state and validates that they can be
		// correctly retrieved.
		Random random = new Random();
		Map<Integer, SortedMap<Integer, Float>> pairs = new HashMap<>();
		for (int i = 0; i < 10; ++i) {
			int numMappings = random.nextInt(19) + 1;

			SortedMap<Integer, Float> mappings = new TreeMap<>();
			for (int j = 0; j < numMappings; j++) {
				Float value = random.nextFloat();

				state.add(i, j, value);
				mappings.put(j, value);
			}

			pairs.put(i, mappings);
		}

		for (Map.Entry<Integer, SortedMap<Integer, Float>> pair : pairs.entrySet()) {
			Integer key = pair.getKey();
			Map<Integer, Float> mappings = pair.getValue();

			for (Map.Entry<Integer, Float> mapping : mappings.entrySet()) {
				assertTrue(state.contains(key, mapping.getKey()));

				Float expectedValue = mapping.getValue();
				Float actualValue = state.get(key, mapping.getKey());
				assertEquals(expectedValue, actualValue);
			}
		}

		assertFalse(state.contains(null, 1));
		assertNull(state.get(null, 1));

		try {
			assertFalse(state.contains(1, null));
		} catch (NullPointerException e) {
			// NaturalComparator does not support null
		}

		try {
			assertNull(state.get(1, null));
		} catch (StateAccessException e) {
			// NaturalComparator does not support null
		}

		assertNull(state.get(1, 1111));
		assertNull(state.get(1111, 0));

		// Removes some mappings from the state and validates that they do
		// not exist in their corresponding lists.
		for (Map.Entry<Integer, SortedMap<Integer, Float>> pair : pairs.entrySet()) {
			Integer key = pair.getKey();
			SortedMap<Integer, Float> mappings = pair.getValue();

			Iterator<Integer> mapKeyIterator = mappings.keySet().iterator();
			int index = 0;
			while (mapKeyIterator.hasNext()) {
				Integer mapKey = mapKeyIterator.next();
				if (key == 0 || index % key == 0) {
					state.remove(key, mapKey);
					mapKeyIterator.remove();
				}
				index++;
			}

			if (mappings.isEmpty()) {
				pair.setValue(null);
			}
		}

		for (Map.Entry<Integer, SortedMap<Integer, Float>> pair : pairs.entrySet()) {
			Map<Integer, Float> expectedValue = pair.getValue();
			Map<Integer, Float> actualValue = state.get(pair.getKey());
			assertEquals(expectedValue, actualValue);
		}

		// Retrieves some mappings from the state
		for (Map.Entry<Integer, SortedMap<Integer, Float>> pair : pairs.entrySet()) {
			Integer key = pair.getKey();
			Map<Integer, Float> mappings = pair.getValue();
			if (mappings == null) {
				continue;
			}

			Set<Integer> retrievedMapKeys = null;
			Map<Integer, Float> retrievedMappings = state.getAll(key, retrievedMapKeys);
			assertNotNull(retrievedMappings);
			assertTrue(retrievedMappings.isEmpty());

			retrievedMapKeys = new HashSet<>();
			retrievedMapKeys.add(null);
			retrievedMappings = state.getAll(key, retrievedMapKeys);
			assertNotNull(retrievedMappings);
			assertTrue(retrievedMappings.isEmpty());

			retrievedMapKeys.add(11111);
			retrievedMappings = state.getAll(key, retrievedMapKeys);
			assertNotNull(retrievedMappings);
			assertTrue(retrievedMappings.isEmpty());

			int index = 0;
			for (Integer mapKey : mappings.keySet()) {
				if (index % 4 == 0) {
					retrievedMapKeys.add(mapKey);
				}
				index++;
			}

			retrievedMappings = state.getAll(key, retrievedMapKeys);
			for (Integer retrievedMapKey : retrievedMapKeys) {
				if (retrievedMapKey == null) {
					continue;
				}

				Float expectedValue = mappings.get(retrievedMapKey);
				Float actualValue = retrievedMappings.get(retrievedMapKey);
				assertEquals(expectedValue, actualValue);
			}
		}

		// Removes some mappings from the state and validates that they do
		// not exist in their corresponding maps.
		for (Map.Entry<Integer, SortedMap<Integer, Float>> pair : pairs.entrySet()) {
			Integer key = pair.getKey();
			Map<Integer, Float> mappings = pair.getValue();
			if (mappings == null) {
				continue;
			}

			Set<Integer> removedMapKeys = new HashSet<>();
			Iterator<Integer> mapKeyIterator = mappings.keySet().iterator();
			int index = 0;
			while (mapKeyIterator.hasNext()){
				Integer mapKey = mapKeyIterator.next();
				if (index % 5 == 0) {
					removedMapKeys.add(mapKey);
					mapKeyIterator.remove();
				}
				index++;
			}
			removedMapKeys.add(11111);

			state.removeAll(key, removedMapKeys);

			if (mappings.isEmpty()) {
				pair.setValue(null);
			}
		}

		for (Map.Entry<Integer, SortedMap<Integer, Float>> pair : pairs.entrySet()) {
			Map<Integer, Float> expectedValue = pair.getValue();
			Map<Integer, Float> actualValue = state.get(pair.getKey());
			assertEquals(expectedValue, actualValue);
		}

		// Retrieves some mappings from the state
		Map<Integer, Set<Integer>> retrievedMap = null;
		Map<Integer, SortedMap<Integer, Float>> retrievedPairs = state.getAll(retrievedMap);
		assertNotNull(retrievedPairs);
		assertTrue(retrievedPairs.isEmpty());

		retrievedMap = new HashMap<>();
		retrievedMap.put(null, Collections.singleton(1));
		retrievedPairs = state.getAll(retrievedMap);
		assertNotNull(retrievedPairs);
		assertTrue(retrievedPairs.isEmpty());

		retrievedMap.put(11111, Collections.singleton(1));
		retrievedPairs = state.getAll(retrievedMap);
		assertNotNull(retrievedPairs);
		assertTrue(retrievedPairs.isEmpty());

		for (Map.Entry<Integer, SortedMap<Integer, Float>> pair : pairs.entrySet()) {
			Integer key = pair.getKey();
			Map<Integer, Float> mappings = pair.getValue();
			if (mappings == null) {
				retrievedMap.put(key, null);
			} else {
				Set<Integer> retrievedMapKeys = new HashSet<>();
				int index = 0;
				for (Integer mapKey : mappings.keySet()) {
					if (index % 6 == 0) {
						retrievedMapKeys.add(mapKey);
					}
					index++;
				}
				retrievedMapKeys.add(11111);

				retrievedMap.put(key, retrievedMapKeys);
			}
		}

		retrievedPairs = state.getAll(retrievedMap);
		for (Map.Entry<Integer, Set<Integer>> retrievedEntry : retrievedMap.entrySet()) {
			Integer retrievedKey = retrievedEntry.getKey();
			Set<Integer> retrievedMapKeys = retrievedEntry.getValue();

			Map<Integer, Float> mappings = pairs.get(retrievedKey);
			Map<Integer, Float> retrievedMappings = retrievedPairs.get(retrievedKey);

			if (retrievedMapKeys == null || retrievedMapKeys.isEmpty()) {
				assertNull(retrievedMappings);
			} else {
				for (Integer retrievedMapKey : retrievedMapKeys) {
					if (retrievedMapKey == null) {
						continue;
					}

					Float expectedValue = mappings == null ? null : mappings.get(retrievedMapKey);
					Float actualValue = retrievedMappings == null ? null : retrievedMappings.get(retrievedMapKey);
					assertEquals(expectedValue, actualValue);
				}
			}
		}

		// Removes some mappings from the state and validates that they do
		// not exist in their corresponding lists.
		Map<Integer, Set<Integer>> removedMap = new HashMap<>();

		for (Map.Entry<Integer, SortedMap<Integer, Float>> pair : pairs.entrySet()) {
			Integer key = pair.getKey();
			Map<Integer, Float> mappings = pair.getValue();
			if (mappings == null) {
				removedMap.put(key, null);
			} else {
				Set<Integer> removedMapKeys = new HashSet<>();
				int index = 0;
				Iterator<Integer> mapKeyIterator = mappings.keySet().iterator();
				while (mapKeyIterator.hasNext()) {
					Integer mapKey = mapKeyIterator.next();
					if (index % 7 == 0) {
						removedMapKeys.add(mapKey);
						mapKeyIterator.remove();
					}
					index++;
				}
				removedMapKeys.add(11111);
				removedMap.put(key, removedMapKeys);

				if (mappings.isEmpty()) {
					pair.setValue(null);
				}
			}
		}

		removedMap.put(11111, Collections.singleton(1));

		state.removeAll(removedMap);
		for (Map.Entry<Integer, SortedMap<Integer, Float>> pair : pairs.entrySet()) {
			Map<Integer, Float> expectedValue = pair.getValue();
			Map<Integer, Float> actualValue = state.get(pair.getKey());
			assertEquals(expectedValue, actualValue);
		}
	}

	@Test
	public void testBoundAccess() throws Exception {
		KeyedSortedMapStateDescriptor<Integer, Integer, Float> descriptor =
			new KeyedSortedMapStateDescriptor<>("boundaccess", IntSerializer.INSTANCE,
				BytewiseComparator.INT_INSTANCE, IntSerializer.INSTANCE, FloatSerializer.INSTANCE);

		KeyedSortedMapState<Integer, Integer, Float> state = backend.getKeyedState(descriptor);
		assertNotNull(state);

		// Adds some mappings into the state and validates that they can be
		// correctly retrieved.
		Random random = new Random();
		Map<Integer, SortedMap<Integer, Float>> pairs = new HashMap<>();
		for (int i = 0; i < 20; ++i) {
			SortedMap<Integer, Float> mappings = new TreeMap<>();
			for (int j = 0; j < i; ++j) {
				Float value = random.nextFloat();
				mappings.put(j, value);
				state.add(i, j, value);
			}

			pairs.put(i, mappings);
		}

		for (Map.Entry<Integer, SortedMap<Integer, Float>> pair : pairs.entrySet()) {
			Integer key = pair.getKey();
			SortedMap<Integer, Float> mappings = pair.getValue();

			Map.Entry<Integer, Float> actualFirstPair = state.firstEntry(key);
			Integer actualFirstKey = actualFirstPair == null ? null : actualFirstPair.getKey();
			Integer expectedFirstKey = mappings.isEmpty() ? null : mappings.firstKey();
			assertEquals(expectedFirstKey, actualFirstKey);

			Float actualFirstValue = actualFirstPair == null ? null : actualFirstPair.getValue();
			Float expectedFirstValue = expectedFirstKey == null ? null : mappings.get(expectedFirstKey);
			assertEquals(expectedFirstValue, actualFirstValue);

			Map.Entry<Integer, Float> actualLastPair = state.lastEntry(key);
			Integer actualLastKey = actualLastPair == null ? null : actualLastPair.getKey();
			Integer expectedLastKey = mappings.isEmpty() ? null : mappings.lastKey();
			assertEquals(expectedLastKey, actualLastKey);

			Float actualLastValue = actualLastPair == null ? null : actualLastPair.getValue();
			Float expectedLastValue = expectedLastKey == null ? null : mappings.get(expectedLastKey);
			assertEquals(expectedLastValue, actualLastValue);
		}
	}

	@Test
	public void testIterator() throws Exception {
		KeyedSortedMapStateDescriptor<Integer, Integer, Float> descriptor =
			new KeyedSortedMapStateDescriptor<>("iteratortest", IntSerializer.INSTANCE,
				BytewiseComparator.INT_INSTANCE, IntSerializer.INSTANCE, FloatSerializer.INSTANCE);

		KeyedSortedMapState<Integer, Integer, Float> state = backend.getKeyedState(descriptor);
		assertNotNull(state);

		// Adds some mappings into the state and validates that they can be
		// correctly retrieved.
		Random random = new Random();
		Map<Integer, SortedMap<Integer, Float>> pairs = new HashMap<>();
		for (int i = 0; i < 20; ++i) {
			SortedMap<Integer, Float> mappings = new TreeMap<>();
			for (int j = 0; j < i; ++j) {
				Float value = random.nextFloat();
				mappings.put(j, value);
				state.add(i, j, value);
			}

			pairs.put(i, mappings);
		}

		for (Map.Entry<Integer, SortedMap<Integer, Float>> pair : pairs.entrySet()) {
			Integer key = pair.getKey();
			SortedMap<Integer, Float> mappings = pair.getValue();

			int[] bounds = new int[] {0, mappings.size() / 3, mappings.size() / 2, mappings.size() * 2 / 3, mappings.size()};

			for (int bound : bounds) {
				Iterator<Map.Entry<Integer, Float>> expectedHeadIterator = mappings.headMap(bound).entrySet().iterator();
				Iterator<Map.Entry<Integer, Float>> actualHeadIterator = state.headIterator(key, bound);
				validateIterator(expectedHeadIterator, actualHeadIterator);

				Iterator<Map.Entry<Integer, Float>> expectedTailIterator = mappings.tailMap(bound).entrySet().iterator();
				Iterator<Map.Entry<Integer, Float>> actualTailIterator = state.tailIterator(key, bound);
				validateIterator(expectedTailIterator, actualTailIterator);
			}

			Iterator<Map.Entry<Integer, Float>> expectedSubIterator1 = mappings.subMap(bounds[0], bounds[4]).entrySet().iterator();
			Iterator<Map.Entry<Integer, Float>> actualSubIterator1 = state.subIterator(key, bounds[0], bounds[4]);
			validateIterator(expectedSubIterator1, actualSubIterator1);

			Iterator<Map.Entry<Integer, Float>> expectedSubIterator2 = mappings.subMap(bounds[1], bounds[3]).entrySet().iterator();
			Iterator<Map.Entry<Integer, Float>> actualSubIterator2 = state.subIterator(key, bounds[1], bounds[3]);
			validateIterator(expectedSubIterator2, actualSubIterator2);

			Iterator<Map.Entry<Integer, Float>> expectedSubIterator3 = mappings.subMap(bounds[2], bounds[2]).entrySet().iterator();
			Iterator<Map.Entry<Integer, Float>> actualSubIterator3 = state.subIterator(key, bounds[2], bounds[2]);
			validateIterator(expectedSubIterator3, actualSubIterator3);
		}
	}

	@Test
	public void testMulitStateAccessParallism() throws Exception {
		KeyedSortedMapStateDescriptor<Integer, Integer, Float> descriptor1 =
			new KeyedSortedMapStateDescriptor<>("iteratortest1", IntSerializer.INSTANCE,
				BytewiseComparator.INT_INSTANCE, IntSerializer.INSTANCE, FloatSerializer.INSTANCE);

		KeyedSortedMapState<Integer, Integer, Float> state1 = backend.getKeyedState(descriptor1);

		KeyedSortedMapStateDescriptor<Integer, Integer, Float> descriptor2 =
			new KeyedSortedMapStateDescriptor<>("iteratortest2", IntSerializer.INSTANCE,
				BytewiseComparator.INT_INSTANCE, IntSerializer.INSTANCE, FloatSerializer.INSTANCE);

		KeyedSortedMapState<Integer, Integer, Float> state2 = backend.getKeyedState(descriptor2);

		int stateCount = 1000;
		ConcurrentHashMap<Tuple2<Integer, Integer>, Float> value1 = new ConcurrentHashMap<>(stateCount);
		Thread thread1 = new Thread(() -> {
			for (int i = 0; i < stateCount; ++i) {
				int subKey = ThreadLocalRandom.current().nextInt();
				Float value = ThreadLocalRandom.current().nextFloat();
				state1.add(i, subKey, value);
				value1.put(Tuple2.of(i, subKey), value);
			}
		});

		ConcurrentHashMap<Tuple2<Integer, Integer>, Float> value2 = new ConcurrentHashMap<>(stateCount);
		Thread thread2 = new Thread(() -> {
			for (int i = 0; i < stateCount; ++i) {
				int subKey = ThreadLocalRandom.current().nextInt();
				Float value = ThreadLocalRandom.current().nextFloat();
				state2.add(i, subKey, value);
				value2.put(Tuple2.of(i, subKey), value);
			}
		});
		thread1.start();
		thread2.start();

		thread1.join();
		thread2.join();

		for (Map.Entry<Tuple2<Integer, Integer>, Float> entry : value1.entrySet()) {
			Tuple2<Integer, Integer> key = entry.getKey();
			Float v1 = state1.get(key.f0, key.f1);
			assertNotNull(v1);
			assertEquals(entry.getValue(), v1);
		}

		for (Map.Entry<Tuple2<Integer, Integer>, Float> entry : value2.entrySet()) {
			Tuple2<Integer, Integer> key = entry.getKey();
			Float v1 = state2.get(key.f0, key.f1);
			assertNotNull(v1);
			assertEquals(entry.getValue(), v1);
		}
	}

	private void validateIterator(
		Iterator<Map.Entry<Integer, Float>> expectedIterator,
		Iterator<Map.Entry<Integer, Float>> actualIterator
	) {
		assertNotNull(actualIterator);

		assertEquals(expectedIterator.hasNext(), actualIterator.hasNext());

		while (expectedIterator.hasNext()) {
			assertTrue(actualIterator.hasNext());

			Map.Entry<Integer, Float> expectedEntry = expectedIterator.next();
			Map.Entry<Integer, Float> actualEntry = actualIterator.next();

			assertEquals(expectedEntry.getKey(), actualEntry.getKey());
			assertEquals(expectedEntry.getValue(), actualEntry.getValue());
		}

		assertEquals(expectedIterator.hasNext(), actualIterator.hasNext());
	}

	private KeyGroupRange getGroupsForSubtask(int maxParallelism, int parallelism, int subtaskIndex) {
		return KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(maxParallelism, parallelism, subtaskIndex);
	}
}

