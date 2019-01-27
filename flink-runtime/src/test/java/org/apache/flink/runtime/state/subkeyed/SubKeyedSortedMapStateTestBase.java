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

package org.apache.flink.runtime.state.subkeyed;

import org.apache.flink.api.common.functions.NaturalComparator;
import org.apache.flink.api.common.typeutils.BytewiseComparator;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.AbstractInternalStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.StateAccessException;
import org.apache.flink.runtime.state.TestLocalRecoveryConfig;

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
 * Unit tests for {@link SubKeyedSortedMapState}.
 */
public abstract class SubKeyedSortedMapStateTestBase {

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
	public void testKeyAndNamespaceAccess() throws Exception {

		SubKeyedSortedMapStateDescriptor<Integer, String, Integer, Float> descriptor =
			new SubKeyedSortedMapStateDescriptor<>("test",
				IntSerializer.INSTANCE, StringSerializer.INSTANCE,
				new NaturalComparator<>(), IntSerializer.INSTANCE, FloatSerializer.INSTANCE);
		SubKeyedSortedMapState<Integer, String, Integer, Float> state = backend.getSubKeyedState(descriptor);
		assertNotNull(state);

		Random random = new Random();

		Map<Integer, Map<String, SortedMap<Integer, Float>>> keyMap = new HashMap<>();
		for (int i = 0; i < 10; ++i) {
			Map<String, SortedMap<Integer, Float>> namespaceMap = new HashMap<>();
			for (int j = 0; j <= i; ++j) {
				String namespace = Integer.toString(j);

				int numMappings = random.nextInt(9) + 1;
				SortedMap<Integer, Float> mappings = new TreeMap<>();
				for (int k = 0; k < numMappings; ++k) {
					Float mapValue = random.nextFloat();
					mappings.put(k, mapValue);
				}

				namespaceMap.put(namespace, mappings);
			}

			keyMap.put(i, namespaceMap);
		}

		// Validates that no entry exists in the state when the state is empty.
		for (Map.Entry<Integer, Map<String, SortedMap<Integer, Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();

			Map<String, SortedMap<Integer, Float>> namespaceMap = keyEntry.getValue();
			for (Map.Entry<String, SortedMap<Integer, Float>> namespaceEntry : namespaceMap.entrySet()) {
				String namespace = namespaceEntry.getKey();
				assertFalse(state.contains(key, namespace));
				assertNull(state.get(key, namespace));
				assertEquals(Collections.emptySortedMap(), state.getOrDefault(key, namespace, Collections.emptySortedMap()));
			}
		}

		// Adds some entries into the state and validates that their values can
		// be correctly retrieved.
		for (Map.Entry<Integer, Map<String, SortedMap<Integer, Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();

			Map<String, SortedMap<Integer, Float>> namespaceMap = keyEntry.getValue();
			for (Map.Entry<String, SortedMap<Integer, Float>> namespaceEntry : namespaceMap.entrySet()) {
				String namespace = namespaceEntry.getKey();
				SortedMap<Integer, Float> value = namespaceEntry.getValue();

				state.addAll(key, namespace, value);
			}
		}

		for (Map.Entry<Integer, Map<String, SortedMap<Integer, Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();

			Map<String, SortedMap<Integer, Float>> namespaceMap = keyEntry.getValue();
			for (Map.Entry<String, SortedMap<Integer, Float>> namespaceEntry : namespaceMap.entrySet()) {
				String namespace = namespaceEntry.getKey();
				assertTrue(state.contains(key, namespace));

				SortedMap<Integer, Float> expectedValue = namespaceEntry.getValue();
				assertEquals(expectedValue, state.get(key, namespace));
				assertEquals(expectedValue, state.getOrDefault(key, namespace, Collections.emptySortedMap()));
			}
		}

		assertFalse(state.contains(null, "1"));
		assertNull(state.get(null, "1"));

		try {
			state.contains(2, "1", null);
		} catch (StateAccessException e) {
			// NaturalComparator does not support null
		}
		assertFalse(state.contains(1, null));
		assertNull(state.get(1, null));

		// Removes some values from the state and validates that the removed
		// values do not exist in the state any more.
		Map<Integer, Set<String>> removedKeyMap = new HashMap<>();

		for (Map.Entry<Integer, Map<String, SortedMap<Integer, Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();
			Set<String> removedNamespaces = new HashSet<>();

			int index = 0;
			Map<String, SortedMap<Integer, Float>> namespaceMap = keyEntry.getValue();
			for (String namespace : namespaceMap.keySet()) {
				if (key == 0 || index % key == 0) {
					state.remove(key, namespace);
					removedNamespaces.add(namespace);
				}
				index++;
			}

			removedKeyMap.put(key, removedNamespaces);
		}

		for (Map.Entry<Integer, Map<String, SortedMap<Integer, Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();
			Set<String> removedNamespaces = removedKeyMap.get(key);

			Map<String, SortedMap<Integer, Float>> namespaceMap = keyEntry.getValue();
			for (Map.Entry<String, SortedMap<Integer, Float>> namespaceEntry : namespaceMap.entrySet()) {
				String namespace = namespaceEntry.getKey();
				if (removedNamespaces.contains(namespace)) {
					assertFalse(state.contains(key, namespace));
					assertNull(state.get(key, namespace));
				} else {
					SortedMap<Integer, Float> expectedValue = namespaceEntry.getValue();
					SortedMap<Integer, Float> actualValue = state.get(key, namespace);
					assertEquals(expectedValue, actualValue);
				}
			}

			namespaceMap.keySet().removeAll(removedNamespaces);
		}

		// Adds more entries into the state and validates that the values of the
		// pairs can be correctly retrieved.
		for (int i = 5; i < 15; ++i) {
			Map<String, SortedMap<Integer, Float>> namespaceMap = keyMap.get(i);
			if (namespaceMap == null) {
				namespaceMap = new HashMap<>();
				keyMap.put(i, namespaceMap);
			}

			for (int j = 0; j < i + 5; ++j) {
				String namespace = Integer.toString(j);

				SortedMap<Integer, Float> addedMappings = new TreeMap<>();
				int numAddedMappings = random.nextInt(9) + 1;
				for (int k = 0; k < numAddedMappings; ++k) {
					addedMappings.put(k, random.nextFloat());
				}

				state.addAll(i, namespace, addedMappings);

				SortedMap<Integer, Float> mappings = namespaceMap.get(namespace);
				if (mappings == null) {
					namespaceMap.put(namespace, addedMappings);
				} else {
					mappings.putAll(addedMappings);
				}
			}
		}

		for (Map.Entry<Integer, Map<String, SortedMap<Integer, Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();
			Map<String, SortedMap<Integer, Float>> expectedNamespaceMap = keyEntry.getValue();
			Map<String, SortedMap<Integer, Float>> actualNamespaceMap = state.getAll(key);
			assertEquals(expectedNamespaceMap, actualNamespaceMap);
		}

		Map<String, SortedMap<Integer, Float>> nullNamespaceMap = state.getAll(null);
		assertNotNull(nullNamespaceMap);
		assertTrue(nullNamespaceMap.isEmpty());

		// Removes some keys from the state and validates that there is no
		// values under these keys.
		Set<Integer> removedKeys = new HashSet<>();
		int index = 0;
		for (Map.Entry<Integer, Map<String, SortedMap<Integer, Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();
			if (index % 4 == 0) {
				state.removeAll(key);
				removedKeys.add(key);
			}
		}

		for (Map.Entry<Integer, Map<String, SortedMap<Integer, Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();
			Map<String, SortedMap<Integer, Float>> actualNamespaceMap = state.getAll(key);
			if (removedKeys.contains(key)) {
				assertNotNull(actualNamespaceMap);
				assertTrue(actualNamespaceMap.isEmpty());
			} else {
				Map<String, SortedMap<Integer, Float>> expectedNamespaceMap = keyEntry.getValue();
				assertEquals(expectedNamespaceMap, actualNamespaceMap);
			}
		}
	}

	@Test
	public void testMapKeyAccess() throws Exception {
		SubKeyedSortedMapStateDescriptor<Integer, String, Integer, Float> descriptor =
			new SubKeyedSortedMapStateDescriptor<>("test",
				IntSerializer.INSTANCE, StringSerializer.INSTANCE,
				new NaturalComparator<>(), IntSerializer.INSTANCE, FloatSerializer.INSTANCE);

		SubKeyedSortedMapState<Integer, String, Integer, Float> state = backend.getSubKeyedState(descriptor);
		assertNotNull(state);

		Random random = new Random();

		// Adds some elements into the state and validates that they can be
		// correctly retrieved.
		Map<Integer, Map<String, SortedMap<Integer, Float>>> keyMap = new HashMap<>();
		for (int i = 0; i < 10; ++i) {
			Map<String, SortedMap<Integer, Float>> namespaceMap = new HashMap<>();
			for (int j = 0; j <= i; ++j) {
				String namespace = Integer.toString(j);

				int numMappings = random.nextInt(9) + 1;
				SortedMap<Integer, Float> mappings = new TreeMap<>();
				for (int k = 0; k < numMappings; ++k) {
					Float element = random.nextFloat();
					state.add(i, namespace, k, element);
					mappings.put(k, element);
				}

				namespaceMap.put(namespace, mappings);
			}

			keyMap.put(i, namespaceMap);
		}

		for (Map.Entry<Integer, Map<String, SortedMap<Integer, Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();

			Map<String, SortedMap<Integer, Float>> namespaceMap = keyEntry.getValue();
			for (Map.Entry<String, SortedMap<Integer, Float>> namespaceEntry : namespaceMap.entrySet()) {
				String namespace = namespaceEntry.getKey();

				SortedMap<Integer, Float> mappings = namespaceEntry.getValue();
				for (Map.Entry<Integer, Float> mapping : mappings.entrySet()) {
					Integer mapKey = mapping.getKey();
					assertTrue(state.contains(key, namespace, mapKey));

					Float expectedMapValue = mapping.getValue();
					Float actualMapValue = state.get(key, namespace, mapKey);
					assertEquals(expectedMapValue, actualMapValue);
				}
			}
		}

		assertNull(state.get(1, "0", 1111));
		assertNull(state.get(1, "1111", 0));
		assertNull(state.get(1111, "0", 0));

		// Removes some elements from the state and validates that they do
		// not exist in their corresponding lists.
		for (Map.Entry<Integer, Map<String, SortedMap<Integer, Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();

			Map<String, SortedMap<Integer, Float>> namespaceMap = keyEntry.getValue();
			for (Map.Entry<String, SortedMap<Integer, Float>> namespaceEntry : namespaceMap.entrySet()) {
				String namespace = namespaceEntry.getKey();
				SortedMap<Integer, Float> mappings = namespaceEntry.getValue();

				int index = 0;
				Iterator<Map.Entry<Integer, Float>> mappingIterator = mappings.entrySet().iterator();
				while (mappingIterator.hasNext()) {
					Integer mapKey = mappingIterator.next().getKey();
					if (key == 0 || index % key == 0) {
						state.remove(key, namespace, mapKey);
						mappingIterator.remove();
					}
					index++;
				}

				if (mappings.isEmpty()) {
					namespaceEntry.setValue(null);
				}
			}
		}

		for (Map.Entry<Integer, Map<String, SortedMap<Integer, Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();

			Map<String, SortedMap<Integer, Float>> namespaceMap = keyEntry.getValue();
			for (Map.Entry<String, SortedMap<Integer, Float>> namespaceEntry : namespaceMap.entrySet()) {
				SortedMap<Integer, Float> expectedValue = namespaceEntry.getValue();
				SortedMap<Integer, Float> actualValue = state.get(key, namespaceEntry.getKey());
				assertEquals(expectedValue, actualValue);
			}
		}

		// Retrieves some values from the state and validates that they are
		// equal to the expected ones.
		for (Map.Entry<Integer, Map<String, SortedMap<Integer, Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();

			Map<String, SortedMap<Integer, Float>> namespaceMap = keyEntry.getValue();
			for (Map.Entry<String, SortedMap<Integer, Float>> namespaceEntry : namespaceMap.entrySet()) {
				String namespace = namespaceEntry.getKey();
				SortedMap<Integer, Float> mappings = namespaceEntry.getValue();
				if (mappings == null) {
					continue;
				}

				Set<Integer> retrievedMapKeys = null;

				SortedMap<Integer, Float> retrievedMappings = state.getAll(key, namespace, retrievedMapKeys);
				assertNull(retrievedMappings);

				retrievedMapKeys = new HashSet<>();
				retrievedMappings = state.getAll(key, namespace, retrievedMapKeys);
				assertNull(retrievedMappings);

				retrievedMapKeys.add(11111);
				retrievedMappings = state.getAll(key, namespace, retrievedMapKeys);
				assertNull(retrievedMappings);

				int index = 0;
				for (Integer mapKey: mappings.keySet()) {
					if (key == 0 || index % key == 0) {
						retrievedMapKeys.add(mapKey);
					}
					index++;
				}

				retrievedMappings = state.getAll(key, namespace, retrievedMapKeys);
				for (Integer retrievedMapKey : retrievedMapKeys) {
					Float actualMapValue = retrievedMappings.get(retrievedMapKey);
					Float expectedMapValue = mappings.get(retrievedMapKey);
					assertEquals(expectedMapValue, actualMapValue);
				}
			}
		}

		// Removes some elements from the state and validates that they do
		// not exist in their corresponding lists.
		for (Map.Entry<Integer, Map<String, SortedMap<Integer, Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();

			Map<String, SortedMap<Integer, Float>> namespaceMap = keyEntry.getValue();
			for (Map.Entry<String, SortedMap<Integer, Float>> namespaceEntry : namespaceMap.entrySet()) {
				String namespace = namespaceEntry.getKey();
				SortedMap<Integer, Float> mappings = namespaceEntry.getValue();
				if (mappings == null) {
					continue;
				}

				Set<Integer> removedMapKeys = new HashSet<>();
				int index = 0;
				for (Integer mapKey : mappings.keySet()) {
					if (key == 0 || index % key == 0) {
						removedMapKeys.add(mapKey);
					}
					index++;
				}

				state.removeAll(key, namespace, removedMapKeys);

				mappings.keySet().removeAll(removedMapKeys);
				if (mappings.isEmpty()) {
					namespaceEntry.setValue(null);
				}
			}
		}

		for (Map.Entry<Integer, Map<String, SortedMap<Integer, Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();

			Map<String, SortedMap<Integer, Float>> namespaceMap = keyEntry.getValue();
			for (Map.Entry<String, SortedMap<Integer, Float>> namespaceEntry : namespaceMap.entrySet()) {
				SortedMap<Integer, Float> expectedValue = namespaceEntry.getValue();
				SortedMap<Integer, Float> actualValue = state.get(key, namespaceEntry.getKey());
				assertEquals(expectedValue, actualValue);
			}
		}
	}

	@Test
	public void testBoundAccess() throws Exception {
		SubKeyedSortedMapStateDescriptor<Integer, String, Integer, Float> descriptor =
			new SubKeyedSortedMapStateDescriptor<>("test",
				IntSerializer.INSTANCE, StringSerializer.INSTANCE,
				BytewiseComparator.INT_INSTANCE, IntSerializer.INSTANCE, FloatSerializer.INSTANCE);

		SubKeyedSortedMapState<Integer, String, Integer, Float> state = backend.getSubKeyedState(descriptor);
		assertNotNull(state);

		// Adds some mappings into the state.
		Random random = new Random();
		Map<Integer, Map<String, SortedMap<Integer, Float>>> keyMap = new HashMap<>();
		for (int i = 0; i < 10; ++i) {
			Map<String, SortedMap<Integer, Float>> namespaceMap = new HashMap<>();
			for (int j = 0; j <= i; ++j) {
				String namespace = Integer.toString(j);

				int numMappings = random.nextInt(9) + 1;
				SortedMap<Integer, Float> mappings = new TreeMap<>();
				for (int k = 0; k < numMappings; ++k) {
					Float element = random.nextFloat();
					state.add(i, namespace, k, element);
					mappings.put(k, element);
				}

				namespaceMap.put(namespace, mappings);
			}

			keyMap.put(i, namespaceMap);
		}

		for (Map.Entry<Integer, Map<String, SortedMap<Integer, Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();

			Map<String, SortedMap<Integer, Float>> namespaceMap = keyEntry.getValue();
			for (Map.Entry<String, SortedMap<Integer, Float>> namespaceEntry : namespaceMap.entrySet()) {
				String namespace = namespaceEntry.getKey();
				SortedMap<Integer, Float> mappings = namespaceEntry.getValue();

				Map.Entry<Integer, Float> actualFirstPair = state.firstEntry(key, namespace);
				Integer actualFirstKey = actualFirstPair == null ? null : actualFirstPair.getKey();
				Integer expectedFirstKey = mappings.isEmpty() ? null : mappings.firstKey();
				assertEquals(expectedFirstKey, actualFirstKey);

				Float actualFirstValue = actualFirstPair == null ? null : actualFirstPair.getValue();
				Float expectedFirstValue = expectedFirstKey == null ? null : mappings.get(expectedFirstKey);
				assertEquals(expectedFirstValue, actualFirstValue);

				Map.Entry<Integer, Float> actualLastPair = state.lastEntry(key, namespace);
				Integer actualLastKey = actualLastPair == null ? null : actualLastPair.getKey();
				Integer expectedLastKey = mappings.isEmpty() ? null : mappings.lastKey();
				assertEquals(expectedLastKey, actualLastKey);

				Float actualLastValue = actualLastPair == null ? null : actualLastPair.getValue();
				Float expectedLastValue = expectedLastKey == null ? null : mappings.get(expectedLastKey);
				assertEquals(expectedLastValue, actualLastValue);
			}
		}
	}

	@Test
	public void testIterator() throws Exception {
		SubKeyedSortedMapStateDescriptor<Integer, String, Integer, Float> descriptor =
			new SubKeyedSortedMapStateDescriptor<>("test",
				IntSerializer.INSTANCE, StringSerializer.INSTANCE,
				BytewiseComparator.INT_INSTANCE, IntSerializer.INSTANCE, FloatSerializer.INSTANCE);

		SubKeyedSortedMapState<Integer, String, Integer, Float> state = backend.getSubKeyedState(descriptor);
		assertNotNull(state);

		// Adds some mappings into the state and validates that they can be
		// correctly retrieved.
		Random random = new Random();
		Map<Integer, Map<String, SortedMap<Integer, Float>>> keyMap = new HashMap<>();
		for (int i = 0; i < 10; ++i) {
			Map<String, SortedMap<Integer, Float>> namespaceMap = new HashMap<>();
			for (int j = 0; j <= i; ++j) {
				String namespace = Integer.toString(j);

				int numMappings = random.nextInt(9) + 1;
				SortedMap<Integer, Float> mappings = new TreeMap<>();
				for (int k = 0; k < numMappings; ++k) {
					Float element = random.nextFloat();
					state.add(i, namespace, k, element);
					mappings.put(k, element);
				}

				namespaceMap.put(namespace, mappings);
			}

			keyMap.put(i, namespaceMap);
		}

		for (Map.Entry<Integer, Map<String, SortedMap<Integer, Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();

			Map<String, SortedMap<Integer, Float>> namespaceMap = keyEntry.getValue();
			for (Map.Entry<String, SortedMap<Integer, Float>> namespaceEntry : namespaceMap.entrySet()) {
				String namespace = namespaceEntry.getKey();

				SortedMap<Integer, Float> mappings = namespaceEntry.getValue();
				int[] bounds = new int[] {0, mappings.size() / 3, mappings.size() / 2, mappings.size() * 2 / 3, mappings.size()};

				for (int bound : bounds) {
					Iterator<Map.Entry<Integer, Float>> expectedHeadIterator = mappings.headMap(bound).entrySet().iterator();
					Iterator<Map.Entry<Integer, Float>> actualHeadIterator = state.headIterator(key, namespace, bound);
					validateIterator(expectedHeadIterator, actualHeadIterator);

					Iterator<Map.Entry<Integer, Float>> expectedTailIterator = mappings.tailMap(bound).entrySet().iterator();
					Iterator<Map.Entry<Integer, Float>> actualTailIterator = state.tailIterator(key, namespace, bound);
					validateIterator(expectedTailIterator, actualTailIterator);
				}

				Iterator<Map.Entry<Integer, Float>> expectedSubIterator1 = mappings.subMap(bounds[0], bounds[4]).entrySet().iterator();
				Iterator<Map.Entry<Integer, Float>> actualSubIterator1 = state.subIterator(key, namespace, bounds[0], bounds[4]);
				validateIterator(expectedSubIterator1, actualSubIterator1);

				Iterator<Map.Entry<Integer, Float>> expectedSubIterator2 = mappings.subMap(bounds[1], bounds[3]).entrySet().iterator();
				Iterator<Map.Entry<Integer, Float>> actualSubIterator2 = state.subIterator(key, namespace, bounds[1], bounds[3]);
				validateIterator(expectedSubIterator2, actualSubIterator2);

				Iterator<Map.Entry<Integer, Float>> expectedSubIterator3 = mappings.subMap(bounds[2], bounds[2]).entrySet().iterator();
				Iterator<Map.Entry<Integer, Float>> actualSubIterator3 = state.subIterator(key, namespace, bounds[2], bounds[2]);
				validateIterator(expectedSubIterator3, actualSubIterator3);
			}
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

	@Test
	public void testMultiStateAccessParallism() throws Exception {
		SubKeyedSortedMapStateDescriptor<Integer, Integer, Integer, Float> descriptor1 =
			new SubKeyedSortedMapStateDescriptor<>("test1",
				IntSerializer.INSTANCE, IntSerializer.INSTANCE,
				BytewiseComparator.INT_INSTANCE,
				IntSerializer.INSTANCE, FloatSerializer.INSTANCE);
		SubKeyedSortedMapState<Integer, Integer, Integer, Float> state1 = backend.getSubKeyedState(descriptor1);

		SubKeyedSortedMapStateDescriptor<Integer, Integer, Integer, Float> descriptor2 =
			new SubKeyedSortedMapStateDescriptor<>("test2",
				IntSerializer.INSTANCE, IntSerializer.INSTANCE,
				BytewiseComparator.INT_INSTANCE,
				IntSerializer.INSTANCE, FloatSerializer.INSTANCE);
		SubKeyedSortedMapState<Integer, Integer, Integer, Float> state2 = backend.getSubKeyedState(descriptor2);

		int stateCount = 1000;
		ConcurrentHashMap<Tuple3<Integer, Integer, Integer>, Float> value1 = new ConcurrentHashMap<>(stateCount);
		Thread thread1 = new Thread(() -> {
			for (int i = 0; i < stateCount; ++i) {
				Integer subKey = ThreadLocalRandom.current().nextInt();
				Integer mapKey = ThreadLocalRandom.current().nextInt();
				Float value = ThreadLocalRandom.current().nextFloat();
				state1.add(i, subKey, mapKey, value);
				value1.put(Tuple3.of(i, subKey, mapKey), value);
			}
		});

		ConcurrentHashMap<Tuple3<Integer, Integer, Integer>, Float> value2 = new ConcurrentHashMap<>(stateCount);
		Thread thread2 = new Thread(() -> {
			for (int i = 0; i < stateCount; ++i) {
				Integer subKey = ThreadLocalRandom.current().nextInt();
				Integer mapKey = ThreadLocalRandom.current().nextInt();
				Float value = ThreadLocalRandom.current().nextFloat();
				state2.add(i, subKey, mapKey, value);
				value2.put(Tuple3.of(i, subKey, mapKey), value);
			}
		});
		thread1.start();
		thread2.start();

		thread1.join();
		thread2.join();

		for (Map.Entry<Tuple3<Integer, Integer, Integer>, Float> entry : value1.entrySet()) {
			Tuple3<Integer, Integer, Integer> key = entry.getKey();
			Float v1 = state1.get(key.f0, key.f1, key.f2);
			assertNotNull(v1);
			assertEquals(entry.getValue(), v1);
		}

		for (Map.Entry<Tuple3<Integer, Integer, Integer>, Float> entry : value2.entrySet()) {
			Tuple3<Integer, Integer, Integer> key = entry.getKey();
			Float v1 = state2.get(key.f0, key.f1, key.f2);
			assertNotNull(v1);
			assertEquals(entry.getValue(), v1);
		}
	}

	@Test
	public void testKeys() throws Exception {
		SubKeyedSortedMapStateDescriptor<Integer, String, Integer, Float> descriptor =
			new SubKeyedSortedMapStateDescriptor<>("test",
				IntSerializer.INSTANCE, StringSerializer.INSTANCE,
				new NaturalComparator<>(), IntSerializer.INSTANCE, FloatSerializer.INSTANCE);
		SubKeyedSortedMapState<Integer, String, Integer, Float> state = backend.getSubKeyedState(descriptor);
		assertNotNull(state);

		String noExistNamespace = "unkonw";
		assertNull(state.keys(noExistNamespace));

		String namespace = "ns";
		String namespace2 = "ns2";

		int keyCount = 10;
		Set<Integer> keys = new HashSet<>(keyCount);

		ThreadLocalRandom random = ThreadLocalRandom.current();

		for (int i = 0; i < keyCount; ++i) {
			Integer key = random.nextInt(keyCount);
			state.add(key, namespace, key, key + 1.1f);
			state.add(key, namespace2, key, key + 1.2f);
			keys.add(key);
		}

		validateKeysEquals(keys, state.keys(namespace));
		validateKeysEquals(keys, state.keys(namespace2));

		for (Integer key : keys) {
			state.remove(key, namespace);
		}

		validateKeysEquals(keys, state.keys(namespace2));
		assertNull(state.keys(namespace));
	}

	private void validateKeysEquals(Set<Integer> expected, Iterable<Integer> actual) {
		Iterator<Integer> actualKeys = actual.iterator();
		int count = 0;

		while (actualKeys.hasNext()) {
			assertTrue(expected.contains(actualKeys.next()));
			count++;
		}

		assertEquals(expected.size(), count);
		assertFalse(actualKeys.hasNext());
	}

	private KeyGroupRange getGroupsForSubtask(int maxParallelism, int parallelism, int subtaskIndex) {
		return KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(maxParallelism, parallelism, subtaskIndex);
	}
}
