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

import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.TestLocalRecoveryConfig;
import org.apache.flink.runtime.state.AbstractInternalStateBackend;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link KeyedMapState}.
 */
public abstract class KeyedMapStateTestBase {

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
		KeyedMapStateDescriptor<Integer, Integer, Float> descriptor =
			new KeyedMapStateDescriptor<>("test", IntSerializer.INSTANCE,
				IntSerializer.INSTANCE, FloatSerializer.INSTANCE);

		KeyedMapState<Integer, Integer, Float> state = backend.getKeyedState(descriptor);
		assertNotNull(state);

		Random random = new Random();
		Map<Integer, Map<Integer, Float>> pairs = new HashMap<>();
		for (int i = 0; i < 10; ++i) {
			int numMappings = random.nextInt(19) + 1;

			Map<Integer, Float> mappings = new HashMap<>();
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
			assertEquals(Collections.emptyMap(), state.getOrDefault(key, Collections.emptyMap()));
		}

		// Adds some lists into the state and validates that they can be
		// correctly retrieved.
		for (Map.Entry<Integer, Map<Integer, Float>> pair : pairs.entrySet()) {
			Integer key = pair.getKey();
			Map<Integer, Float> mappings = pair.getValue();
			state.addAll(key, mappings);
		}

		for (Map.Entry<Integer, Map<Integer, Float>> pair : pairs.entrySet()) {
			assertTrue(state.contains(pair.getKey()));

			Map<Integer, Float> expectedValue = pair.getValue();
			assertEquals(expectedValue, state.get(pair.getKey()));
			assertEquals(expectedValue, state.getOrDefault(pair.getKey(), Collections.emptyMap()));
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

		for (Map.Entry<Integer, Map<Integer, Float>> pair : pairs.entrySet()) {
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

		Map<Integer, Map<Integer, Float>> addedPairs = new HashMap<>();
		for (int i = 5; i < 15; ++i) {
			int numMappings = random.nextInt(19) + 1;
			Map<Integer, Float> addedMappings = new HashMap<>();
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

		for (Map.Entry<Integer, Map<Integer, Float>> pair : pairs.entrySet()) {
			Map<Integer, Float> expectedValue = pair.getValue();
			Map<Integer, Float> actualValue = state.get(pair.getKey());
			assertEquals(expectedValue, actualValue);
		}

		// Retrieves the values of some pairs and validates the correctness of
		// these values.
		Set<Integer> retrievedKeys = null;
		Map<Integer, Map<Integer, Float>> retrievedPairs = state.getAll(retrievedKeys);
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

		// Tests for state#entries
		for (int i = 0; i < 10; ++i) {
			Map<Integer, Float> mappings = pairs.get(i);
			Iterable<Map.Entry<Integer, Float>> entryIterable = state.entries(i);

			assertNotNull(entryIterable);

			if (mappings == null) {
				assertFalse(entryIterable.iterator().hasNext());
				continue;
			}

			for (Map.Entry<Integer, Float> entry : entryIterable) {
				Integer key = entry.getKey();

				Float expectedValue = mappings.get(key);
				Float actualValue = entry.getValue();
				assertEquals(expectedValue, actualValue);
			}
		}

		// Tests for state#mapKeys
		for (int i = 0; i < 10; ++i) {
			Map<Integer, Float> mappings = pairs.get(i);
			Iterable<Integer> keyIterable = state.mapKeys(i);

			assertNotNull(keyIterable);

			if (mappings == null) {
				assertFalse(keyIterable.iterator().hasNext());
				continue;
			}

			for (Integer key : keyIterable) {
				Float actualValue = state.get(i, key);
				Float expectedVluae = mappings.get(key);
				assertEquals(expectedVluae, actualValue);
			}
		}

		// Tests for state#values
		for (int i = 0; i < 10; ++i) {
			Map<Integer, Float> mappings = pairs.get(i);
			Iterable<Float> valueIterable = state.mapValues(i);

			assertNotNull(valueIterable);

			if (mappings == null) {
				assertFalse(valueIterable.iterator().hasNext());
				continue;
			}

			for (Float value : valueIterable) {
				assertTrue(mappings.containsValue(value));
			}
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
		KeyedMapStateDescriptor<Integer, Integer, Float> descriptor =
			new KeyedMapStateDescriptor<>("test", IntSerializer.INSTANCE,
				IntSerializer.INSTANCE, FloatSerializer.INSTANCE);

		KeyedMapState<Integer, Integer, Float> state = backend.getKeyedState(descriptor);
		assertNotNull(state);

		// Adds some mappings into the state and validates that they can be
		// correctly retrieved.
		Random random = new Random();
		Map<Integer, Map<Integer, Float>> pairs = new HashMap<>();
		for (int i = 0; i < 10; ++i) {
			int numMappings = random.nextInt(19) + 1;

			Map<Integer, Float> mappings = new HashMap<>();
			for (int j = 0; j < numMappings; j++) {
				Float value = random.nextFloat();

				state.add(i, j, value);
				mappings.put(j, value);
			}

			pairs.put(i, mappings);
		}

		for (Map.Entry<Integer, Map<Integer, Float>> pair : pairs.entrySet()) {
			Integer key = pair.getKey();
			Map<Integer, Float> mappings = pair.getValue();

			for (Map.Entry<Integer, Float> mapping : mappings.entrySet()) {
				assertTrue(state.contains(key, mapping.getKey()));

				Float expectedValue = mapping.getValue();
				Float actualValue = state.get(key, mapping.getKey());
				assertEquals(expectedValue, actualValue);
			}
		}

		// Tests for state#entries
		for (int i = 0; i < 10; ++i) {
			Map<Integer, Float> mappings = pairs.get(i);
			Iterable<Map.Entry<Integer, Float>> entryIterable = state.entries(i);
			if (mappings == null) {
				assertNull(entryIterable);
				continue;
			}

			assertNotNull(entryIterable);

			for (Map.Entry<Integer, Float> entry : entryIterable) {
				Integer key = entry.getKey();
				Float expectedValue = mappings.get(key);
				Float actualValue = entry.getValue();
				assertEquals(expectedValue, actualValue);
			}
		}

		// Tests for state#keys
		for (int i = 0; i < 10; ++i) {
			Map<Integer, Float> mappings = pairs.get(i);
			Iterable<Integer> keyIterable = state.mapKeys(i);
			if (mappings == null) {
				assertNull(keyIterable);
				continue;
			}

			assertNotNull(state.mapKeys(i));

			for (Integer key : keyIterable) {
				Float actualValue = state.get(i, key);
				Float expectedVluae = mappings.get(key);

				assertEquals(expectedVluae, actualValue);
			}
		}

		// Tests for state#values
		for (int i = 0; i < 10; ++i) {
			Map<Integer, Float> mappings = pairs.get(i);
			Iterable<Float> valueIterable = state.mapValues(i);

			if (mappings == null) {
				assertNull(valueIterable);
				continue;
			}

			assertNotNull(valueIterable);

			for (Float value : valueIterable) {
				assertTrue(mappings.containsValue(value));
			}
		}

		assertFalse(state.contains(null, 1));
		assertNull(state.get(null, 1));

		assertFalse(state.contains(1, null));
		assertNull(state.get(1, null));

		assertNull(state.get(1, 1111));
		assertNull(state.get(1111, 0));

		// Removes some mappings from the state and validates that they do
		// not exist in their corresponding maps.
		for (Map.Entry<Integer, Map<Integer, Float>> pair : pairs.entrySet()) {
			Integer key = pair.getKey();
			Map<Integer, Float> mappings = pair.getValue();

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

		for (Map.Entry<Integer, Map<Integer, Float>> pair : pairs.entrySet()) {
			Map<Integer, Float> expectedValue = pair.getValue();
			Map<Integer, Float> actualValue = state.get(pair.getKey());
			assertEquals(expectedValue, actualValue);
		}

		// Retrieves some mappings from the state
		for (Map.Entry<Integer, Map<Integer, Float>> pair : pairs.entrySet()) {
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
				Float expectedValue = mappings.get(retrievedMapKey);
				Float actualValue = retrievedMappings.get(retrievedMapKey);
				assertEquals(expectedValue, actualValue);
			}
		}

		// Removes some mappings from the state and validates that they do
		// not exist in their corresponding maps.
		for (Map.Entry<Integer, Map<Integer, Float>> pair : pairs.entrySet()) {
			Integer key = pair.getKey();
			Map<Integer, Float> mappings = pair.getValue();
			if (mappings == null) {
				continue;
			}

			Set<Integer> removedMapKeys = new HashSet<>();
			Iterator<Integer> mapKeyIterator = mappings.keySet().iterator();
			int index = 0;
			while (mapKeyIterator.hasNext()) {
				Integer mapKey = mapKeyIterator.next();
				if (index % 5 == 0) {
					removedMapKeys.add(mapKey);
					mapKeyIterator.remove();
				}
				index++;
			}
			removedMapKeys.add(null);
			removedMapKeys.add(11111);

			state.removeAll(key, removedMapKeys);

			if (mappings.isEmpty()) {
				pair.setValue(null);
			}
		}

		for (Map.Entry<Integer, Map<Integer, Float>> pair : pairs.entrySet()) {
			Map<Integer, Float> expectedValue = pair.getValue();
			Map<Integer, Float> actualValue = state.get(pair.getKey());
			assertEquals(expectedValue, actualValue);
		}

		// Retrieves some mappings from the state
		Map<Integer, Set<Integer>> retrievedMap = null;
		Map<Integer, Map<Integer, Float>> retrievedPairs = state.getAll(retrievedMap);
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

		for (Map.Entry<Integer, Map<Integer, Float>> pair : pairs.entrySet()) {
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
				retrievedMapKeys.add(null);
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
					Float expectedValue = mappings == null ? null : mappings.get(retrievedMapKey);
					Float actualValue = retrievedMappings == null ? null : retrievedMappings.get(retrievedMapKey);
					assertEquals(expectedValue, actualValue);
				}
			}
		}

		// Removes some mappings from the state and validates that they do
		// not exist in their corresponding lists.
		Map<Integer, Set<Integer>> removedMap = new HashMap<>();

		for (Map.Entry<Integer, Map<Integer, Float>> pair : pairs.entrySet()) {
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
				removedMapKeys.add(null);
				removedMapKeys.add(11111);
				removedMap.put(key, removedMapKeys);

				if (mappings.isEmpty()) {
					pair.setValue(null);
				}
			}
		}

		removedMap.put(null, Collections.singleton(1));
		removedMap.put(11111, Collections.singleton(1));

		state.removeAll(removedMap);
		for (Map.Entry<Integer, Map<Integer, Float>> pair : pairs.entrySet()) {
			Map<Integer, Float> expectedValue = pair.getValue();
			Map<Integer, Float> actualValue = state.get(pair.getKey());
			assertEquals(expectedValue, actualValue);
		}
	}

	@Test
	public void testkeyMapIndependentIterator() throws Exception {
		KeyedMapStateDescriptor<Integer, Integer, Float> descriptor =
			new KeyedMapStateDescriptor<>("test", IntSerializer.INSTANCE,
				IntSerializer.INSTANCE, FloatSerializer.INSTANCE);

		KeyedMapState<Integer, Integer, Float> state = backend.getKeyedState(descriptor);
		assertNotNull(state);

		Random random = new Random();
		Map<Integer, Map<Integer, Float>> pairs = new HashMap<>();
		for (int i = 0; i < 10; ++i) {
			int numMappings = random.nextInt(19) + 1;

			Map<Integer, Float> mappings = new HashMap<>();
			for (int j = 0; j < numMappings; j++) {
				Float value = random.nextFloat();

				state.add(i, j, value);
				mappings.put(j, value);
			}

			pairs.put(i, mappings);
		}

		for (int key = 0; key < 10; ++key) {
			Map<Integer, Float> mappings = pairs.get(key);
			Iterable<Map.Entry<Integer, Float>> entryIterable = state.entries(key);
			if (mappings == null) {
				assertNull(entryIterable);
				continue;
			}

			assertNotNull(entryIterable);

			Iterator<Map.Entry<Integer, Float>> iter1 = entryIterable.iterator();
			Iterator<Map.Entry<Integer, Float>> iter2 = entryIterable.iterator();

			int iterNum = 0;
			Set<Map.Entry<Integer, Float>> mappingEntry = mappings.entrySet();
			while (iter1.hasNext() && iter2.hasNext()) {
				Map.Entry<Integer, Float> entry1 = iter1.next();
				assertTrue(mappingEntry.contains(entry1));

				Map.Entry<Integer, Float> entry2 = iter2.next();
				assertTrue(mappingEntry.contains(entry2));

				iterNum++;
			}

			assertEquals(iterNum, mappings.size());
		}

		for (int key = 0; key < 10; ++key) {
			Map<Integer, Float> mappings = pairs.get(key);
			Iterable<Integer> keyIterable = state.mapKeys(key);
			if (mappings == null) {
				assertNull(keyIterable);
				continue;
			}

			assertNotNull(keyIterable);

			Iterator<Integer> iter1 = keyIterable.iterator();
			Iterator<Integer> iter2 = keyIterable.iterator();

			int iterNum = 0;
			Set<Integer> keySet = mappings.keySet();
			while (iter1.hasNext() && iter2.hasNext()) {
				Integer key1 = iter1.next();
				assertTrue(keySet.contains(key1));

				Integer key2 = iter2.next();
				assertTrue(keySet.contains(key2));

				iterNum++;
			}

			assertEquals(keySet.size(), iterNum);
		}

		for (int key = 0; key < 10; ++key) {
			Map<Integer, Float> mappings = pairs.get(key);
			Iterable<Float> valueIterable = state.mapValues(key);
			if (mappings == null) {
				assertNull(valueIterable);
				continue;
			}

			assertNotNull(valueIterable);

			Iterator<Float> iter1 = valueIterable.iterator();
			Iterator<Float> iter2 = valueIterable.iterator();

			int iterNum = 0;
			Collection<Float> valueSet = mappings.values();
			while (iter1.hasNext() && iter2.hasNext()) {
				Float value1 = iter1.next();
				assertTrue(valueSet.contains(value1));

				Float value2 = iter2.next();
				assertTrue(valueSet.contains(value2));

				iterNum++;
			}

			assertEquals(iterNum, valueSet.size());
		}
	}

	@Test
	public void testGetAllRemoveAll() throws Exception {
		KeyedMapStateDescriptor<Integer, Integer, Float> descriptor =
			new KeyedMapStateDescriptor<>("test", IntSerializer.INSTANCE,
				IntSerializer.INSTANCE, FloatSerializer.INSTANCE);

		KeyedMapState<Integer, Integer, Float> state = backend.getKeyedState(descriptor);
		assertNotNull(state);

		Map<Integer, Map<Integer, Float>> pairs = new HashMap<>();
		populateState(pairs, state);

		Map<Integer, Map<Integer, Float>> allState = state.getAll();
		assertEquals(allState, pairs);

		state.removeAll();
		allState = state.getAll();
		assertTrue(allState.isEmpty());
	}

	@Test
	public void testKeys() throws Exception {
		KeyedMapStateDescriptor<Integer, Integer, Float> descriptor =
			new KeyedMapStateDescriptor<>("test", IntSerializer.INSTANCE,
				IntSerializer.INSTANCE, FloatSerializer.INSTANCE);

		KeyedMapState<Integer, Integer, Float> state = backend.getKeyedState(descriptor);
		assertNotNull(state);

		Map<Integer, Map<Integer, Float>> pairs = new HashMap<>();
		populateState(pairs, state);

		Iterable<Integer> keyIterable = state.keys();
		Set<Integer> actualKeys = new HashSet<>();
		for (Integer k : keyIterable) {
			actualKeys.add(k);
		}
		assertEquals(actualKeys, pairs.keySet());

		Iterator<Integer> iter1 = keyIterable.iterator();
		Iterator<Integer> iter2 = keyIterable.iterator();

		Set<Integer> keySet1 = new HashSet<>();
		Set<Integer> keySet2 = new HashSet<>();

		while (iter1.hasNext() && iter2.hasNext()) {
			Integer k1 = iter1.next();
			Integer k2 = iter2.next();
			assertEquals(k1, k2);

			keySet1.add(k1);
			keySet2.add(k2);
		}
		assertEquals(keySet1, pairs.keySet());
		assertEquals(keySet2, pairs.keySet());
	}

	@Test
	public void testMulitStateAccessParallism() throws Exception {
		KeyedMapStateDescriptor<Integer, Integer, Float> descriptor1 =
			new KeyedMapStateDescriptor<>("test1", IntSerializer.INSTANCE,
				IntSerializer.INSTANCE, FloatSerializer.INSTANCE);

		KeyedMapState<Integer, Integer, Float> state1 = backend.getKeyedState(descriptor1);

		KeyedMapStateDescriptor<Integer, Integer, Float> descriptor2 =
			new KeyedMapStateDescriptor<>("test2", IntSerializer.INSTANCE,
				IntSerializer.INSTANCE, FloatSerializer.INSTANCE);

		KeyedMapState<Integer, Integer, Float> state2 = backend.getKeyedState(descriptor2);

		int stateCount = 1000;
		ConcurrentHashMap<Tuple2<Integer, Integer>, Float> value1 = new ConcurrentHashMap<>(stateCount);
		Thread thread1 = new Thread(() -> {
			for (int i = 0; i < stateCount; ++i) {
				int mapKey = ThreadLocalRandom.current().nextInt();
				Float value = ThreadLocalRandom.current().nextFloat();
				state1.add(i, mapKey, value);
				value1.put(Tuple2.of(i, mapKey), value);
			}
		});

		ConcurrentHashMap<Tuple2<Integer, Integer>, Float> value2 = new ConcurrentHashMap<>(stateCount);
		Thread thread2 = new Thread(() -> {
			for (int i = 0; i < stateCount; ++i) {
				int mapKey = ThreadLocalRandom.current().nextInt();
				Float value = ThreadLocalRandom.current().nextFloat();
				state2.add(i, mapKey, value);
				value2.put(Tuple2.of(i, mapKey), value);
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

	private void populateState(Map<Integer, Map<Integer, Float>> pairs, KeyedMapState<Integer, Integer, Float> state) {
		Random random = new Random();
		for (int i = 0; i < 10; ++i) {
			int numMappings = random.nextInt(19) + 1;

			Map<Integer, Float> mappings = new HashMap<>();
			for (int j = 0; j < numMappings; j++) {
				Float mapValue = random.nextFloat();
				mappings.put(j, mapValue);
			}

			pairs.put(i, mappings);
			state.addAll(i, mappings);
		}
	}

	private KeyGroupRange getGroupsForSubtask(int maxParallelism, int parallelism, int subtaskIndex) {
		return KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(maxParallelism, parallelism, subtaskIndex);
	}
}

