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

import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.AbstractInternalStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.TestLocalRecoveryConfig;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
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
 * Unit tests for {@link SubKeyedMapState}.
 */
public abstract class SubKeyedMapStateTestBase {

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

		SubKeyedMapStateDescriptor<Integer, String, Integer, Float> descriptor =
			new SubKeyedMapStateDescriptor<>("test",
				IntSerializer.INSTANCE, StringSerializer.INSTANCE,
				IntSerializer.INSTANCE, FloatSerializer.INSTANCE);
		SubKeyedMapState<Integer, String, Integer, Float> state = backend.getSubKeyedState(descriptor);
		assertNotNull(state);

		Random random = new Random();

		Map<Integer, Map<String, Map<Integer, Float>>> keyMap = new HashMap<>();
		for (int i = 0; i < 10; ++i) {
			Map<String, Map<Integer, Float>> namespaceMap = new HashMap<>();
			for (int j = 0; j <= i; ++j) {
				String namespace = Integer.toString(j);

				int numMappings = random.nextInt(9) + 1;
				Map<Integer, Float> mappings = new HashMap<>(numMappings);
				for (int k = 0; k < numMappings; ++k) {
					Float mapValue = random.nextFloat();
					mappings.put(k, mapValue);
				}

				namespaceMap.put(namespace, mappings);
			}

			keyMap.put(i, namespaceMap);
		}

		// Validates that no entry exists in the state when the state is empty.
		for (Map.Entry<Integer, Map<String, Map<Integer, Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();

			Map<String, Map<Integer, Float>> namespaceMap = keyEntry.getValue();
			for (Map.Entry<String, Map<Integer, Float>> namespaceEntry : namespaceMap.entrySet()) {
				String namespace = namespaceEntry.getKey();
				assertFalse(state.contains(key, namespace));
				assertNull(state.get(key, namespace));
				assertEquals(Collections.emptyMap(), state.getOrDefault(key, namespace, Collections.emptyMap()));
			}
		}

		// Adds some entries into the state and validates that their values can
		// be correctly retrieved.
		for (Map.Entry<Integer, Map<String, Map<Integer, Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();

			Map<String, Map<Integer, Float>> namespaceMap = keyEntry.getValue();
			for (Map.Entry<String, Map<Integer, Float>> namespaceEntry : namespaceMap.entrySet()) {
				String namespace = namespaceEntry.getKey();
				Map<Integer, Float> value = namespaceEntry.getValue();

				state.addAll(key, namespace, value);
			}
		}

		for (Map.Entry<Integer, Map<String, Map<Integer, Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();

			Map<String, Map<Integer, Float>> namespaceMap = keyEntry.getValue();
			for (Map.Entry<String, Map<Integer, Float>> namespaceEntry : namespaceMap.entrySet()) {
				String namespace = namespaceEntry.getKey();
				assertTrue(state.contains(key, namespace));

				Map<Integer, Float> expectedValue = namespaceEntry.getValue();
				assertEquals(expectedValue, state.get(key, namespace));
				assertEquals(expectedValue, state.getOrDefault(key, namespace, Collections.emptyMap()));
			}
		}

		assertFalse(state.contains(null, "1"));
		assertNull(state.get(null, "1"));

		assertFalse(state.contains(1, null));
		assertNull(state.get(1, null));

		// Removes some values from the state and validates that the removed
		// values do not exist in the state any more.
		Map<Integer, Set<String>> removedKeyMap = new HashMap<>();

		for (Map.Entry<Integer, Map<String, Map<Integer, Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();
			Set<String> removedNamespaces = new HashSet<>();

			int index = 0;
			Map<String, Map<Integer, Float>> namespaceMap = keyEntry.getValue();
			for (String namespace : namespaceMap.keySet()) {
				if (key == 0 || index % key == 0) {
					state.remove(key, namespace);
					removedNamespaces.add(namespace);
				}
				index++;
			}

			removedKeyMap.put(key, removedNamespaces);
		}

		for (Map.Entry<Integer, Map<String, Map<Integer, Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();
			Set<String> removedNamespaces = removedKeyMap.get(key);

			Map<String, Map<Integer, Float>> namespaceMap = keyEntry.getValue();
			for (Map.Entry<String, Map<Integer, Float>> namespaceEntry : namespaceMap.entrySet()) {
				String namespace = namespaceEntry.getKey();
				if (removedNamespaces.contains(namespace)) {
					assertFalse(state.contains(key, namespace));
					assertNull(state.get(key, namespace));
				} else {
					Map<Integer, Float> expectedValue = namespaceEntry.getValue();
					Map<Integer, Float> actualValue = state.get(key, namespace);
					assertEquals(expectedValue, actualValue);
				}
			}

			namespaceMap.keySet().removeAll(removedNamespaces);
		}

		// Adds more entries into the state and validates that the values of the
		// pairs can be correctly retrieved.
		for (int i = 5; i < 15; ++i) {
			Map<String, Map<Integer, Float>> namespaceMap = keyMap.get(i);
			if (namespaceMap == null) {
				namespaceMap = new HashMap<>();
				keyMap.put(i, namespaceMap);
			}

			for (int j = 0; j < i + 5; ++j) {
				String namespace = Integer.toString(j);

				Map<Integer, Float> addedMappings = new HashMap<>();
				int numAddedMappings = random.nextInt(9) + 1;
				for (int k = 0; k < numAddedMappings; ++k) {
					addedMappings.put(k, random.nextFloat());
				}

				state.addAll(i, namespace, addedMappings);

				Map<Integer, Float> mappings = namespaceMap.get(namespace);
				if (mappings == null) {
					namespaceMap.put(namespace, addedMappings);
				} else {
					mappings.putAll(addedMappings);
				}
			}
		}

		for (Map.Entry<Integer, Map<String, Map<Integer, Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();
			Map<String, Map<Integer, Float>> expectedNamespaceMap = keyEntry.getValue();
			Map<String, Map<Integer, Float>> actualNamespaceMap = state.getAll(key);
			assertEquals(expectedNamespaceMap, actualNamespaceMap);
		}

		Map<String, Map<Integer, Float>> nullNamespaceMap = state.getAll(null);
		assertNotNull(nullNamespaceMap);
		assertTrue(nullNamespaceMap.isEmpty());

		// Removes some keys from the state and validates that there is no
		// values under these keys.
		Set<Integer> removedKeys = new HashSet<>();
		int index = 0;
		for (Map.Entry<Integer, Map<String, Map<Integer, Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();
			if (index % 4 == 0) {
				state.removeAll(key);
				removedKeys.add(key);
			}
			index++;
		}

		for (Map.Entry<Integer, Map<String, Map<Integer, Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();
			Map<String, Map<Integer, Float>> actualNamespaceMap = state.getAll(key);
			if (removedKeys.contains(key)) {
				assertNotNull(actualNamespaceMap);
				assertTrue(actualNamespaceMap.isEmpty());
			} else {
				Map<String, Map<Integer, Float>> expectedNamespaceMap = keyEntry.getValue();
				assertEquals(expectedNamespaceMap, actualNamespaceMap);
			}
		}

		for (int key = 0; key < 15; ++key) { // key
			Map<String, Map<Integer, Float>> namespaceMap = keyMap.get(key);
			assertNotNull(namespaceMap);
			if (removedKeys.contains(key)) {
				continue;
			}

			for (int i = 0; i < key + 5; ++i) { // namespace
				String namespace = Integer.toString(i);
				Map<Integer, Float> mappings = namespaceMap.get(namespace);
				Iterable<Map.Entry<Integer, Float>> stateIterable = state.entries(key, namespace);

				assertNotNull(stateIterable);

				if (mappings == null) {
					assertFalse(stateIterable.iterator().hasNext());
					continue;
				}

				Set<Map.Entry<Integer, Float>> entrySet = mappings.entrySet();
				for (Map.Entry<Integer, Float> entry : stateIterable) {
					assertTrue(entrySet.contains(entry));
				}
			}
		}

		for (int key = 0; key < 15; ++key) {
			Map<String, Map<Integer, Float>> namespaceMap = keyMap.get(key);
			assertNotNull(namespaceMap);
			if (removedKeys.contains(key)) {
				continue;
			}

			for (int i = 0; i < key + 5; ++i) {
				String namespace = Integer.toString(i);
				Map<Integer, Float> mappings = namespaceMap.get(namespace);
				Iterable<Integer> keyIterable = state.keys(key, namespace);

				assertNotNull(keyIterable);

				if (mappings == null) {
					assertFalse(keyIterable.iterator().hasNext());
					continue;
				}

				Set<Integer> keySet = mappings.keySet();
				for (Integer mk : keyIterable) {
					assertTrue(keySet.contains(mk));
				}
			}
		}

		for (int key = 0; key < 15; ++key) {
			Map<String, Map<Integer, Float>> namespaceMap = keyMap.get(key);
			assertNotNull(namespaceMap);
			if (removedKeys.contains(key)) {
				continue;
			}

			for (int i = 0; i < key + 5; ++i) {
				String namespace = Integer.toString(i);
				Map<Integer, Float> mappings = namespaceMap.get(namespace);
				Iterable<Float> valueIterable = state.values(key, namespace);

				assertNotNull(valueIterable);

				if (mappings == null) {
					assertFalse(valueIterable.iterator().hasNext());
					continue;
				}

				Collection<Float> valueCollection = mappings.values();
				for (Float mv : valueIterable) {
					assertTrue(valueCollection.contains(mv));
				}
			}
		}
	}

	@Test
	public void testMapKeyAccess() throws Exception {
		SubKeyedMapStateDescriptor<Integer, String, Integer, Float> descriptor =
			new SubKeyedMapStateDescriptor<>("test",
				IntSerializer.INSTANCE, StringSerializer.INSTANCE,
				IntSerializer.INSTANCE, FloatSerializer.INSTANCE);

		SubKeyedMapState<Integer, String, Integer, Float> state = backend.getSubKeyedState(descriptor);
		assertNotNull(state);

		Random random = new Random();

		// Adds some elements into the state and validates that they can be
		// correctly retrieved.
		Map<Integer, Map<String, Map<Integer, Float>>> keyMap = new HashMap<>();
		for (int i = 0; i < 10; ++i) {
			Map<String, Map<Integer, Float>> namespaceMap = new HashMap<>();
			for (int j = 0; j <= i; ++j) {
				String namespace = Integer.toString(j);

				int numMappings = random.nextInt(9) + 1;
				Map<Integer, Float> mappings = new HashMap<>(numMappings);
				for (int k = 0; k < numMappings; ++k) {
					Float element = random.nextFloat();
					state.add(i, namespace, k, element);
					mappings.put(k, element);
				}

				namespaceMap.put(namespace, mappings);
			}

			keyMap.put(i, namespaceMap);
		}

		for (Map.Entry<Integer, Map<String, Map<Integer, Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();

			Map<String, Map<Integer, Float>> namespaceMap = keyEntry.getValue();
			for (Map.Entry<String, Map<Integer, Float>> namespaceEntry : namespaceMap.entrySet()) {
				String namespace = namespaceEntry.getKey();

				Map<Integer, Float> mappings = namespaceEntry.getValue();
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
		for (Map.Entry<Integer, Map<String, Map<Integer, Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();

			Map<String, Map<Integer, Float>> namespaceMap = keyEntry.getValue();
			for (Map.Entry<String, Map<Integer, Float>> namespaceEntry : namespaceMap.entrySet()) {
				String namespace = namespaceEntry.getKey();
				Map<Integer, Float> mappings = namespaceEntry.getValue();

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

		for (Map.Entry<Integer, Map<String, Map<Integer, Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();

			Map<String, Map<Integer, Float>> namespaceMap = keyEntry.getValue();
			for (Map.Entry<String, Map<Integer, Float>> namespaceEntry : namespaceMap.entrySet()) {
				Map<Integer, Float> expectedValue = namespaceEntry.getValue();
				Map<Integer, Float> actualValue = state.get(key, namespaceEntry.getKey());
				assertEquals(expectedValue, actualValue);
			}
		}

		// Retrieves some values from the state and validates that they are
		// equal to the expected ones.
		for (Map.Entry<Integer, Map<String, Map<Integer, Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();

			Map<String, Map<Integer, Float>> namespaceMap = keyEntry.getValue();
			for (Map.Entry<String, Map<Integer, Float>> namespaceEntry : namespaceMap.entrySet()) {
				String namespace = namespaceEntry.getKey();
				Map<Integer, Float> mappings = namespaceEntry.getValue();
				if (mappings == null) {
					continue;
				}

				Set<Integer> retrievedMapKeys = null;

				Map<Integer, Float> retrievedMappings = state.getAll(key, namespace, retrievedMapKeys);
				assertNull(retrievedMappings);

				retrievedMapKeys = new HashSet<>();
				retrievedMappings = state.getAll(key, namespace, retrievedMapKeys);
				assertNull(retrievedMappings);

				retrievedMapKeys.add(null);
				retrievedMappings = state.getAll(key, namespace, retrievedMapKeys);
				assertNull(retrievedMappings);

				retrievedMapKeys.add(11111);
				retrievedMappings = state.getAll(key, namespace, retrievedMapKeys);
				assertNull(retrievedMappings);

				int index = 0;
				for (Integer mapKey : mappings.keySet()) {
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
		for (Map.Entry<Integer, Map<String, Map<Integer, Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();

			Map<String, Map<Integer, Float>> namespaceMap = keyEntry.getValue();
			for (Map.Entry<String, Map<Integer, Float>> namespaceEntry : namespaceMap.entrySet()) {
				String namespace = namespaceEntry.getKey();
				Map<Integer, Float> mappings = namespaceEntry.getValue();
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

		for (Map.Entry<Integer, Map<String, Map<Integer, Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();

			Map<String, Map<Integer, Float>> namespaceMap = keyEntry.getValue();
			for (Map.Entry<String, Map<Integer, Float>> namespaceEntry : namespaceMap.entrySet()) {
				Map<Integer, Float> expectedValue = namespaceEntry.getValue();
				Map<Integer, Float> actualValue = state.get(key, namespaceEntry.getKey());
				assertEquals(expectedValue, actualValue);
			}
		}
	}

	@Test
	public void testsubKeyedMapIndependentIterator() throws Exception {
		SubKeyedMapStateDescriptor<Integer, String, Integer, Float> descriptor =
			new SubKeyedMapStateDescriptor<>("test",
				IntSerializer.INSTANCE, StringSerializer.INSTANCE,
				IntSerializer.INSTANCE, FloatSerializer.INSTANCE);
		SubKeyedMapState<Integer, String, Integer, Float> state = backend.getSubKeyedState(descriptor);
		assertNotNull(state);

		Random random = new Random();

		Map<Integer, Map<String, Map<Integer, Float>>> keyMap = new HashMap<>();
		for (int i = 0; i < 10; ++i) {
			Map<String, Map<Integer, Float>> namespaceMap = new HashMap<>();
			for (int j = 0; j < i; ++j) {
				String namespace = Integer.toString(j);

				int numMappings = random.nextInt(9) + 1;
				Map<Integer, Float> mappings = new HashMap<>(numMappings);
				for (int k = 0; k < numMappings; ++k) {
					Float mapValue = (i == 0 && j == 0) ? null : random.nextFloat();
					mappings.put(k, mapValue);
				}

				namespaceMap.put(namespace, mappings);
				state.addAll(i, namespace, mappings);
			}
			keyMap.put(i, namespaceMap);
		}

		for (int key = 0; key < 10; ++key) { // key
			Map<String, Map<Integer, Float>> namespaceMap = keyMap.get(key);
			assertNotNull(namespaceMap);

			for (int i = 0; i < key; ++i) { // namespace
				String namespace = Integer.toString(i);
				Map<Integer, Float> mappings = namespaceMap.get(namespace);
				assertNotNull(mappings);

				Iterable<Map.Entry<Integer, Float>> entryIterable = state.entries(key, namespace);
				Iterator<Map.Entry<Integer, Float>> iter1 = entryIterable.iterator();
				Iterator<Map.Entry<Integer, Float>> iter2 = entryIterable.iterator();

				int iterNum = 0;
				Set<Map.Entry<Integer, Float>> entrySet = mappings.entrySet();
				while (iter1.hasNext() && iter2.hasNext()) {
					Map.Entry<Integer, Float> entry1 = iter1.next();
					assertTrue(entrySet.contains(entry1));

					Map.Entry<Integer, Float> entry2 = iter2.next();
					assertTrue(entrySet.contains(entry2));

					iterNum++;
				}
				assertEquals(iterNum, entrySet.size());
			}
		}

		for (int key = 0; key < 10; ++key) {
			Map<String, Map<Integer, Float>> namespaceMap = keyMap.get(key);
			assertNotNull(namespaceMap);

			for (int i = 0; i < key; ++i) {
				String namespace = Integer.toString(i);
				Map<Integer, Float> mappings = namespaceMap.get(namespace);
				assertNotNull(mappings);

				Iterable<Integer> keyIterable = state.keys(key, namespace);
				Iterator<Integer> iter1 = keyIterable.iterator();
				Iterator<Integer> iter2 = keyIterable.iterator();

				int keyIterNum = 0;
				Set<Integer> keySet = mappings.keySet();
				while (iter1.hasNext() && iter2.hasNext()) {
					Integer key1 = iter1.next();
					assertTrue(keySet.contains(key1));

					Integer key2 = iter2.next();
					assertTrue(keySet.contains(key2));

					keyIterNum++;
				}
				assertEquals(keyIterNum, keySet.size());
			}
		}

		for (int key = 0; key < 10; ++key) {
			Map<String, Map<Integer, Float>> namespaceMap = keyMap.get(key);
			assertNotNull(namespaceMap);

			for (int i = 0; i < key; ++i) {
				String namespace = Integer.toString(i);
				Map<Integer, Float> mappings = namespaceMap.get(namespace);
				assertNotNull(mappings);

				Iterable<Float> valueIterable = state.values(key, namespace);
				Iterator<Float> iter1 = valueIterable.iterator();
				Iterator<Float> iter2 = valueIterable.iterator();

				int valueIterNum = 0;
				Collection<Float> valueCollection = mappings.values();
				while (iter1.hasNext() && iter2.hasNext()) {
					Float value1 = iter1.next();
					assertTrue(valueCollection.contains(value1));

					Float value2 = iter2.next();
					assertTrue(valueCollection.contains(value2));
					valueIterNum++;
				}
				assertEquals(valueIterNum, valueCollection.size());
			}
		}
	}

	@Test
	public void testIterator() throws Exception {
		SubKeyedMapStateDescriptor<Integer, String, Integer, Float> descriptor =
			new SubKeyedMapStateDescriptor<>("test",
				IntSerializer.INSTANCE, StringSerializer.INSTANCE,
				IntSerializer.INSTANCE, FloatSerializer.INSTANCE);
		SubKeyedMapState<Integer, String, Integer, Float> state = backend.getSubKeyedState(descriptor);
		assertNotNull(state);

		Random random = new Random();

		Map<Integer, Map<String, Map<Integer, Float>>> keyMap = new HashMap<>();
		for (int i = 0; i < 10; ++i) {
			Map<String, Map<Integer, Float>> namespaceMap = new HashMap<>();
			for (int j = 0; j <= i; ++j) {
				String namespace = Integer.toString(j);

				int numMappings = random.nextInt(9) + 1;
				Map<Integer, Float> mappings = new HashMap<>(numMappings);
				for (int k = 0; k < numMappings; ++k) {
					Float mapValue = random.nextFloat();
					mappings.put(k, mapValue);
				}

				namespaceMap.put(namespace, mappings);
				state.addAll(i, namespace, mappings);
			}
			keyMap.put(i, namespaceMap);
		}

		state.removeAll(6);
		Iterator<String> iter1 = state.iterator(6);
		assertFalse(iter1.hasNext());

		state.remove(1, "0");
		state.remove(1, "1");
		Iterator<String> iter2 = state.iterator(1);
		try {
			iter1.next();
		} catch (NoSuchElementException e) {
			// ignore this exception.
		}
		assertFalse(iter2.hasNext());

		state.add(1, "0", 1, 1.0f);
		iter2 = state.iterator(1);
		assertTrue(iter2.hasNext());
		String namespace1 = iter2.next();
		assertEquals(namespace1, "0");
		assertFalse(iter2.hasNext());

		state.removeAll(2, "1", keyMap.get(2).get("1").keySet());
		Iterator<String> iter3 = state.iterator(2);
		Set<String> namespace3 = new HashSet<>();
		while (iter3.hasNext()) {
			String n = iter3.next();
			namespace3.add(n);
		}
		Set<String> expectedSet = new HashSet<>();
		expectedSet.add("0");
		expectedSet.add("2");
		assertEquals(expectedSet, namespace3);

		iter3 = state.iterator(2);
		iter3.next();
		iter3.remove();
		assertTrue(iter3.hasNext());
		String acturalNamespace = iter3.next();
		assertFalse(iter3.hasNext());
		Map<Integer, Float> acturalList = state.get(2, acturalNamespace);
		Map<Integer, Float> excpectList = keyMap.get(2).get("2");
		assertEquals(excpectList, acturalList);

		Iterator<String> iter4 = state.iterator(3);
		while (iter4.hasNext()) {
			iter4.next();
			iter4.remove();
		}

		for (int i = 0; i <= 3; ++i) {
			String namespace = Integer.toString(i);
			Map<Integer, Float> value = state.get(3, namespace);
			assertNull(value);
		}

		Iterator<String> iter5 = state.iterator(5);
		try {
			iter5.remove();
		} catch (IllegalStateException e) {
			// ignore this exception.
		}
		iter5.next();
		iter5.remove();
		try {
			iter5.remove();
		} catch (IllegalStateException e) {
			// ignore this exception.
		}
	}

	@Test
	public void testMultiStateAccessParallism() throws Exception {
		SubKeyedMapStateDescriptor<Integer, Integer, Integer, Float> descriptor1 =
			new SubKeyedMapStateDescriptor<>("test1",
				IntSerializer.INSTANCE, IntSerializer.INSTANCE,
				IntSerializer.INSTANCE, FloatSerializer.INSTANCE);
		SubKeyedMapState<Integer, Integer, Integer, Float> state1 = backend.getSubKeyedState(descriptor1);

		SubKeyedMapStateDescriptor<Integer, Integer, Integer, Float> descriptor2 =
			new SubKeyedMapStateDescriptor<>("test2",
				IntSerializer.INSTANCE, IntSerializer.INSTANCE,
				IntSerializer.INSTANCE, FloatSerializer.INSTANCE);
		SubKeyedMapState<Integer, Integer, Integer, Float> state2 = backend.getSubKeyedState(descriptor2);

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
		SubKeyedMapStateDescriptor<Integer, String, Integer, Float> descriptor =
			new SubKeyedMapStateDescriptor<>("test",
				IntSerializer.INSTANCE, StringSerializer.INSTANCE,
				IntSerializer.INSTANCE, FloatSerializer.INSTANCE);
		SubKeyedMapState<Integer, String, Integer, Float> state = backend.getSubKeyedState(descriptor);
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

