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
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.TestLocalRecoveryConfig;
import org.apache.flink.runtime.state.AbstractInternalStateBackend;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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
 * Unit tests for {@link SubKeyedListState}.
 */
public abstract class SubKeyedListStateTestBase {

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

		SubKeyedListStateDescriptor<Integer, String, Float> descriptor =
			new SubKeyedListStateDescriptor<>("test",
				IntSerializer.INSTANCE, StringSerializer.INSTANCE,
				FloatSerializer.INSTANCE);
		SubKeyedListState<Integer, String, Float> state = backend.getSubKeyedState(descriptor);
		assertNotNull(state);

		Random random = new Random();

		Map<Integer, Map<String, List<Float>>> keyMap = new HashMap<>();
		for (int i = 0; i < 10; ++i) {
			Map<String, List<Float>> namespaceMap = new HashMap<>();
			for (int j = 0; j <= i; ++j) {
				String namespace = Integer.toString(j);

				int numElements = random.nextInt(9) + 1;
				List<Float> elements = new ArrayList<>(numElements);
				for (int k = 0; k < numElements; ++k) {
					Float element = random.nextFloat();
					elements.add(element);
				}

				namespaceMap.put(namespace, elements);
			}

			keyMap.put(i, namespaceMap);
		}

		// Validates that no entry exists in the state when the state is empty.
		for (Map.Entry<Integer, Map<String, List<Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();

			Map<String, List<Float>> namespaceMap = keyEntry.getValue();
			for (Map.Entry<String, List<Float>> namespaceEntry : namespaceMap.entrySet()) {
				String namespace = namespaceEntry.getKey();
				assertFalse(state.contains(key, namespace));
				assertNull(state.get(key, namespace));
				assertEquals(Collections.emptyList(), state.getOrDefault(key, namespace, Collections.emptyList()));
			}
		}

		// Adds some entries into the state and validates that their values can
		// be correctly retrieved.
		for (Map.Entry<Integer, Map<String, List<Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();

			Map<String, List<Float>> namespaceMap = keyEntry.getValue();
			for (Map.Entry<String, List<Float>> namespaceEntry : namespaceMap.entrySet()) {
				String namespace = namespaceEntry.getKey();
				List<Float> value = namespaceEntry.getValue();

				state.addAll(key, namespace, value);
			}
		}

		for (Map.Entry<Integer, Map<String, List<Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();

			Map<String, List<Float>> namespaceMap = keyEntry.getValue();
			for (Map.Entry<String, List<Float>> namespaceEntry : namespaceMap.entrySet()) {
				String namespace = namespaceEntry.getKey();
				assertTrue(state.contains(key, namespace));

				List<Float> expectedValue = namespaceEntry.getValue();
				assertEquals(expectedValue, state.get(key, namespace));
				assertEquals(expectedValue, state.getOrDefault(key, namespace, Collections.emptyList()));
			}
		}

		assertFalse(state.contains(null, "1"));
		assertNull(state.get(null, "1"));

		assertFalse(state.contains(1, null));
		assertNull(state.get(1, null));

		// Removes some values from the state and validates that the removed
		// values do not exist in the state any more.
		Map<Integer, Set<String>> removedKeyMap = new HashMap<>();

		for (Map.Entry<Integer, Map<String, List<Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();
			Set<String> removedNamespaces = new HashSet<>();

			int index = 0;
			Map<String, List<Float>> namespaceMap = keyEntry.getValue();
			for (String namespace : namespaceMap.keySet()) {
				if (key == 0 || index % key == 0) {
					state.remove(key, namespace);
					removedNamespaces.add(namespace);
				}
				index++;
			}

			removedKeyMap.put(key, removedNamespaces);
		}

		for (Map.Entry<Integer, Map<String, List<Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();
			Set<String> removedNamespaces = removedKeyMap.get(key);

			Map<String, List<Float>> namespaceMap = keyEntry.getValue();
			for (Map.Entry<String, List<Float>> namespaceEntry : namespaceMap.entrySet()) {
				String namespace = namespaceEntry.getKey();
				if (removedNamespaces.contains(namespace)) {
					assertFalse(state.contains(key, namespace));
					assertNull(state.get(key, namespace));
				} else {
					List<Float> expectedValue = namespaceEntry.getValue();
					List<Float> actualValue = state.get(key, namespace);
					assertEquals(expectedValue, actualValue);
				}
			}

			namespaceMap.keySet().removeAll(removedNamespaces);
		}

		// Adds more entries into the state and validates that the values of the
		// pairs can be correctly retrieved.
		for (int i = 5; i < 15; ++i) {
			Map<String, List<Float>> namespaceMap = keyMap.get(i);
			if (namespaceMap == null) {
				namespaceMap = new HashMap<>();
				keyMap.put(i, namespaceMap);
			}

			for (int j = 0; j < i + 5; ++j) {
				String namespace = Integer.toString(j);

				List<Float> addedElements = new ArrayList<>();
				int numElements = random.nextInt(9) + 1;
				for (int k = 0; k < numElements; ++k) {
					addedElements.add(random.nextFloat());
				}

				state.addAll(i, namespace, addedElements);

				List<Float> elements = namespaceMap.get(namespace);
				if (elements == null) {
					namespaceMap.put(namespace, addedElements);
				} else {
					elements.addAll(addedElements);
				}
			}
		}

		for (Map.Entry<Integer, Map<String, List<Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();
			Map<String, List<Float>> expectedNamespaceMap = keyEntry.getValue();
			Map<String, List<Float>> actualNamespaceMap = state.getAll(key);
			assertEquals(expectedNamespaceMap, actualNamespaceMap);
		}

		Map<String, List<Float>> nullNamespaceMap = state.getAll(null);
		assertNotNull(nullNamespaceMap);
		assertTrue(nullNamespaceMap.isEmpty());

		// Removes some keys from the state and validates that there is no
		// values under these keys.
		Set<Integer> removedKeys = new HashSet<>();
		int index = 0;
		for (Map.Entry<Integer, Map<String, List<Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();
			if (index % 4 == 0) {
				state.removeAll(key);
				removedKeys.add(key);
			}
			index++;
		}

		for (Map.Entry<Integer, Map<String, List<Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();
			Map<String, List<Float>> actualNamespaceMap = state.getAll(key);
			if (removedKeys.contains(key)) {
				assertNotNull(actualNamespaceMap);
				assertTrue(actualNamespaceMap.isEmpty());
			} else {
				Map<String, List<Float>> expectedNamespaceMap = keyEntry.getValue();
				assertEquals(expectedNamespaceMap, actualNamespaceMap);
			}
		}
	}

	@Test
	public void testElementAccess() throws Exception {
		SubKeyedListStateDescriptor<Integer, String, Float> descriptor =
			new SubKeyedListStateDescriptor<>("test",
				IntSerializer.INSTANCE, StringSerializer.INSTANCE,
				FloatSerializer.INSTANCE);

		SubKeyedListState<Integer, String, Float> state = backend.getSubKeyedState(descriptor);
		assertNotNull(state);

		Random random = new Random();

		// Adds some elements into the state and validates that they can be
		// correctly retrieved.
		Map<Integer, Map<String, List<Float>>> keyMap = new HashMap<>();
		for (int i = 0; i < 10; ++i) {
			Map<String, List<Float>> namespaceMap = new HashMap<>();
			for (int j = 0; j <= i; ++j) {
				String namespace = Integer.toString(j);

				int numElements = random.nextInt(9) + 1;
				List<Float> elements = new ArrayList<>(numElements);
				for (int k = 0; k < numElements; ++k) {
					Float element = random.nextFloat();
					state.add(i, namespace, element);
					elements.add(element);
				}

				namespaceMap.put(namespace, elements);
			}

			keyMap.put(i, namespaceMap);
		}

		// Removes some elements from the state and validates that they do
		// not exist in their corresponding lists.
		for (Map.Entry<Integer, Map<String, List<Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();

			Map<String, List<Float>> namespaceMap = keyEntry.getValue();
			for (Map.Entry<String, List<Float>> namespaceEntry : namespaceMap.entrySet()) {
				String namespace = namespaceEntry.getKey();
				List<Float> elements = namespaceEntry.getValue();

				int index = 0;
				Iterator<Float> elementIterator = elements.iterator();
				while (elementIterator.hasNext()) {
					Float element = elementIterator.next();
					if (key == 0 || index % key == 0) {
						assertTrue(state.remove(key, namespace, element));
						elementIterator.remove();
					}
					index++;
				}

				if (elements.isEmpty()) {
					namespaceEntry.setValue(null);
				}
			}
		}

		for (Map.Entry<Integer, Map<String, List<Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();

			Map<String, List<Float>> namespaceMap = keyEntry.getValue();
			for (Map.Entry<String, List<Float>> namespaceEntry : namespaceMap.entrySet()) {
				List<Float> expectedValue = namespaceEntry.getValue();
				List<Float> actualValue = state.get(key, namespaceEntry.getKey());
				assertEquals(expectedValue, actualValue);
			}
		}

		// Removes some elements from the state and validates that they do
		// not exist in their corresponding lists.
		for (Map.Entry<Integer, Map<String, List<Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();

			Map<String, List<Float>> namespaceMap = keyEntry.getValue();
			for (Map.Entry<String, List<Float>> namespaceEntry : namespaceMap.entrySet()) {
				String namespace = namespaceEntry.getKey();
				List<Float> elements = namespaceEntry.getValue();
				if (elements == null) {
					continue;
				}

				List<Float> removedElements = new ArrayList<>();
				int index = 0;
				for (Float element : elements) {
					if (key == 0 || index % key == 0) {
						removedElements.add(element);
					}
					index++;
				}

				assertTrue(state.removeAll(key, namespace, removedElements));

				elements.removeAll(removedElements);
				if (elements.isEmpty()) {
					namespaceEntry.setValue(null);
				}
			}
		}

		for (Map.Entry<Integer, Map<String, List<Float>>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();

			Map<String, List<Float>> namespaceMap = keyEntry.getValue();
			for (Map.Entry<String, List<Float>> namespaceEntry : namespaceMap.entrySet()) {
				List<Float> expectedValue = namespaceEntry.getValue();
				List<Float> actualValue = state.get(key, namespaceEntry.getKey());
				assertEquals(expectedValue, actualValue);
			}
		}
	}

	@Test
	public void testIterator() throws Exception {
		SubKeyedListStateDescriptor<Integer, String, Float> descriptor =
			new SubKeyedListStateDescriptor<>("test",
				IntSerializer.INSTANCE, StringSerializer.INSTANCE,
				FloatSerializer.INSTANCE);
		SubKeyedListState<Integer, String, Float> state = backend.getSubKeyedState(descriptor);
		assertNotNull(state);

		Random random = new Random();

		Map<Integer, Map<String, List<Float>>> keyMap = new HashMap<>();
		int keyNumber = 10;
		int leastElementPerKey = 1;
		populateState(keyMap, state, keyNumber, leastElementPerKey);

		for (int key = 0; key < 10; ++key) {
			Iterator<String> iter = state.iterator(key);

			Set<String> namespaceSet = new HashSet<>();
			for (int j = 0; j <= key; ++j) {
				namespaceSet.add(Integer.toString(j));
			}

			int cnt = 0;
			while (iter.hasNext()) {
				String n = iter.next();
				assertTrue(namespaceSet.contains(n));
				cnt++;
			}
			assertEquals(cnt, namespaceSet.size());
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

		state.add(1, "0", 1.0f);
		iter2 = state.iterator(1);
		assertTrue(iter2.hasNext());
		String namespace1 = iter2.next();
		assertEquals(namespace1, "0");
		assertFalse(iter2.hasNext());

		state.removeAll(2, "1", keyMap.get(2).get("1"));
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
		List<Float> acturalList = state.get(2, acturalNamespace);
		List<Float> excpectList = keyMap.get(2).get("2");
		assertEquals(excpectList, acturalList);

		Iterator<String> iter4 = state.iterator(3);
		while (iter4.hasNext()) {
			iter4.next();
			iter4.remove();
		}

		for (int i = 0; i <= 3; ++i) {
			String namespace = Integer.toString(i);
			List<Float> value = state.get(3, namespace);
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
	public void testPeekPoll() throws Exception {
		SubKeyedListStateDescriptor<Integer, String, Float> descriptor =
			new SubKeyedListStateDescriptor<>("test",
				IntSerializer.INSTANCE, StringSerializer.INSTANCE,
				FloatSerializer.INSTANCE);
		SubKeyedListState<Integer, String, Float> state = backend.getSubKeyedState(descriptor);
		assertNotNull(state);

		Map<Integer, Map<String, List<Float>>> keyMap = new HashMap<>();
		int keyNumber = 10;
		int leastElementPerKey = 2;
		populateState(keyMap, state, keyNumber, leastElementPerKey);

		for (int key = 0; key < keyNumber; ++key) {
			for (int j = 0; j <= key; ++j) {
				String namespace = Integer.toString(j);
				Float expectedValue = keyMap.get(key).get(namespace).get(0);
				Float firstActualValue = state.peek(key, namespace);
				assertEquals(expectedValue, firstActualValue);

				Float secondActualValue = state.peek(key, namespace);
				assertEquals(expectedValue, secondActualValue);
			}
		}
		assertNull(state.peek(keyNumber, "1"));

		for (int key = 0; key < keyNumber; ++key) {
			for (int j = 0; j <= key; ++j) {
				String namespace = Integer.toString(j);
				Float expectedValue = keyMap.get(key).get(namespace).get(0);
				Float firstActualValue = state.poll(key, namespace);
				assertEquals(expectedValue, firstActualValue);

				Float secondExpectedValue = keyMap.get(key).get(namespace).get(1);
				Float secondActualValue = state.poll(key, namespace);
				assertEquals(secondExpectedValue, secondActualValue);
			}
		}
		assertNull(state.poll(keyNumber, "1"));
	}

	@Test
	public void testPutPutAll() throws Exception {
		SubKeyedListStateDescriptor<Integer, String, Long> descriptor =
			new SubKeyedListStateDescriptor<>("test",
				IntSerializer.INSTANCE, StringSerializer.INSTANCE,
				LongSerializer.INSTANCE);
		SubKeyedListState<Integer, String, Long> state = backend.getSubKeyedState(descriptor);
		assertNotNull(state);

		String namespace = "namespace";
		state.put(3, namespace, 4L);

		List<Long> actualList = state.get(3, namespace);
		assertEquals(1, actualList.size());
		assertTrue(4 == actualList.get(0));

		// state#putAll
		List<Long> addValue = new ArrayList<>();
		addValue.add(1L);
		addValue.add(2L);
		addValue.add(3L);
		state.putAll(3, namespace, addValue);

		actualList = state.get(3, namespace);
		assertEquals(addValue.size(), actualList.size());
		for (Long item : actualList) {
			assertTrue(addValue.contains(item));
		}
	}

	@Test
	public void testMulitStateAccessParallism() throws Exception {
		SubKeyedListStateDescriptor<Integer, Integer, Long> descriptor1 =
			new SubKeyedListStateDescriptor<>("test1",
				IntSerializer.INSTANCE, IntSerializer.INSTANCE,
				LongSerializer.INSTANCE);
		SubKeyedListState<Integer, Integer, Long> state1 = backend.getSubKeyedState(descriptor1);

		SubKeyedListStateDescriptor<Integer, Integer, Long> descriptor2 =
			new SubKeyedListStateDescriptor<>("test2",
				IntSerializer.INSTANCE, IntSerializer.INSTANCE,
				LongSerializer.INSTANCE);
		SubKeyedListState<Integer, Integer, Long> state2 = backend.getSubKeyedState(descriptor2);

		int stateCount = 1000;
		ConcurrentHashMap<Tuple2<Integer, Integer>, Long> value1 = new ConcurrentHashMap<>(stateCount);
		Thread thread1 = new Thread(() -> {
			for (int i = 0; i < stateCount; ++i) {
				Long value = ThreadLocalRandom.current().nextLong();
				Integer subKey = ThreadLocalRandom.current().nextInt();
				state1.put(i, subKey, value);
				value1.put(Tuple2.of(i, subKey), value);
			}
		});

		ConcurrentHashMap<Tuple2<Integer, Integer>, Long> value2 = new ConcurrentHashMap<>(stateCount);
		Thread thread2 = new Thread(() -> {
			for (int i = 0; i < stateCount; ++i) {
				Long value = ThreadLocalRandom.current().nextLong();
				Integer subKey = ThreadLocalRandom.current().nextInt();
				state2.put(i, subKey, value);
				value2.put(Tuple2.of(i, subKey), value);
			}
		});
		thread1.start();
		thread2.start();

		thread1.join();
		thread2.join();

		for (Map.Entry<Tuple2<Integer, Integer>, Long> entry : value1.entrySet()) {
			Tuple2<Integer, Integer> key = entry.getKey();
			List<Long> v = state1.get(key.f0, key.f1);
			assertEquals(1, v.size());
			assertEquals(entry.getValue(), v.get(0));
		}
		for (Map.Entry<Tuple2<Integer, Integer>, Long> entry : value2.entrySet()) {
			Tuple2<Integer, Integer> key = entry.getKey();
			List<Long> v = state2.get(key.f0, key.f1);
			assertEquals(1, v.size());
			assertEquals(entry.getValue(), v.get(0));
		}
	}

	@Test
	public void testKeys() throws Exception {
		SubKeyedListStateDescriptor<Integer, String, Long> descriptor =
			new SubKeyedListStateDescriptor<>("test",
				IntSerializer.INSTANCE, StringSerializer.INSTANCE,
				LongSerializer.INSTANCE);
		SubKeyedListState<Integer, String, Long> state = backend.getSubKeyedState(descriptor);
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
			state.put(key, namespace, key + 1L);
			state.put(key, namespace2, key + 2L);
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

	private void populateState(
		Map<Integer, Map<String, List<Float>>> keyMap,
		SubKeyedListState<Integer, String, Float> state,
		int keyNumber,
		int leastElementPerKey
	) {
		Random random = new Random();

		for (int i = 0; i < keyNumber; ++i) {
			Map<String, List<Float>> namespaceMap = new HashMap<>();
			for (int j = 0; j <= i; ++j) {
				String namespace = Integer.toString(j);

				int numElements = random.nextInt(9) + leastElementPerKey;
				List<Float> elements = new ArrayList<>(numElements);
				for (int k = 0; k < numElements; ++k) {
					Float element = random.nextFloat();
					elements.add(element);
				}

				namespaceMap.put(namespace, elements);
				state.addAll(i, namespace, elements);
			}

			keyMap.put(i, namespaceMap);
		}
	}

	private KeyGroupRange getGroupsForSubtask(int maxParallelism, int parallelism, int subtaskIndex) {
		return KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(maxParallelism, parallelism, subtaskIndex);
	}
}

