/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.TriFunction;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

import static org.apache.flink.runtime.state.heap.CopyOnWriteSkipListStateMap.DEFAULT_MAX_KEYS_TO_DELETE_ONE_TIME;
import static org.apache.flink.runtime.state.heap.CopyOnWriteSkipListStateMapTestUtils.addToReferenceState;
import static org.apache.flink.runtime.state.heap.CopyOnWriteSkipListStateMapTestUtils.createEmptyStateMap;
import static org.apache.flink.runtime.state.heap.CopyOnWriteSkipListStateMapTestUtils.createStateMapForTesting;
import static org.apache.flink.runtime.state.heap.CopyOnWriteSkipListStateMapTestUtils.getAllValuesOfNode;
import static org.apache.flink.runtime.state.heap.CopyOnWriteSkipListStateMapTestUtils.removeFromReferenceState;
import static org.apache.flink.runtime.state.heap.CopyOnWriteSkipListStateMapTestUtils.snapshotReferenceStates;
import static org.apache.flink.runtime.state.heap.CopyOnWriteSkipListStateMapTestUtils.verifySnapshotWithTransform;
import static org.apache.flink.runtime.state.heap.CopyOnWriteSkipListStateMapTestUtils.verifySnapshotWithoutTransform;
import static org.apache.flink.runtime.state.heap.CopyOnWriteSkipListStateMapTestUtils.verifyState;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link CopyOnWriteSkipListStateMap}.
 */
public class CopyOnWriteSkipListStateMapComplexOpTest extends TestLogger {

	private final TypeSerializer<Integer> keySerializer = IntSerializer.INSTANCE;
	private final TypeSerializer<Long> namespaceSerializer = LongSerializer.INSTANCE;
	private final TypeSerializer<String> stateSerializer = StringSerializer.INSTANCE;
	private final int initNamespaceNumber = 10;
	private final int initKeyNumber = 100;
	private final int initTotalSize = initNamespaceNumber * initKeyNumber;
	private final StateSnapshotTransformer<String> transformer = new StateSnapshotTransformer<String>() {
		@Nullable
		@Override
		public String filterOrTransform(@Nullable String value) {
			if (value == null) {
				return null;
			}
			int op = value.hashCode() % 3;
			switch (op) {
				case 0:
					return null;
				case 1:
					return value + "-transform";
				default:
					return value;
			}
		}
	};

	/**
	 * We always create a space allocator and state map with some states. Note that Any test case need to test
	 * a state map from empty state should not use these pre-created instances.
	 */
	private TestAllocator spaceAllocator;
	private CopyOnWriteSkipListStateMap<Integer, Long, String> stateMapWithStates;
	private Map<Long, Map<Integer, String>> referenceStates;

	@Before
	public void setUp() {
		int maxAllocateSize = 256;
		spaceAllocator = new TestAllocator(maxAllocateSize);
		// do not remove states physically when get, put, remove and snapshot
		stateMapWithStates = createStateMapForTesting(0, 1.0f, spaceAllocator);
		referenceStates = new HashMap<>();
		// put some states
		for (long namespace = 0; namespace < initNamespaceNumber; namespace++) {
			for (int key = 0; key < initKeyNumber; key++) {
				String state = String.valueOf(key * namespace);
				stateMapWithStates.put(key, namespace, state);
				if (referenceStates != null) {
					referenceStates.computeIfAbsent(namespace, (none) -> new HashMap<>()).put(key, state);
				}
			}
		}
	}

	@After
	public void tearDown() {
		stateMapWithStates.close();
		referenceStates.clear();
		IOUtils.closeQuietly(spaceAllocator);
	}

	/**
	 * Test remove namespace.
	 */
	@Test
	public void testPurgeNamespace() {
		verify(purgeNamespace(false));
	}

	/**
	 * Test remove namespace.
	 */
	@Test
	public void testPurgeNamespaceWithSnapshot() {
		CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot = stateMapWithStates.stateSnapshot();
		TestExecutionResult result = purgeNamespace(true);
		snapshot.release();
		verify(result);
	}

	@Nonnull
	private TestExecutionResult purgeNamespace(boolean withSnapshot) {
		int totalSize = initTotalSize;
		int totalSizeIncludingLogicalRemove = totalSize;
		int totalSpaceSize = totalSize * 2;
		// empty half of the namespaces
		Set<Long> removedNamespaces = new HashSet<>();
		int cnt = 0;
		for (Map.Entry<Long, Map<Integer, String>> namespaceEntry : referenceStates.entrySet()) {
			if (cnt++ % 2 != 0) {
				continue;
			}
			long namespace = namespaceEntry.getKey();
			removedNamespaces.add(namespace);
			for (Map.Entry<Integer, String> keyEntry : namespaceEntry.getValue().entrySet()) {
				int key = keyEntry.getKey();
				stateMapWithStates.remove(key, namespace);
				totalSize--;
				if (withSnapshot) {
					// logical remove with copy-on-write
					totalSpaceSize++;
				} else {
					// physical remove
					totalSizeIncludingLogicalRemove--;
					totalSpaceSize -= 2;
				}
			}
		}

		for (long namespace : removedNamespaces) {
			referenceStates.remove(namespace);
			// verify namespace related stuff.
			assertEquals(0, stateMapWithStates.sizeOfNamespace(namespace));
			assertFalse(stateMapWithStates.getKeys(namespace).iterator().hasNext());
		}

		return new TestExecutionResult(totalSize, totalSizeIncludingLogicalRemove, totalSpaceSize);
	}

	/**
	 * Test put -> put during snapshot, the first put should trigger copy-on-write and the second shouldn't.
	 */
	@Test
	public void testPutAndPutWithSnapshot() {
		CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot = stateMapWithStates.stateSnapshot();
		TestExecutionResult result = testPutAndPut();
		snapshot.release();
		verify(result);
	}

	@Nonnull
	private TestExecutionResult testPutAndPut() {
		final int key = 1;
		final long namespace = 1L;
		final String value = "11";
		final String newValue = "111";
		int totalSize = initTotalSize;
		// put (key 1, namespace 1)
		int totalSpaceNumber =
			putExistingKeyWithSnapshot(totalSize, totalSize * 2, key, namespace, value);

		// put (key 1, namespace 1) again, old value should be replaced and space will not increase
		assertEquals("11", stateMapWithStates.putAndGetOld(key, namespace, newValue));
		addToReferenceState(referenceStates, key, namespace, newValue);
		assertEquals("111", stateMapWithStates.get(key, namespace));
		assertTrue(stateMapWithStates.containsKey(key, namespace));
		return new TestExecutionResult(totalSize, totalSize, totalSpaceNumber);
	}

	/**
	 * Test put -> remove during snapshot, put should trigger copy-on-write and remove shouldn't.
	 */
	@Test
	public void testPutAndRemoveWithSnapshot() {
		CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot = stateMapWithStates.stateSnapshot();
		TestExecutionResult result = testPutAndRemove();
		snapshot.release();
		verify(result);
	}

	@Nonnull
	private TestExecutionResult testPutAndRemove() {
		final int key = 6;
		final long namespace = 6L;
		final String value = "66";
		int totalSize = initTotalSize;
		int totalLogicallyRemovedKey = 0;
		int totalSizeIncludingLogicalRemovedKey = totalSize;
		CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot = null;

		// put (key 6, namespace 6)
		int totalSpaceNumber =
			putExistingKeyWithSnapshot(totalSize, totalSize * 2, key, namespace, value);

		// remove (key 6, namespace 6), and it should be logically removed
		assertEquals(value, stateMapWithStates.removeAndGetOld(key, namespace));
		removeFromReferenceState(referenceStates, key, namespace);
		totalSize--;
		// with snapshot, it should be a logical remove
		totalLogicallyRemovedKey++;
		assertNull(stateMapWithStates.get(key, namespace));
		assertFalse(stateMapWithStates.containsKey(key, namespace));
		assertEquals(totalLogicallyRemovedKey, stateMapWithStates.getLogicallyRemovedNodes().size());

		return new TestExecutionResult(totalSize, totalSizeIncludingLogicalRemovedKey, totalSpaceNumber);
	}

	private int putExistingKeyWithSnapshot(
		int totalSize,
		int initTotalSpaceSize,
		int key,
		long namespace,
		String value) {

		int totalSpaceNumber = initTotalSpaceSize;
		stateMapWithStates.put(key, namespace, value);
		addToReferenceState(referenceStates, key, namespace, value);
		// copy-on-write should happen, a space for new value should be allocated
		totalSpaceNumber += 1;
		assertEquals(totalSpaceNumber, spaceAllocator.getTotalSpaceNumber());
		assertEquals(value, stateMapWithStates.get(key, namespace));
		assertTrue(stateMapWithStates.containsKey(key, namespace));
		assertThat(stateMapWithStates.size(), is(totalSize));
		verifyState(referenceStates, stateMapWithStates);
		return totalSpaceNumber;
	}

	/**
	 * Test remove -> put during snapshot, remove should trigger copy-on-write and put shouldn't.
	 */
	@Test
	public void testRemoveAndPutWithSnapshot() {
		CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot = stateMapWithStates.stateSnapshot();
		TestExecutionResult result = testRemoveAndPut();
		snapshot.release();
		verify(result);
	}

	@Nonnull
	private TestExecutionResult testRemoveAndPut() {
		final int key = 8;
		final long namespace = 8L;
		final String value = "64";
		int totalSize = initTotalSize;
		// remove (key 8, namespace 8)
		RemoveResult result =
			removeExistingKeyWithSnapshot(totalSize, stateMapWithStates, referenceStates,
				totalSize * 2, key, namespace, value);
		totalSize--;
		int totalLogicallyRemovedKey = result.totalLogicallyRemovedKey;
		int totalSizeIncludingLogicalRemovedKey = result.totalSizeIncludingLogicalRemovedKey;
		int totalSpaceNumber = result.totalSpaceNumber;

		// put (key 8, namespace 8) again
		assertNull(stateMapWithStates.putAndGetOld(key, namespace, value));
		addToReferenceState(referenceStates, key, namespace, value);
		totalSize++;
		totalLogicallyRemovedKey--;
		assertEquals(value, stateMapWithStates.get(key, namespace));
		assertTrue(stateMapWithStates.containsKey(key, namespace));
		assertEquals(totalLogicallyRemovedKey, stateMapWithStates.getLogicallyRemovedNodes().size());
		return new TestExecutionResult(totalSize, totalSizeIncludingLogicalRemovedKey, totalSpaceNumber);
	}

	/**
	 * Test remove -> remove during snapshot, the first remove should trigger copy-on-write and the second shouldn't.
	 */
	@Test
	public void testRemoveAndRemoveWithSnapshot() {
		final int key = 4;
		final long namespace = 4L;
		final String value = "16";
		int totalSize = initTotalSize;
		CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot = stateMapWithStates.stateSnapshot();

		// remove (key 4, namespace 4)
		RemoveResult result =
			removeExistingKeyWithSnapshot(totalSize, stateMapWithStates, referenceStates,
				totalSize * 2, key, namespace, value);
		totalSize--;
		int totalLogicallyRemovedKey = result.totalLogicallyRemovedKey;
		int totalSizeIncludingLogicalRemovedKey = result.totalSizeIncludingLogicalRemovedKey;
		int totalSpaceNumber = result.totalSpaceNumber;

		// remove (key 4, namespace 4) again, and nothing should happen
		assertNull(stateMapWithStates.removeAndGetOld(key, namespace));
		assertEquals(totalLogicallyRemovedKey, stateMapWithStates.getLogicallyRemovedNodes().size());

		snapshot.release();

		// remove (key 4, namespace 4) again after snapshot released, should be physically removed
		assertNull(stateMapWithStates.removeAndGetOld(key, namespace));
		totalLogicallyRemovedKey--;
		totalSizeIncludingLogicalRemovedKey--;
		totalSpaceNumber -= 3; // key, value and one copy-on-write by remove
		assertEquals(totalLogicallyRemovedKey, stateMapWithStates.getLogicallyRemovedNodes().size());

		verify(new TestExecutionResult(totalSize, totalSizeIncludingLogicalRemovedKey, totalSpaceNumber));
	}

	@Nonnull
	private RemoveResult removeExistingKeyWithSnapshot(
		int totalSize,
		@Nonnull CopyOnWriteSkipListStateMap<Integer, Long, String> stateMap,
		Map<Long, Map<Integer, String>> referenceStates,
		int initTotalSpaceSize,
		int key,
		long namespace,
		String value) {

		int totalLogicallyRemovedKey = 0;
		int totalSpaceNumber = initTotalSpaceSize;
		int totalSizeIncludingLogicalRemovedKey = totalSize;
		assertEquals(value, stateMap.removeAndGetOld(key, namespace));
		removeFromReferenceState(referenceStates, key, namespace);
		totalSize--;
		// with snapshot, it should be a logical remove with copy-on-write
		totalLogicallyRemovedKey++;
		totalSpaceNumber += 1;
		assertNull(stateMap.get(key, namespace));
		assertFalse(stateMap.containsKey(key, namespace));
		assertThat(stateMap.size(), is(totalSize));
		assertEquals(totalLogicallyRemovedKey, stateMap.getLogicallyRemovedNodes().size());
		assertEquals(totalSpaceNumber, spaceAllocator.getTotalSpaceNumber());
		assertEquals(totalSizeIncludingLogicalRemovedKey, stateMap.totalSize());
		verifyState(referenceStates, stateMap);
		return new RemoveResult(totalLogicallyRemovedKey, totalSizeIncludingLogicalRemovedKey, totalSpaceNumber);
	}

	/**
	 * Test snapshot unmodifiable.
	 */
	@Test
	public void testSnapshotUnmodifiable() throws IOException {
		final int key = 1;
		final int newKey = 101;
		final long namespace = 1L;
		final long newNamespace = initNamespaceNumber + 1;
		final String newValue = "11";
		CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot = stateMapWithStates.stateSnapshot();
		Map<Long, Map<Integer, String>> expectedSnapshot = snapshotReferenceStates(referenceStates);
		// make sure update existing key won't change snapshot
		processAndVerifySnapshot(expectedSnapshot, snapshot, stateMapWithStates, map -> map.put(key, namespace, newValue));
		// make sure insert new key won't change snapshot
		processAndVerifySnapshot(expectedSnapshot, snapshot, stateMapWithStates, map -> map.put(newKey, newNamespace, newValue));
		// make sure remove existing key won't change snapshot
		processAndVerifySnapshot(expectedSnapshot, snapshot, stateMapWithStates, map -> map.remove(key, namespace));
		snapshot.release();
		stateMapWithStates.close();
	}

	private void processAndVerifySnapshot(
		Map<Long, Map<Integer, String>> expectedSnapshot,
		CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot,
		CopyOnWriteSkipListStateMap<Integer, Long, String> stateMap,
		@Nonnull Consumer<CopyOnWriteSkipListStateMap<Integer, Long, String>> consumer) throws IOException {
		consumer.accept(stateMap);
		verifySnapshotWithoutTransform(
			expectedSnapshot, snapshot, keySerializer, namespaceSerializer, stateSerializer);
	}

	/**
	 * Tests that remove states physically when get is invoked.
	 */
	@Test
	public void testPhysicallyRemoveWithGet() throws IOException {
		testPhysicallyRemoveWithFunction(
			(map, reference, i) -> {
				map.get(i, (long) i);
				return 0;
			});
	}

	/**
	 * Tests that remove states physically when contains is invoked.
	 */
	@Test
	public void testPhysicallyRemoveWithContains() throws IOException {
		testPhysicallyRemoveWithFunction(
			(map, reference, i) -> {
				assertFalse(map.containsKey(i, (long) i));
				return 0;
			});
	}

	/**
	 * Tests that remove states physically when remove is invoked.
	 */
	@Test
	public void testPhysicallyRemoveWithRemove() throws IOException {
		testPhysicallyRemoveWithFunction(
			(map, reference, i) -> {
				map.remove(i, (long) i);
				return 0;
			});
	}

	/**
	 * Tests that remove states physically when removeAndGetOld is invoked.
	 */
	@Test
	public void testPhysicallyRemoveWithRemoveAndGetOld() throws IOException {
		testPhysicallyRemoveWithFunction(
			(map, reference, i) -> {
				assertNull(map.removeAndGetOld(i, (long) i));
				return 0;
			});
	}

	/**
	 * Tests that remove states physically when put is invoked.
	 */
	@Test
	public void testPhysicallyRemoveWithPut() throws IOException {
		testPhysicallyRemoveWithFunction(
			(map, reference, i) -> {
				map.put(i, (long) i, String.valueOf(i));
				addToReferenceState(reference, i, (long) i, String.valueOf(i));
				return 1;
			});
	}

	/**
	 * Tests that remove states physically when putAndGetOld is invoked.
	 */
	@Test
	public void testPhysicallyRemoveWithPutAndGetOld() throws IOException {
		testPhysicallyRemoveWithFunction(
			(map, reference, i) -> {
				assertNull(map.putAndGetOld(i, (long) i, String.valueOf(i)));
				addToReferenceState(reference, i, (long) i, String.valueOf(i));
				return 1;
			});
	}

	/**
	 * Tests remove states physically when the given function is applied.
	 *
	 * @param f the function to apply for each test iteration, with [stateMap, referenceStates, testRoundIndex] as input
	 *          and returns the delta size caused by applying function.
	 * @throws IOException if unexpected error occurs.
	 */
	private void testPhysicallyRemoveWithFunction(
		TriFunction<
			CopyOnWriteSkipListStateMap<Integer, Long, String>,
			Map<Long, Map<Integer, String>>,
			Integer,
			Integer> f) throws IOException {

		TestAllocator spaceAllocator = new TestAllocator(256);
		CopyOnWriteSkipListStateMap<Integer, Long, String> stateMap =
			createEmptyStateMap(2, 1.0f, spaceAllocator);
		// map to store expected states, namespace -> key -> state
		Map<Long, Map<Integer, String>> referenceStates = new HashMap<>();

		// here we use a trick that put all odd namespace to state map, and get/put/remove even namespace
		// so that all logically removed nodes can be accessed
		prepareLogicallyRemovedStates(
			referenceStates, stateMap, keySerializer, namespaceSerializer, stateSerializer);
		int expectedSize = 0;
		for (int i = 0; i <= 100; i += 2) {
			expectedSize += f.apply(stateMap, referenceStates, i);
		}
		assertEquals(expectedSize, stateMap.size());
		assertEquals(expectedSize, stateMap.totalSize());
		assertEquals(expectedSize * 2, spaceAllocator.getTotalSpaceNumber());
		verifyState(referenceStates, stateMap);
		stateMap.close();
	}

	/**
	 * Tests that remove states physically during sync part of snapshot.
	 */
	@Test
	public void testPhysicallyRemoveDuringSyncPartOfSnapshot() throws IOException {
		TestAllocator spaceAllocator = new TestAllocator(256);
		// set logicalRemovedKeysRatio to 0 so that all logically removed states will be deleted when snapshot
		CopyOnWriteSkipListStateMap<Integer, Long, String> stateMap =
			createEmptyStateMap(0, 0.0f, spaceAllocator);

		// map to store expected states, namespace -> key -> state
		Map<Long, Map<Integer, String>> referenceStates = new HashMap<>();
		int totalStateSize = 0;
		int totalSizeIncludingLogicallyRemovedStates = 0;

		// put some states
		for (int i = 1; i <= 100; i++) {
			totalStateSize++;
			totalSizeIncludingLogicallyRemovedStates++;
			stateMap.put(i, (long) i, String.valueOf(i));
			addToReferenceState(referenceStates, i, (long) i, String.valueOf(i));
		}
		verifyState(referenceStates, stateMap);

		assertEquals(totalStateSize * 2, spaceAllocator.getTotalSpaceNumber());

		Map<Long, Map<Integer, String>> expectedSnapshot1 = snapshotReferenceStates(referenceStates);
		CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot1 = stateMap.stateSnapshot();

		// remove all states logically
		for (int i = 1; i <= 100; i++) {
			totalStateSize--;
			stateMap.remove(i, (long) i);
			removeFromReferenceState(referenceStates, i, (long) i);
		}
		assertEquals(totalSizeIncludingLogicallyRemovedStates * 3, spaceAllocator.getTotalSpaceNumber());
		assertEquals(0, totalStateSize);
		assertEquals(totalStateSize, stateMap.size());
		assertEquals(totalSizeIncludingLogicallyRemovedStates, stateMap.totalSize());
		assertEquals(totalSizeIncludingLogicallyRemovedStates, stateMap.getLogicallyRemovedNodes().size());
		verifyState(referenceStates, stateMap);

		verifySnapshotWithoutTransform(
			expectedSnapshot1, snapshot1, keySerializer, namespaceSerializer, stateSerializer);
		snapshot1.release();

		// no spaces should be free
		assertEquals(totalSizeIncludingLogicallyRemovedStates * 3, spaceAllocator.getTotalSpaceNumber());
		verifyState(referenceStates, stateMap);

		Map<Long, Map<Integer, String>> expectedSnapshot2 = snapshotReferenceStates(referenceStates);
		CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot2 = stateMap.stateSnapshot();

		// all state should be removed physically
		int totalSizeIncludingLogicallyRemovedStatesAfterSecondSnapshot = 0;
		assertEquals(totalStateSize, stateMap.totalSize());
		assertEquals(totalSizeIncludingLogicallyRemovedStatesAfterSecondSnapshot, stateMap.totalSize());
		assertEquals(totalSizeIncludingLogicallyRemovedStatesAfterSecondSnapshot,
			stateMap.getLogicallyRemovedNodes().size());
		assertEquals(0, spaceAllocator.getTotalSpaceNumber());

		verifySnapshotWithoutTransform(
			expectedSnapshot2, snapshot2, keySerializer, namespaceSerializer, stateSerializer);
		snapshot2.release();

		assertEquals(0, stateMap.size());
		assertEquals(0, stateMap.totalSize());
		assertEquals(0, spaceAllocator.getTotalSpaceNumber());
		assertFalse(stateMap.iterator().hasNext());

		stateMap.close();
	}

	/**
	 * Tests that snapshots prune useless values.
	 */
	@Test
	public void testSnapshotPruneValues() throws IOException {
		TestAllocator spaceAllocator = new TestAllocator(256);
		// set logicalRemovedKeysRatio to 0 so that all logically removed states will be deleted when snapshot
		CopyOnWriteSkipListStateMap<Integer, Long, String> stateMap =
			createEmptyStateMap(DEFAULT_MAX_KEYS_TO_DELETE_ONE_TIME, 0.0f, spaceAllocator);

		// firstly build value chain and snapshots as follows
		//  ------      ------      ------      ------
		// |  v3  | -> |  v2  | -> |  v1  | -> |  v0  |
		//  ------      ------      ------      ------
		//    |            |           |          |
		// snapshot4   snapshot3   snapshot2   snapshot1
		// snapshot5
		// snapshot6

		List<String> referenceValues = new ArrayList<>();

		// build v0
		stateMap.put(1, 1L, "0");
		referenceValues.add(0, "0");
		// get the pointer to the node
		long node = stateMap.getLevelIndexHeader().getNextNode(0);

		// take snapshot1
		CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot1 = stateMap.stateSnapshot();

		// build v1
		stateMap.put(1, 1L, "1");
		referenceValues.add(0, "1");

		// take snapshot2
		CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot2 = stateMap.stateSnapshot();

		// build v2
		stateMap.put(1, 1L, "2");
		referenceValues.add(0, "2");

		// take snapshot3
		CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot3 = stateMap.stateSnapshot();

		// build v3
		stateMap.put(1, 1L, "3");
		referenceValues.add(0, "3");

		// take snapshot4
		CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot4 = stateMap.stateSnapshot();

		// take snapshot5
		CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot5 = stateMap.stateSnapshot();

		// take snapshot6
		CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot6 = stateMap.stateSnapshot();

		assertEquals(6, stateMap.getStateMapVersion());
		assertEquals(6, stateMap.getHighestRequiredSnapshotVersionPlusOne());
		assertEquals(6, stateMap.getSnapshotVersions().size());
		assertEquals(5, spaceAllocator.getTotalSpaceNumber());
		assertEquals(referenceValues, getAllValuesOfNode(stateMap, spaceAllocator, node));

		Map<Long, Map<Integer, String>> referenceStates = new HashMap<>();
		referenceStates.put(1L, new HashMap<>());
		referenceStates.get(1L).put(1, "0");

		// complete snapshot 1, and no value will be removed
		verifySnapshotWithoutTransform(
			referenceStates, snapshot1, keySerializer, namespaceSerializer, stateSerializer);
		snapshot1.release();
		assertEquals(referenceValues, getAllValuesOfNode(stateMap, spaceAllocator, node));
		assertEquals(5, spaceAllocator.getTotalSpaceNumber());
		assertEquals(1, stateMap.getHighestFinishedSnapshotVersion());

		// complete snapshot 3, and v0 will be removed
		referenceStates.get(1L).put(1, "2");
		verifySnapshotWithoutTransform(
			referenceStates, snapshot3, keySerializer, namespaceSerializer, stateSerializer);
		snapshot3.release();
		referenceValues.remove(referenceValues.size() - 1);
		assertEquals(referenceValues, getAllValuesOfNode(stateMap, spaceAllocator, node));
		assertEquals(4, spaceAllocator.getTotalSpaceNumber());
		assertEquals(1, stateMap.getHighestFinishedSnapshotVersion());

		// complete snapshot 2, and no value will be removed
		referenceStates.get(1L).put(1, "1");
		verifySnapshotWithoutTransform(
			referenceStates, snapshot2, keySerializer, namespaceSerializer, stateSerializer);
		snapshot2.release();
		assertEquals(referenceValues, getAllValuesOfNode(stateMap, spaceAllocator, node));
		assertEquals(4, spaceAllocator.getTotalSpaceNumber());
		assertEquals(3, stateMap.getHighestFinishedSnapshotVersion());

		// add node to pruning set to prevent snapshot4 to prune
		stateMap.getPruningValueNodes().add(node);
		// complete snapshot 4, and no value will be removed
		referenceStates.get(1L).put(1, "3");
		verifySnapshotWithoutTransform(
			referenceStates, snapshot4, keySerializer, namespaceSerializer, stateSerializer);
		snapshot4.release();
		assertEquals(referenceValues, getAllValuesOfNode(stateMap, spaceAllocator, node));
		assertEquals(4, spaceAllocator.getTotalSpaceNumber());
		assertEquals(4, stateMap.getHighestFinishedSnapshotVersion());

		stateMap.getPruningValueNodes().remove(node);

		// complete snapshot 5, v1 and v2 will be removed
		verifySnapshotWithoutTransform(
			referenceStates, snapshot5, keySerializer, namespaceSerializer, stateSerializer);
		snapshot5.release();
		referenceValues.remove(referenceValues.size() - 1);
		referenceValues.remove(referenceValues.size() - 1);
		assertEquals(referenceValues, getAllValuesOfNode(stateMap, spaceAllocator, node));
		assertEquals(2, spaceAllocator.getTotalSpaceNumber());
		assertEquals(5, stateMap.getHighestFinishedSnapshotVersion());

		// complete snapshot 6, no value will be removed
		verifySnapshotWithoutTransform(
			referenceStates, snapshot6, keySerializer, namespaceSerializer, stateSerializer);
		snapshot6.release();
		assertEquals(referenceValues, getAllValuesOfNode(stateMap, spaceAllocator, node));
		assertEquals(2, spaceAllocator.getTotalSpaceNumber());
		assertEquals(6, stateMap.getHighestFinishedSnapshotVersion());

		assertEquals("3", stateMap.removeAndGetOld(1, 1L));
		assertEquals(0 , stateMap.size());
		assertEquals(0, spaceAllocator.getTotalSpaceNumber());

		stateMap.close();
	}

	/**
	 * Tests concurrent snapshots.
	 */
	@Test
	public void testConcurrentSnapshots() throws IOException {
		TestAllocator spaceAllocator = new TestAllocator(256);
		// set logicalRemovedKeysRatio to 0 so that all logically removed states will be deleted when snapshot
		CopyOnWriteSkipListStateMap<Integer, Long, String> stateMap =
			createEmptyStateMap(DEFAULT_MAX_KEYS_TO_DELETE_ONE_TIME, 0.0f, spaceAllocator);

		// snapshot order: create snapshot1 -> update states -> create snapshot2 -> update states
		// -> create snapshot3 -> update states -> complete snapshot2 -> update states -> complete snapshot1
		// -> create snapshot4 -> update states -> complete snapshot3 -> update states -> complete snapshot4
		// -> create snapshot5 -> complete snapshot5

		// map to store expected states, namespace -> key -> state
		Map<Long, Map<Integer, String>> referenceStates = new HashMap<>();

		// update states
		updateStateForConcurrentSnapshots(referenceStates, stateMap, 0);
		verifyState(referenceStates, stateMap);

		// create snapshot1
		Map<Long, Map<Integer, String>> expectedSnapshot1 = snapshotReferenceStates(referenceStates);
		CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot1 = stateMap.stateSnapshot();

		// update states
		updateStateForConcurrentSnapshots(referenceStates, stateMap, 1);
		verifyState(referenceStates, stateMap);

		// create snapshot2
		Map<Long, Map<Integer, String>> expectedSnapshot2 = snapshotReferenceStates(referenceStates);
		CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot2 = stateMap.stateSnapshot();

		// update states
		updateStateForConcurrentSnapshots(referenceStates, stateMap, 2);
		verifyState(referenceStates, stateMap);

		// create snapshot3
		Map<Long, Map<Integer, String>> expectedSnapshot3 = snapshotReferenceStates(referenceStates);
		CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot3 = stateMap.stateSnapshot();

		// update states
		updateStateForConcurrentSnapshots(referenceStates, stateMap, 3);
		verifyState(referenceStates, stateMap);

		// complete snapshot2
		verifySnapshotWithTransform(
			expectedSnapshot2, snapshot2, transformer, keySerializer, namespaceSerializer, stateSerializer);
		snapshot2.release();

		// update states
		updateStateForConcurrentSnapshots(referenceStates, stateMap, 4);
		verifyState(referenceStates, stateMap);

		// complete snapshot1
		verifySnapshotWithoutTransform(
			expectedSnapshot1, snapshot1, keySerializer, namespaceSerializer, stateSerializer);
		snapshot1.release();

		// create snapshot4
		Map<Long, Map<Integer, String>> expectedSnapshot4 = snapshotReferenceStates(referenceStates);
		CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot4 = stateMap.stateSnapshot();

		// update states
		updateStateForConcurrentSnapshots(referenceStates, stateMap, 5);
		verifyState(referenceStates, stateMap);

		// complete snapshot3
		verifySnapshotWithTransform(
			expectedSnapshot3, snapshot3, transformer, keySerializer, namespaceSerializer, stateSerializer);
		snapshot3.release();

		// update states
		updateStateForConcurrentSnapshots(referenceStates, stateMap, 6);
		verifyState(referenceStates, stateMap);

		// complete snapshot4
		verifySnapshotWithTransform(
			expectedSnapshot4, snapshot4, transformer, keySerializer, namespaceSerializer, stateSerializer);
		snapshot4.release();

		verifyState(referenceStates, stateMap);

		// create snapshot5
		Map<Long, Map<Integer, String>> expectedSnapshot5 = snapshotReferenceStates(referenceStates);
		CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot5 = stateMap.stateSnapshot();

		// complete snapshot5
		verifySnapshotWithTransform(
			expectedSnapshot5, snapshot5, transformer, keySerializer, namespaceSerializer, stateSerializer);
		snapshot5.release();

		verifyState(referenceStates, stateMap);

		stateMap.close();
	}

	private void prepareLogicallyRemovedStates(
		Map<Long, Map<Integer, String>> referenceStates,
		CopyOnWriteSkipListStateMap<Integer, Long, String> stateMap,
		TypeSerializer<Integer> keySerializer,
		TypeSerializer<Long> namespaceSerializer,
		TypeSerializer<String> stateSerializer) throws IOException {
		// put some states
		for (int i = 1; i <= 100; i += 2) {
			stateMap.put(i, (long) i, String.valueOf(i));
			addToReferenceState(referenceStates, i, (long) i, String.valueOf(i));
		}
		verifyState(referenceStates, stateMap);

		Map<Long, Map<Integer, String>> expectedSnapshot1 = snapshotReferenceStates(referenceStates);
		CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot1 = stateMap.stateSnapshot();

		// remove all states logically
		for (int i = 1; i <= 100; i += 2) {
			stateMap.remove(i, (long) i);
			removeFromReferenceState(referenceStates, i, (long) i);
		}
		verifyState(referenceStates, stateMap);

		verifySnapshotWithoutTransform(
			expectedSnapshot1, snapshot1, keySerializer, namespaceSerializer, stateSerializer);
		snapshot1.release();
	}

	private void updateStateForConcurrentSnapshots(
		@Nonnull Map<Long, Map<Integer, String>> referenceStates,
		CopyOnWriteSkipListStateMap<Integer, Long, String> stateMap,
		int updateCounter) {
		ThreadLocalRandom random = ThreadLocalRandom.current();
		// update and remove some states
		int op = 0;
		Set<Long> removedNamespaceSet = new HashSet<>();
		for (Map.Entry<Long, Map<Integer, String>> namespaceEntry : referenceStates.entrySet()) {
			long namespace = namespaceEntry.getKey();
			for (Map.Entry<Integer, String> keyEntry : namespaceEntry.getValue().entrySet()) {
				int key = keyEntry.getKey();
				// namespace should be equal to key
				assertEquals(namespace, key);
				switch (op) {
					case 0:
						stateMap.remove(key, namespace);
						removedNamespaceSet.add(namespace);
						op = 1;
						break;
					case 1:
						String state = String.valueOf(random.nextLong());
						stateMap.put(key, namespace, state);
						keyEntry.setValue(state);
						op = 2;
						break;
					default:
						op = 0;
						break;
				}
			}
		}
		for (long namespace : removedNamespaceSet) {
			referenceStates.remove(namespace);
		}

		for (int i = 0; i < 100; i++) {
			int key = updateCounter + i * 50;
			long namespace = key;
			String state = String.valueOf(key * namespace);
			stateMap.put(key, namespace, state);
			addToReferenceState(referenceStates, key, namespace, state);
		}
	}

	private void verify(@Nonnull TestExecutionResult result) {
		assertEquals(result.totalSize, stateMapWithStates.size());
		assertEquals(result.totalSizeIncludingLogicalRemove, stateMapWithStates.totalSize());
		assertEquals(result.totalSpaceSize, spaceAllocator.getTotalSpaceNumber());
		verifyState(referenceStates, stateMapWithStates);
	}

	private class TestExecutionResult {
		final int totalSize;
		final int totalSizeIncludingLogicalRemove;
		final int totalSpaceSize;

		private TestExecutionResult(
			int totalSize,
			int totalSizeIncludingLogicalRemove,
			int totalSpaceSize) {

			this.totalSize = totalSize;
			this.totalSizeIncludingLogicalRemove = totalSizeIncludingLogicalRemove;
			this.totalSpaceSize = totalSpaceSize;
		}
	}

	private class RemoveResult {
		final int totalLogicallyRemovedKey;
		final int totalSizeIncludingLogicalRemovedKey;
		final int totalSpaceNumber;

		private RemoveResult(int totalLogicallyRemovedKey, int totalSizeIncludingLogicalRemovedKey, int totalSpaceNumber) {
			this.totalLogicallyRemovedKey = totalLogicallyRemovedKey;
			this.totalSpaceNumber = totalSpaceNumber;
			this.totalSizeIncludingLogicalRemovedKey = totalSizeIncludingLogicalRemovedKey;
		}
	}

}
