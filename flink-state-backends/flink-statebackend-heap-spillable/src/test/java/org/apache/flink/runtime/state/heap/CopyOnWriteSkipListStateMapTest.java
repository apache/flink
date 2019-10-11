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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.heap.space.Allocator;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.TriFunction;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static org.apache.flink.runtime.state.heap.CopyOnWriteSkipListStateMap.DEFAULT_LOGICAL_REMOVED_KEYS_RATIO;
import static org.apache.flink.runtime.state.heap.CopyOnWriteSkipListStateMap.DEFAULT_MAX_KEYS_TO_DELETE_ONE_TIME;
import static org.apache.flink.runtime.state.heap.SkipListUtils.NIL_NODE;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link CopyOnWriteSkipListStateMap}.
 */
public class CopyOnWriteSkipListStateMapTest extends TestLogger {

	private TestAllocator spaceAllocator;
	private final TypeSerializer<Integer> keySerializer = IntSerializer.INSTANCE;
	private final TypeSerializer<Long> namespaceSerializer = LongSerializer.INSTANCE;
	private final TypeSerializer<String> stateSerializer = StringSerializer.INSTANCE;
	private final ThreadLocalRandom random = ThreadLocalRandom.current();
	private final int initNamespaceNumber = 10;
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

	@Before
	public void setUp() {
		int maxAllocateSize = 256;
		spaceAllocator = new TestAllocator(maxAllocateSize);
	}

	@After
	public void tearDown() {
		IOUtils.closeQuietly(spaceAllocator);
	}

	/**
	 * Test initialization of state map.
	 */
	@Test
	public void testInitStateMap() {
		CopyOnWriteSkipListStateMap<Integer, Long, String> stateMap = createStateMapForTesting();

		assertTrue(stateMap.isEmpty());
		assertEquals(0, stateMap.size());
		assertEquals(0, stateMap.totalSize());
		assertEquals(0, stateMap.getRequestCount());
		assertTrue(stateMap.getLogicallyRemovedNodes().isEmpty());
		assertEquals(0, stateMap.getHighestRequiredSnapshotVersionPlusOne());
		assertEquals(0, stateMap.getHighestFinishedSnapshotVersion());
		assertTrue(stateMap.getSnapshotVersions().isEmpty());
		assertTrue(stateMap.getPruningValueNodes().isEmpty());
		assertEquals(0, stateMap.getResourceGuard().getLeaseCount());
		assertFalse(stateMap.getResourceGuard().isClosed());
		assertFalse(stateMap.isClosed());

		assertNull(stateMap.get(0, 0L));
		assertFalse(stateMap.containsKey(1, 2L));
		assertNull(stateMap.removeAndGetOld(3, 4L));
		assertFalse(stateMap.getKeys(-92L).iterator().hasNext());
		assertEquals(0, stateMap.sizeOfNamespace(8L));
		assertFalse(stateMap.iterator().hasNext());
		assertFalse(stateMap.getStateIncrementalVisitor(100).hasNext());

		stateMap.close();
	}

	/**
	 * Test state put operation.
	 */
	@Test
	public void testPutState() {
		testWithFunction((totalSize, stateMap, referenceStates) -> getDefaultSizes(totalSize));
	}

	/**
	 * Test remove existing state.
	 */
	@Test
	public void testRemoveExistingState() {
		testRemoveState(false, false);
	}

	/**
	 * Test remove and get existing state.
	 */
	@Test
	public void testRemoveAndGetExistingState() {
		testRemoveState(false, true);
	}

	/**
	 * Test remove absent state.
	 */
	@Test
	public void testRemoveAbsentState() {
		testRemoveState(true, true);
	}

	/**
	 * Test remove previously removed state.
	 */
	@Test
	public void testPutPreviouslyRemovedState() {
		testWithFunction(
			(totalSize, stateMap, referenceStates) -> applyFunctionAfterRemove(stateMap, referenceStates,
				(removedCnt, removedStates) -> {
					int size = totalSize - removedCnt;
					for (Map.Entry<Long, Set<Integer>> entry : removedStates.entrySet()) {
						long namespace = entry.getKey();
						for (int key : entry.getValue()) {
							size++;
							String state = String.valueOf(key * namespace);
							assertNull(stateMap.putAndGetOld(key, namespace, state));
							referenceStates.computeIfAbsent(namespace, (none) -> new HashMap<>()).put(key, String.valueOf(state));
						}
					}
					return getDefaultSizes(size);
				}
			)
		);
	}

	private void testRemoveState(boolean removeAbsent, boolean getOld) {
		testWithFunction(
			(totalSize, stateMap, referenceStates) -> {
				if (removeAbsent) {
					totalSize -= removeAbsentState(stateMap, referenceStates);
				} else {
					totalSize -= removeExistingState(stateMap, referenceStates, getOld);
				}
				return getDefaultSizes(totalSize);
			});
	}

	private int removeExistingState(
		CopyOnWriteSkipListStateMap<Integer, Long, String> stateMap,
		@Nonnull Map<Long, Map<Integer, String>> referenceStates,
		boolean getOld) {
		int removedCnt = 0;
		for (Map.Entry<Long, Map<Integer, String>> namespaceEntry : referenceStates.entrySet()) {
			long namespace = namespaceEntry.getKey();
			Map<Integer, String> kvMap = namespaceEntry.getValue();
			Iterator<Map.Entry<Integer, String>> kvIterator = kvMap.entrySet().iterator();
			while (kvIterator.hasNext()) {
				Map.Entry<Integer, String> keyEntry = kvIterator.next();
				if (random.nextBoolean()) {
					int key = keyEntry.getKey();
					String state = keyEntry.getValue();
					removedCnt++;
					// remove from state map
					if (getOld) {
						assertEquals(state, stateMap.removeAndGetOld(key, namespace));
					} else {
						stateMap.remove(key, namespace);
					}
					// remove from reference to keep in accordance
					kvIterator.remove();
				}
			}
		}
		return removedCnt;
	}

	private int removeAbsentState(
			CopyOnWriteSkipListStateMap<Integer, Long, String> stateMap,
			Map<Long, Map<Integer, String>> referenceStates) {
		return applyFunctionAfterRemove(
			stateMap,
			referenceStates,
			(removedCnt, removedStates) -> {
				// remove the same keys again, which would be absent already
				for (Map.Entry<Long, Set<Integer>> entry : removedStates.entrySet()) {
					long namespace = entry.getKey();
					for (int key : entry.getValue()) {
						assertNull(stateMap.removeAndGetOld(key, namespace));
					}
				}
				return removedCnt;
			}
		);
	}

	/**
	 * Apply the given function after removing some states.
	 *
	 * @param stateMap the state map to test against.
	 * @param referenceStates the reference of states for correctness verfication.
	 * @param function a {@link BiFunction} which takes [removedCnt, removedStates] as input parameters.
	 * @param <R> The type of the result returned by the function.
	 * @return The result of applying the given function.
	 */
	private <R> R applyFunctionAfterRemove(
		CopyOnWriteSkipListStateMap<Integer, Long, String> stateMap,
		@Nonnull Map<Long, Map<Integer, String>> referenceStates,
		BiFunction<Integer, Map<Long, Set<Integer>>, R> function) {
		int removedCnt = 0;
		Map<Long, Set<Integer>> removedStates = new HashMap<>();
		// remove some state
		for (Map.Entry<Long, Map<Integer, String>> namespaceEntry : referenceStates.entrySet()) {
			long namespace = namespaceEntry.getKey();
			Map<Integer, String> kvMap = namespaceEntry.getValue();
			Iterator<Map.Entry<Integer, String>> kvIterator = kvMap.entrySet().iterator();
			while (kvIterator.hasNext()) {
				Map.Entry<Integer, String> keyEntry = kvIterator.next();
				if (random.nextBoolean()) {
					int key = keyEntry.getKey();
					removedCnt++;
					removedStates.computeIfAbsent(namespace, (none) -> new HashSet<>()).add(key);
					// remove from state map
					stateMap.remove(key, namespace);
					// remove from reference to keep in accordance
					kvIterator.remove();
				}
			}
		}
		return function.apply(removedCnt, removedStates);
	}

	/**
	 * Test state update operation.
	 */
	@Test
	public void testUpdateState() {
		testWithFunction(
			(totalSize, stateMap, referenceStates) -> {
				// update some states
				for (Map.Entry<Long, Map<Integer, String>> namespaceEntry : referenceStates.entrySet()) {
					long namespace = namespaceEntry.getKey();
					for (Map.Entry<Integer, String> keyEntry : namespaceEntry.getValue().entrySet()) {
						if (random.nextBoolean()) {
							int key = keyEntry.getKey();
							String state = keyEntry.getValue();
							String newState = state + "-update";
							keyEntry.setValue(newState);
							if (random.nextBoolean()) {
								stateMap.put(key, namespace, newState);
							} else {
								assertEquals(state, stateMap.putAndGetOld(key, namespace, newState));
							}
						}
					}
				}
				return getDefaultSizes(totalSize);
			});
	}

	/**
	 * Test transform existing state.
	 */
	@Test
	public void testTransformExistingState() throws Exception {
		final AtomicReference<Exception> exceptionRef = new AtomicReference<>(null);
		testWithFunction(
			(totalSize, stateMap, referenceStates) -> {
				StateTransformationFunction<String, Integer> function =
					(String prevState, Integer value) -> prevState == null ? String.valueOf(value) : prevState + value;
				// transform existing states
				for (Map.Entry<Long, Map<Integer, String>> namespaceEntry : referenceStates.entrySet()) {
					long namespace = namespaceEntry.getKey();
					try {
						for (Map.Entry<Integer, String> keyEntry : namespaceEntry.getValue().entrySet()) {
							if (random.nextBoolean()) {
								int key = keyEntry.getKey();
								String state = keyEntry.getValue();
								int delta = random.nextInt();
								String newState = function.apply(state, delta);
								keyEntry.setValue(newState);
								stateMap.transform(key, namespace, delta, function);
							}
						}
					} catch (Exception e) {
						exceptionRef.set(e);
					}
				}
				return getDefaultSizes(totalSize);
			});
		Exception e = exceptionRef.get();
		if (e != null) {
			throw e;
		}
	}

	/**
	 * Test transform with previous absent state.
	 */
	@Test
	public void testTransformNewState() throws Exception {
		final AtomicReference<Exception> exceptionRef = new AtomicReference<>(null);
		testWithFunction(
			(totalSize, stateMap, referenceStates) -> {
				StateTransformationFunction<String, Integer> function =
					(String prevState, Integer value) -> prevState == null ? String.valueOf(value) : prevState + value;
				// transform some new states
				for (long namespace = initNamespaceNumber; namespace < initNamespaceNumber + 5; namespace++) {
					for (int key = 0; key < 100; key++) {
						totalSize++;
						int value = (int) (key * namespace);
						try {
							stateMap.transform(key, namespace, value, function);
							String state = function.apply(null, value);
							referenceStates.computeIfAbsent(namespace, (none) -> new HashMap<>()).put(key, state);
						} catch (Exception e) {
							exceptionRef.set(e);
						}
					}
				}
				return getDefaultSizes(totalSize);
			});
		Exception e = exceptionRef.get();
		if (e != null) {
			throw e;
		}
	}

	/**
	 * Test remove namespace.
	 */
	@Test
	public void testPurgeNamespace() {
		testPurgeNamespace(false);
	}

	/**
	 * Test remove namespace.
	 */
	@Test
	public void testPurgeNamespaceWithSnapshot() {
		testPurgeNamespace(true);
	}

	private void testPurgeNamespace(boolean withSnapshot) {
		testWithFunction(
			(totalSize, stateMap, referenceStates) -> {
				CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot = null;
				int totalSizeIncludingLogicalRemove = totalSize;
				int totalSpaceSize = totalSize * 2;
				if (withSnapshot) {
					snapshot = stateMap.stateSnapshot();
				}
				// empty some namespaces
				Set<Long> removedNamespaces = new HashSet<>();
				for (Map.Entry<Long, Map<Integer, String>> namespaceEntry : referenceStates.entrySet()) {
					if (random.nextBoolean()) {
						long namespace = namespaceEntry.getKey();
						removedNamespaces.add(namespace);
						for (Map.Entry<Integer, String> keyEntry : namespaceEntry.getValue().entrySet()) {
							int key = keyEntry.getKey();
							if (random.nextBoolean()) {
								stateMap.remove(key, namespace);
							} else {
								assertEquals(keyEntry.getValue(), stateMap.removeAndGetOld(key, namespace));
							}
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
				}

				for (long namespace : removedNamespaces) {
					referenceStates.remove(namespace);
					// verify namespace related stuff.
					assertEquals(0, stateMap.sizeOfNamespace(namespace));
					assertFalse(stateMap.getKeys(namespace).iterator().hasNext());
				}
				if (withSnapshot) {
					snapshot.release();
				}
				return new Tuple3<>(totalSize, totalSizeIncludingLogicalRemove, totalSpaceSize);
			}
		);
	}

	/**
	 * Test close operation.
	 */
	@Test
	public void testClose() {
		CopyOnWriteSkipListStateMap<Integer, Long, String> stateMap = createStateMapForTesting();
		putStates(stateMap, null);
		stateMap.close();
		assertTrue(stateMap.isClosed());
		assertEquals(0, stateMap.size());
		assertEquals(0, stateMap.totalSize());
		assertEquals(0, spaceAllocator.getTotalSpaceNumber());
	}

	/**
	 * Test with the given function.
	 *
	 * @param function a {@link TriFunction} with [totalSizeBeforeFunction, stateMap, referenceStates] as input
	 *                 parameters and returns the [totalSize, totalSizeIncludingLogicalRemovedKey, totalSpaceSize]
	 *                 tuple after applying the function.
	 */
	private void testWithFunction(
		@Nonnull TriFunction<
			Integer,
			CopyOnWriteSkipListStateMap<Integer, Long, String>,
			Map<Long, Map<Integer, String>>,
			Tuple3<Integer, Integer, Integer>> function) {
		// do not remove states physically when get, put, remove and snapshot
		CopyOnWriteSkipListStateMap<Integer, Long, String> stateMap = createStateMapForTesting(0, 1.0f);
		// map to store expected states, namespace -> key -> state
		Map<Long, Map<Integer, String>> referenceStates = new HashMap<>();
		int totalSize = putStates(stateMap, referenceStates);
		Tuple3 tuple3 = function.apply(totalSize, stateMap, referenceStates);
		totalSize = (int) tuple3.f0;
		int totalSizeIncludingLogicalRemove = (int) tuple3.f1;
		int totalSpaceSize = (int) tuple3.f2;
		assertEquals(totalSize, stateMap.size());
		assertEquals(totalSizeIncludingLogicalRemove, stateMap.totalSize());
		assertEquals(totalSpaceSize, spaceAllocator.getTotalSpaceNumber());
		verifyState(referenceStates, stateMap);
		stateMap.close();
	}

	private int putStates(
		CopyOnWriteSkipListStateMap<Integer, Long, String> stateMap,
		Map<Long, Map<Integer, String>> referenceStates) {
		int totalSize = 0;
		for (long namespace = 0; namespace < initNamespaceNumber; namespace++) {
			for (int key = 0; key < 100; key++) {
				totalSize++;
				String state = String.valueOf(key * namespace);
				if (random.nextBoolean()) {
					stateMap.put(key, namespace, state);
				} else {
					assertNull(stateMap.putAndGetOld(key, namespace, state));
				}
				if (referenceStates != null) {
					referenceStates.computeIfAbsent(namespace, (none) -> new HashMap<>()).put(key, state);
				}
			}
		}
		assertEquals(totalSize, stateMap.size());
		assertEquals(totalSize, stateMap.totalSize());
		return totalSize;
	}

	/**
	 * By default there's no remove/copy-on-write, so space cost would be two times (for key and value) of entry number.
	 *
	 * @param totalSize the total number of valid entries in state map.
	 * @return the default tuple3 of [entry_number, entry_number_including_logical_remove, space_size]
	 */
	private Tuple3<Integer, Integer, Integer> getDefaultSizes(int totalSize) {
		return new Tuple3<>(totalSize, totalSize, totalSize * 2);
	}

	/**
	 * Test snapshot empty state map.
	 */
	@Test
	public void testSnapshotEmptyStateMap() throws IOException {
		CopyOnWriteSkipListStateMap<Integer, Long, String> stateMap = createStateMapForTesting();
		// map to store expected states, namespace -> key -> state
		Map<Long, Map<Integer, String>> referenceStates = new HashMap<>();
		// take snapshot on an empty state map
		Map<Long, Map<Integer, String>> expectedSnapshot = snapshotReferenceStates(referenceStates);
		CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot = stateMap.stateSnapshot();
		assertEquals(1, stateMap.getHighestRequiredSnapshotVersionPlusOne());
		assertEquals(1, stateMap.getSnapshotVersions().size());
		assertThat(stateMap.getSnapshotVersions(), contains(1));
		assertEquals(1, stateMap.getResourceGuard().getLeaseCount());
		verifySnapshotWithoutTransform(
			expectedSnapshot, snapshot, keySerializer, namespaceSerializer, stateSerializer);
		verifySnapshotWithTransform(
			expectedSnapshot, snapshot, transformer, keySerializer, namespaceSerializer, stateSerializer);
		snapshot.release();
		stateMap.close();
	}

	/**
	 * Test snapshot release.
	 */
	@Test
	public void testReleaseSnapshot() {
		CopyOnWriteSkipListStateMap<Integer, Long, String> stateMap = createStateMapForTesting();
		int expectedSnapshotVersion = 0;
		int round = 10;
		for (int i = 0; i < round; i++) {
			assertEquals(expectedSnapshotVersion, stateMap.getStateMapVersion());
			assertEquals(expectedSnapshotVersion, stateMap.getHighestFinishedSnapshotVersion());
			CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot = stateMap.stateSnapshot();
			expectedSnapshotVersion++;
			snapshot.release();
			assertEquals(0, stateMap.getHighestRequiredSnapshotVersionPlusOne());
			assertTrue(stateMap.getSnapshotVersions().isEmpty());
			assertEquals(0, stateMap.getResourceGuard().getLeaseCount());
		}
		stateMap.close();
	}

	/**
	 * Test basic snapshot correctness.
	 */
	@Test
	public void testBasicSnapshot() throws IOException {
		CopyOnWriteSkipListStateMap<Integer, Long, String> stateMap = createStateMapForTesting();
		// map to store expected states, namespace -> key -> state
		Map<Long, Map<Integer, String>> referenceStates = new HashMap<>();
		// take an empty snapshot
		CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot = stateMap.stateSnapshot();
		snapshot.release();
		// put some states
		putStates(stateMap, referenceStates);
		// take the 2nd snapshot with data
		Map<Long, Map<Integer, String>> expectedSnapshot = snapshotReferenceStates(referenceStates);
		CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot2 = stateMap.stateSnapshot();
		assertEquals(2, stateMap.getStateMapVersion());
		assertEquals(2, stateMap.getHighestRequiredSnapshotVersionPlusOne());
		assertEquals(1, stateMap.getSnapshotVersions().size());
		assertThat(stateMap.getSnapshotVersions(), contains(2));
		assertEquals(1, stateMap.getResourceGuard().getLeaseCount());
		verifyState(referenceStates, stateMap);
		verifySnapshotWithoutTransform(
			expectedSnapshot, snapshot2, keySerializer, namespaceSerializer, stateSerializer);
		snapshot2.release();
		stateMap.close();
	}

	/**
	 * Test put -> put without snapshot.
	 */
	@Test
	public void testPutAndPut() {
		testPutAndPut(false);
	}

	/**
	 * Test put -> put during snapshot, the first put should trigger copy-on-write and the second shouldn't.
	 */
	@Test
	public void testPutAndPutWithSnapshot() {
		testPutAndPut(true);
	}

	private void testPutAndPut(boolean withSnapshot) {
		final int key = 1;
		final long namespace = 1L;
		final String value = "11";
		final String newValue = "111";
		testWithFunction(
			(totalSize, stateMap, referenceStates) -> {
				CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot = null;
				if (withSnapshot) {
					// take a snapshot
					snapshot = stateMap.stateSnapshot();
				}
				// put (key 1, namespace 1)
				int totalSpaceNumber =
					putExistingKey(totalSize, stateMap, referenceStates,
						totalSize * 2, key, namespace, value, withSnapshot);

				// put (key 1, namespace 1) again, old value should be replaced and space will not increase
				assertEquals("11", stateMap.putAndGetOld(key, namespace, newValue));
				addToReferenceState(referenceStates, key, namespace, newValue);
				assertEquals("111", stateMap.get(key, namespace));
				assertTrue(stateMap.containsKey(key, namespace));
				if (withSnapshot) {
					snapshot.release();
				}
				return new Tuple3<>(totalSize, totalSize, totalSpaceNumber);
			}
		);
	}

	/**
	 * Test put -> remove without snapshot.
	 */
	@Test
	public void testPutAndRemove() {
		testPutAndRemove(false);
	}

	/**
	 * Test put -> remove during snapshot, put should trigger copy-on-write and remove shouldn't.
	 */
	@Test
	public void testPutAndRemoveWithSnapshot() {
		testPutAndRemove(true);
	}

	private void testPutAndRemove(boolean withSnapshot) {
		final int key = 6;
		final long namespace = 6L;
		final String value = "66";
		testWithFunction(
			(totalSize, stateMap, referenceStates) -> {
				int totalLogicallyRemovedKey = 0;
				int totalSizeIncludingLogicalRemovedKey = totalSize;
				CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot = null;
				if (withSnapshot) {
					// take a snapshot
					snapshot = stateMap.stateSnapshot();
				}
				// put (key 6, namespace 6)
				int totalSpaceNumber =
					putExistingKey(totalSize, stateMap, referenceStates,
						totalSize * 2, key, namespace, value, withSnapshot);

				// remove (key 6, namespace 6), and it should be logically removed
				assertEquals(value, stateMap.removeAndGetOld(key, namespace));
				removeFromReferenceState(referenceStates, key, namespace);
				totalSize--;
				if (withSnapshot) {
					// with snapshot, it should be a logical remove
					totalLogicallyRemovedKey++;
				} else {
					// without snapshot, it should be a physical remove
					totalSizeIncludingLogicalRemovedKey--;
					totalSpaceNumber -= 2;
				}
				assertNull(stateMap.get(key, namespace));
				assertFalse(stateMap.containsKey(key, namespace));
				assertEquals(totalLogicallyRemovedKey, stateMap.getLogicallyRemovedNodes().size());

				if (withSnapshot) {
					snapshot.release();
				}
				return new Tuple3<>(totalSize, totalSizeIncludingLogicalRemovedKey, totalSpaceNumber);
			}
		);
	}

	private int putExistingKey(
		int totalSize,
		CopyOnWriteSkipListStateMap<Integer, Long, String> stateMap,
		Map<Long, Map<Integer, String>> referenceStates,
		int initTotalSpaceSize,
		int key,
		long namespace,
		String value,
		boolean withSnapshot) {
		int totalSpaceNumber = initTotalSpaceSize;
		stateMap.put(key, namespace, value);
		addToReferenceState(referenceStates, key, namespace, value);
		if (withSnapshot) {
			// copy-on-write should happen, a space for new value should be allocated
			totalSpaceNumber += 1;
		}
		assertEquals(totalSpaceNumber, spaceAllocator.getTotalSpaceNumber());
		assertEquals(value, stateMap.get(key, namespace));
		assertTrue(stateMap.containsKey(key, namespace));
		assertThat(stateMap.size(), is(totalSize));
		verifyState(referenceStates, stateMap);
		return totalSpaceNumber;
	}

	/**
	 * Test remove -> put without snapshot.
	 */
	@Test
	public void testRemoveAndPut() {
		testRemoveAndPut(false);
	}

	/**
	 * Test remove -> put during snapshot, remove should trigger copy-on-write and put shouldn't.
	 */
	@Test
	public void testRemoveAndPutWithSnapshot() {
		testRemoveAndPut(true);
	}

	private void testRemoveAndPut(boolean withSnapshot) {
		final int key = 8;
		final long namespace = 8L;
		final String value = "64";
		testWithFunction(
			(totalSize, stateMap, referenceStates) -> {
				CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot = null;
				if (withSnapshot) {
					// take a snapshot
					snapshot = stateMap.stateSnapshot();
				}
				// remove (key 8, namespace 8)
				Tuple3<Integer, Integer, Integer> tuple3 =
					removeExistingKey(totalSize, stateMap, referenceStates,
						totalSize * 2, key, namespace, value, withSnapshot);
				totalSize--;
				int totalLogicallyRemovedKey = tuple3.f0;
				int totalSizeIncludingLogicalRemovedKey = tuple3.f1;
				int totalSpaceNumber = tuple3.f2;

				// put (key 8, namespace 8) again
				assertNull(stateMap.putAndGetOld(key, namespace, value));
				addToReferenceState(referenceStates, key, namespace, value);
				totalSize++;
				if (withSnapshot) {
					totalLogicallyRemovedKey--;
				} else {
					totalSpaceNumber += 2;
					totalSizeIncludingLogicalRemovedKey++;
				}
				assertEquals(value, stateMap.get(key, namespace));
				assertTrue(stateMap.containsKey(key, namespace));
				assertEquals(totalLogicallyRemovedKey, stateMap.getLogicallyRemovedNodes().size());
				if (withSnapshot) {
					snapshot.release();
				}
				return new Tuple3<>(totalSize, totalSizeIncludingLogicalRemovedKey, totalSpaceNumber);
			}
		);
	}

	/**
	 * Test remove -> remove without snapshot.
	 */
	@Test
	public void testRemoveAndRemove() {
		testRemoveAndRemove(false);
	}

	/**
	 * Test remove -> remove during snapshot, the first remove should trigger copy-on-write and the second shouldn't.
	 */
	@Test
	public void testRemoveAndRemoveWithSnapshot() {
		testRemoveAndRemove(true);
	}

	private void testRemoveAndRemove(boolean withSnapshot) {
		final int key = 4;
		final long namespace = 4L;
		final String value = "16";
		testWithFunction(
			(totalSize, stateMap, referenceStates) -> {
				CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot = null;
				if (withSnapshot) {
					// take a snapshot
					snapshot = stateMap.stateSnapshot();
				}
				// remove (key 4, namespace 4)
				Tuple3<Integer, Integer, Integer> tuple3 =
					removeExistingKey(totalSize, stateMap, referenceStates,
						totalSize * 2, key, namespace, value, withSnapshot);
				totalSize--;
				int totalLogicallyRemovedKey = tuple3.f0;
				int totalSizeIncludingLogicalRemovedKey = tuple3.f1;
				int totalSpaceNumber = tuple3.f2;

				// remove (key 4, namespace 4) again, and nothing should happen
				assertNull(stateMap.removeAndGetOld(key, namespace));
				assertEquals(totalLogicallyRemovedKey, stateMap.getLogicallyRemovedNodes().size());
				if (withSnapshot) {
					snapshot.release();
					// remove (key 4, namespace 4) again after snapshot released, should be physically removed
					assertNull(stateMap.removeAndGetOld(key, namespace));
					totalLogicallyRemovedKey--;
					totalSizeIncludingLogicalRemovedKey--;
					totalSpaceNumber -= 3; // key, value and one copy-on-write by remove
					assertEquals(totalLogicallyRemovedKey, stateMap.getLogicallyRemovedNodes().size());
				}
				return new Tuple3<>(totalSize, totalSizeIncludingLogicalRemovedKey, totalSpaceNumber);
			}
		);
	}

	private Tuple3<Integer, Integer, Integer> removeExistingKey(
		int totalSize,
		@Nonnull CopyOnWriteSkipListStateMap<Integer, Long, String> stateMap,
		Map<Long, Map<Integer, String>> referenceStates,
		int initTotalSpaceSize,
		int key,
		long namespace,
		String value,
		boolean withSnapshot) {
		int totalLogicallyRemovedKey = 0;
		int totalSpaceNumber = initTotalSpaceSize;
		int totalSizeIncludingLogicalRemovedKey = totalSize;
		assertEquals(value, stateMap.removeAndGetOld(key, namespace));
		removeFromReferenceState(referenceStates, key, namespace);
		totalSize--;
		if (withSnapshot) {
			// with snapshot, it should be a logical remove with copy-on-write
			totalLogicallyRemovedKey++;
			totalSpaceNumber += 1;
		} else {
			// without snapshot, it should be a physical remove
			totalSizeIncludingLogicalRemovedKey--;
			totalSpaceNumber -= 2;
		}
		assertNull(stateMap.get(key, namespace));
		assertFalse(stateMap.containsKey(key, namespace));
		assertThat(stateMap.size(), is(totalSize));
		assertEquals(totalLogicallyRemovedKey, stateMap.getLogicallyRemovedNodes().size());
		assertEquals(totalSpaceNumber, spaceAllocator.getTotalSpaceNumber());
		assertEquals(totalSizeIncludingLogicalRemovedKey, stateMap.totalSize());
		verifyState(referenceStates, stateMap);
		return new Tuple3<>(totalLogicallyRemovedKey, totalSizeIncludingLogicalRemovedKey, totalSpaceNumber);
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
		CopyOnWriteSkipListStateMap<Integer, Long, String> stateMap = createStateMapForTesting();
		// map to store expected states, namespace -> key -> state
		Map<Long, Map<Integer, String>> referenceStates = new HashMap<>();
		putStates(stateMap, referenceStates);
		CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot = stateMap.stateSnapshot();
		Map<Long, Map<Integer, String>> expectedSnapshot = snapshotReferenceStates(referenceStates);
		// make sure update existing key won't change snapshot
		processAndVerifySnapshot(expectedSnapshot, snapshot, stateMap, map -> map.put(key, namespace, newValue));
		// make sure insert new key won't change snapshot
		processAndVerifySnapshot(expectedSnapshot, snapshot, stateMap, map -> map.put(newKey, newNamespace, newValue));
		// make sure remove existing key won't change snapshot
		processAndVerifySnapshot(expectedSnapshot, snapshot, stateMap, map -> map.remove(key, namespace));
		snapshot.release();
		stateMap.close();
	}

	private void processAndVerifySnapshot(
		Map<Long, Map<Integer, String>> expectedSnapshot,
		CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot,
		CopyOnWriteSkipListStateMap<Integer, Long, String> stateMap,
		Consumer<CopyOnWriteSkipListStateMap<Integer, Long, String>> consumer) throws IOException {
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

		CopyOnWriteSkipListStateMap<Integer, Long, String> stateMap = createStateMapForTesting(2, 1.0f);
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
		// set logicalRemovedKeysRatio to 0 so that all logically removed states will be deleted when snapshot
		CopyOnWriteSkipListStateMap<Integer, Long, String> stateMap = createStateMapForTesting(0, 0.0f);

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
		// set logicalRemovedKeysRatio to 0 so that all logically removed states will be deleted when snapshot
		CopyOnWriteSkipListStateMap<Integer, Long, String> stateMap = createStateMapForTesting();

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
		// set logicalRemovedKeysRatio to 0 so that all logically removed states will be deleted when snapshot
		CopyOnWriteSkipListStateMap<Integer, Long, String> stateMap = createStateMapForTesting();

		StateSnapshotTransformer<String> transformer = new StateSnapshotTransformer<String>() {
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

	/**
	 * Test next/update/remove during global iteration of StateIncrementalVisitor.
	 */
	@Test
	public void testStateIncrementalVisitor() {
		// set logicalRemovedKeysRatio to 0 so that all logically removed states will be deleted when snapshot
		CopyOnWriteSkipListStateMap<Integer, Long, String> stateMap = createStateMapForTesting();

		// map to store expected states, namespace -> key -> state
		Map<Long, Map<Integer, String>> referenceStates = new HashMap<>();

		// put some states
		for (long namespace = 0;  namespace < 15; namespace++) {
			for (int key = 0; key < 20; key++) {
				String state = String.valueOf(namespace * key);
				stateMap.put(key, namespace, state);
				addToReferenceState(referenceStates, key, namespace, state);
			}
		}
		verifyState(referenceStates, stateMap);

		ThreadLocalRandom random = ThreadLocalRandom.current();
		InternalKvState.StateIncrementalVisitor<Integer, Long, String> visitor =
			stateMap.getStateIncrementalVisitor(5);
		while (visitor.hasNext()) {
			for (StateEntry<Integer, Long, String> stateEntry : visitor.nextEntries()) {
				int key = stateEntry.getKey();
				long namespace = stateEntry.getNamespace();
				String state = stateEntry.getState();
				assertEquals(state, stateMap.get(key, namespace));
				int op = random.nextInt(3);
				switch (op) {
					case 0:
						visitor.remove(stateEntry);
						removeFromReferenceState(referenceStates, key, namespace);
						break;
					case 1:
						String newState = state + "-update";
						visitor.update(stateEntry, newState);
						addToReferenceState(referenceStates, key, namespace, newState);
						break;
					default:
						break;
				}
			}
		}
		verifyState(referenceStates, stateMap);

		// validates that visitor will be invalid after state map is closed
		InternalKvState.StateIncrementalVisitor<Integer, Long, String> closedVisitor =
			stateMap.getStateIncrementalVisitor(5);
		assertTrue(closedVisitor.hasNext());

		stateMap.close();
		assertFalse(closedVisitor.hasNext());
	}

	@Test
	public void testPutAndGetNodeWithNoneZeroOffset() {
		CopyOnWriteSkipListStateMap<Integer, Long, String> stateMap = createStateMapForTesting();
		final int key = 10;
		final long namespace = 0L;
		final String valueString = "test";
		SkipListKeySerializer<Integer, Long> skipListKeySerializer = new SkipListKeySerializer<>(keySerializer, namespaceSerializer);
		SkipListValueSerializer<String> skipListValueSerializer = new SkipListValueSerializer<>(stateSerializer);
		byte[] keyBytes = skipListKeySerializer.serialize(key, namespace);
		byte[] constructedKeyBytes = new byte[keyBytes.length + 1];
		System.arraycopy(keyBytes, 0, constructedKeyBytes, 1, keyBytes.length);
		ByteBuffer keyByteBuffer = ByteBuffer.wrap(constructedKeyBytes);
		int keyLen = keyBytes.length;
		byte[] value = skipListValueSerializer.serialize(valueString);
		stateMap.putNode(keyByteBuffer, 1, keyLen, value, false);
		String state = stateMap.getNode(keyByteBuffer, 1, keyLen);
		assertThat(state, is(valueString));
	}

	private void prepareLogicallyRemovedStates(
		Map<Long, Map<Integer, String>> referenceStates,
		CopyOnWriteSkipListStateMap<Integer, Long, String> stateMap,
		TypeSerializer<Integer> keySerializer,
		TypeSerializer<Long> namespaceSerializer,
		TypeSerializer<String> stateSerializer) throws IOException {
		int totalStateSize = 0;
		// put some states
		for (int i = 1; i <= 100; i += 2) {
			totalStateSize++;
			stateMap.put(i, (long) i, String.valueOf(i));
			addToReferenceState(referenceStates, i, (long) i, String.valueOf(i));
		}
		verifyState(referenceStates, stateMap);

		Map<Long, Map<Integer, String>> expectedSnapshot1 = snapshotReferenceStates(referenceStates);
		CopyOnWriteSkipListStateMapSnapshot<Integer, Long, String> snapshot1 = stateMap.stateSnapshot();

		// remove all states logically
		for (int i = 1; i <= 100; i += 2) {
			totalStateSize--;
			stateMap.remove(i, (long) i);
			removeFromReferenceState(referenceStates, i, (long) i);
		}
		verifyState(referenceStates, stateMap);

		verifySnapshotWithoutTransform(
			expectedSnapshot1, snapshot1, keySerializer, namespaceSerializer, stateSerializer);
		snapshot1.release();
	}

	private void updateStateForConcurrentSnapshots(
		Map<Long, Map<Integer, String>> referenceStates,
		CopyOnWriteSkipListStateMap<Integer, Long, String> stateMap,
		int updateCounter) {
		ThreadLocalRandom random = ThreadLocalRandom.current();
		// update and remove some states
		Set<Long> removedNamespaceSet = new HashSet<>();
		for (Map.Entry<Long, Map<Integer, String>> namespaceEntry : referenceStates.entrySet()) {
			long namespace = namespaceEntry.getKey();
			for (Map.Entry<Integer, String> keyEntry : namespaceEntry.getValue().entrySet()) {
				int key = keyEntry.getKey();
				// namespace should be equal to key
				assertEquals(namespace, key);
				int op = random.nextInt(3);
				switch (op) {
					case 0:
						stateMap.remove(key, namespace);
						removedNamespaceSet.add(namespace);
						break;
					case 1:
						String state = String.valueOf(random.nextLong());
						stateMap.put(key, namespace, state);
						keyEntry.setValue(state);
						break;
					default:
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

	private <K, N, S> void addToReferenceState(Map<N, Map<K, S>> referenceStates, K key, N namespace, S state) {
		referenceStates.computeIfAbsent(namespace, (none) -> new HashMap<>()).put(key, state);
	}

	private <K, N, S> void removeFromReferenceState(Map<N, Map<K, S>> referenceStates, K key, N namespace) {
		Map<K, S> keyMap = referenceStates.get(namespace);
		if (keyMap == null) {
			return;
		}
		keyMap.remove(key);
		if (keyMap.isEmpty()) {
			referenceStates.remove(namespace);
		}
	}

	private <K, N, S> void verifyState(
		Map<N, Map<K, S>> referenceStates,
		CopyOnWriteSkipListStateMap<K, N, S> stateMap) {

		// validates get(K, N)
		for (Map.Entry<N, Map<K, S>> entry : referenceStates.entrySet()) {
			N namespace = entry.getKey();
			for (Map.Entry<K, S> keyEntry : entry.getValue().entrySet()) {
				K key = keyEntry.getKey();
				S state = keyEntry.getValue();
				assertEquals(state, stateMap.get(key, namespace));
				assertTrue(stateMap.containsKey(key, namespace));
			}
		}

		// validates getKeys(N) and sizeOfNamespace(N)
		for (Map.Entry<N, Map<K, S>> entry : referenceStates.entrySet()) {
			N namespace = entry.getKey();
			Set<K> expectedKeySet = new HashSet<>(entry.getValue().keySet());
			assertEquals(expectedKeySet.size(), stateMap.sizeOfNamespace(namespace));
			Iterator<K> keyIterator = stateMap.getKeys(namespace).iterator();
			while (keyIterator.hasNext()) {
				K key = keyIterator.next();
				assertTrue(expectedKeySet.remove(key));
			}
			assertTrue(expectedKeySet.isEmpty());
		}

		// validates iterator()
		Map<N, Map<K, S>> actualStates = new HashMap<>();
		Iterator<StateEntry<K, N, S>> iterator = stateMap.iterator();
		while (iterator.hasNext()) {
			StateEntry<K, N, S> entry = iterator.next();
			S oldState = actualStates.computeIfAbsent(entry.getNamespace(), (none) -> new HashMap<>())
				.put(entry.getKey(), entry.getState());
			assertNull(oldState);
		}
		referenceStates.forEach(
			(ns, kvMap) -> {
				if (kvMap.isEmpty()) {
					assertThat(actualStates.get(ns), nullValue());
				} else {
					assertEquals(kvMap, actualStates.get(ns));
				}
			});

		// validates getStateIncrementalVisitor()
		InternalKvState.StateIncrementalVisitor<K, N, S> visitor =
			stateMap.getStateIncrementalVisitor(2);
		actualStates.clear();
		while (visitor.hasNext()) {
			Collection<StateEntry<K, N, S>> collection = visitor.nextEntries();
			for (StateEntry<K, N, S> entry : collection) {
				S oldState = actualStates.computeIfAbsent(entry.getNamespace(), (none) -> new HashMap<>())
					.put(entry.getKey(), entry.getState());
				assertNull(oldState);
			}
		}
		referenceStates.forEach(
			(ns, kvMap) -> {
				if (kvMap.isEmpty()) {
					assertThat(actualStates.get(ns), nullValue());
				} else {
					assertEquals(kvMap, actualStates.get(ns));
				}
			});
	}

	private <K, N, S> Map<N, Map<K, S>> snapshotReferenceStates(Map<N, Map<K, S>> referenceStates) {
		Map<N, Map<K, S>> snapshot = new HashMap<>();
		referenceStates.forEach((namespace, keyMap) -> snapshot.put(namespace, new HashMap<>(keyMap)));
		return snapshot;
	}

	private <K, N, S>void verifySnapshotWithoutTransform(
		Map<N, Map<K, S>> referenceStates,
		CopyOnWriteSkipListStateMapSnapshot<K, N, S> snapshot,
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<S> stateSerializer) throws IOException {
		ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
		DataOutputView outputView = new DataOutputViewStreamWrapper(outputStream);
		snapshot.writeState(keySerializer, namespaceSerializer, stateSerializer, outputView, null);

		Map<N, Map<K, S>> actualStates = readStateFromSnapshot(
			outputStream.toByteArray(), keySerializer, namespaceSerializer, stateSerializer);
		assertEquals(referenceStates, actualStates);
	}

	private <K, N, S> void verifySnapshotWithTransform(
		Map<N, Map<K, S>> referenceStates,
		CopyOnWriteSkipListStateMapSnapshot<K, N, S> snapshot,
		StateSnapshotTransformer<S> transformer,
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<S> stateSerializer) throws IOException {
		ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
		DataOutputView outputView = new DataOutputViewStreamWrapper(outputStream);
		snapshot.writeState(keySerializer, namespaceSerializer, stateSerializer, outputView, transformer);

		Map<N, Map<K, S>> transformedStates = new HashMap<>();
		for (Map.Entry<N, Map<K, S>> namespaceEntry : referenceStates.entrySet()) {
			for (Map.Entry<K, S> keyEntry : namespaceEntry.getValue().entrySet()) {
				S state = transformer.filterOrTransform(keyEntry.getValue());
				if (state != null) {
					transformedStates.computeIfAbsent(namespaceEntry.getKey(), (none) -> new HashMap<>())
						.put(keyEntry.getKey(), state);
				}
			}
		}

		Map<N, Map<K, S>> actualStates = readStateFromSnapshot(
			outputStream.toByteArray(), keySerializer, namespaceSerializer, stateSerializer);
		assertEquals(transformedStates, actualStates);
	}

	private <K, N, S> Map<N, Map<K, S>> readStateFromSnapshot(
		byte[] data,
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<S> stateSerializer) throws IOException {
		ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(data);
		DataInputView dataInputView = new DataInputViewStreamWrapper(inputStream);
		int size = dataInputView.readInt();

		Map<N, Map<K, S>> states = new HashMap<>();
		for (int i = 0; i < size; i++) {
			N namespace = namespaceSerializer.deserialize(dataInputView);
			K key = keySerializer.deserialize(dataInputView);
			S state = stateSerializer.deserialize(dataInputView);
			states.computeIfAbsent(namespace, (none) -> new HashMap<>()).put(key, state);
		}

		return states;
	}

	private List<String> getAllValuesOfNode(
		CopyOnWriteSkipListStateMap<Integer, Long, String> stateMap,
		Allocator spaceAllocator,
		long node) {
		List<String> values = new ArrayList<>();
		long valuePointer = SkipListUtils.helpGetValuePointer(node, spaceAllocator);
		while (valuePointer != NIL_NODE) {
			values.add(stateMap.helpGetState(valuePointer));
			valuePointer = SkipListUtils.helpGetNextValuePointer(valuePointer, spaceAllocator);
		}
		return values;
	}

	private CopyOnWriteSkipListStateMap<Integer, Long, String> createStateMapForTesting() {
		return createStateMapForTesting(
			DEFAULT_MAX_KEYS_TO_DELETE_ONE_TIME,
			DEFAULT_LOGICAL_REMOVED_KEYS_RATIO);
	}

	private CopyOnWriteSkipListStateMap<Integer, Long, String> createStateMapForTesting(
			int keysToDelete,
			float logicalKeysRemoveRatio) {
		return new CopyOnWriteSkipListStateMap<>(
			keySerializer,
			namespaceSerializer,
			stateSerializer,
			spaceAllocator,
			keysToDelete,
			logicalKeysRemoveRatio);
	}
}
