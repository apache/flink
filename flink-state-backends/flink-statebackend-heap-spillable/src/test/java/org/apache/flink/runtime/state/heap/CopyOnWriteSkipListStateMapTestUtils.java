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
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.heap.space.Allocator;
import org.apache.flink.runtime.state.internal.InternalKvState;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.runtime.state.heap.CopyOnWriteSkipListStateMap.DEFAULT_LOGICAL_REMOVED_KEYS_RATIO;
import static org.apache.flink.runtime.state.heap.CopyOnWriteSkipListStateMap.DEFAULT_MAX_KEYS_TO_DELETE_ONE_TIME;
import static org.apache.flink.runtime.state.heap.SkipListUtils.NIL_NODE;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Utils for CopyOnWriteSkipListStateMap test.
 */
class CopyOnWriteSkipListStateMapTestUtils {
	@Nonnull
	static CopyOnWriteSkipListStateMap<Integer, Long, String> createEmptyStateMap(
		int keysToDelete,
		float logicalKeysRemoveRatio,
		Allocator spaceAllocator) {
		return createStateMapForTesting(
			keysToDelete,
			logicalKeysRemoveRatio,
			spaceAllocator);
	}

	@Nonnull
	static CopyOnWriteSkipListStateMap<Integer, Long, String> createEmptyStateMap() {
		return createStateMapForTesting(
			DEFAULT_MAX_KEYS_TO_DELETE_ONE_TIME,
			DEFAULT_LOGICAL_REMOVED_KEYS_RATIO,
			new TestAllocator(256));
	}

	@Nonnull
	static CopyOnWriteSkipListStateMap<Integer, Long, String> createStateMapForTesting(
		int keysToDelete,
		float logicalKeysRemoveRatio,
		Allocator spaceAllocator) {
		return new CopyOnWriteSkipListStateMap<>(
			IntSerializer.INSTANCE,
			LongSerializer.INSTANCE,
			StringSerializer.INSTANCE,
			spaceAllocator,
			keysToDelete,
			logicalKeysRemoveRatio);
	}

	static <K, N, S> void verifyState(
		@Nonnull Map<N, Map<K, S>> referenceStates,
		@Nonnull CopyOnWriteSkipListStateMap<K, N, S> stateMap) {

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

	static <K, N, S> void addToReferenceState(@Nonnull Map<N, Map<K, S>> referenceStates, K key, N namespace, S state) {
		referenceStates.computeIfAbsent(namespace, (none) -> new HashMap<>()).put(key, state);
	}

	static <K, N, S> void removeFromReferenceState(@Nonnull Map<N, Map<K, S>> referenceStates, K key, N namespace) {
		Map<K, S> keyMap = referenceStates.get(namespace);
		if (keyMap == null) {
			return;
		}
		keyMap.remove(key);
		if (keyMap.isEmpty()) {
			referenceStates.remove(namespace);
		}
	}

	static <K, N, S> void verifySnapshotWithoutTransform(
		Map<N, Map<K, S>> referenceStates,
		@Nonnull CopyOnWriteSkipListStateMapSnapshot<K, N, S> snapshot,
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

	static <K, N, S> void verifySnapshotWithTransform(
		@Nonnull Map<N, Map<K, S>> referenceStates,
		@Nonnull CopyOnWriteSkipListStateMapSnapshot<K, N, S> snapshot,
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

	private static <K, N, S> Map<N, Map<K, S>> readStateFromSnapshot(
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

	static <K, N, S> Map<N, Map<K, S>> snapshotReferenceStates(@Nonnull Map<N, Map<K, S>> referenceStates) {
		Map<N, Map<K, S>> snapshot = new HashMap<>();
		referenceStates.forEach((namespace, keyMap) -> snapshot.put(namespace, new HashMap<>(keyMap)));
		return snapshot;
	}

	static List<String> getAllValuesOfNode(
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
}
