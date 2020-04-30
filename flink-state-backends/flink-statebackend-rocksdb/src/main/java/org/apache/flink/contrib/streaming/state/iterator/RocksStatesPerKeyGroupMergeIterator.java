/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.iterator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksIteratorWrapper;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Iterator that merges multiple RocksDB iterators to partition all states into contiguous key-groups.
 * The resulting iteration sequence is ordered by (key-group, kv-state).
 */
public class RocksStatesPerKeyGroupMergeIterator implements AutoCloseable {

	private final PriorityQueue<RocksSingleStateIterator> heap;
	private final int keyGroupPrefixByteCount;
	private boolean newKeyGroup;
	private boolean newKVState;
	private boolean valid;
	private RocksSingleStateIterator currentSubIterator;

	private static final List<Comparator<RocksSingleStateIterator>> COMPARATORS;

	static {
		int maxBytes = 2;
		COMPARATORS = new ArrayList<>(maxBytes);
		for (int i = 0; i < maxBytes; ++i) {
			final int currentBytes = i + 1;
			COMPARATORS.add((o1, o2) -> {
				int arrayCmpRes = compareKeyGroupsForByteArrays(
					o1.getCurrentKey(), o2.getCurrentKey(), currentBytes);
				return arrayCmpRes == 0 ? o1.getKvStateId() - o2.getKvStateId() : arrayCmpRes;
			});
		}
	}

	public RocksStatesPerKeyGroupMergeIterator(
		List<Tuple2<RocksIteratorWrapper, Integer>> kvStateIterators,
		final int keyGroupPrefixByteCount) {
		Preconditions.checkNotNull(kvStateIterators);
		Preconditions.checkArgument(keyGroupPrefixByteCount >= 1);

		this.keyGroupPrefixByteCount = keyGroupPrefixByteCount;

		if (kvStateIterators.size() > 0) {
			this.heap = buildIteratorHeap(kvStateIterators);
			this.valid = !heap.isEmpty();
			this.currentSubIterator = heap.poll();
			kvStateIterators.clear();
		} else {
			// creating a PriorityQueue of size 0 results in an exception.
			this.heap = null;
			this.valid = false;
		}

		this.newKeyGroup = true;
		this.newKVState = true;
	}

	/**
	 * Advances the iterator. Should only be called if {@link #isValid()} returned true.
	 * Valid flag can only change after calling {@link #next()}.
	 */
	public void next() {
		newKeyGroup = false;
		newKVState = false;

		final RocksIteratorWrapper rocksIterator = currentSubIterator.getIterator();
		rocksIterator.next();

		byte[] oldKey = currentSubIterator.getCurrentKey();
		if (rocksIterator.isValid()) {

			currentSubIterator.setCurrentKey(rocksIterator.key());

			if (isDifferentKeyGroup(oldKey, currentSubIterator.getCurrentKey())) {
				heap.offer(currentSubIterator);
				currentSubIterator = heap.remove();
				newKVState = currentSubIterator.getIterator() != rocksIterator;
				detectNewKeyGroup(oldKey);
			}
		} else {
			IOUtils.closeQuietly(rocksIterator);

			if (heap.isEmpty()) {
				currentSubIterator = null;
				valid = false;
			} else {
				currentSubIterator = heap.remove();
				newKVState = true;
				detectNewKeyGroup(oldKey);
			}
		}
	}

	private PriorityQueue<RocksSingleStateIterator> buildIteratorHeap(
		List<Tuple2<RocksIteratorWrapper, Integer>> kvStateIterators) {

		Comparator<RocksSingleStateIterator> iteratorComparator = COMPARATORS.get(keyGroupPrefixByteCount - 1);

		PriorityQueue<RocksSingleStateIterator> iteratorPriorityQueue =
			new PriorityQueue<>(kvStateIterators.size(), iteratorComparator);

		for (Tuple2<RocksIteratorWrapper, Integer> rocksIteratorWithKVStateId : kvStateIterators) {
			final RocksIteratorWrapper rocksIterator = rocksIteratorWithKVStateId.f0;
			rocksIterator.seekToFirst();
			if (rocksIterator.isValid()) {
				iteratorPriorityQueue.offer(
					new RocksSingleStateIterator(rocksIterator, rocksIteratorWithKVStateId.f1));
			} else {
				IOUtils.closeQuietly(rocksIterator);
			}
		}
		return iteratorPriorityQueue;
	}

	private boolean isDifferentKeyGroup(byte[] a, byte[] b) {
		return 0 != compareKeyGroupsForByteArrays(a, b, keyGroupPrefixByteCount);
	}

	private void detectNewKeyGroup(byte[] oldKey) {
		if (isDifferentKeyGroup(oldKey, currentSubIterator.getCurrentKey())) {
			newKeyGroup = true;
		}
	}

	/**
	 * @return key-group for the current key
	 */
	public int keyGroup() {
		final byte[] currentKey = currentSubIterator.getCurrentKey();
		int result = 0;
		//big endian decode
		for (int i = 0; i < keyGroupPrefixByteCount; ++i) {
			result <<= 8;
			result |= (currentKey[i] & 0xFF);
		}
		return result;
	}

	public byte[] key() {
		return currentSubIterator.getCurrentKey();
	}

	public byte[] value() {
		return currentSubIterator.getIterator().value();
	}

	/**
	 * @return Id of K/V state to which the current key belongs.
	 */
	public int kvStateId() {
		return currentSubIterator.getKvStateId();
	}

	/**
	 * Indicates if current key starts a new k/v-state, i.e. belong to a different k/v-state than it's predecessor.
	 * @return true iff the current key belong to a different k/v-state than it's predecessor.
	 */
	public boolean isNewKeyValueState() {
		return newKVState;
	}

	/**
	 * Indicates if current key starts a new key-group, i.e. belong to a different key-group than it's predecessor.
	 * @return true iff the current key belong to a different key-group than it's predecessor.
	 */
	public boolean isNewKeyGroup() {
		return newKeyGroup;
	}

	/**
	 * Check if the iterator is still valid. Getters like {@link #key()}, {@link #value()}, etc. as well as
	 * {@link #next()} should only be called if valid returned true. Should be checked after each call to
	 * {@link #next()} before accessing iterator state.
	 * @return True iff this iterator is valid.
	 */
	public boolean isValid() {
		return valid;
	}

	private static int compareKeyGroupsForByteArrays(byte[] a, byte[] b, int len) {
		for (int i = 0; i < len; ++i) {
			int diff = (a[i] & 0xFF) - (b[i] & 0xFF);
			if (diff != 0) {
				return diff;
			}
		}
		return 0;
	}

	@Override
	public void close() {
		IOUtils.closeQuietly(currentSubIterator);
		currentSubIterator = null;

		IOUtils.closeAllQuietly(heap);
		heap.clear();
	}
}
