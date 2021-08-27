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
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.KeyValueStateIterator;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import org.rocksdb.ReadOptions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Iterator that merges multiple RocksDB iterators to partition all states into contiguous
 * key-groups. The resulting iteration sequence is ordered by (key-group, kv-state).
 */
public class RocksStatesPerKeyGroupMergeIterator implements KeyValueStateIterator {

    private final CloseableRegistry closeableRegistry;
    private final PriorityQueue<SingleStateIterator> heap;
    private final int keyGroupPrefixByteCount;
    private boolean newKeyGroup;
    private boolean newKVState;
    private boolean valid;
    private SingleStateIterator currentSubIterator;

    private static final List<Comparator<SingleStateIterator>> COMPARATORS;

    static {
        int maxBytes = 2;
        COMPARATORS = new ArrayList<>(maxBytes);
        for (int i = 0; i < maxBytes; ++i) {
            final int currentBytes = i + 1;
            COMPARATORS.add(
                    (o1, o2) -> {
                        int arrayCmpRes =
                                compareKeyGroupsForByteArrays(o1.key(), o2.key(), currentBytes);
                        return arrayCmpRes == 0
                                ? o1.getKvStateId() - o2.getKvStateId()
                                : arrayCmpRes;
                    });
        }
    }

    /**
     * Creates a new {@link RocksStatesPerKeyGroupMergeIterator}. The iterator takes ownership of
     * passed in resources, such as the {@link ReadOptions}, and becomes responsible for closing
     * them.
     */
    public RocksStatesPerKeyGroupMergeIterator(
            final CloseableRegistry closeableRegistry,
            List<Tuple2<RocksIteratorWrapper, Integer>> kvStateIterators,
            List<SingleStateIterator> heapPriorityQueueIterators,
            final int keyGroupPrefixByteCount)
            throws IOException {
        Preconditions.checkNotNull(closeableRegistry);
        Preconditions.checkNotNull(kvStateIterators);
        Preconditions.checkArgument(keyGroupPrefixByteCount >= 1);

        this.closeableRegistry = closeableRegistry;
        this.keyGroupPrefixByteCount = keyGroupPrefixByteCount;

        if (kvStateIterators.size() > 0 || heapPriorityQueueIterators.size() > 0) {
            this.heap = buildIteratorHeap(kvStateIterators, heapPriorityQueueIterators);
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

    @Override
    public void next() {
        newKeyGroup = false;
        newKVState = false;

        byte[] oldKey = currentSubIterator.key();
        currentSubIterator.next();
        if (currentSubIterator.isValid()) {
            if (isDifferentKeyGroup(oldKey, currentSubIterator.key())) {
                SingleStateIterator oldIterator = currentSubIterator;
                heap.offer(currentSubIterator);
                currentSubIterator = heap.remove();
                newKVState = currentSubIterator != oldIterator;
                detectNewKeyGroup(oldKey);
            }
        } else {
            if (closeableRegistry.unregisterCloseable(currentSubIterator)) {
                IOUtils.closeQuietly(currentSubIterator);
            }

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

    private PriorityQueue<SingleStateIterator> buildIteratorHeap(
            List<Tuple2<RocksIteratorWrapper, Integer>> kvStateIterators,
            List<SingleStateIterator> heapPriorityQueueIterators)
            throws IOException {

        Comparator<SingleStateIterator> iteratorComparator =
                COMPARATORS.get(keyGroupPrefixByteCount - 1);

        PriorityQueue<SingleStateIterator> iteratorPriorityQueue =
                new PriorityQueue<>(
                        kvStateIterators.size() + heapPriorityQueueIterators.size(),
                        iteratorComparator);

        for (Tuple2<RocksIteratorWrapper, Integer> rocksIteratorWithKVStateId : kvStateIterators) {
            final RocksIteratorWrapper rocksIterator = rocksIteratorWithKVStateId.f0;
            rocksIterator.seekToFirst();
            if (rocksIterator.isValid()) {
                RocksSingleStateIterator wrappingIterator =
                        new RocksSingleStateIterator(rocksIterator, rocksIteratorWithKVStateId.f1);
                iteratorPriorityQueue.offer(wrappingIterator);
                closeableRegistry.registerCloseable(wrappingIterator);
                closeableRegistry.unregisterCloseable(rocksIterator);
            } else {
                if (closeableRegistry.unregisterCloseable(rocksIterator)) {
                    IOUtils.closeQuietly(rocksIterator);
                }
            }
        }

        for (SingleStateIterator heapQueueIterator : heapPriorityQueueIterators) {
            if (heapQueueIterator.isValid()) {
                iteratorPriorityQueue.offer(heapQueueIterator);
                closeableRegistry.registerCloseable(heapQueueIterator);
            } else {
                IOUtils.closeQuietly(heapQueueIterator);
            }
        }

        return iteratorPriorityQueue;
    }

    private boolean isDifferentKeyGroup(byte[] a, byte[] b) {
        return 0 != compareKeyGroupsForByteArrays(a, b, keyGroupPrefixByteCount);
    }

    private void detectNewKeyGroup(byte[] oldKey) {
        if (isDifferentKeyGroup(oldKey, currentSubIterator.key())) {
            newKeyGroup = true;
        }
    }

    @Override
    public int keyGroup() {
        final byte[] currentKey = currentSubIterator.key();
        int result = 0;
        // big endian decode
        for (int i = 0; i < keyGroupPrefixByteCount; ++i) {
            result <<= 8;
            result |= (currentKey[i] & 0xFF);
        }
        return result;
    }

    @Override
    public byte[] key() {
        return currentSubIterator.key();
    }

    @Override
    public byte[] value() {
        return currentSubIterator.value();
    }

    @Override
    public int kvStateId() {
        return currentSubIterator.getKvStateId();
    }

    @Override
    public boolean isNewKeyValueState() {
        return newKVState;
    }

    @Override
    public boolean isNewKeyGroup() {
        return newKeyGroup;
    }

    @Override
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
        IOUtils.closeQuietly(closeableRegistry);

        if (heap != null) {
            heap.clear();
        }
    }
}
