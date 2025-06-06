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

package org.apache.flink.state.rocksdb.iterator;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.state.rocksdb.RocksIteratorWrapper;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

import static org.apache.flink.state.rocksdb.iterator.AbstractRocksStateKeysIterator.isMatchingNameSpace;

/**
 * Adapter class to bridge between {@link RocksIteratorWrapper} and {@link Iterator} to iterate over
 * the keys. This class is not thread safe.
 *
 * @param <K> the type of the iterated objects, which are keys in RocksDB.
 */
public class RocksMultiStateKeysIterator<K> implements AutoCloseable, Iterator<K> {

    private final List<RocksIteratorWrapper> iterators;
    private final List<String> states;
    private final TypeSerializer<K> keySerializer;
    private final List<Boolean> ambiguousKeyPossibles;
    private final int keyGroupPrefixBytes;
    private final byte[] namespaceBytes;
    private final DataInputDeserializer byteArrayDataInputView;

    private final byte[][] iteratorKeys;
    private final int[] iteratorKeysToRemove;
    private K previousKey;
    private K nextKey;

    public RocksMultiStateKeysIterator(
            List<RocksIteratorWrapper> iterators,
            List<String> states,
            @Nonnull TypeSerializer<K> keySerializer,
            int keyGroupPrefixBytes,
            List<Boolean> ambiguousKeyPossibles,
            @Nonnull byte[] namespaceBytes) {
        this.iterators = iterators;
        this.states = states;
        this.keySerializer = keySerializer;
        this.ambiguousKeyPossibles = ambiguousKeyPossibles;
        this.keyGroupPrefixBytes = keyGroupPrefixBytes;
        this.namespaceBytes = namespaceBytes;
        this.byteArrayDataInputView = new DataInputDeserializer();
        this.iteratorKeys = new byte[iterators.size()][];
        Arrays.fill(iteratorKeys, null);
        this.iteratorKeysToRemove = new int[iterators.size()];
        Arrays.fill(iteratorKeysToRemove, -1);
        this.previousKey = null;
        this.nextKey = null;
    }

    @Override
    public boolean hasNext() {
        try {
            while (nextKey == null && hasDataToProcess()) {
                pullKeysFromIterators();
                K smallestIteratorKey = calculateSmallestKeyFromLocalData();
                if (smallestIteratorKey != null) {
                    previousKey = smallestIteratorKey;
                    nextKey = smallestIteratorKey;
                }
            }
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    "Failed to access states [" + String.join(",", states) + "]", e);
        }
        return nextKey != null;
    }

    private boolean hasDataToProcess() {
        boolean result = iterators.stream().anyMatch(RocksIteratorWrapper::isValid);
        if (!result) {
            for (int i = 0; i < iterators.size(); ++i) {
                if (iteratorKeys[i] != null) {
                    result = true;
                    break;
                }
            }
        }
        return result;
    }

    private void pullKeysFromIterators() {
        for (int i = 0; i < iterators.size(); ++i) {
            RocksIteratorWrapper iterator = iterators.get(i);
            if (iteratorKeys[i] == null && iterator.isValid()) {
                iteratorKeys[i] = iterator.key();
                iterator.next();
            }
        }
    }

    @Nullable
    private K calculateSmallestKeyFromLocalData() throws IOException {
        int smallestIteratorKeyIndex = -1;
        byte[] smallestIteratorKey = null;
        int iteratorKeysToRemoveIndex = 0;
        for (int i = 0; i < iteratorKeys.length; ++i) {
            byte[] iteratorKey = iteratorKeys[i];
            if (iteratorKey != null) {
                boolean update = smallestIteratorKey == null;
                if (!update) {
                    int cmp = Arrays.compare(iteratorKey, smallestIteratorKey);
                    if (cmp < 0) {
                        update = true;
                    } else if (cmp == 0) {
                        iteratorKeysToRemove[iteratorKeysToRemoveIndex++] = i;
                    }
                }

                if (update) {
                    smallestIteratorKeyIndex = i;
                    smallestIteratorKey = iteratorKey;
                    Arrays.fill(iteratorKeysToRemove, -1);
                    iteratorKeysToRemoveIndex = 0;
                    iteratorKeysToRemove[iteratorKeysToRemoveIndex++] = i;
                }
            }
        }

        if (smallestIteratorKey != null) {
            for (int i = 0; i < iteratorKeysToRemoveIndex; ++i) {
                iteratorKeys[iteratorKeysToRemove[i]] = null;
            }
            byteArrayDataInputView.setBuffer(
                    smallestIteratorKey,
                    keyGroupPrefixBytes,
                    smallestIteratorKey.length - keyGroupPrefixBytes);
            final K smallestIteratorKeyValue =
                    CompositeKeySerializationUtils.readKey(
                            keySerializer,
                            byteArrayDataInputView,
                            ambiguousKeyPossibles.get(smallestIteratorKeyIndex));
            if (isMatchingNameSpace(
                            smallestIteratorKey,
                            byteArrayDataInputView.getPosition(),
                            namespaceBytes)
                    && !Objects.equals(previousKey, smallestIteratorKeyValue)) {
                return smallestIteratorKeyValue;
            }
        }

        return null;
    }

    @Override
    public K next() {
        if (!hasNext()) {
            throw new NoSuchElementException(
                    "Failed to access states [" + String.join(",", states) + "]");
        }

        K tmpKey = nextKey;
        nextKey = null;
        return tmpKey;
    }

    @Override
    public void close() {
        for (RocksIteratorWrapper iterator : iterators) {
            iterator.close();
        }
    }
}
