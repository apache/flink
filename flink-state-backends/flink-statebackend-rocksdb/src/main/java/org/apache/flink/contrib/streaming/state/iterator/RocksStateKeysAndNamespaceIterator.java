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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBKeySerializationUtils;
import org.apache.flink.contrib.streaming.state.RocksIteratorWrapper;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nonnull;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Adapter class to bridge between {@link RocksIteratorWrapper} and {@link Iterator} to iterate over
 * the keys and namespaces. This class is not thread safe.
 *
 * @param <K> the type of the iterated keys in RocksDB.
 * @param <N> the type of the iterated namespaces in RocksDB.
 */
public class RocksStateKeysAndNamespaceIterator<K, N> extends AbstractRocksStateKeysIterator<K>
        implements Iterator<Tuple2<K, N>> {

    @Nonnull private final TypeSerializer<N> namespaceSerializer;

    private Tuple2<K, N> nextKey;

    public RocksStateKeysAndNamespaceIterator(
            @Nonnull RocksIteratorWrapper iterator,
            @Nonnull String state,
            @Nonnull TypeSerializer<K> keySerializer,
            @Nonnull TypeSerializer<N> namespaceSerializer,
            int keyGroupPrefixBytes,
            boolean ambiguousKeyPossible) {
        super(iterator, state, keySerializer, keyGroupPrefixBytes, ambiguousKeyPossible);

        this.namespaceSerializer = namespaceSerializer;
        this.nextKey = null;
    }

    @Override
    public boolean hasNext() {
        try {
            while (nextKey == null && iterator.isValid()) {

                final byte[] keyBytes = iterator.key();
                final K currentKey = deserializeKey(keyBytes, byteArrayDataInputView);
                final N currentNamespace =
                        RocksDBKeySerializationUtils.readNamespace(
                                namespaceSerializer, byteArrayDataInputView, ambiguousKeyPossible);
                nextKey = Tuple2.of(currentKey, currentNamespace);
                iterator.next();
            }
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to access state [" + state + "]", e);
        }
        return nextKey != null;
    }

    @Override
    public Tuple2<K, N> next() {
        if (!hasNext()) {
            throw new NoSuchElementException("Failed to access state [" + state + "]");
        }

        Tuple2<K, N> tmpKey = nextKey;
        nextKey = null;
        return tmpKey;
    }
}
