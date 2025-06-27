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

package org.apache.flink.state.forst;

import org.apache.flink.api.common.state.v2.StateIterator;
import org.apache.flink.core.asyncprocessing.InternalAsyncFuture;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;

import org.forstdb.RocksIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/** The ForSt {@link ForStDBIterRequest} which returns the entries of a ForStMapState. */
public class ForStDBMapEntryIterRequest<K, N, UK, UV>
        extends ForStDBIterRequest<K, N, UK, UV, Map.Entry<UK, UV>> {

    private final InternalAsyncFuture<StateIterator<Map.Entry<UK, UV>>> future;

    public ForStDBMapEntryIterRequest(
            ContextKey<K, N> contextKey,
            ForStMapState<K, N, UK, UV> table,
            StateRequestHandler stateRequestHandler,
            @Nullable RocksIterator rocksIterator,
            InternalAsyncFuture<StateIterator<Map.Entry<UK, UV>>> future) {
        super(contextKey, table, stateRequestHandler, rocksIterator);
        this.future = future;
    }

    @Override
    public void completeStateFutureExceptionally(String message, Throwable ex) {
        future.completeExceptionally(message, ex);
    }

    @Override
    public Collection<Map.Entry<UK, UV>> deserializeElement(
            List<RawEntry> entries, int userKeyOffset) throws IOException {
        Collection<Map.Entry<UK, UV>> deserializedEntries = new ArrayList<>(entries.size());
        for (RawEntry en : entries) {
            // Since this value might be written from sync mode, the user value can be null.
            UV userValue = deserializeUserValue(en.rawValueBytes);
            if (userValue != null) {
                UK userKey = deserializeUserKey(en.rawKeyBytes, userKeyOffset);
                deserializedEntries.add(new MapEntry<>(userKey, userValue));
            }
        }
        return deserializedEntries;
    }

    @Override
    public void buildIteratorAndCompleteFuture(
            Collection<Map.Entry<UK, UV>> partialResult, boolean encounterEnd) {
        ForStMapIterator<Map.Entry<UK, UV>> stateIterator =
                new ForStMapIterator<>(
                        table,
                        StateRequestType.MAP_ITER,
                        StateRequestType.ITERATOR_LOADING,
                        stateRequestHandler,
                        partialResult,
                        encounterEnd,
                        rocksIterator);

        future.complete(stateIterator);
    }

    /**
     * The map entry to store the serialized key and value.
     *
     * @param <K> The type of key.
     * @param <V> The type of value.
     */
    static class MapEntry<K, V> implements Map.Entry<K, V> {
        K key;
        V value;

        public MapEntry(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public V setValue(V value) {
            this.value = value;
            return value;
        }
    }
}
