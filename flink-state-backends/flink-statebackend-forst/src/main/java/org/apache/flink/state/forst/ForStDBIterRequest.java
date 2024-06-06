/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.forst;

import org.apache.flink.api.common.state.v2.StateIterator;
import org.apache.flink.core.state.InternalStateFuture;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.util.Preconditions;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * The Iter access request for ForStDB.
 *
 * @param <T> The type of value in iterator returned by get request.
 */
public class ForStDBIterRequest<T> {

    /** The type of result returned by iterator. */
    public enum ResultType {
        KEY,
        VALUE,
        ENTRY,
    }

    private final ResultType resultType;

    /**
     * ContextKey that use to calculate prefix bytes. All entries under the same key have the same
     * prefix, hence we can stop iterating once coming across an entry with a different prefix.
     */
    @Nonnull private final ContextKey contextKey;

    /** The table that generated iter requests. */
    private final ForStMapState table;

    /**
     * The state request handler, used for {@link
     * org.apache.flink.runtime.asyncprocessing.StateRequestType#ITERATOR_LOADING}.
     */
    private final StateRequestHandler stateRequestHandler;

    private final InternalStateFuture<StateIterator<T>> future;

    private final int keyGroupPrefixBytes;

    /** The bytes to seek to. If null, seek start from the {@link #getKeyPrefixBytes}. */
    private final byte[] toSeekBytes;

    public ForStDBIterRequest(
            ResultType type,
            ContextKey contextKey,
            ForStMapState table,
            StateRequestHandler stateRequestHandler,
            InternalStateFuture<StateIterator<T>> future,
            byte[] toSeekBytes) {
        this.resultType = type;
        this.contextKey = contextKey;
        this.table = table;
        this.stateRequestHandler = stateRequestHandler;
        this.future = future;
        this.keyGroupPrefixBytes = table.getKeyGroupPrefixBytes();
        this.toSeekBytes = toSeekBytes;
    }

    public void process(RocksDB db, int cacheSizeLimit) throws IOException {
        try (RocksIterator iter = db.newIterator(table.getColumnFamilyHandle())) {
            // step 1: seek to the first key
            byte[] prefix = getKeyPrefixBytes();
            int userKeyOffset = prefix.length;
            if (toSeekBytes != null) {
                iter.seek(toSeekBytes);
            } else {
                iter.seek(prefix);
            }

            // step 2: iterate the entries, read at most cacheSizeLimit entries at a time. If not
            // read all at once, other entries will be loaded in a new ITERATOR_LOADING request.
            List<RawEntry> entries = new ArrayList<>(cacheSizeLimit);
            boolean encounterEnd = false;
            byte[] nextSeek = prefix;
            while (iter.isValid() && entries.size() < cacheSizeLimit) {
                byte[] key = iter.key();
                if (startWithKeyPrefix(prefix, key, keyGroupPrefixBytes)) {
                    entries.add(new RawEntry(key, iter.value()));
                    nextSeek = key;
                } else {
                    encounterEnd = true;
                    break;
                }
                iter.next();
            }
            if (iter.isValid()) {
                nextSeek = iter.key();
            }
            if (entries.size() < cacheSizeLimit) {
                encounterEnd = true;
            }

            // step 3: deserialize the entries
            Collection<Object> deserializedEntries = new ArrayList<>(entries.size());
            switch (resultType) {
                case KEY:
                    {
                        for (RawEntry en : entries) {
                            deserializedEntries.add(
                                    deserializeUserKey(en.rawKeyBytes, userKeyOffset));
                        }
                        break;
                    }
                case VALUE:
                    {
                        for (RawEntry en : entries) {
                            deserializedEntries.add(deserializeUserValue(en.rawValueBytes));
                        }
                        break;
                    }
                case ENTRY:
                    {
                        for (RawEntry en : entries) {
                            deserializedEntries.add(
                                    new MapEntry(
                                            deserializeUserKey(en.rawKeyBytes, userKeyOffset),
                                            deserializeUserValue(en.rawValueBytes)));
                        }
                        break;
                    }
                default:
                    throw new IllegalStateException("Unknown result type: " + resultType);
            }

            // step 4: construct a ForStMapIterator.
            ForStMapIterator stateIterator =
                    new ForStMapIterator<>(
                            table,
                            resultType,
                            StateRequestType.ITERATOR_LOADING,
                            stateRequestHandler,
                            deserializedEntries,
                            encounterEnd,
                            nextSeek);

            completeStateFuture(stateIterator);
        }
    }

    public byte[] getKeyPrefixBytes() throws IOException {
        Preconditions.checkState(contextKey.getUserKey() == null);
        return table.serializeKey(contextKey);
    }

    public Object deserializeUserValue(byte[] valueBytes) throws IOException {
        return table.deserializeValue(valueBytes);
    }

    public Object deserializeUserKey(byte[] userKeyBytes, int userKeyOffset) throws IOException {
        Object userKey = table.deserializeUserKey(userKeyBytes, userKeyOffset);
        return userKey;
    }

    public void completeStateFuture(StateIterator<T> iterator) {
        future.complete(iterator);
    }

    public void completeStateFutureExceptionally(String message, Throwable ex) {
        future.completeExceptionally(message, ex);
    }

    /**
     * Check if the raw key bytes start with the key prefix bytes.
     *
     * @param keyPrefixBytes the key prefix bytes.
     * @param rawKeyBytes the raw key bytes.
     * @param kgPrefixBytes the number of key group prefix bytes.
     * @return true if the raw key bytes start with the key prefix bytes.
     */
    public static boolean startWithKeyPrefix(
            byte[] keyPrefixBytes, byte[] rawKeyBytes, int kgPrefixBytes) {
        if (rawKeyBytes.length < keyPrefixBytes.length) {
            return false;
        }
        for (int i = keyPrefixBytes.length; --i >= kgPrefixBytes; ) {
            if (rawKeyBytes[i] != keyPrefixBytes[i]) {
                return false;
            }
        }
        return true;
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

    /** The entry to store the raw key and value. */
    static class RawEntry {
        /**
         * The raw bytes of the key stored in RocksDB. Each user key is stored in RocksDB with the
         * format #KeyGroup#Key#Namespace#UserKey.
         */
        private final byte[] rawKeyBytes;

        /** The raw bytes of the value stored in RocksDB. */
        private final byte[] rawValueBytes;

        public RawEntry(byte[] rawKeyBytes, byte[] rawValueBytes) {
            this.rawKeyBytes = rawKeyBytes;
            this.rawValueBytes = rawValueBytes;
        }
    }
}
