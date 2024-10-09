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

import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.util.Preconditions;

import org.forstdb.RocksDB;
import org.forstdb.RocksIterator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * The abstract iterator access request for ForStDB.
 *
 * @param <K> The type of key in iterator.
 * @param <N> The type of namespace in iterator.
 * @param <UK> The type of user key in iterator.
 * @param <UV> The type of user value in iterator.
 * @param <R> The type of result.
 */
public abstract class ForStDBIterRequest<K, N, UK, UV, R> implements Closeable {

    /**
     * ContextKey that use to calculate prefix bytes. All entries under the same key have the same
     * prefix, hence we can stop iterating once coming across an entry with a different prefix.
     */
    @Nonnull final ContextKey<K, N> contextKey;

    /** The table that generated iter requests. */
    final ForStMapState<K, N, UK, UV> table;

    /**
     * The state request handler, used for {@link
     * org.apache.flink.runtime.asyncprocessing.StateRequestType#ITERATOR_LOADING}.
     */
    final StateRequestHandler stateRequestHandler;

    final int keyGroupPrefixBytes;

    /**
     * The rocksdb iterator. If null, create a new rocksdb iterator and seek start from the {@link
     * #getKeyPrefixBytes}.
     */
    @Nullable RocksIterator rocksIterator;

    public ForStDBIterRequest(
            ContextKey<K, N> contextKey,
            ForStMapState<K, N, UK, UV> table,
            StateRequestHandler stateRequestHandler,
            RocksIterator rocksIterator) {
        this.contextKey = contextKey;
        this.table = table;
        this.stateRequestHandler = stateRequestHandler;
        this.keyGroupPrefixBytes = table.getKeyGroupPrefixBytes();
        this.rocksIterator = rocksIterator;
    }

    protected UV deserializeUserValue(byte[] valueBytes) throws IOException {
        return table.deserializeValue(valueBytes);
    }

    protected UK deserializeUserKey(byte[] userKeyBytes, int userKeyOffset) throws IOException {
        return table.deserializeUserKey(userKeyBytes, userKeyOffset);
    }

    protected byte[] getKeyPrefixBytes() throws IOException {
        Preconditions.checkState(contextKey.getUserKey() == null);
        return table.serializeKey(contextKey);
    }

    /**
     * Check if the raw key bytes start with the key prefix bytes.
     *
     * @param keyPrefixBytes the key prefix bytes.
     * @param rawKeyBytes the raw key bytes.
     * @param kgPrefixBytes the number of key group prefix bytes.
     * @return true if the raw key bytes start with the key prefix bytes.
     */
    protected static boolean startWithKeyPrefix(
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

    public void process(RocksDB db, int cacheSizeLimit) throws IOException {
        // step 1: setup iterator, seek to the key
        byte[] prefix = getKeyPrefixBytes();
        int userKeyOffset = prefix.length;
        if (rocksIterator == null) {
            rocksIterator = db.newIterator(table.getColumnFamilyHandle());
            rocksIterator.seek(prefix);
        }

        // step 2: iterate the entries, read at most cacheSizeLimit entries at a time. If not
        // read all at once, other entries will be loaded in a new ITERATOR_LOADING request.
        List<RawEntry> entries = new ArrayList<>(cacheSizeLimit);
        boolean encounterEnd = false;
        while (rocksIterator.isValid() && entries.size() < cacheSizeLimit) {
            byte[] key = rocksIterator.key();
            if (startWithKeyPrefix(prefix, key, keyGroupPrefixBytes)) {
                entries.add(new RawEntry(key, rocksIterator.value()));
            } else {
                encounterEnd = true;
                rocksIterator.close();
                rocksIterator = null;
                break;
            }
            rocksIterator.next();
        }

        if (!encounterEnd && (entries.size() < cacheSizeLimit || !rocksIterator.isValid())) {
            encounterEnd = true;
            rocksIterator.close();
            rocksIterator = null;
        }

        // step 3: deserialize the entries
        Collection<R> deserializedEntries = deserializeElement(entries, userKeyOffset);

        // step 4: construct a ForStMapIterator.
        buildIteratorAndCompleteFuture(deserializedEntries, encounterEnd);
    }

    public abstract void completeStateFutureExceptionally(String message, Throwable ex);

    public abstract Collection<R> deserializeElement(List<RawEntry> entries, int userKeyOffset)
            throws IOException;

    public abstract void buildIteratorAndCompleteFuture(
            Collection<R> partialResult, boolean encounterEnd);

    public void close() throws IOException {
        if (rocksIterator != null) {
            rocksIterator.close();
            rocksIterator = null;
        }
    }

    /** The entry to store the raw key and value. */
    static class RawEntry {
        /**
         * The raw bytes of the key stored in RocksDB. Each user key is stored in RocksDB with the
         * format #KeyGroup#Key#Namespace#UserKey.
         */
        final byte[] rawKeyBytes;

        /** The raw bytes of the value stored in RocksDB. */
        final byte[] rawValueBytes;

        public RawEntry(byte[] rawKeyBytes, byte[] rawValueBytes) {
            this.rawKeyBytes = rawKeyBytes;
            this.rawValueBytes = rawValueBytes;
        }
    }
}
