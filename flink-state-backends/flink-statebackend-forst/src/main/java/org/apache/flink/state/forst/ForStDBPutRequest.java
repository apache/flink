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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.state.InternalStateFuture;

import org.forstdb.ColumnFamilyHandle;
import org.forstdb.RocksDB;
import org.forstdb.RocksDBException;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * The Put access request for ForStDB.
 *
 * @param <K> The type of key in put access request.
 * @param <N> The type of namespace in put access request.
 * @param <V> The type of value in put access request.
 */
public class ForStDBPutRequest<K, N, V> {

    final ContextKey<K, N> key;
    @Nullable final V value;
    final boolean isMerge;
    final ForStInnerTable<K, N, V> table;
    final InternalStateFuture<Void> future;

    ForStDBPutRequest(
            ContextKey<K, N> key,
            V value,
            boolean isMerge,
            ForStInnerTable<K, N, V> table,
            InternalStateFuture<Void> future) {
        this.key = key;
        this.value = value;
        this.isMerge = isMerge;
        this.table = table;
        this.future = future;
    }

    public void process(ForStDBWriteBatchWrapper writeBatchWrapper, RocksDB db)
            throws IOException, RocksDBException {
        if (value == null) {
            writeBatchWrapper.remove(table.getColumnFamilyHandle(), buildSerializedKey());
        } else if (isMerge) {
            writeBatchWrapper.merge(
                    table.getColumnFamilyHandle(), buildSerializedKey(), buildSerializedValue());
        } else {
            writeBatchWrapper.put(
                    table.getColumnFamilyHandle(), buildSerializedKey(), buildSerializedValue());
        }
    }

    public byte[] buildSerializedKey() throws IOException {
        return table.serializeKey(key);
    }

    public byte[] buildSerializedValue() throws IOException {
        assert value != null;
        return table.serializeValue(value);
    }

    public void completeStateFuture() {
        future.complete(null);
    }

    public void completeStateFutureExceptionally(String message, Throwable ex) {
        future.completeExceptionally(message, ex);
    }

    /**
     * If the value of the ForStDBPutRequest is null, then the request will signify the deletion of
     * the data associated with that key.
     */
    static <K, N, V> ForStDBPutRequest<K, N, V> of(
            ContextKey<K, N> key,
            @Nullable V value,
            ForStInnerTable<K, N, V> table,
            InternalStateFuture<Void> future) {
        return new ForStDBPutRequest<>(key, value, false, table, future);
    }

    static <K, N, V> ForStDBPutRequest<K, N, V> ofMerge(
            ContextKey<K, N> key,
            @Nullable V value,
            ForStInnerTable<K, N, V> table,
            InternalStateFuture<Void> future) {
        return new ForStDBPutRequest<>(key, value, true, table, future);
    }

    // --------------- For testing usage ---------------
    @VisibleForTesting
    public boolean valueIsNull() {
        return value == null;
    }

    @VisibleForTesting
    public ColumnFamilyHandle getColumnFamilyHandle() {
        return table.getColumnFamilyHandle();
    }
}
