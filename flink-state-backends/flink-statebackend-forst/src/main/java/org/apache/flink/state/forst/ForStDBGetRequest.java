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

import org.apache.flink.core.state.InternalStateFuture;

import org.rocksdb.ColumnFamilyHandle;

import java.io.IOException;

/**
 * The Get access request for ForStDB.
 *
 * @param <K> The type of key in get access request.
 * @param <V> The type of value returned by get request.
 */
public class ForStDBGetRequest<K, V> {

    private final K key;
    private final ForStInnerTable<K, V> table;
    private final InternalStateFuture<V> future;

    private ForStDBGetRequest(K key, ForStInnerTable<K, V> table, InternalStateFuture<V> future) {
        this.key = key;
        this.table = table;
        this.future = future;
    }

    public byte[] buildSerializedKey() throws IOException {
        return table.serializeKey(key);
    }

    public ColumnFamilyHandle getColumnFamilyHandle() {
        return table.getColumnFamilyHandle();
    }

    public void completeStateFuture(byte[] bytesValue) throws IOException {
        if (bytesValue == null) {
            future.complete(null);
            return;
        }
        V value = table.deserializeValue(bytesValue);
        future.complete(value);
    }

    public void completeStateFutureExceptionally(String message, Throwable ex) {
        future.completeExceptionally(message, ex);
    }

    static <K, V> ForStDBGetRequest<K, V> of(
            K key, ForStInnerTable<K, V> table, InternalStateFuture<V> future) {
        return new ForStDBGetRequest<>(key, table, future);
    }
}
