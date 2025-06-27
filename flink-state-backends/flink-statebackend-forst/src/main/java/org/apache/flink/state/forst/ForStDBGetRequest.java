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

import org.apache.flink.core.asyncprocessing.InternalAsyncFuture;

import org.forstdb.ColumnFamilyHandle;
import org.forstdb.RocksDB;
import org.forstdb.RocksDBException;

import java.io.IOException;

/**
 * The Get access request for ForStDB.
 *
 * @param <K> The type of key in get access request.
 * @param <N> The type of namespace in put access request.
 * @param <V> The type of value returned by get request.
 * @param <R> The type of returned value in state future.
 */
public abstract class ForStDBGetRequest<K, N, V, R> {

    final ContextKey<K, N> key;
    final ForStInnerTable<K, N, V> table;
    final InternalAsyncFuture<R> future;

    ForStDBGetRequest(
            ContextKey<K, N> key, ForStInnerTable<K, N, V> table, InternalAsyncFuture<R> future) {
        this.key = key;
        this.table = table;
        this.future = future;
    }

    public void process(RocksDB db) throws IOException, RocksDBException {
        byte[] key = buildSerializedKey();
        byte[] value = db.get(getColumnFamilyHandle(), key);
        completeStateFuture(value);
    }

    public byte[] buildSerializedKey() throws IOException {
        return table.serializeKey(key);
    }

    public ColumnFamilyHandle getColumnFamilyHandle() {
        return table.getColumnFamilyHandle();
    }

    public abstract void completeStateFuture(byte[] bytesValue) throws IOException;

    public void completeStateFutureExceptionally(String message, Throwable ex) {
        future.completeExceptionally(message, ex);
    }
}
