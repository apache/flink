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

import org.rocksdb.RocksDB;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/**
 * The writeBatch operation implementation for ForStDB.
 *
 * @param <K> The type of key in put access request.
 * @param <V> The type of value in put access request.
 */
public class ForStWriteBatchOperation<K, V> implements ForStDBOperation<Void> {

    private static final int PER_RECORD_ESTIMATE_BYTES = 100;

    private final RocksDB db;

    private final List<PutRequest<K, V>> batchRequest;

    private final WriteOptions writeOptions;

    private final Executor executor;

    ForStWriteBatchOperation(
            RocksDB db,
            List<PutRequest<K, V>> batchRequest,
            WriteOptions writeOptions,
            Executor executor) {
        this.db = db;
        this.batchRequest = batchRequest;
        this.writeOptions = writeOptions;
        this.executor = executor;
    }

    @Override
    public CompletableFuture<Void> process() {
        return CompletableFuture.runAsync(
                () -> {
                    try (WriteBatch writeBatch =
                            new WriteBatch(batchRequest.size() * PER_RECORD_ESTIMATE_BYTES)) {
                        for (PutRequest<K, V> request : batchRequest) {
                            ForStInnerTable<K, V> table = request.table;
                            if (request.value == null) {
                                // put(key, null) == delete(key)
                                writeBatch.delete(
                                        table.getColumnFamilyHandle(),
                                        table.serializeKey(request.key));
                            } else {
                                writeBatch.put(
                                        table.getColumnFamilyHandle(),
                                        table.serializeKey(request.key),
                                        table.serializeValue(request.value));
                            }
                        }
                        db.write(writeOptions, writeBatch);
                    } catch (Exception e) {
                        throw new CompletionException("Error while adding data to ForStDB", e);
                    }
                },
                executor);
    }

    /** The Put access request for ForStDB. */
    static class PutRequest<K, V> {
        final K key;
        @Nullable final V value;
        final ForStInnerTable<K, V> table;

        private PutRequest(K key, V value, ForStInnerTable<K, V> table) {
            this.key = key;
            this.value = value;
            this.table = table;
        }

        /**
         * If the value of the PutRequest is null, then the request will signify the deletion of the
         * data associated with that key.
         */
        static <K, V> PutRequest<K, V> of(K key, @Nullable V value, ForStInnerTable<K, V> table) {
            return new PutRequest<>(key, value, table);
        }
    }
}
