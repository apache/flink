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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The general-purpose multiGet operation implementation for ForStDB, which simulates multiGet by
 * calling the Get API multiple times with multiple threads.
 *
 * @param <K> The type of key in get access request.
 * @param <V> The type of value in get access request.
 */
public class ForStGeneralMultiGetOperation<K, V> implements ForStDBOperation<List<V>> {

    private static final Logger LOG = LoggerFactory.getLogger(ForStGeneralMultiGetOperation.class);

    private final RocksDB db;

    private final List<GetRequest<K, V>> batchRequest;

    private final Executor executor;

    ForStGeneralMultiGetOperation(
            RocksDB db, List<GetRequest<K, V>> batchRequest, Executor executor) {
        this.db = db;
        this.batchRequest = batchRequest;
        this.executor = executor;
    }

    @Override
    public CompletableFuture<List<V>> process() {

        CompletableFuture<List<V>> future = new CompletableFuture<>();
        @SuppressWarnings("unchecked")
        V[] result = (V[]) new Object[batchRequest.size()];
        Arrays.fill(result, null);

        AtomicInteger counter = new AtomicInteger(batchRequest.size());
        for (int i = 0; i < batchRequest.size(); i++) {
            GetRequest<K, V> request = batchRequest.get(i);
            final int index = i;
            executor.execute(
                    () -> {
                        try {
                            ForStInnerTable<K, V> table = request.table;
                            byte[] key = table.serializeKey(request.key);
                            byte[] value = db.get(table.getColumnFamilyHandle(), key);
                            if (value != null) {
                                result[index] = table.deserializeValue(value);
                            }
                        } catch (Exception e) {
                            LOG.warn(
                                    "Error when process general multiGet operation for forStDB", e);
                            future.completeExceptionally(e);
                        } finally {
                            if (counter.decrementAndGet() == 0
                                    && !future.isCompletedExceptionally()) {
                                future.complete(Arrays.asList(result));
                            }
                        }
                    });
        }
        return future;
    }

    /** The Get access request for ForStDB. */
    static class GetRequest<K, V> {
        final K key;
        final ForStInnerTable<K, V> table;

        private GetRequest(K key, ForStInnerTable<K, V> table) {
            this.key = key;
            this.table = table;
        }

        static <K, V> GetRequest<K, V> of(K key, ForStInnerTable<K, V> table) {
            return new GetRequest<>(key, table);
        }
    }
}
