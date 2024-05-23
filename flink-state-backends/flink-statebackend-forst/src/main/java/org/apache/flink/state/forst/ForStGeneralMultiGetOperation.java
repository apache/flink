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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The general-purpose multiGet operation implementation for ForStDB, which simulates multiGet by
 * calling the Get API multiple times with multiple threads.
 */
public class ForStGeneralMultiGetOperation implements ForStDBOperation {

    private static final Logger LOG = LoggerFactory.getLogger(ForStGeneralMultiGetOperation.class);

    private final RocksDB db;

    private final List<ForStDBGetRequest<?, ?>> batchRequest;

    private final Executor executor;

    ForStGeneralMultiGetOperation(
            RocksDB db, List<ForStDBGetRequest<?, ?>> batchRequest, Executor executor) {
        this.db = db;
        this.batchRequest = batchRequest;
        this.executor = executor;
    }

    @Override
    public CompletableFuture<Void> process() {
        // TODO: Use MultiGet to optimize this implement

        CompletableFuture<Void> future = new CompletableFuture<>();

        AtomicReference<Exception> error = new AtomicReference<>();
        AtomicInteger counter = new AtomicInteger(batchRequest.size());
        for (int i = 0; i < batchRequest.size(); i++) {
            ForStDBGetRequest<?, ?> request = batchRequest.get(i);
            executor.execute(
                    () -> {
                        try {
                            if (error.get() == null) {
                                byte[] key = request.buildSerializedKey();
                                byte[] value = db.get(request.getColumnFamilyHandle(), key);
                                request.completeStateFuture(value);
                            } else {
                                request.completeStateFutureExceptionally(
                                        "Error already occurred in other state request of the same "
                                                + "group, failed the state request directly",
                                        error.get());
                            }
                        } catch (Exception e) {
                            error.set(e);
                            request.completeStateFutureExceptionally(
                                    "Error when execute ForStDb get operation", e);
                            future.completeExceptionally(e);
                        } finally {
                            if (counter.decrementAndGet() == 0
                                    && !future.isCompletedExceptionally()) {
                                future.complete(null);
                            }
                        }
                    });
        }
        return future;
    }
}
