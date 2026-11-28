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

import org.forstdb.RocksDB;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The iterate operation implementation for ForStDB, which leverages rocksdb's iterator directly.
 */
public class ForStIterateOperation implements ForStDBOperation {

    public static final int CACHE_SIZE_LIMIT = 128;

    private final RocksDB db;

    private final List<ForStDBIterRequest<?, ?, ?, ?, ?>> batchRequest;

    private final Executor executor;

    private final Runnable subProcessFinished;

    ForStIterateOperation(
            RocksDB db, List<ForStDBIterRequest<?, ?, ?, ?, ?>> batchRequest, Executor executor) {
        this(db, batchRequest, executor, null);
    }

    ForStIterateOperation(
            RocksDB db,
            List<ForStDBIterRequest<?, ?, ?, ?, ?>> batchRequest,
            Executor executor,
            Runnable subProcessFinished) {
        this.db = db;
        this.batchRequest = batchRequest;
        this.executor = executor;
        this.subProcessFinished = subProcessFinished;
    }

    @Override
    public CompletableFuture<Void> process() {
        CompletableFuture<Void> future = new CompletableFuture<>();

        AtomicReference<Exception> error = new AtomicReference<>();
        AtomicInteger counter = new AtomicInteger(batchRequest.size());
        for (int i = 0; i < batchRequest.size(); i++) {
            ForStDBIterRequest<?, ?, ?, ?, ?> request = batchRequest.get(i);
            executor.execute(
                    () -> {
                        // todo: config read options
                        try {
                            if (error.get() == null) {
                                request.process(db, CACHE_SIZE_LIMIT);
                            } else {
                                failRequest(request, error.get());
                            }
                        } catch (Exception e) {
                            error.set(e);
                            failRequest(request, e);
                            future.completeExceptionally(e);
                        } finally {
                            if (counter.decrementAndGet() == 0
                                    && !future.isCompletedExceptionally()) {
                                future.complete(null);
                            }
                            if (subProcessFinished != null) {
                                subProcessFinished.run();
                            }
                        }
                    });
        }
        return future;
    }

    /**
     * Closes the request's native iterator on its owning worker thread and completes its state
     * future exceptionally.
     */
    private static void failRequest(ForStDBIterRequest<?, ?, ?, ?, ?> request, Throwable failure) {
        try {
            request.close();
        } catch (Throwable closeFailure) {
            if (closeFailure != failure) {
                failure.addSuppressed(closeFailure);
            }
        }
        request.completeStateFutureExceptionally(
                "Error when execute ForStDb iterate operation", failure);
    }

    @Override
    public int subProcessCount() {
        return batchRequest.size();
    }
}
