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
import org.forstdb.WriteOptions;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/** The writeBatch operation implementation for ForStDB. */
public class ForStWriteBatchOperation implements ForStDBOperation {

    private final RocksDB db;

    private final List<ForStDBPutRequest<?, ?, ?>> batchRequest;

    private final WriteOptions writeOptions;

    private final Executor executor;

    private final Runnable subProcessFinished;

    ForStWriteBatchOperation(
            RocksDB db,
            List<ForStDBPutRequest<?, ?, ?>> batchRequest,
            WriteOptions writeOptions,
            Executor executor) {
        this(db, batchRequest, writeOptions, executor, null);
    }

    ForStWriteBatchOperation(
            RocksDB db,
            List<ForStDBPutRequest<?, ?, ?>> batchRequest,
            WriteOptions writeOptions,
            Executor executor,
            Runnable subProcessFinished) {
        this.db = db;
        this.batchRequest = batchRequest;
        this.writeOptions = writeOptions;
        this.executor = executor;
        this.subProcessFinished = subProcessFinished;
    }

    @Override
    public CompletableFuture<Void> process() {
        return CompletableFuture.runAsync(
                () -> {
                    try (ForStDBWriteBatchWrapper writeBatch =
                            new ForStDBWriteBatchWrapper(db, writeOptions, batchRequest.size())) {
                        for (ForStDBPutRequest<?, ?, ?> request : batchRequest) {
                            request.process(writeBatch, db);
                        }
                        writeBatch.flush();
                        for (ForStDBPutRequest<?, ?, ?> request : batchRequest) {
                            request.completeStateFuture();
                        }
                    } catch (Exception e) {
                        String msg = "Error while write batch data to ForStDB.";
                        for (ForStDBPutRequest<?, ?, ?> request : batchRequest) {
                            // fail every state request in this batch
                            request.completeStateFutureExceptionally(msg, e);
                        }
                        // fail the whole batch operation
                        throw new CompletionException(msg, e);
                    } finally {
                        if (subProcessFinished != null) {
                            subProcessFinished.run();
                        }
                    }
                },
                executor);
    }

    @Override
    public int subProcessCount() {
        return batchRequest.size();
    }
}
