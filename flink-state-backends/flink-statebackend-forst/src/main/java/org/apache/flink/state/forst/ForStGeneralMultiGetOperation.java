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

import org.forstdb.ColumnFamilyHandle;
import org.forstdb.ReadOptions;
import org.forstdb.RocksDB;

import java.io.IOException;
import java.util.ArrayList;
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

    private final RocksDB db;
    private final List<ForStDBGetRequest<?, ?, ?, ?>> batchRequest;

    List<List<ForStDBGetRequest<?, ?, ?, ?>>> splitRequests;
    List<ForStDBGetRequest<?, ?, ?, ?>> mapCheckRequests;

    private final Executor executor;

    private final Runnable subProcessFinished;

    private final int readIoParallelism;

    ForStGeneralMultiGetOperation(
            RocksDB db, List<ForStDBGetRequest<?, ?, ?, ?>> batchRequest, Executor executor) {
        this(db, batchRequest, executor, 1, null);
    }

    ForStGeneralMultiGetOperation(
            RocksDB db,
            List<ForStDBGetRequest<?, ?, ?, ?>> batchRequest,
            Executor executor,
            int readIoParallelism,
            Runnable subProcessFinished) {
        this.db = db;
        this.batchRequest = batchRequest;
        this.executor = executor;
        this.subProcessFinished = subProcessFinished;
        this.readIoParallelism = readIoParallelism;
        this.splitRequests = new ArrayList<>();
        this.mapCheckRequests = new ArrayList<>();
        classifyAndSplitRequests(splitRequests, mapCheckRequests);
    }

    @Override
    public CompletableFuture<Void> process() {

        CompletableFuture<Void> future = new CompletableFuture<>();
        AtomicReference<Exception> error = new AtomicReference<>();
        AtomicInteger counter = new AtomicInteger(batchRequest.size());

        processOneByOne(mapCheckRequests, error, counter, future);
        for (List<ForStDBGetRequest<?, ?, ?, ?>> getRequests : splitRequests) {
            executor.execute(
                    () -> {
                        try {
                            ReadOptions readOptions = new ReadOptions();
                            readOptions.setReadaheadSize(0);
                            List<byte[]> keys = new ArrayList<>(getRequests.size());
                            List<ColumnFamilyHandle> columnFamilyHandles =
                                    new ArrayList<>(getRequests.size());

                            for (int i = 0; i < getRequests.size(); i++) {
                                ForStDBGetRequest<?, ?, ?, ?> request = getRequests.get(i);
                                try {
                                    if (error.get() == null) {
                                        byte[] key = request.buildSerializedKey();
                                        keys.add(key);
                                        columnFamilyHandles.add(request.getColumnFamilyHandle());
                                    } else {
                                        completeExceptionallyRequest(
                                                request,
                                                "Error already occurred in other state request of the same group, failed the state request directly",
                                                error.get());
                                    }
                                } catch (IOException e) {
                                    error.set(e);
                                    completeExceptionallyRequest(
                                            request,
                                            "Error when execute ForStDb serialized get key",
                                            e);
                                    future.completeExceptionally(e);
                                }
                            }
                            if (error.get() != null) {
                                return;
                            }
                            List<byte[]> values = null;
                            try {
                                values = db.multiGetAsList(readOptions, columnFamilyHandles, keys);
                            } catch (Exception e) {
                                error.set(e);
                                future.completeExceptionally(e);
                                for (int i = 0; i < getRequests.size(); i++) {
                                    completeExceptionallyRequest(
                                            getRequests.get(i), "Error occurred when multiGet", e);
                                }
                            }
                            if (error.get() != null) {
                                return;
                            }
                            for (int i = 0; i < getRequests.size(); i++) {
                                ForStDBGetRequest<?, ?, ?, ?> request = getRequests.get(i);
                                try {
                                    if (error.get() == null) {
                                        request.completeStateFuture(values.get(i));
                                    } else {
                                        completeExceptionallyRequest(
                                                request,
                                                "Error already occurred in other state request of the same "
                                                        + "group, failed the state request directly",
                                                error.get());
                                    }
                                } catch (Exception e) {
                                    error.set(e);
                                    completeExceptionallyRequest(
                                            request, "Error when complete get future.", e);
                                    future.completeExceptionally(e);
                                }
                            }

                            if (counter.addAndGet(-getRequests.size()) == 0
                                    && !future.isCompletedExceptionally()) {
                                future.complete(null);
                            }

                        } finally {
                            if (subProcessFinished != null) {
                                subProcessFinished.run();
                            }
                        }
                    });
        }
        return future;
    }

    private void completeExceptionallyRequest(
            ForStDBGetRequest<?, ?, ?, ?> request, String message, Exception e) {
        request.completeStateFutureExceptionally(message, e);
    }

    private void classifyAndSplitRequests(
            List<List<ForStDBGetRequest<?, ?, ?, ?>>> splitRequests,
            List<ForStDBGetRequest<?, ?, ?, ?>> mapCheckRequests) {
        List<ForStDBGetRequest<?, ?, ?, ?>> getRequests = new ArrayList<>();
        for (int i = 0; i < batchRequest.size(); i++) {
            ForStDBGetRequest<?, ?, ?, ?> request = batchRequest.get(i);
            if (request instanceof ForStDBMapCheckRequest) {
                mapCheckRequests.add(request);
            } else {
                getRequests.add(request);
            }
        }

        for (int p = 0; p < readIoParallelism; p++) {
            int startIndex = getRequests.size() * p / readIoParallelism;
            int endIndex = getRequests.size() * (p + 1) / readIoParallelism;
            if (startIndex < endIndex) {
                splitRequests.add(new ArrayList<>());
            }
            for (int i = startIndex; i < endIndex; i++) {
                splitRequests.get(splitRequests.size() - 1).add(getRequests.get(i));
            }
        }
    }

    private void processOneByOne(
            List<ForStDBGetRequest<?, ?, ?, ?>> requests,
            AtomicReference<Exception> error,
            AtomicInteger counter,
            CompletableFuture<Void> future) {
        for (int i = 0; i < requests.size(); i++) {
            ForStDBGetRequest<?, ?, ?, ?> request = requests.get(i);
            executor.execute(
                    () -> {
                        try {
                            if (error.get() == null) {
                                request.process(db);
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
                                    && !future.isCompletedExceptionally()
                                    && !future.isDone()) {
                                future.complete(null);
                            }
                            if (subProcessFinished != null) {
                                subProcessFinished.run();
                            }
                        }
                    });
        }
    }

    @Override
    public int subProcessCount() {
        return mapCheckRequests.size() + splitRequests.size();
    }
}
