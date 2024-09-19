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

import org.apache.flink.runtime.asyncprocessing.StateExecutor;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestContainer;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;

import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The {@link StateExecutor} implementation which executing batch {@link StateRequest}s for
 * ForStStateBackend.
 */
public class ForStStateExecutor implements StateExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(ForStStateExecutor.class);

    /**
     * The coordinator thread which schedules the execution of multiple batches of stateRequests.
     * The number of coordinator threads is 1 to ensure that multiple batches of stateRequests can
     * be executed sequentially.
     */
    private final ExecutorService coordinatorThread;

    /** The worker thread that actually executes the read {@link StateRequest}s. */
    private final ExecutorService readThreads;

    /** The worker thread that actually executes the write {@link StateRequest}s. */
    private final ExecutorService writeThreads;

    private final RocksDB db;

    private final WriteOptions writeOptions;

    private Throwable executionError;

    public ForStStateExecutor(
            int readIoParallelism, int writeIoParallelism, RocksDB db, WriteOptions writeOptions) {
        Preconditions.checkState(readIoParallelism > 0 || writeIoParallelism > 0);
        this.coordinatorThread =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory("ForSt-StateExecutor-Coordinator"));
        if (readIoParallelism <= 0 || writeIoParallelism <= 0) {
            this.readThreads =
                    Executors.newFixedThreadPool(
                            Math.max(readIoParallelism, writeIoParallelism),
                            new ExecutorThreadFactory("ForSt-StateExecutor-IO"));
            this.writeThreads = null;
        } else {
            this.readThreads =
                    Executors.newFixedThreadPool(
                            readIoParallelism,
                            new ExecutorThreadFactory("ForSt-StateExecutor-read-IO"));

            this.writeThreads =
                    Executors.newFixedThreadPool(
                            writeIoParallelism,
                            new ExecutorThreadFactory("ForSt-StateExecutor-write-IO"));
        }
        this.db = db;
        this.writeOptions = writeOptions;
    }

    @Override
    public CompletableFuture<Void> executeBatchRequests(
            StateRequestContainer stateRequestContainer) {
        checkState();
        Preconditions.checkArgument(stateRequestContainer instanceof ForStStateRequestClassifier);
        ForStStateRequestClassifier stateRequestClassifier =
                (ForStStateRequestClassifier) stateRequestContainer;
        CompletableFuture<Void> resultFuture = new CompletableFuture<>();
        coordinatorThread.execute(
                () -> {
                    long startTime = System.currentTimeMillis();
                    List<CompletableFuture<Void>> futures = new ArrayList<>(3);
                    List<ForStDBPutRequest<?, ?, ?>> putRequests =
                            stateRequestClassifier.pollDbPutRequests();
                    if (!putRequests.isEmpty()) {
                        ForStWriteBatchOperation writeOperations =
                                new ForStWriteBatchOperation(
                                        db,
                                        putRequests,
                                        writeOptions,
                                        writeThreads == null ? readThreads : writeThreads);
                        futures.add(writeOperations.process());
                    }

                    List<ForStDBGetRequest<?, ?, ?, ?>> getRequests =
                            stateRequestClassifier.pollDbGetRequests();
                    if (!getRequests.isEmpty()) {
                        ForStGeneralMultiGetOperation getOperations =
                                new ForStGeneralMultiGetOperation(db, getRequests, readThreads);
                        futures.add(getOperations.process());
                    }

                    List<ForStDBIterRequest<?, ?, ?, ?, ?>> iterRequests =
                            stateRequestClassifier.pollDbIterRequests();
                    if (!iterRequests.isEmpty()) {
                        ForStIterateOperation iterOperations =
                                new ForStIterateOperation(db, iterRequests, readThreads);
                        futures.add(iterOperations.process());
                    }

                    FutureUtils.combineAll(futures)
                            .thenAcceptAsync(
                                    (e) -> {
                                        long duration = System.currentTimeMillis() - startTime;
                                        LOG.debug(
                                                "Complete executing a batch of state requests, putRequest size {}, getRequest size {}, iterRequest size {}, duration {} ms",
                                                putRequests.size(),
                                                getRequests.size(),
                                                iterRequests.size(),
                                                duration);
                                        resultFuture.complete(null);
                                    },
                                    coordinatorThread)
                            .exceptionally(
                                    e -> {
                                        try {
                                            for (ForStDBIterRequest<?, ?, ?, ?, ?> iterRequest :
                                                    iterRequests) {
                                                iterRequest.close();
                                            }
                                        } catch (IOException ioException) {
                                            LOG.error("Close iterRequests fail", ioException);
                                        }
                                        executionError = e;
                                        resultFuture.completeExceptionally(e);
                                        return null;
                                    });
                });
        return resultFuture;
    }

    @Override
    public StateRequestContainer createStateRequestContainer() {
        checkState();
        return new ForStStateRequestClassifier();
    }

    private void checkState() {
        if (executionError != null) {
            throw new IllegalStateException(
                    "previous state request already failed : ", executionError);
        }
    }

    @Override
    public void shutdown() {
        readThreads.shutdown();
        if (writeThreads != null) {
            writeThreads.shutdown();
        }
        coordinatorThread.shutdown();
        LOG.info("Shutting down the ForStStateExecutor.");
    }
}
