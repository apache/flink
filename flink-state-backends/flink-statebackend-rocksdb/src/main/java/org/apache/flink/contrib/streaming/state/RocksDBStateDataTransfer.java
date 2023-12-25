/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FixedRetryStrategy;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import java.io.Closeable;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.util.concurrent.Executors.newDirectExecutorService;

/** Data transfer base class for {@link RocksDBKeyedStateBackend}. */
class RocksDBStateDataTransfer implements Closeable {

    protected final ExecutorService executorService;

    /** ExecutorService to run data transfer operations that can be retried on exceptions. */
    protected final ScheduledExecutorService retryExecutorService;

    private static final int DEFAULT_RETRY_TIMES = 3;
    private static final Duration DEFAULT_RETRY_DELAY = Duration.ofSeconds(1L);

    RocksDBStateDataTransfer(int threadNum) {
        if (threadNum > 1) {
            executorService =
                    Executors.newFixedThreadPool(
                            threadNum, new ExecutorThreadFactory("Flink-RocksDBStateDataTransfer"));
        } else {
            executorService = newDirectExecutorService();
        }

        retryExecutorService =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory("Flink-RocksDBStateDataTransfer-Retry"));
    }

    @Override
    public void close() {
        executorService.shutdownNow();
        retryExecutorService.shutdownNow();
    }

    protected <T> CompletableFuture<T> retry(CompletableFuture<T> dataTransferOperation) {
        return FutureUtils.retryWithDelay(
                () -> dataTransferOperation,
                new FixedRetryStrategy(DEFAULT_RETRY_TIMES, DEFAULT_RETRY_DELAY),
                throwable -> true,
                new ScheduledExecutorServiceAdapter(retryExecutorService));
    }
}
