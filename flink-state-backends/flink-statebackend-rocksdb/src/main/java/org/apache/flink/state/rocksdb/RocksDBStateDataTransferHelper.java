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

package org.apache.flink.state.rocksdb;

import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.concurrent.Executors.newDirectExecutorService;

/** Data transfer helper for {@link RocksDBKeyedStateBackend}. */
public final class RocksDBStateDataTransferHelper implements Closeable {

    private final ExecutorService executorService;
    private final Closeable closeable;

    public static RocksDBStateDataTransferHelper forThreadNum(int threadNum) {
        ExecutorService executorService = getExecutorService(threadNum);
        return new RocksDBStateDataTransferHelper(executorService, executorService::shutdownNow);
    }

    public static RocksDBStateDataTransferHelper forExecutor(ExecutorService executorService) {
        return new RocksDBStateDataTransferHelper(executorService, () -> {});
    }

    public static RocksDBStateDataTransferHelper forThreadNumIfSpecified(
            int threadNum, ExecutorService executorService) {
        if (threadNum >= 0) {
            return forThreadNum(threadNum);
        } else {
            return forExecutor(executorService);
        }
    }

    private static ExecutorService getExecutorService(int threadNum) {
        if (threadNum > 1) {
            return Executors.newFixedThreadPool(
                    threadNum, new ExecutorThreadFactory("Flink-RocksDBStateDataTransferHelper"));
        } else {
            return newDirectExecutorService();
        }
    }

    RocksDBStateDataTransferHelper(ExecutorService executorService, Closeable closeable) {
        this.executorService = checkNotNull(executorService);
        this.closeable = checkNotNull(closeable);
    }

    @Override
    public void close() throws IOException {
        closeable.close();
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }
}
