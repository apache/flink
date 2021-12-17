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

package org.apache.flink.runtime.rpc;

import org.apache.flink.api.common.time.Time;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

/**
 * Extended {@link MainThreadExecutable} interface which allows to run unfenced runnables in the
 * main thread.
 */
public interface FencedMainThreadExecutable extends MainThreadExecutable {

    /**
     * Run the given runnable in the main thread without attaching a fencing token.
     *
     * @param runnable to run in the main thread without validating the fencing token.
     */
    void runAsyncWithoutFencing(Runnable runnable);

    /**
     * Run the given callable in the main thread without attaching a fencing token.
     *
     * @param callable to run in the main thread without validating the fencing token.
     * @param timeout for the operation
     * @param <V> type of the callable result
     * @return Future containing the callable result
     */
    <V> CompletableFuture<V> callAsyncWithoutFencing(Callable<V> callable, Time timeout);
}
