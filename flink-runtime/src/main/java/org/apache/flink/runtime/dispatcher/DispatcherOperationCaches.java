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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.rest.handler.async.CompletedOperationCache;
import org.apache.flink.runtime.rest.handler.job.AsynchronousJobOperationKey;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Encapsulates caches for results of asynchronous operations triggered by the {@link Dispatcher}.
 */
public class DispatcherOperationCaches {
    private final CompletedOperationCache<AsynchronousJobOperationKey, String>
            savepointTriggerCache;

    @VisibleForTesting
    public DispatcherOperationCaches() {
        this(RestOptions.ASYNC_OPERATION_STORE_DURATION.defaultValue());
    }

    @VisibleForTesting
    public DispatcherOperationCaches(Duration cacheDuration) {
        savepointTriggerCache = new CompletedOperationCache<>(cacheDuration);
    }

    public CompletedOperationCache<AsynchronousJobOperationKey, String> getSavepointTriggerCache() {
        return savepointTriggerCache;
    }

    public CompletableFuture<Void> shutdownCaches() {
        return savepointTriggerCache.closeAsync();
    }
}
