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

package org.apache.flink.table.runtime.functions.table.lookup.fullcache;

import org.apache.flink.table.connector.source.lookup.cache.trigger.CacheReloadTrigger;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** Runtime implementation of {@link CacheReloadTrigger.Context}. */
public class ReloadTriggerContext implements CacheReloadTrigger.Context {

    private final Supplier<CompletableFuture<Void>> cacheLoader;
    private final Consumer<Throwable> reloadFailCallback;

    public ReloadTriggerContext(
            Supplier<CompletableFuture<Void>> cacheLoader, Consumer<Throwable> reloadFailCallback) {
        this.cacheLoader = cacheLoader;
        this.reloadFailCallback = reloadFailCallback;
    }

    @Override
    public long currentProcessingTime() {
        // TODO add processingTime into FunctionContext
        return System.currentTimeMillis();
    }

    @Override
    public long currentWatermark() {
        // TODO add watermarks into FunctionContext
        throw new UnsupportedOperationException(
                "Watermarks are currently unsupported in cache reload triggers.");
    }

    @Override
    public CompletableFuture<Void> triggerReload() {
        return cacheLoader
                .get()
                .exceptionally(
                        th -> {
                            reloadFailCallback.accept(th);
                            return null;
                        });
    }
}
