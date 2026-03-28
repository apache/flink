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

package org.apache.flink.fs.s3native;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.TimeUnit.SECONDS;

/** Cache for S3 client providers per bucket config. Thread-safe. */
@Internal
class S3ClientProviderCache {

    private static final Logger LOG = LoggerFactory.getLogger(S3ClientProviderCache.class);

    private static final long CLIENT_CACHE_CLOSE_TIMEOUT_SECONDS = 30;

    private final Map<String, S3ClientProvider> clientCache = new HashMap<>();
    private final Object cacheLock = new Object();
    private boolean closed = false;

    /**
     * Returns cached or newly created provider for bucket; falls back to defaultProvider if no
     * bucket config.
     */
    public S3ClientProvider getOrCreateProvider(
            String bucketName,
            @Nullable S3BucketConfig bucketConfig,
            S3ClientProvider defaultProvider,
            S3ClientProviderFactory providerFactory) {

        if (bucketConfig == null) {
            return defaultProvider;
        }

        synchronized (cacheLock) {
            if (closed) {
                throw new IllegalStateException("S3ClientProviderCache is closed");
            }

            return clientCache.computeIfAbsent(
                    bucketName,
                    k -> {
                        LOG.debug("Creating S3ClientProvider for bucket: {}", bucketName);
                        return providerFactory.createProvider(bucketConfig);
                    });
        }
    }

    /** Closes all cached providers. */
    public CompletableFuture<Void> closeAsync() {
        CompletableFuture<Void>[] futures;
        synchronized (cacheLock) {
            closed = true;
            futures =
                    clientCache.values().stream()
                            .map(S3ClientProvider::closeAsync)
                            .toArray(CompletableFuture[]::new);
        }

        return FutureUtils.waitForAll(java.util.Arrays.asList(futures))
                .orTimeout(CLIENT_CACHE_CLOSE_TIMEOUT_SECONDS, SECONDS)
                .thenRun(() -> {})
                .exceptionally(
                        ex -> {
                            LOG.error(
                                    "S3ClientProviderCache close timed out after {} seconds",
                                    CLIENT_CACHE_CLOSE_TIMEOUT_SECONDS,
                                    ex);
                            return null;
                        });
    }

    @FunctionalInterface
    interface S3ClientProviderFactory {
        S3ClientProvider createProvider(S3BucketConfig bucketConfig);
    }
}
