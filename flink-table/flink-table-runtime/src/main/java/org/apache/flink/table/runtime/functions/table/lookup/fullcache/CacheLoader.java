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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.apache.flink.metrics.groups.CacheMetricGroup;
import org.apache.flink.table.connector.source.ScanTableSource.ScanRuntimeProvider;
import org.apache.flink.table.data.RowData;

import org.apache.flink.shaded.guava31.com.google.common.base.Joiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.flink.runtime.metrics.groups.InternalCacheMetricGroup.UNINITIALIZED;

/**
 * Abstract task that loads data in Full cache from source provided by {@link ScanRuntimeProvider}.
 */
public abstract class CacheLoader implements AutoCloseable, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(CacheLoader.class);
    protected static final long TIMEOUT_AFTER_INTERRUPT_MS = 10000;

    protected transient volatile ConcurrentHashMap<RowData, Collection<RowData>> cache;

    // 2 reloads can't be executed simultaneously, so they are performed under lock
    private final ReentrantLock reloadLock = new ReentrantLock();
    // runtime waits for the first load to complete to start an execution lookup join
    private transient CountDownLatch firstLoadLatch;
    private transient ExecutorService reloadExecutor;

    // Cache metrics
    private transient Counter loadCounter;
    private transient Counter loadFailuresCounter;
    private volatile long latestLoadTimeMs = UNINITIALIZED;

    protected volatile boolean isStopped;

    /** @return whether reload was successful and was not interrupted. */
    protected abstract boolean updateCache() throws Exception;

    public void open(Configuration parameters, ClassLoader userCodeClassLoader) throws Exception {
        firstLoadLatch = new CountDownLatch(1);
        reloadExecutor =
                Executors.newSingleThreadExecutor(
                        r -> {
                            Thread thread = Executors.defaultThreadFactory().newThread(r);
                            thread.setName("full-cache-loader-executor");
                            thread.setContextClassLoader(userCodeClassLoader);
                            return thread;
                        });
    }

    public void initializeMetrics(CacheMetricGroup cacheMetricGroup) {
        if (loadCounter == null) {
            loadCounter = new ThreadSafeSimpleCounter();
        }
        if (loadFailuresCounter == null) {
            loadFailuresCounter = new ThreadSafeSimpleCounter();
        }
        if (cache == null) {
            cache = new ConcurrentHashMap<>();
        }
        // Register metrics
        cacheMetricGroup.loadCounter(loadCounter);
        cacheMetricGroup.numLoadFailuresCounter(loadFailuresCounter);
        cacheMetricGroup.numCachedRecordsGauge(() -> (long) cache.size());
        cacheMetricGroup.latestLoadTimeGauge(() -> latestLoadTimeMs);
        // TODO support metric numCachedBytesGauge
    }

    public ConcurrentHashMap<RowData, Collection<RowData>> getCache() {
        return cache;
    }

    public void awaitFirstLoad() throws InterruptedException {
        firstLoadLatch.await();
    }

    public CompletableFuture<Void> reloadAsync() {
        return CompletableFuture.runAsync(this::reload, reloadExecutor);
    }

    private void reload() {
        if (isStopped) {
            return;
        }
        // 2 reloads can't be executed simultaneously
        reloadLock.lock();
        try {
            LOG.info("Lookup 'FULL' cache loading triggered.");
            long start = System.currentTimeMillis();
            boolean success = updateCache();
            latestLoadTimeMs = System.currentTimeMillis() - start;
            if (success) {
                loadCounter.inc();
                LOG.info(
                        "Lookup 'FULL' cache loading finished. Time elapsed - {} ms. Number of records - {}.",
                        latestLoadTimeMs,
                        cache.size());
            } else {
                LOG.info("Active lookup 'FULL' cache reload has been interrupted.");
            }
            if (LOG.isDebugEnabled()) {
                // 'if' guard statement prevents us from transforming cache to string
                LOG.debug(
                        "Cache content: \n{\n\t{}\n}",
                        Joiner.on(",\n\t").withKeyValueSeparator(" = ").join(cache));
            }
        } catch (Exception e) {
            loadFailuresCounter.inc();
            isStopped = true;
            throw new RuntimeException("Failed to reload lookup 'FULL' cache.", e);
        } finally {
            reloadLock.unlock();
            firstLoadLatch.countDown();
        }
    }

    @Override
    public void close() throws Exception {
        isStopped = true;
        if (reloadExecutor != null) {
            reloadExecutor.shutdownNow(); // interrupt active reload
            if (!reloadExecutor.awaitTermination(
                    TIMEOUT_AFTER_INTERRUPT_MS, TimeUnit.MILLISECONDS)) {
                throw new TimeoutException(
                        "Lookup 'FULL' cache reload thread was not terminated after closing in timeout of "
                                + TIMEOUT_AFTER_INTERRUPT_MS
                                + " ms.");
            }
        }
        if (cache != null) {
            cache.clear();
        }
    }
}
