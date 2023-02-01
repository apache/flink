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

package org.apache.flink.table.runtime.functions.table.lookup.fullcache.inputformat;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.functions.table.lookup.fullcache.CacheLoader;
import org.apache.flink.table.runtime.keyselector.GenericRowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** {@link CacheLoader} that used {@link InputFormat} for loading data into the cache. */
public class InputFormatCacheLoader extends CacheLoader {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(InputFormatCacheLoader.class);

    private final InputFormat<RowData, InputSplit> initialInputFormat;
    private final GenericRowDataKeySelector keySelector;
    private final RowDataSerializer cacheEntriesSerializer;

    private transient Configuration parameters;

    public InputFormatCacheLoader(
            InputFormat<RowData, ?> initialInputFormat,
            GenericRowDataKeySelector keySelector,
            RowDataSerializer cacheEntriesSerializer) {
        // noinspection unchecked
        this.initialInputFormat = (InputFormat<RowData, InputSplit>) initialInputFormat;
        this.keySelector = keySelector;
        this.cacheEntriesSerializer = cacheEntriesSerializer;
    }

    @Override
    public void open(Configuration parameters, ClassLoader userCodeClassLoader) throws Exception {
        super.open(parameters, userCodeClassLoader);
        this.parameters = parameters;
        this.initialInputFormat.configure(parameters);
    }

    @Override
    protected boolean updateCache() throws Exception {
        InputSplit[] inputSplits = createInputSplits();
        int numSplits = inputSplits.length;
        int concurrencyLevel = getConcurrencyLevel(numSplits);
        // load data into the another copy of cache
        // notice: it requires twice more memory, but on the other hand we don't need any blocking;
        // cache has default initialCapacity and loadFactor, but overridden concurrencyLevel
        ConcurrentHashMap<RowData, Collection<RowData>> newCache =
                new ConcurrentHashMap<>(16, 0.75f, concurrencyLevel);
        Deque<InputSplitCacheLoadTask> cacheLoadTasks =
                Arrays.stream(inputSplits)
                        .map(split -> createCacheLoadTask(split, newCache))
                        .collect(Collectors.toCollection(ArrayDeque::new));
        // run first task and create concurrencyLevel - 1 threads to run remaining tasks
        ExecutorService cacheLoadTaskService = null;
        boolean wasInterrupted = false;
        try {
            InputSplitCacheLoadTask firstTask = cacheLoadTasks.pop();
            CompletableFuture<?> otherTasksFuture = null;
            if (!cacheLoadTasks.isEmpty()) {
                cacheLoadTaskService = Executors.newFixedThreadPool(concurrencyLevel - 1);
                ExecutorService finalExecutor = cacheLoadTaskService;
                CompletableFuture<?>[] futures =
                        cacheLoadTasks.stream()
                                .map(task -> CompletableFuture.runAsync(task, finalExecutor))
                                .toArray(CompletableFuture<?>[]::new);
                otherTasksFuture = CompletableFuture.allOf(futures);
            }
            firstTask.run(); // normally finishes if interrupted and saves interrupted status
            if (otherTasksFuture != null) {
                otherTasksFuture.get(); // if any exception occurs it will be thrown here
            }
        } catch (InterruptedException ignored) { // we use interrupt to close reload threads
            Thread.currentThread().interrupt(); // restore interrupted status
        } finally {
            wasInterrupted = Thread.interrupted();
            if (cacheLoadTaskService != null) {
                // if main cache reload thread encountered an exception,
                // it interrupts underlying InputSplitCacheLoadTasks threads
                cacheLoadTaskService.shutdownNow();
                if (!cacheLoadTaskService.awaitTermination(
                        TIMEOUT_AFTER_INTERRUPT_MS, TimeUnit.MILLISECONDS)) {
                    LOG.error(
                            "ExecutorService with InputSplit cache loading tasks was not terminated in timeout of {} ms.",
                            TIMEOUT_AFTER_INTERRUPT_MS);
                }
            }
        }
        cache = newCache; // reassigning cache field is safe, because it's volatile
        return !wasInterrupted;
    }

    private InputSplitCacheLoadTask createCacheLoadTask(
            InputSplit inputSplit, ConcurrentHashMap<RowData, Collection<RowData>> newCache) {
        try {
            // InputFormat and KeySelector are not thread-safe,
            // so we create copies of them for each task
            InputFormat<RowData, InputSplit> inputFormat =
                    InstantiationUtil.clone(initialInputFormat);
            inputFormat.configure(parameters);
            return new InputSplitCacheLoadTask(
                    newCache, keySelector.copy(), cacheEntriesSerializer, inputFormat, inputSplit);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create InputFormatCacheLoadTask", e);
        }
    }

    private InputSplit[] createInputSplits() throws IOException {
        InputSplit[] inputSplits = this.initialInputFormat.createInputSplits(1);
        if (LOG.isDebugEnabled()) {
            // 'if' guard statement prevents us from transforming splits to string
            LOG.debug(
                    "InputFormat created {} InputSplits: {}",
                    inputSplits.length,
                    Arrays.deepToString(inputSplits));
        }
        Preconditions.checkState(
                inputSplits.length >= 1,
                "InputFormat must provide at least one input split to load data into the lookup 'FULL' cache.");
        return inputSplits;
    }

    private int getConcurrencyLevel(int numSplits) {
        // creating many threads will cause performance issues because of context switching
        int numOfCores = Runtime.getRuntime().availableProcessors();
        return Math.min(numSplits, numOfCores);
    }
}
