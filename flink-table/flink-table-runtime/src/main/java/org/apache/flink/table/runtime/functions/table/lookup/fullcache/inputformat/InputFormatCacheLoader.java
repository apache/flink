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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/** {@link CacheLoader} that used {@link InputFormat} for loading data into the cache. */
public class InputFormatCacheLoader extends CacheLoader {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(InputFormatCacheLoader.class);

    private final InputFormat<RowData, InputSplit> initialInputFormat;
    private final GenericRowDataKeySelector keySelector;
    private final RowDataSerializer cacheEntriesSerializer;

    private transient volatile List<InputSplitCacheLoadTask> cacheLoadTasks;
    private transient Configuration parameters;

    private volatile boolean isStopped;

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
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.parameters = parameters;
        this.initialInputFormat.configure(parameters);
    }

    @Override
    protected void reloadCache() throws Exception {
        InputSplit[] inputSplits = createInputSplits();
        int numSplits = inputSplits.length;
        // load data into the another copy of cache
        // notice: it requires twice more memory, but on the other hand we don't need any blocking
        // cache has default initialCapacity and loadFactor, but overridden concurrencyLevel
        ConcurrentHashMap<RowData, Collection<RowData>> newCache =
                new ConcurrentHashMap<>(16, 0.75f, getConcurrencyLevel(numSplits));
        this.cacheLoadTasks =
                Arrays.stream(inputSplits)
                        .map(split -> createCacheLoadTask(split, newCache))
                        .collect(Collectors.toList());

        // run first task and start numTasks - 1 threads to run remaining tasks
        ExecutorService cacheLoadTaskService = null;
        List<Future<?>> futures = null;
        if (numSplits > 1) {
            futures = new ArrayList<>();
            int numThreads = getConcurrencyLevel(numSplits) - 1;
            cacheLoadTaskService = Executors.newFixedThreadPool(numThreads);
            for (int i = 1; i < numSplits; i++) {
                Future<?> future = cacheLoadTaskService.submit(cacheLoadTasks.get(i));
                futures.add(future);
            }
        }
        cacheLoadTasks.get(0).run();
        if (cacheLoadTaskService != null) {
            for (Future<?> future : futures) {
                future.get(); // if any exception occurs it will be thrown here
            }
            cacheLoadTaskService.shutdownNow();
        }
        if (!isStopped) {
            // reassigning cache field is safe, because it's volatile
            cache = newCache;
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        isStopped = true;
        if (cacheLoadTasks != null) {
            cacheLoadTasks.forEach(InputSplitCacheLoadTask::stopRunning);
        }
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
