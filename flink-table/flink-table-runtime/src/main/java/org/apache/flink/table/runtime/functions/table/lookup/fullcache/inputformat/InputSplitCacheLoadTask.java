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
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.keyselector.GenericRowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.RowKind;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Parallel task that loads data into the cache from {@link InputFormat} with specified {@link
 * InputSplit}.
 */
public class InputSplitCacheLoadTask implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(InputSplitCacheLoadTask.class);

    private final ConcurrentHashMap<RowData, Collection<RowData>> cache;
    private final GenericRowDataKeySelector keySelector;
    private final RowDataSerializer cacheEntriesSerializer;
    private final InputFormat<RowData, InputSplit> inputFormat;
    private final InputSplit inputSplit;

    public InputSplitCacheLoadTask(
            ConcurrentHashMap<RowData, Collection<RowData>> cache,
            GenericRowDataKeySelector keySelector,
            RowDataSerializer cacheEntriesSerializer,
            InputFormat<RowData, InputSplit> inputFormat,
            InputSplit inputSplit) {
        this.cache = cache;
        this.keySelector = keySelector;
        this.inputFormat = inputFormat;
        this.cacheEntriesSerializer = cacheEntriesSerializer;
        this.inputSplit = inputSplit;
        keySelector.open();
    }

    @Override
    public void run() {
        try {
            if (inputFormat instanceof RichInputFormat) {
                ((RichInputFormat<?, ?>) inputFormat).openInputFormat();
            }
            inputFormat.open(inputSplit);
            RowData nextElement = new BinaryRowData(cacheEntriesSerializer.getArity());
            while (!inputFormat.reachedEnd() && !Thread.currentThread().isInterrupted()) {
                nextElement = inputFormat.nextRecord(nextElement);
                if (nextElement != null) {
                    if (nextElement.getRowKind() != RowKind.INSERT) {
                        throw new IllegalStateException(
                                "InputFormat must provide only INSERT records in lookup 'FULL' cache. Received record "
                                        + nextElement);
                    }
                    RowData record = cacheEntriesSerializer.copy(nextElement);
                    RowData key = keySelector.getKey(record);
                    if (hasNoNulls(key)) {
                        Collection<RowData> resultRows =
                                cache.computeIfAbsent(
                                        key,
                                        // collection must be thread-safe and can possibly
                                        // contain duplicates
                                        k -> new ConcurrentLinkedQueue<>());
                        resultRows.add(record);
                    }
                } else {
                    break;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to load data into the lookup 'FULL' cache from InputSplit "
                            + inputSplit,
                    e);
        } finally {
            try {
                inputFormat.close();
                if (inputFormat instanceof RichInputFormat) {
                    ((RichInputFormat<?, ?>) inputFormat).closeInputFormat();
                }
            } catch (IOException e) {
                LOG.error("Failed to close InputFormat.", e);
            }
        }
    }

    private static boolean hasNoNulls(RowData keys) {
        // in SQL comparison 'null = null' returns false
        // IS NOT DISTINCT FROM (which returns true) is not supported in lookup join
        // so to prevent equality of 2 nulls when searching in cache
        // we must not store rows with nulls in order not to violate semantics of '='
        for (int i = 0; i < keys.getArity(); i++) {
            if (keys.isNullAt(i)) {
                return false;
            }
        }
        return true;
    }
}
