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

package org.apache.flink.table.runtime.operators.rank.utils;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.rank.AbstractTopNFunction;
import org.apache.flink.table.runtime.operators.rank.AppendOnlyTopNFunction;
import org.apache.flink.table.runtime.operators.rank.TopNBuffer;
import org.apache.flink.table.runtime.operators.rank.async.AsyncStateAppendOnlyTopNFunction;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.guava33.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava33.com.google.common.cache.CacheBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A helper to help do the logic 'Top-n' for append-only stream in {@link AppendOnlyTopNFunction}
 * and {@link AsyncStateAppendOnlyTopNFunction}.
 */
public abstract class AppendOnlyTopNHelper extends AbstractTopNFunction.AbstractTopNHelper {

    private static final Logger LOG = LoggerFactory.getLogger(AppendOnlyTopNHelper.class);

    // the kvSortedMap stores mapping from partition key to it's buffer
    private final Cache<RowData, TopNBuffer> kvSortedMap;

    private final long topNSize;

    public AppendOnlyTopNHelper(AbstractTopNFunction topNFunction, long cacheSize, long topNSize) {
        super(topNFunction);

        this.topNSize = topNSize;

        int lruCacheSize = Math.max(1, (int) (cacheSize / topNSize));
        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
        if (ttlConfig.isEnabled()) {
            cacheBuilder.expireAfterWrite(
                    ttlConfig.getTimeToLive().toMillis(), TimeUnit.MILLISECONDS);
        }
        kvSortedMap = cacheBuilder.maximumSize(lruCacheSize).build();

        LOG.info("Top{} operator is using LRU caches key-size: {}", topNSize, lruCacheSize);
    }

    public void registerMetric() {
        registerMetric(kvSortedMap.size() * topNSize);
    }

    @Nullable
    public TopNBuffer getTopNBufferFromCache(RowData currentKey) {
        return kvSortedMap.getIfPresent(currentKey);
    }

    public void saveTopNBufferToCache(RowData currentKey, TopNBuffer topNBuffer) {
        kvSortedMap.put(currentKey, topNBuffer);
    }

    /**
     * The without-number-algorithm can't handle topN with offset, so use the with-number-algorithm
     * to handle offset.
     */
    public void processElementWithRowNumber(
            TopNBuffer buffer, RowData sortKey, RowData input, long rankEnd, Collector<RowData> out)
            throws Exception {
        Iterator<Map.Entry<RowData, Collection<RowData>>> iterator = buffer.entrySet().iterator();
        long currentRank = 0L;
        boolean findsSortKey = false;
        RowData currentRow = null;
        while (iterator.hasNext() && isInRankEnd(currentRank, rankEnd)) {
            Map.Entry<RowData, Collection<RowData>> entry = iterator.next();
            Collection<RowData> records = entry.getValue();
            // meet its own sort key
            if (!findsSortKey && entry.getKey().equals(sortKey)) {
                currentRank += records.size();
                currentRow = input;
                findsSortKey = true;
            } else if (findsSortKey) {
                Iterator<RowData> recordsIter = records.iterator();
                while (recordsIter.hasNext() && isInRankEnd(currentRank, rankEnd)) {
                    RowData prevRow = recordsIter.next();
                    collectUpdateBefore(out, prevRow, currentRank, rankEnd);
                    collectUpdateAfter(out, currentRow, currentRank, rankEnd);
                    currentRow = prevRow;
                    currentRank += 1;
                }
            } else {
                currentRank += records.size();
            }
        }
        if (isInRankEnd(currentRank, rankEnd)) {
            // there is no enough elements in Top-N, emit INSERT message for the new record.
            collectInsert(out, currentRow, currentRank, rankEnd);
        }

        // remove the records associated to the sort key which is out of topN
        List<RowData> toDeleteSortKeys = new ArrayList<>();
        while (iterator.hasNext()) {
            Map.Entry<RowData, Collection<RowData>> entry = iterator.next();
            RowData key = entry.getKey();
            removeFromState(key);
            toDeleteSortKeys.add(key);
        }
        for (RowData toDeleteKey : toDeleteSortKeys) {
            buffer.removeAll(toDeleteKey);
        }
    }

    public void processElementWithoutRowNumber(
            TopNBuffer buffer, RowData input, long rankEnd, Collector<RowData> out)
            throws Exception {
        // remove retired element
        if (buffer.getCurrentTopNum() > rankEnd) {
            Map.Entry<RowData, Collection<RowData>> lastEntry = buffer.lastEntry();
            RowData lastKey = lastEntry.getKey();
            Collection<RowData> lastList = lastEntry.getValue();
            RowData lastElement = buffer.lastElement();
            int size = lastList.size();
            // remove last one
            if (size <= 1) {
                buffer.removeAll(lastKey);
                removeFromState(lastKey);
            } else {
                buffer.removeLast();
                // last element has been removed from lastList, we have to copy a new collection
                // for lastList to avoid mutating state values, see CopyOnWriteStateMap,
                // otherwise, the result might be corrupt.
                // don't need to perform a deep copy, because RowData elements will not be updated
                updateState(lastKey, new ArrayList<>(lastList));
            }
            if (size == 0 || input.equals(lastElement)) {
                return;
            } else {
                // lastElement shouldn't be null
                collectDelete(out, lastElement);
            }
        }
        // it first appears in the TopN, send INSERT message
        collectInsert(out, input);
    }

    protected abstract void removeFromState(RowData key) throws Exception;

    protected abstract void updateState(RowData key, List<RowData> value) throws Exception;
}
