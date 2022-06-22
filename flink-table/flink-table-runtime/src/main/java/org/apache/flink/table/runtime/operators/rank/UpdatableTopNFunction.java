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

package org.apache.flink.table.runtime.operators.rank;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.ErrorHandlingUtil;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.guava30.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava30.com.google.common.cache.RemovalCause;
import org.apache.flink.shaded.guava30.com.google.common.cache.RemovalListener;
import org.apache.flink.shaded.guava30.com.google.common.cache.RemovalNotification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A TopN function could handle updating stream. It is a fast version of {@link
 * RetractableTopNFunction} which only hold top n data in state, and keep sorted map in heap.
 * However, the function only works in some special scenarios: 1. sort field collation is ascending
 * and its mono is decreasing, or sort field collation is descending and its mono is increasing 2.
 * input data has unique keys and unique key must contain partition key 3. input stream could not
 * contain DELETE record or UPDATE_BEFORE record
 */
public class UpdatableTopNFunction extends AbstractTopNFunction implements CheckpointedFunction {

    private static final long serialVersionUID = 6786508184355952781L;

    private static final Logger LOG = LoggerFactory.getLogger(UpdatableTopNFunction.class);

    private final InternalTypeInfo<RowData> rowKeyType;
    private final long cacheSize;
    private final ExecutionConfigOptions.StateStaleErrorHandling stateStaleErrorHandling;

    // a map state stores mapping from row key to record which is in topN
    // in tuple2, f0 is the record row, f1 is the index in the list of the same sort_key
    // the f1 is used to preserve the record order in the same sort_key
    private transient MapState<RowData, Tuple2<RowData, Integer>> dataState;

    // a buffer stores mapping from sort key to rowKey list
    private transient TopNBuffer buffer;

    // a HashMap stores mapping from rowKey to record, a heap mirror to dataState
    private transient Map<RowData, RankRow> rowKeyMap;

    // the kvRowKeyMap store mapping from partitionKey to its rowKeyMap.
    private transient Cache<RowData, Tuple2<TopNBuffer, Map<RowData, RankRow>>> kvRowKeyMap;

    private final TypeSerializer<RowData> inputRowSer;
    private final KeySelector<RowData, RowData> rowKeySelector;

    public UpdatableTopNFunction(
            StateTtlConfig ttlConfig,
            InternalTypeInfo<RowData> inputRowType,
            RowDataKeySelector rowKeySelector,
            GeneratedRecordComparator generatedRecordComparator,
            RowDataKeySelector sortKeySelector,
            RankType rankType,
            RankRange rankRange,
            boolean generateUpdateBefore,
            boolean outputRankNumber,
            long cacheSize,
            ExecutionConfigOptions.StateStaleErrorHandling stateStaleErrorHandling) {
        super(
                ttlConfig,
                inputRowType,
                generatedRecordComparator,
                sortKeySelector,
                rankType,
                rankRange,
                generateUpdateBefore,
                outputRankNumber);
        this.rowKeyType = rowKeySelector.getProducedType();
        this.cacheSize = cacheSize;
        this.inputRowSer = inputRowType.createSerializer(new ExecutionConfig());
        this.rowKeySelector = rowKeySelector;
        this.stateStaleErrorHandling = stateStaleErrorHandling;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        int lruCacheSize = Math.max(1, (int) (cacheSize / getDefaultTopNSize()));
        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
        if (ttlConfig.isEnabled()) {
            cacheBuilder.expireAfterWrite(
                    ttlConfig.getTtl().toMilliseconds(), TimeUnit.MILLISECONDS);
        }
        kvRowKeyMap =
                cacheBuilder
                        .maximumSize(lruCacheSize)
                        .removalListener(new CacheRemovalListener())
                        .build();

        LOG.info(
                "Top{} operator is using LRU caches key-size: {}",
                getDefaultTopNSize(),
                lruCacheSize);

        TupleTypeInfo<Tuple2<RowData, Integer>> valueTypeInfo =
                new TupleTypeInfo<>(inputRowType, Types.INT);
        MapStateDescriptor<RowData, Tuple2<RowData, Integer>> mapStateDescriptor =
                new MapStateDescriptor<>("data-state-with-update", rowKeyType, valueTypeInfo);
        if (ttlConfig.isEnabled()) {
            mapStateDescriptor.enableTimeToLive(ttlConfig);
        }
        dataState = getRuntimeContext().getMapState(mapStateDescriptor);

        // metrics
        registerMetric(cacheSize);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // nothing to do
    }

    @Override
    public void processElement(RowData input, Context context, Collector<RowData> out)
            throws Exception {
        initHeapStates();
        initRankEnd(input);
        if (outputRankNumber || hasOffset()) {
            // the without-number-algorithm can't handle topN with offset,
            // so use the with-number-algorithm to handle offset
            processElementWithRowNumber(input, out);
        } else {
            processElementWithoutRowNumber(input, out);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        for (Map.Entry<RowData, Tuple2<TopNBuffer, Map<RowData, RankRow>>> entry :
                kvRowKeyMap.asMap().entrySet()) {
            RowData partitionKey = entry.getKey();
            Map<RowData, RankRow> currentRowKeyMap = entry.getValue().f1;
            keyContext.setCurrentKey(partitionKey);
            flushBufferToState(currentRowKeyMap);
        }
    }

    private void initHeapStates() throws Exception {
        requestCount += 1;
        RowData partitionKey = (RowData) keyContext.getCurrentKey();
        Tuple2<TopNBuffer, Map<RowData, RankRow>> tuple2 = kvRowKeyMap.getIfPresent(partitionKey);
        if (tuple2 == null) {
            buffer = new TopNBuffer(sortKeyComparator, LinkedHashSet::new);
            rowKeyMap = new HashMap<>();
            kvRowKeyMap.put(partitionKey, new Tuple2<>(buffer, rowKeyMap));

            // restore sorted map
            Iterator<Map.Entry<RowData, Tuple2<RowData, Integer>>> iter = dataState.iterator();
            if (iter != null) {
                // a temp map associate sort key to tuple2<index, record>
                Map<RowData, TreeMap<Integer, RowData>> tempSortedMap = new HashMap<>();
                while (iter.hasNext()) {
                    Map.Entry<RowData, Tuple2<RowData, Integer>> entry = iter.next();
                    RowData rowKey = entry.getKey();
                    Tuple2<RowData, Integer> recordAndInnerRank = entry.getValue();
                    RowData record = recordAndInnerRank.f0;
                    Integer innerRank = recordAndInnerRank.f1;
                    rowKeyMap.put(rowKey, new RankRow(record, innerRank, false));

                    // insert into temp sort map to preserve the record order in the same sort key
                    RowData sortKey = sortKeySelector.getKey(record);
                    TreeMap<Integer, RowData> treeMap =
                            tempSortedMap.computeIfAbsent(sortKey, k -> new TreeMap<>());
                    treeMap.put(innerRank, rowKey);
                }

                // build sorted map from the temp map
                for (Map.Entry<RowData, TreeMap<Integer, RowData>> entry :
                        tempSortedMap.entrySet()) {
                    RowData sortKey = entry.getKey();
                    TreeMap<Integer, RowData> treeMap = entry.getValue();
                    for (Map.Entry<Integer, RowData> treeMapEntry : treeMap.entrySet()) {
                        Integer innerRank = treeMapEntry.getKey();
                        RowData recordRowKey = treeMapEntry.getValue();
                        int size = buffer.put(sortKey, recordRowKey);
                        if (innerRank != size) {
                            LOG.warn(
                                    "Failed to build sorted map from state, this may result in wrong result. "
                                            + "The sort key is {}, partition key is {}, "
                                            + "treeMap is {}. The expected inner rank is {}, but current size is {}.",
                                    sortKey,
                                    partitionKey,
                                    treeMap,
                                    innerRank,
                                    size);
                        }
                    }
                }
            }
        } else {
            hitCount += 1;
            buffer = tuple2.f0;
            rowKeyMap = tuple2.f1;
        }
    }

    private void processElementWithRowNumber(RowData inputRow, Collector<RowData> out)
            throws Exception {
        RowData sortKey = sortKeySelector.getKey(inputRow);
        RowData rowKey = rowKeySelector.getKey(inputRow);
        if (rowKeyMap.containsKey(rowKey)) {
            // it is an updated record which is in the topN, in this scenario,
            // the new sort key must be higher than old sort key, this is guaranteed by rules
            RankRow oldRow = rowKeyMap.get(rowKey);
            RowData oldSortKey = sortKeySelector.getKey(oldRow.row);
            int compare = sortKeyComparator.compare(sortKey, oldSortKey);
            if (compare == 0) {
                // sort key is not changed, so the rank is the same, only output the row
                Tuple2<Integer, Integer> rankAndInnerRank = rowNumber(sortKey, rowKey, buffer);
                int rank = rankAndInnerRank.f0;
                int innerRank = rankAndInnerRank.f1;
                rowKeyMap.put(rowKey, new RankRow(inputRowSer.copy(inputRow), innerRank, true));
                collectUpdateBefore(out, oldRow.row, rank); // retract old record
                collectUpdateAfter(out, inputRow, rank);
                return;
            } else {
                Tuple2<Integer, Integer> oldRankAndInnerRank =
                        rowNumber(oldSortKey, rowKey, buffer);
                int oldRank = oldRankAndInnerRank.f0;
                // remove old sort key
                buffer.remove(oldSortKey, rowKey);
                // add new sort key
                int size = buffer.put(sortKey, rowKey);
                rowKeyMap.put(rowKey, new RankRow(inputRowSer.copy(inputRow), size, true));
                // update inner rank of records under the old sort key
                updateInnerRank(oldSortKey);

                if (compare < 0) {
                    // sortKey is higher than oldSortKey
                    emitRecordsWithRowNumber(sortKey, inputRow, out, oldSortKey, oldRow, oldRank);
                } else {
                    String inputRowStr = rowConverter.toExternal(inputRow).toString();
                    String errorMsg =
                            String.format(
                                    "The input retract record:{%s}'s sort key: %s is lower than old"
                                            + " sort key: %s, this break the monotonicity on sort key field"
                                            + " which is guaranteed by the sql semantic. It's highly "
                                            + "possible upstream stateful operator has shorter state ttl "
                                            + "than the stream records is that cause the staled record "
                                            + "cleared by state ttl.",
                                    inputRowStr,
                                    sortKeyConverter.toExternal(sortKey).toString(),
                                    sortKeyConverter.toExternal(oldSortKey).toString());

                    ErrorHandlingUtil.handleStateStaleError(
                            ttlConfig, stateStaleErrorHandling, errorMsg, LOG);

                    // continue processing if stateStaledErrorHandling is not set to ERROR
                    if (stateStaleErrorHandling
                            != ExecutionConfigOptions.StateStaleErrorHandling.ERROR) {
                        Tuple2<Integer, Integer> newRankAndInnerRank =
                                rowNumber(sortKey, rowKey, buffer);
                        int newRank = newRankAndInnerRank.f0;
                        // affect rank range: [oldRank, newRank]
                        emitRecordsWithRowNumberIgnoreStateError(
                                inputRow, newRank, oldRow, oldRank, out);
                    }
                }
            }
        } else if (checkSortKeyInBufferRange(sortKey, buffer)) {
            // it is a new record but is in the topN, insert sort key into buffer
            int size = buffer.put(sortKey, rowKey);
            rowKeyMap.put(rowKey, new RankRow(inputRowSer.copy(inputRow), size, true));

            // emit records
            emitRecordsWithRowNumber(sortKey, inputRow, out);
        }
    }

    private Tuple2<Integer, Integer> rowNumber(RowData sortKey, RowData rowKey, TopNBuffer buffer) {
        Iterator<Map.Entry<RowData, Collection<RowData>>> iterator = buffer.entrySet().iterator();
        int curRank = 1;
        while (iterator.hasNext()) {
            Map.Entry<RowData, Collection<RowData>> entry = iterator.next();
            RowData curKey = entry.getKey();
            Collection<RowData> rowKeys = entry.getValue();
            if (curKey.equals(sortKey)) {
                Iterator<RowData> rowKeysIter = rowKeys.iterator();
                int innerRank = 1;
                while (rowKeysIter.hasNext()) {
                    if (rowKey.equals(rowKeysIter.next())) {
                        return Tuple2.of(curRank, innerRank);
                    } else {
                        innerRank += 1;
                        curRank += 1;
                    }
                }
            } else {
                curRank += rowKeys.size();
            }
        }
        String errorMsg =
                String.format(
                        "Failed to find the sortKey: %s, rowkey: %s in the buffer. This should never happen",
                        sortKey, rowKey);
        LOG.error(errorMsg);
        throw new RuntimeException(errorMsg);
    }

    private void emitRecordsWithRowNumberIgnoreStateError(
            RowData newRow, int newRank, RankRow oldRow, int oldRank, Collector<RowData> out) {
        Iterator<Map.Entry<RowData, Collection<RowData>>> iterator = buffer.entrySet().iterator();
        int currentRank = 0;
        RowData currentRow = null;

        // emit UB of the old row first
        collectUpdateBefore(out, oldRow.row, oldRank);
        // update all other affected rank rows
        affected:
        while (iterator.hasNext()) {
            Map.Entry<RowData, Collection<RowData>> entry = iterator.next();
            Collection<RowData> rowKeys = entry.getValue();
            Iterator<RowData> rowKeyIter = rowKeys.iterator();
            while (rowKeyIter.hasNext()) {
                RowData rowKey = rowKeyIter.next();
                currentRank += 1;
                currentRow = rowKeyMap.get(rowKey).row;
                if (currentRank == newRank) {
                    break affected;
                }
                if (oldRank <= currentRank) {
                    collectUpdateBefore(out, currentRow, currentRank + 1);
                    collectUpdateAfter(out, currentRow, currentRank);
                }
            }
        }
        // at last emit UA of the new row
        collectUpdateAfter(out, newRow, newRank);
    }

    private void emitRecordsWithRowNumber(RowData sortKey, RowData inputRow, Collector<RowData> out)
            throws Exception {
        emitRecordsWithRowNumber(sortKey, inputRow, out, null, null, -1);
    }

    private void emitRecordsWithRowNumber(
            RowData sortKey,
            RowData inputRow,
            Collector<RowData> out,
            RowData oldSortKey,
            RankRow oldRow,
            int oldRank)
            throws Exception {

        Iterator<Map.Entry<RowData, Collection<RowData>>> iterator = buffer.entrySet().iterator();
        int currentRank = 0;
        RowData currentRow = null;
        // whether we have found the sort key in the buffer
        boolean findsSortKey = false;
        boolean oldRowRetracted = false;

        while (iterator.hasNext() && isInRankEnd(currentRank)) {
            Map.Entry<RowData, Collection<RowData>> entry = iterator.next();
            RowData curSortKey = entry.getKey();
            Collection<RowData> rowKeys = entry.getValue();
            // meet its own sort key
            if (!findsSortKey && curSortKey.equals(sortKey)) {
                currentRank += rowKeys.size();
                currentRow = inputRow;
                findsSortKey = true;
            } else if (findsSortKey) {
                if (oldSortKey == null) {
                    // this is a new row, emit updates for all rows in the topn
                    Iterator<RowData> rowKeyIter = rowKeys.iterator();
                    while (rowKeyIter.hasNext() && isInRankEnd(currentRank)) {
                        RowData rowKey = rowKeyIter.next();
                        RankRow prevRow = rowKeyMap.get(rowKey);
                        collectUpdateBefore(out, prevRow.row, currentRank);
                        if (currentRow == inputRow && oldRow != null && !oldRowRetracted) {
                            // In a same unique key, we should retract oldRow first.
                            collectUpdateBefore(out, oldRow.row, oldRank);
                            oldRowRetracted = true;
                        }
                        collectUpdateAfter(out, currentRow, currentRank);
                        currentRow = prevRow.row;
                        currentRank += 1;
                    }
                } else {
                    int compare = sortKeyComparator.compare(curSortKey, oldSortKey);
                    if (compare <= 0) {
                        // current sort key is higher than old sort key,
                        // the rank of current record is changed, need to update the following rank
                        Iterator<RowData> rowKeyIter = rowKeys.iterator();
                        while (rowKeyIter.hasNext() && currentRank < oldRank) {
                            RowData rowKey = rowKeyIter.next();
                            RankRow prevRow = rowKeyMap.get(rowKey);
                            collectUpdateBefore(out, prevRow.row, currentRank);
                            if (currentRow == inputRow && oldRow != null && !oldRowRetracted) {
                                // In a same unique key, we should retract oldRow first.
                                collectUpdateBefore(out, oldRow.row, oldRank);
                                oldRowRetracted = true;
                            }
                            collectUpdateAfter(out, currentRow, currentRank);
                            currentRow = prevRow.row;
                            currentRank += 1;
                        }
                    } else {
                        // current sort key is smaller than old sort key,
                        // the following rank is not changed, so skip
                        break;
                    }
                }
            } else {
                currentRank += rowKeys.size();
            }
        }
        if (isInRankEnd(currentRank)) {
            if (oldRow == null) {
                // input is a new record, and there is no enough elements in Top-N
                // so emit INSERT message for the new record.
                collectInsert(out, currentRow, currentRank);
            } else {
                // input is an update record, current we reach the old rank position of
                // the old record, so emit UPDATE_BEFORE and UPDATE_AFTER for this rank number
                checkArgument(currentRank == oldRank);
                if (!oldRowRetracted) {
                    collectUpdateBefore(out, oldRow.row, oldRank);
                }
                collectUpdateAfter(out, currentRow, currentRank);
            }
            // this is either a new record within top-n range or an update record,
            // so top-n elements don't overflow, there is no need to remove records out of Top-N
            return;
        }

        // remove the records associated to the sort key which is out of topN
        List<RowData> toDeleteSortKeys = new ArrayList<>();
        while (iterator.hasNext()) {
            Map.Entry<RowData, Collection<RowData>> entry = iterator.next();
            Collection<RowData> rowKeys = entry.getValue();
            for (RowData rowKey : rowKeys) {
                rowKeyMap.remove(rowKey);
                dataState.remove(rowKey);
            }
            toDeleteSortKeys.add(entry.getKey());
        }
        for (RowData toDeleteKey : toDeleteSortKeys) {
            buffer.removeAll(toDeleteKey);
        }
    }

    private void processElementWithoutRowNumber(RowData inputRow, Collector<RowData> out)
            throws Exception {
        RowData sortKey = sortKeySelector.getKey(inputRow);
        RowData rowKey = rowKeySelector.getKey(inputRow);
        if (rowKeyMap.containsKey(rowKey)) {
            // it is an updated record which is in the topN, in this scenario,
            // the new sort key must be higher than old sort key, this is guaranteed by rules
            RankRow oldRow = rowKeyMap.get(rowKey);
            RowData oldSortKey = sortKeySelector.getKey(oldRow.row);
            if (!oldSortKey.equals(sortKey)) {
                // remove old sort key
                buffer.remove(oldSortKey, rowKey);
                // add new sort key
                int size = buffer.put(sortKey, rowKey);
                rowKeyMap.put(rowKey, new RankRow(inputRowSer.copy(inputRow), size, true));
                // update inner rank of records under the old sort key
                updateInnerRank(oldSortKey);
            } else {
                // row content may change, so we need to update row in map
                rowKeyMap.put(
                        rowKey, new RankRow(inputRowSer.copy(inputRow), oldRow.innerRank, true));
            }
            // row content may change, so a UPDATE_BEFORE is needed
            collectUpdateBefore(out, oldRow.row);
            collectUpdateAfter(out, inputRow);
        } else if (checkSortKeyInBufferRange(sortKey, buffer)) {
            // it is an new record but is in the topN, insert sort key into buffer
            int size = buffer.put(sortKey, rowKey);
            rowKeyMap.put(rowKey, new RankRow(inputRowSer.copy(inputRow), size, true));
            // remove retired element
            if (buffer.getCurrentTopNum() > rankEnd) {
                RowData lastRowKey = buffer.removeLast();
                if (lastRowKey != null) {
                    RankRow lastRow = rowKeyMap.remove(lastRowKey);
                    dataState.remove(lastRowKey);
                    // always send a delete message
                    collectDelete(out, lastRow.row);
                }
            }
            // new record in the TopN, send INSERT message
            collectInsert(out, inputRow);
        }
    }

    private void flushBufferToState(Map<RowData, RankRow> curRowKeyMap) throws Exception {
        for (Map.Entry<RowData, RankRow> entry : curRowKeyMap.entrySet()) {
            RowData key = entry.getKey();
            RankRow rankRow = entry.getValue();
            if (rankRow.dirty) {
                // should update state
                dataState.put(key, Tuple2.of(rankRow.row, rankRow.innerRank));
                rankRow.dirty = false;
            }
        }
    }

    private void updateInnerRank(RowData oldSortKey) {
        Collection<RowData> list = buffer.get(oldSortKey);
        if (list != null) {
            Iterator<RowData> iter = list.iterator();
            int innerRank = 1;
            while (iter.hasNext()) {
                RowData rowKey = iter.next();
                RankRow row = rowKeyMap.get(rowKey);
                if (row.innerRank != innerRank) {
                    row.innerRank = innerRank;
                    row.dirty = true;
                }
                innerRank += 1;
            }
        }
    }

    private class CacheRemovalListener
            implements RemovalListener<RowData, Tuple2<TopNBuffer, Map<RowData, RankRow>>> {

        @Override
        public void onRemoval(
                RemovalNotification<RowData, Tuple2<TopNBuffer, Map<RowData, RankRow>>>
                        notification) {
            if (notification.getCause() != RemovalCause.SIZE) {
                // Don't flush values to state if cause is ttl expired
                return;
            }

            RowData partitionKey = notification.getKey();
            Tuple2<TopNBuffer, Map<RowData, RankRow>> value = notification.getValue();
            if (partitionKey == null || value == null) {
                return;
            }

            RowData previousKey = (RowData) keyContext.getCurrentKey();
            keyContext.setCurrentKey(partitionKey);
            try {
                flushBufferToState(value.f1);
            } catch (Throwable e) {
                LOG.error("Fail to synchronize state!", e);
                throw new RuntimeException(e);
            } finally {
                keyContext.setCurrentKey(previousKey);
            }
        }
    }

    private static class RankRow {
        private final RowData row;
        private int innerRank;
        private boolean dirty;

        private RankRow(RowData row, int innerRank, boolean dirty) {
            this.row = row;
            this.innerRank = innerRank;
            this.dirty = dirty;
        }
    }
}
