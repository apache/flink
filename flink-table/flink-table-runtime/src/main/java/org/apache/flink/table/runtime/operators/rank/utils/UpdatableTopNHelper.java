package org.apache.flink.table.runtime.operators.rank.utils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.RowRowConverter;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.rank.AbstractTopNFunction;
import org.apache.flink.table.runtime.operators.rank.TopNBuffer;
import org.apache.flink.table.runtime.operators.rank.TopNBufferCacheRemovalListener;
import org.apache.flink.table.runtime.operators.rank.asyncprocessing.AsyncStateUpdatableTopNFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.guava32.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava32.com.google.common.cache.CacheBuilder;

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

public abstract class UpdatableTopNHelper extends AbstractTopNFunction.AbstractTopNHelper {

    private static final Logger LOG =
            LoggerFactory.getLogger(AsyncStateUpdatableTopNFunction.class);

    private final long cacheSize;

    // flag to skip records with non-exist error instead to fail, true by default.
    private final boolean lenient = true;

    // data converter for logging only.
    private transient DataStructureConverter rowConverter;
    private transient DataStructureConverter sortKeyConverter;

    // a buffer stores mapping from sort key to rowKey list
    protected transient TopNBuffer buffer;

    // a HashMap stores mapping from rowKey to record, a heap mirror to dataState
    private transient Map<RowData, RankRow> rowKeyMap;

    // the kvRowKeyMap store mapping from partitionKey to its rowKeyMap.
    private transient Cache<RowData, Tuple2<TopNBuffer, Map<RowData, RankRow>>> kvRowKeyMap;

    private final TypeSerializer<RowData> inputRowSer;
    private final KeySelector<RowData, RowData> rowKeySelector;

    public UpdatableTopNHelper(
            AbstractTopNFunction topNFunction,
            long cacheSize,
            long topNSize,
            KeySelector<RowData, RowData> rowKeySelector,
            TypeSerializer<RowData> inputRowSer,
            DataType dataType,
            ClassLoader userCodeClassLoader) {
        super(topNFunction);

        this.rowConverter = RowRowConverter.create(dataType);
        this.sortKeyConverter =
                RowRowConverter.create(
                        ((RowDataKeySelector) sortKeySelector).getProducedType().getDataType());

        rowConverter.open(userCodeClassLoader);
        sortKeyConverter.open(userCodeClassLoader);

        int lruCacheSize = Math.max(1, (int) (cacheSize / topNSize));
        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
        if (ttlConfig.isEnabled()) {
            cacheBuilder.expireAfterWrite(
                    ttlConfig.getTimeToLive().toMillis(), TimeUnit.MILLISECONDS);
        }
        kvRowKeyMap =
                cacheBuilder
                        .maximumSize(lruCacheSize)
                        .removalListener(
                                new TopNBufferCacheRemovalListener<>(
                                        keyContext, this::flushBufferToState))
                        .build();

        LOG.info("Top{} operator is using LRU caches key-size: {}", topNSize, lruCacheSize);

        this.rowKeySelector = rowKeySelector;
        this.inputRowSer = inputRowSer;
        this.cacheSize = cacheSize;
    }

    public void processElementWithRowNumber(RowData inputRow, Collector<RowData> out)
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
                    if (lenient) {
                        LOG.warn(errorMsg);
                        Tuple2<Integer, Integer> newRankAndInnerRank =
                                rowNumber(sortKey, rowKey, buffer);
                        int newRank = newRankAndInnerRank.f0;
                        // affect rank range: [oldRank, newRank]
                        emitRecordsWithRowNumberIgnoreStateError(
                                inputRow, newRank, oldRow, oldRank, out);
                    } else {
                        throw new RuntimeException(errorMsg);
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

    public void processElementWithoutRowNumber(RowData inputRow, Collector<RowData> out)
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
            if (buffer.getCurrentTopNum() > getRankEnd()) {
                RowData lastRowKey = buffer.removeLast();
                if (lastRowKey != null) {
                    RankRow lastRow = rowKeyMap.remove(lastRowKey);
                    removeDataState(lastRowKey);
                    // always send a delete message
                    collectDelete(out, lastRow.row);
                }
            }
            // new record in the TopN, send INSERT message
            collectInsert(out, inputRow);
        }
    }

    public void flushAllCacheToState() throws Exception {
        for (Map.Entry<RowData, Tuple2<TopNBuffer, Map<RowData, RankRow>>> entry :
                kvRowKeyMap.asMap().entrySet()) {
            RowData partitionKey = entry.getKey();
            flushBufferToState(partitionKey, entry.getValue());
        }
    }

    public void initHeapStates() throws Exception {
        accRequestCount();
        RowData partitionKey = (RowData) keyContext.getCurrentKey();
        Tuple2<TopNBuffer, Map<RowData, RankRow>> tuple2 = kvRowKeyMap.getIfPresent(partitionKey);
        if (tuple2 == null) {
            buffer = new TopNBuffer(sortKeyComparator, LinkedHashSet::new);
            rowKeyMap = new HashMap<>();
            kvRowKeyMap.put(partitionKey, new Tuple2<>(buffer, rowKeyMap));

            // restore sorted map
            Iterator<Map.Entry<RowData, Tuple2<RowData, Integer>>> iter = getDataStateIterator();
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
            accHitCount();
            buffer = tuple2.f0;
            rowKeyMap = tuple2.f1;
        }
    }

    public void registerMetric() {
        registerMetric(cacheSize);
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
        LOG.error(
                "Failed to find the sortKey: {}, rowkey: {} in the buffer. This should never happen",
                sortKey,
                rowKey);
        throw new RuntimeException(
                "Failed to find the sortKey, rowkey in the buffer. This should never happen");
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
                removeDataState(rowKey);
            }
            toDeleteSortKeys.add(entry.getKey());
        }
        for (RowData toDeleteKey : toDeleteSortKeys) {
            buffer.removeAll(toDeleteKey);
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

    protected abstract void flushBufferToState(
            RowData currentKey, Tuple2<TopNBuffer, Map<RowData, RankRow>> bufferEntry)
            throws Exception;

    protected abstract void removeDataState(RowData rowKey) throws Exception;

    protected abstract Iterator<Map.Entry<RowData, Tuple2<RowData, Integer>>> getDataStateIterator()
            throws Exception;

    protected abstract boolean isInRankEnd(long rank);

    protected abstract long getRankEnd();
}
