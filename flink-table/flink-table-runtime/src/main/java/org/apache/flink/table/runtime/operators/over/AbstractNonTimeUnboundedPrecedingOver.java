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

package org.apache.flink.table.runtime.operators.over;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.apache.flink.table.runtime.util.StateConfigUtil.createTtlConfig;

/**
 * The NonTimeRangeUnboundedPrecedingFunction class is a specialized implementation for processing
 * unbounded OVER window aggregations, particularly for non-time-based range queries in Apache
 * Flink. It maintains strict ordering of rows within partitions and handles the full changelog
 * lifecycle (inserts, updates, deletes).
 *
 * <p>Key Components and Assumptions
 *
 * <p>Data Structure Design: (1) Maintains a sorted list of tuples containing sort keys and lists of
 * IDs for each key (2) Each incoming row is assigned a unique Long ID (starting from
 * Long.MIN_VALUE) (3) Uses multiple state types to track rows, sort orders, and aggregations
 *
 * <p>State Management: (1) idState: Counter for generating unique row IDs (2) sortedListState:
 * Ordered list of sort keys with their associated row IDs (3) valueMapState: Maps IDs to their
 * corresponding input rows (4) accMapState: Maps sort keys to their accumulated values
 *
 * <p>Processing Model: (1) For inserts/updates: Adds rows to the appropriate position based on sort
 * key (2) For deletes: Removes rows by matching both sort key and row content (3) Recalculates
 * aggregates for affected rows and emits the appropriate events (4) Skips redundant events when
 * accumulators haven't changed to reduce network traffic
 *
 * <p>Optimization Assumptions: (1) Skip emitting updates when accumulators haven't changed to
 * reduce network traffic (2) Uses state TTL for automatic cleanup of stale data (3) Carefully
 * manages row state to support incremental calculations
 *
 * <p>Retraction Handling: (1) Handles retraction mode (DELETE/UPDATE_BEFORE) events properly (2)
 * Supports the proper processing of changelog streams
 *
 * <p>Limitations
 *
 * <p>Linear search performance: - The current implementation uses a linear search to find the
 * correct position for each sort key. This can be optimized using a binary search for large state
 * sizes.
 *
 * <p>State size and performance: - The implementation maintains multiple state types that could
 * grow large with high cardinality data
 *
 * <p>Linear recalculation: - When processing updates, all subsequent elements need to be
 * recalculated, which could be inefficient for large windows
 */
public abstract class AbstractNonTimeUnboundedPrecedingOver<K>
        extends KeyedProcessFunction<K, RowData, RowData> {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG =
            LoggerFactory.getLogger(AbstractNonTimeUnboundedPrecedingOver.class);

    private final long stateRetentionTime;

    private final GeneratedAggsHandleFunction generatedAggsHandler;
    private final GeneratedRecordEqualiser generatedRecordEqualiser;
    private final GeneratedRecordEqualiser generatedSortKeyEqualiser;
    private final GeneratedRecordComparator generatedSortKeyComparator;

    // The util to compare two rows based on the sort attribute.
    private transient Comparator<RowData> sortKeyComparator;

    final KeySelector<RowData, RowData> sortKeySelector;
    // The record equaliser used to equal RowData.
    transient RecordEqualiser valueEqualiser;
    private transient RecordEqualiser sortKeyEqualiser;

    private final LogicalType[] accTypes;
    private final LogicalType[] inputFieldTypes;
    private final LogicalType[] sortKeyTypes;
    transient JoinedRowData output;

    private final InternalTypeInfo<RowData> accKeyRowTypeInfo;

    // state to hold the Long ID counter
    transient ValueState<Long> idState;
    @VisibleForTesting transient ValueStateDescriptor<Long> idStateDescriptor;

    // state to hold a sorted list each containing a tuple of sort key and list of IDs
    transient ValueState<List<Tuple2<RowData, List<Long>>>> sortedListState;

    @VisibleForTesting
    transient ValueStateDescriptor<List<Tuple2<RowData, List<Long>>>> sortedListStateDescriptor;

    // state to hold ID and its associated input row until state ttl expires
    transient MapState<Long, RowData> valueMapState;
    @VisibleForTesting transient MapStateDescriptor<Long, RowData> valueStateDescriptor;
    // state to hold sortKey and its associated accumulator
    transient MapState<RowData, RowData> accMapState;
    @VisibleForTesting transient MapStateDescriptor<RowData, RowData> accStateDescriptor;

    transient AggsHandleFunction aggFuncs;

    // Metrics
    private static final String IDS_NOT_FOUND_METRIC_NAME = "numOfIdsNotFound";
    transient Counter numOfIdsNotFound;
    private static final String SORT_KEYS_NOT_FOUND_METRIC_NAME = "numOfSortKeysNotFound";
    transient Counter numOfSortKeysNotFound;

    @VisibleForTesting
    protected Counter getNumOfIdsNotFound() {
        return numOfIdsNotFound;
    }

    @VisibleForTesting
    protected Counter getNumOfSortKeysNotFound() {
        return numOfSortKeysNotFound;
    }

    public AbstractNonTimeUnboundedPrecedingOver(
            long stateRetentionTime,
            GeneratedAggsHandleFunction genAggsHandler,
            GeneratedRecordEqualiser genRecordEqualiser,
            GeneratedRecordEqualiser genSortKeyEqualiser,
            GeneratedRecordComparator genSortKeyComparator,
            LogicalType[] accTypes,
            LogicalType[] inputFieldTypes,
            LogicalType[] sortKeyTypes,
            RowDataKeySelector sortKeySelector,
            InternalTypeInfo<RowData> accKeyRowTypeInfo) {
        this.stateRetentionTime = stateRetentionTime;
        this.generatedAggsHandler = genAggsHandler;
        this.generatedRecordEqualiser = genRecordEqualiser;
        this.generatedSortKeyEqualiser = genSortKeyEqualiser;
        this.generatedSortKeyComparator = genSortKeyComparator;
        this.accTypes = accTypes;
        this.inputFieldTypes = inputFieldTypes;
        this.sortKeyTypes = sortKeyTypes;
        this.sortKeySelector = sortKeySelector;
        this.accKeyRowTypeInfo = accKeyRowTypeInfo;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        // Initialize agg functions
        aggFuncs = generatedAggsHandler.newInstance(getRuntimeContext().getUserCodeClassLoader());
        aggFuncs.open(new PerKeyStateDataViewStore(getRuntimeContext()));

        // Initialize output record
        output = new JoinedRowData();

        // Initialize value/row equaliser
        valueEqualiser =
                generatedRecordEqualiser.newInstance(getRuntimeContext().getUserCodeClassLoader());

        // Initialize sortKey equaliser
        sortKeyEqualiser =
                generatedSortKeyEqualiser.newInstance(getRuntimeContext().getUserCodeClassLoader());

        // Initialize sort comparator
        sortKeyComparator =
                generatedSortKeyComparator.newInstance(
                        getRuntimeContext().getUserCodeClassLoader());

        StateTtlConfig ttlConfig = createTtlConfig(stateRetentionTime);

        // Initialize state to maintain id counter
        idStateDescriptor = new ValueStateDescriptor<Long>("idState", Long.class);
        if (ttlConfig.isEnabled()) {
            idStateDescriptor.enableTimeToLive(ttlConfig);
        }
        idState = getRuntimeContext().getState(idStateDescriptor);

        // Input elements are all binary rows as they came from network
        InternalTypeInfo<RowData> inputRowTypeInfo = InternalTypeInfo.ofFields(inputFieldTypes);
        InternalTypeInfo<RowData> sortKeyRowTypeInfo = InternalTypeInfo.ofFields(this.sortKeyTypes);

        // Initialize state which maintains a sorted list of tuples(sortKey, List of IDs)
        ListTypeInfo<Long> idListTypeInfo = new ListTypeInfo<Long>(Types.LONG);
        ListTypeInfo<Tuple2<RowData, List<Long>>> listTypeInfo =
                new ListTypeInfo<>(new TupleTypeInfo<>(sortKeyRowTypeInfo, idListTypeInfo));
        sortedListStateDescriptor =
                new ValueStateDescriptor<List<Tuple2<RowData, List<Long>>>>(
                        "sortedListState", listTypeInfo);
        if (ttlConfig.isEnabled()) {
            sortedListStateDescriptor.enableTimeToLive(ttlConfig);
        }
        sortedListState = getRuntimeContext().getState(sortedListStateDescriptor);

        // Initialize state which maintains the actual row
        valueStateDescriptor =
                new MapStateDescriptor<Long, RowData>(
                        "valueMapState", Types.LONG, inputRowTypeInfo);
        if (ttlConfig.isEnabled()) {
            valueStateDescriptor.enableTimeToLive(ttlConfig);
        }
        valueMapState = getRuntimeContext().getMapState(valueStateDescriptor);

        // Initialize accumulator state per row
        InternalTypeInfo<RowData> accTypeInfo = InternalTypeInfo.ofFields(accTypes);
        accStateDescriptor =
                new MapStateDescriptor<RowData, RowData>(
                        "accMapState", sortKeyRowTypeInfo, accTypeInfo);
        if (ttlConfig.isEnabled()) {
            accStateDescriptor.enableTimeToLive(ttlConfig);
        }
        accMapState = getRuntimeContext().getMapState(accStateDescriptor);

        // metrics
        this.numOfIdsNotFound =
                getRuntimeContext().getMetricGroup().counter(IDS_NOT_FOUND_METRIC_NAME);
        this.numOfSortKeysNotFound =
                getRuntimeContext().getMetricGroup().counter(SORT_KEYS_NOT_FOUND_METRIC_NAME);
    }

    /**
     * Puts an element from the input stream into state or removes it from state if the input is a
     * retraction. Emits the aggregated value for the newly inserted element and updates all results
     * that are affected by the added or removed row. To comply with the sql RANGE syntax, emits the
     * same aggregated value for all elements with the same sort key. To comply with the sql ROWS
     * syntax, emits different aggregated values for all elements with the same sort key.
     *
     * @param input The input value.
     * @param ctx A {@link Context} that allows querying the timestamp of the element and getting a
     *     TimerService for registering timers and querying the time. The context is only valid
     *     during the invocation of this method, do not store it.
     * @param out The collector for returning result values.
     * @throws Exception
     */
    @Override
    public void processElement(
            RowData input,
            KeyedProcessFunction<K, RowData, RowData>.Context ctx,
            Collector<RowData> out)
            throws Exception {
        RowKind rowKind = input.getRowKind();

        switch (rowKind) {
            case INSERT:
            case UPDATE_AFTER:
                insertIntoSortedList(input, out);
                break;

            case DELETE:
            case UPDATE_BEFORE:
                removeFromSortedList(input, out);
                break;
        }

        // Reset acc state since we can have out of order inserts in the ordered list
        aggFuncs.resetAccumulators();
        aggFuncs.cleanup();
    }

    /**
     * Adds a new element(insRow) to a sortedList. The sortedList contains a list of Tuple(SortKey,
     * List[Long Ids]>). Extracts the inputSortKey from the insRow and compares it with every
     * element in the sortedList. If a sortKey already exists in the sortedList for the input, add
     * the id to the list of ids and update the sortedList, otherwise find the right position in the
     * sortedList and add a new entry in the sortedList. After the insRow is successfully inserted,
     * an INSERT/UPDATE_AFTER event is emitted for the newly inserted element, and for all
     * subsequent elements an UPDATE_BEFORE and UPDATE_AFTER event is emitted based on the previous
     * and newly aggregated values. Some updates are skipped if the previously accumulated value is
     * the same as the newly accumulated value to save on network bandwidth and downstream
     * processing including writing the result to the sink system. Implementation differs for rows
     * vs range.
     *
     * @param insRow The input value.
     * @param out The collector for returning result values.
     * @throws Exception
     */
    abstract void insertIntoSortedList(RowData insRow, Collector<RowData> out) throws Exception;

    /**
     * Process all sort keys starting from startPos location. Calculates the new agg value for all
     * ids belonging to the same sort key. Finally, emits the old and new aggregated values for the
     * ids if the newly aggregated value is different from the previously aggregated values.
     * Implementation differs for rows vs range.
     *
     * @param sortedList
     * @param startPos
     * @param currAcc
     * @param out
     * @throws Exception
     */
    abstract void processRemainingElements(
            List<Tuple2<RowData, List<Long>>> sortedList,
            int startPos,
            RowData currAcc,
            Collector<RowData> out)
            throws Exception;

    /**
     * Helper method to re-accumulate the aggregated value for all ids after an id was inserted to
     * the end of the ids list. Implementation differs for rows vs range.
     *
     * @param currAcc
     * @param ids
     * @param insRow
     * @throws Exception
     */
    abstract void reAccumulateIdsAfterInsert(RowData currAcc, List<Long> ids, RowData insRow)
            throws Exception;

    /**
     * @return the next id after reading from the idState
     * @throws IOException
     */
    Long getNextId() throws IOException {
        Long id = idState.value();
        if (id == null) {
            id = Long.MIN_VALUE;
        }
        return id;
    }

    /**
     * @return the sortedList containing sortKeys and Ids with the same sortKey
     * @throws IOException
     */
    List<Tuple2<RowData, List<Long>>> getSortedList() throws IOException {
        List<Tuple2<RowData, List<Long>>> sortedList = sortedListState.value();
        if (sortedList == null) {
            sortedList = new ArrayList<>();
        }
        return sortedList;
    }

    /**
     * Returns the position of the index where the inputRow must be inserted, updated or deleted.
     * For insertion, if a suitable position is not found, return -1 to be inserted at the end of
     * the list. For deletion, if the sortKey is not found, return -1 to indicate the sortKey was
     * not found in the list.
     *
     * @param sortedList
     * @param inputSortKey
     * @param isEquals
     * @return Tuple2(Integer, Boolean) where Integer is the index position in the sortedList and
     *     Boolean indicates if the element should be inserted or updated
     */
    Tuple2<Integer, Boolean> findIndexOfSortKey(
            List<Tuple2<RowData, List<Long>>> sortedList, RowData inputSortKey, boolean isEquals) {
        // TODO: Optimize by using Binary Search
        for (int i = 0; i < sortedList.size(); i++) {
            RowData curSortKey = sortedList.get(i).f0;
            if (isEquals && sortKeyEqualiser.equals(curSortKey, inputSortKey)) {
                return new Tuple2<>(i, true);
            } else {
                int compareResult = sortKeyComparator.compare(curSortKey, inputSortKey);
                if (compareResult == 0) {
                    // Found inputSortKey
                    return new Tuple2<>(i, false);
                } else if (compareResult > 0) {
                    // Found curSortKey which is greater than inputSortKey
                    return new Tuple2<>(i, true);
                }
            }
        }
        // Return not found
        return new Tuple2<>(-1, true);
    }

    /**
     * Helper method to set accumulator and get the value.
     *
     * @param accumulator
     * @return
     * @throws Exception
     */
    RowData setAccumulatorAndGetValue(RowData accumulator) throws Exception {
        aggFuncs.setAccumulators(accumulator);
        return aggFuncs.getValue();
    }

    /**
     * Helper method to set the accumulator based on the prevIndex.
     *
     * @param sortedList
     * @param prevIndex
     * @throws Exception
     */
    void setAccumulatorOfPrevRow(List<Tuple2<RowData, List<Long>>> sortedList, int prevIndex)
            throws Exception {
        if (prevIndex < 0) {
            aggFuncs.createAccumulators();
        } else {
            RowData prevAcc = accMapState.get(sortedList.get(prevIndex).f0);
            if (prevAcc == null) {
                aggFuncs.createAccumulators();
            } else {
                aggFuncs.setAccumulators(prevAcc);
            }
        }
    }

    /**
     * Helper method to set the accumulator based on the prevIndex and the ids index j.
     *
     * @param sortedList
     * @param prevIndex
     * @param idIndex
     * @throws Exception
     */
    void setAccumulatorOfPrevId(
            List<Tuple2<RowData, List<Long>>> sortedList, int prevIndex, int idIndex)
            throws Exception {
        if (prevIndex < 0) {
            aggFuncs.createAccumulators();
        } else {
            int jIndex = idIndex == -1 ? sortedList.get(prevIndex).f1.size() - 1 : idIndex;
            RowData prevAcc =
                    accMapState.get(GenericRowData.of(sortedList.get(prevIndex).f1.get(jIndex)));
            if (prevAcc == null) {
                aggFuncs.createAccumulators();
            } else {
                aggFuncs.setAccumulators(prevAcc);
            }
        }
    }

    /**
     * Helper method to send updates for ids.
     *
     * @param ids
     * @param idxOfChangedRow
     * @param out
     * @param rowKind
     * @param changedRow
     * @param prevAggValue
     * @param currAggValue
     */
    abstract void sendUpdatesForIds(
            List<Long> ids,
            int idxOfChangedRow,
            Collector<RowData> out,
            RowKind rowKind,
            RowData changedRow,
            RowData prevAggValue,
            RowData currAggValue)
            throws Exception;

    /**
     * Emits updates for all ids. The id belonging to idxOfChangedRow location either emits a DELETE
     * or INSERT/UPDATE_AFTER, depending on whether the id is being added or removed from ids. All
     * other ids except the id at position idxOfChangedRow emit old and new aggregated values in the
     * form of UPDATE_BEFORE and UPDATE_AFTER respectively.
     *
     * @param ids
     * @param idxOfChangedRow
     * @param prevAcc
     * @param currAcc
     * @param rowKind
     * @param changedRow
     * @param out
     * @throws Exception
     */
    void emitUpdatesForIds(
            List<Long> ids,
            int idxOfChangedRow,
            RowData prevAcc,
            RowData currAcc,
            RowKind rowKind,
            RowData changedRow,
            Collector<RowData> out)
            throws Exception {
        RowData prevAggValue = setAccumulatorAndGetValue(prevAcc);
        RowData currAggValue = setAccumulatorAndGetValue(currAcc);

        if (prevAcc.equals(currAcc)) {
            // Only send update for changed row i.e. either INSERT or DELETE
            sendUpdateForChangedRow(out, rowKind, changedRow, prevAggValue, currAggValue);
            // Previous accumulator is the same as the current accumulator
            // Skip sending downstream updates in such cases to reduce network traffic
            LOG.debug("Prev accumulator is same as curr accumulator. Skipping further updates.");
            return;
        }

        sendUpdatesForIds(
                ids, idxOfChangedRow, out, rowKind, changedRow, prevAggValue, currAggValue);
    }

    /**
     * Helper method to send update for the changed row. If the rowKind is DELETE, send DELETE. If
     * the rowKind is INSERT, send INSERT or UPDATE_AFTER.
     *
     * @param out
     * @param rowKind
     * @param changedRow
     * @param prevAggValue
     * @param currAggValue
     */
    void sendUpdateForChangedRow(
            Collector<RowData> out,
            RowKind rowKind,
            RowData changedRow,
            RowData prevAggValue,
            RowData currAggValue) {
        if (rowKind == RowKind.DELETE) {
            collectDelete(out, changedRow, prevAggValue);
        } else {
            // For insert, emit either Insert or UpdateAfter
            collectInsertOrUpdateAfter(out, changedRow, rowKind, currAggValue);
        }
    }

    void collectInsertOrUpdateAfter(
            Collector<RowData> out, RowData value, RowKind rowKind, RowData currAggValue) {
        output.setRowKind(rowKind);
        output.replace(value, currAggValue);
        out.collect(output);
    }

    void collectUpdateBefore(Collector<RowData> out, RowData rowValue, RowData prevAggValue) {
        output.setRowKind(RowKind.UPDATE_BEFORE);
        output.replace(rowValue, prevAggValue);
        out.collect(output);
    }

    void collectUpdateAfter(Collector<RowData> out, RowData rowValue, RowData currAggValue) {
        output.setRowKind(RowKind.UPDATE_AFTER);
        output.replace(rowValue, currAggValue);
        out.collect(output);
    }

    void collectDelete(Collector<RowData> out, RowData rowValue, RowData prevAggValue) {
        output.setRowKind(RowKind.DELETE);
        output.replace(rowValue, prevAggValue);
        out.collect(output);
    }

    /**
     * Removes the row matching delRow from the sortedList. Matching is done based on the sortKeys.
     * Once matched, the actual row is compared for all ids belonging to the same sortKey. After the
     * element is removed, a DELETE event is emitted for the removed element, and for all subsequent
     * elements an UPDATE_BEFORE and UPDATE_AFTER event is emitted based on the previous and newly
     * aggregated values. Updates are skipped if the previously accumulated value is the same as the
     * newly accumulated value to save on network bandwidth. Implementation differs for rows vs
     * range.
     *
     * @param delRow
     * @param out
     * @throws Exception
     */
    abstract void removeFromSortedList(RowData delRow, Collector<RowData> out) throws Exception;

    /**
     * Helper method to re-accumulate the aggregated value for all ids without the id that will be
     * removed.
     *
     * @param ids
     * @param delRow
     * @return the index position of the id that should be removed
     * @throws Exception
     */
    int reAccumulateIdsAndGetRemoveIndex(List<Long> ids, RowData delRow) throws Exception {
        int removeIndex = -1;
        for (int j = 0; j < ids.size(); j++) {
            RowData curValue = valueMapState.get(ids.get(j));
            if (valueEqualiser.equals(curValue, delRow)) {
                removeIndex = j;
            } else {
                aggFuncs.accumulate(curValue);
            }
        }
        return removeIndex;
    }

    /**
     * Remove the ith element from the sortedList if the list of ids is empty and return the ith
     * location OR update the sorted list with the list of ids and return the next location of the
     * sortedList.
     *
     * @param sortedList
     * @param idx
     * @param ids
     * @param curSortKey
     * @return
     */
    int removeIdFromSortedList(
            List<Tuple2<RowData, List<Long>>> sortedList,
            int idx,
            List<Long> ids,
            RowData curSortKey) {
        if (ids.isEmpty()) {
            sortedList.remove(idx);
        } else {
            // Update sortedList without the removed id
            sortedList.set(idx, new Tuple2<RowData, List<Long>>(curSortKey, ids));
            idx++;
        }
        return idx;
    }

    /**
     * Helper method to find the index of the id to be removed from the list of ids.
     *
     * @param ids
     * @param delRow
     * @return
     * @throws Exception
     */
    int findIndexOfIdToBeRemoved(List<Long> ids, RowData delRow) throws Exception {
        for (int j = 0; j < ids.size(); j++) {
            RowData curValue = valueMapState.get(ids.get(j));
            if (valueEqualiser.equals(curValue, delRow)) {
                return j;
            }
        }
        return -1;
    }

    /**
     * Helper method to get the previous accumulator based on the index of the sortedList and the
     * index of the list of ids.
     *
     * @param sortedList
     * @param i
     * @param j
     * @return
     * @throws Exception
     */
    RowData getPreviousAccumulator(List<Tuple2<RowData, List<Long>>> sortedList, int i, int j)
            throws Exception {
        if (j == 0) {
            if (i - 1 < 0) {
                return null;
            } else {
                Long prevId = sortedList.get(i - 1).f1.get(sortedList.get(i - 1).f1.size() - 1);
                return accMapState.get(GenericRowData.of(prevId));
            }
        } else {
            return accMapState.get(GenericRowData.of(sortedList.get(i).f1.get(j - 1)));
        }
    }

    /**
     * Helper method to re-accumulate the aggregated value for all ids after the id was removed from
     * the list of ids.
     *
     * @param ids
     * @param removeIndex
     * @param out
     * @throws Exception
     */
    void reAccumulateIdsAndEmitUpdates(List<Long> ids, int removeIndex, Collector<RowData> out)
            throws Exception {
        for (int j = removeIndex; j < ids.size(); j++) {
            RowData value = valueMapState.get(ids.get(j));
            if (j == removeIndex) {
                collectDelete(out, value, accMapState.get(GenericRowData.of(ids.get(j))));
            } else {
                aggFuncs.accumulate(value);
                // Logic to early out
                if (aggFuncs.getValue().equals(accMapState.get(GenericRowData.of(ids.get(j))))) {
                    break;
                }
                collectUpdateBefore(out, value, accMapState.get(GenericRowData.of(ids.get(j))));
                collectUpdateAfter(out, value, aggFuncs.getValue());
                accMapState.put(GenericRowData.of(ids.get(j)), aggFuncs.getValue());
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (null != aggFuncs) {
            aggFuncs.close();
        }
    }
}
