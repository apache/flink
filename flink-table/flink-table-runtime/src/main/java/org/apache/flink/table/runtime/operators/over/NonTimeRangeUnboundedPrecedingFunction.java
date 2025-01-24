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

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.functions.KeyedProcessFunctionWithCleanupState;
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

/** A basic implementation to support non-time range unbounded over aggregate with retract mode. */
public class NonTimeRangeUnboundedPrecedingFunction<K>
        extends KeyedProcessFunctionWithCleanupState<K, RowData, RowData> {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG =
            LoggerFactory.getLogger(NonTimeRangeUnboundedPrecedingFunction.class);

    private final GeneratedAggsHandleFunction generatedAggsHandler;
    private final GeneratedRecordEqualiser generatedRecordEqualiser;
    private final GeneratedRecordEqualiser generatedSortKeyEqualiser;
    private final GeneratedRecordComparator generatedRecordComparator;

    // The util to compare two rows based on the sort attribute.
    private transient Comparator<RowData> sortKeyComparator;

    protected final KeySelector<RowData, RowData> sortKeySelector;
    // The record equaliser used to equal RowData.
    private transient RecordEqualiser valueEqualiser;
    private transient RecordEqualiser sortKeyEqualiser;

    private final LogicalType[] accTypes;
    private final LogicalType[] inputFieldTypes;
    private final LogicalType[] sortKeyTypes;
    protected transient JoinedRowData output;

    // state to hold the Long ID counter
    private transient ValueState<Long> idState;

    // state to hold a sorted list each containing a tuple of sort key with an artificial id
    // The artificial id acts as the key in the valueMapState
    private transient ValueState<List<Tuple2<RowData, List<Long>>>> sortedListState;
    // state to hold rows until state ttl expires
    private transient MapState<Long, RowData> valueMapState;
    // state to hold sortKey and its associated accumulator
    private transient MapState<RowData, RowData> accMapState;

    protected transient AggsHandleFunction currFunction;

    public NonTimeRangeUnboundedPrecedingFunction(
            long minRetentionTime,
            long maxRetentionTime,
            GeneratedAggsHandleFunction genAggsHandler,
            GeneratedRecordEqualiser genRecordEqualiser,
            GeneratedRecordEqualiser genSortKeyEqualiser,
            GeneratedRecordComparator genRecordComparator,
            LogicalType[] accTypes,
            LogicalType[] inputFieldTypes,
            LogicalType[] sortKeyTypes,
            RowDataKeySelector sortKeySelector) {
        super(minRetentionTime, maxRetentionTime);
        this.generatedAggsHandler = genAggsHandler;
        this.generatedRecordEqualiser = genRecordEqualiser;
        this.generatedSortKeyEqualiser = genSortKeyEqualiser;
        this.generatedRecordComparator = genRecordComparator;
        this.accTypes = accTypes;
        this.inputFieldTypes = inputFieldTypes;
        this.sortKeyTypes = sortKeyTypes;
        this.sortKeySelector = sortKeySelector;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        // Initialize agg functions
        currFunction =
                generatedAggsHandler.newInstance(getRuntimeContext().getUserCodeClassLoader());
        currFunction.open(new PerKeyStateDataViewStore(getRuntimeContext()));

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
                generatedRecordComparator.newInstance(getRuntimeContext().getUserCodeClassLoader());

        // Initialize state to maintain id counter
        ValueStateDescriptor<Long> idStateDescriptor =
                new ValueStateDescriptor<Long>("idState", Long.class);
        idState = getRuntimeContext().getState(idStateDescriptor);

        // Input elements are all binary rows as they came from network
        InternalTypeInfo<RowData> inputRowTypeInfo = InternalTypeInfo.ofFields(inputFieldTypes);
        InternalTypeInfo<RowData> sortKeyRowTypeInfo = InternalTypeInfo.ofFields(this.sortKeyTypes);

        // Initialize state which maintains a sorted list of tuples(sortKey, List of IDs)
        ListTypeInfo<Long> idListTypeInfo = new ListTypeInfo<Long>(Types.LONG);
        ListTypeInfo<Tuple2<RowData, List<Long>>> listTypeInfo =
                new ListTypeInfo<>(new TupleTypeInfo<>(sortKeyRowTypeInfo, idListTypeInfo));
        ValueStateDescriptor<List<Tuple2<RowData, List<Long>>>> sortedListStateDescriptor =
                new ValueStateDescriptor<List<Tuple2<RowData, List<Long>>>>(
                        "sortedListState", listTypeInfo);
        sortedListState = getRuntimeContext().getState(sortedListStateDescriptor);

        // Initialize state which maintains the actual row
        MapStateDescriptor<Long, RowData> valueStateDescriptor =
                new MapStateDescriptor<Long, RowData>(
                        "valueMapState", Types.LONG, inputRowTypeInfo);
        valueMapState = getRuntimeContext().getMapState(valueStateDescriptor);

        // Initialize accumulator state per row
        InternalTypeInfo<RowData> accTypeInfo = InternalTypeInfo.ofFields(accTypes);
        MapStateDescriptor<RowData, RowData> accStateDescriptor =
                new MapStateDescriptor<RowData, RowData>(
                        "accMapState", sortKeyRowTypeInfo, accTypeInfo);
        accMapState = getRuntimeContext().getMapState(accStateDescriptor);

        initCleanupTimeState("NonTimeUnboundedPrecedingFunctionCleanupTime");
    }

    /**
     * Puts an element from the input stream into state. Emits the aggregated value for the newly
     * inserted element. For append stream emits updates(UB, UA) for all elements which are present
     * after the newly inserted element. For retract stream emits a DELETE for the element and
     * thereafter emits updates(UB, UA) for all elements which are present after the retracted
     * element. Emits the same aggregated value for all elements with the same sortKey to comply
     * with the sql RANGE syntax.
     *
     * @param input The input value.
     * @param ctx A {@link Context} that allows querying the timestamp of the element and getting
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
        // register state-cleanup timer
        registerProcessingCleanupTimer(ctx, ctx.timerService().currentProcessingTime());

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

        // Reset acc state since we can have out of order inserts into the ordered list
        currFunction.resetAccumulators();
        currFunction.cleanup();
    }

    /**
     * Adds a new element(input) to a sortedList. The sortedList contains a list of Tuple<SortKey,
     * List<Long Ids>> Extracts the inputSortKey from the input and compares it with every element
     * in the sortedList If an sortKey already exists in the sortedList for the input, add the id to
     * the list of ids and update the sortedList Otherwise find the right position in the sortedList
     * and add a new entry in the middle. If no matching sortKey is found, add a new entry in the
     * end of the sortedList. After and element is inserted, an INSERT/UPDATE_AFTER event is emitted
     * for the newly inserted element, and for all subsequent elements an UPDATE_BEFORE and
     * UPDATE_AFTER event is emitted based on the previous and newly aggregated values. Updates are
     * skipped if the previously accumulated value is the same as the newly accumulated value to
     * save on network bandwidth.
     *
     * @param input The input value.
     * @param out The collector for returning result values.
     * @throws Exception
     */
    private void insertIntoSortedList(RowData input, Collector<RowData> out) throws Exception {
        Long id = getNextId();
        List<Tuple2<RowData, List<Long>>> sortedList = getSortedList();
        RowKind origRowKind = input.getRowKind();
        input.setRowKind(RowKind.INSERT);
        RowData inputSortKey = sortKeySelector.getKey(input);
        int i;
        boolean isInserted = false;
        for (i = 0; i < sortedList.size(); i++) {
            RowData curSortKey = sortedList.get(i).f0;
            List<Long> ids = new ArrayList<>(sortedList.get(i).f1);
            if (sortKeyComparator.compare(curSortKey, inputSortKey) == 0) {
                // Insert at the (i)th location by adding the new id to the list of ids
                ids.add(id);
                sortedList.set(i, new Tuple2<>(curSortKey, ids));
                setAccumulatorOfPrevRow(sortedList, i - 1);
                reAccumulateIdsAfterInsert(ids, input);
                emitUpdatesForIds(
                        ids,
                        ids.size() - 1,
                        accMapState.get(curSortKey),
                        currFunction.getValue(),
                        origRowKind,
                        input,
                        out);
                isInserted = true;
                i++;
                break;
            } else if (sortKeyComparator.compare(curSortKey, inputSortKey) > 0) {
                // Insert in the middle
                sortedList.add(i, new Tuple2<>(inputSortKey, List.of(id)));
                setAccumulatorOfPrevRow(sortedList, i - 1);
                currFunction.accumulate(input);
                collectInsertOrUpdateAfter(out, input, origRowKind);
                isInserted = true;
                i++;
                break;
            }
        }

        if (!isInserted) {
            // Insert at the end
            sortedList.add(new Tuple2<>(inputSortKey, List.of(id)));
            setAccumulatorOfPrevRow(sortedList, sortedList.size() - 2);
            currFunction.accumulate(input);
            collectInsertOrUpdateAfter(out, input, origRowKind);
            i++;
        }

        addToState(id, input, inputSortKey, currFunction.getAccumulators(), sortedList);

        processRemainingElements(sortedList, i, out);
    }

    /**
     * @return the next id after reading from the idState
     * @throws IOException
     */
    private Long getNextId() throws IOException {
        Long id = idState.value();
        if (id == null) {
            id = 0L;
        }
        return id;
    }

    /**
     * @return the sortedList containing sortKeys and Ids with the same sortKey
     * @throws IOException
     */
    private List<Tuple2<RowData, List<Long>>> getSortedList() throws IOException {
        List<Tuple2<RowData, List<Long>>> sortedList = sortedListState.value();
        if (sortedList == null) {
            sortedList = new ArrayList<>();
        }
        return sortedList;
    }

    /**
     * Helper method to set the accumulator based on the prevIndex
     *
     * @param sortedList
     * @param prevIndex
     * @throws Exception
     */
    private void setAccumulatorOfPrevRow(
            List<Tuple2<RowData, List<Long>>> sortedList, int prevIndex) throws Exception {
        if (prevIndex < 0) {
            currFunction.createAccumulators();
        } else {
            RowData prevAcc = accMapState.get(sortedList.get(prevIndex).f0);
            if (prevAcc == null) {
                // TODO: Throw exception since this could due to state cleaning
                currFunction.createAccumulators();
            } else {
                currFunction.setAccumulators(prevAcc);
            }
        }
    }

    /**
     * Helper method to re-accumulate the aggregated value for all ids after an id was inserted to
     * the end of the ids list.
     *
     * @param ids
     * @param input
     * @throws Exception
     */
    private void reAccumulateIdsAfterInsert(List<Long> ids, RowData input) throws Exception {
        // Update acc value for all ids
        for (int j = 0; j < ids.size(); j++) {
            if (j == ids.size() - 1) {
                // Update acc for newly inserted value
                currFunction.accumulate(input);
            } else {
                // Get rowValue for id and re-accumulate it
                RowData value = valueMapState.get(ids.get(j));
                currFunction.accumulate(value);
            }
        }
    }

    /**
     * Emits updates for all ids. The id belonging to idx location either emits a DELETE or
     * INSERT/UPDATE_AFTER, depending on whether the id is being added to ids or removed from ids.
     * All other ids except the id at position idx emit old and new aggregated values in the form of
     * UPDATE_BEFORE and UPDATE_AFTER respectively.
     *
     * @param ids
     * @param idx
     * @param prevAcc
     * @param currAcc
     * @param rowKind
     * @param input
     * @param out
     * @throws Exception
     */
    private void emitUpdatesForIds(
            List<Long> ids,
            int idx,
            RowData prevAcc,
            RowData currAcc,
            RowKind rowKind,
            RowData input,
            Collector<RowData> out)
            throws Exception {
        for (int j = 0; j < ids.size(); j++) {
            if (j == idx) {
                if (rowKind == RowKind.DELETE) {
                    // TODO: change to collectDelete
                    collectUpdateBefore(out, input, prevAcc);
                } else {
                    // For insert, emit either Insert or UpdateAfter
                    collectInsertOrUpdateAfter(out, input, rowKind);
                }
            } else {
                if (prevAcc.equals(currAcc)) {
                    // Previous accumulator is the same as the current accumulator
                    // Skip sending downstream updates in such cases to reduce network traffic
                    LOG.info("Prev accumulator is same as curr accumulator. Skipping updates.");
                    continue;
                }
                RowData value = valueMapState.get(ids.get(j));
                collectUpdateBefore(out, value, prevAcc);
                collectUpdateAfter(out, value, currAcc);
            }
        }
    }

    private void addToState(
            Long id,
            RowData input,
            RowData inputSortKey,
            RowData accumulators,
            List<Tuple2<RowData, List<Long>>> sortedList)
            throws Exception {
        valueMapState.put(id, input);
        accMapState.put(inputSortKey, accumulators);
        sortedListState.update(sortedList);
        idState.update(++id);
    }

    /**
     * Process all sort keys starting from startPos location. Calculates the new agg value for all
     * ids belonging to the same sort key. Finally, emits the old and new aggregated values for the
     * ids if the newly aggregated value is different from the previously aggregated values.
     *
     * @param sortedList
     * @param startPos
     * @param out
     * @throws Exception
     */
    private void processRemainingElements(
            List<Tuple2<RowData, List<Long>>> sortedList, int startPos, Collector<RowData> out)
            throws Exception {
        // Send updates for remaining sort keys after the inserted/removed idx
        for (int i = startPos; i < sortedList.size(); i++) {
            Tuple2<RowData, List<Long>> sortKeyAndIds = sortedList.get(i);
            RowData curSortKey = sortKeyAndIds.f0;
            List<Long> ids = sortKeyAndIds.f1;
            // Get previous accumulator
            RowData prevAcc = accMapState.get(curSortKey);

            // Calculate new agg value for all ids with the same sortKey
            for (int j = 0; j < ids.size(); j++) {
                RowData value = valueMapState.get(ids.get(j));
                currFunction.accumulate(value);
            }
            // Emit old and new agg values for all ids with the same sortKey
            for (int j = 0; j < ids.size(); j++) {
                RowData value = valueMapState.get(ids.get(j));
                if (prevAcc.equals(currFunction.getAccumulators())) {
                    // Previous accumulator is the same as the current accumulator.
                    // This means all the ids will have no change in the accumulated value.
                    // Skip sending downstream updates in such cases to reduce network traffic
                    LOG.info("Prev accumulator is same as curr accumulator. Skipping updates.");
                    break;
                }
                collectUpdateBefore(out, value, prevAcc);
                collectUpdateAfter(out, value, currFunction.getValue());
            }
            // update accumulated state for sortKey if prevAcc != currAcc
            if (!prevAcc.equals(currFunction.getAccumulators())) {
                accMapState.put(curSortKey, currFunction.getAccumulators());
            }
        }
    }

    private void collectInsertOrUpdateAfter(Collector<RowData> out, RowData value, RowKind rowKind)
            throws Exception {
        output.setRowKind(rowKind);
        output.replace(value, currFunction.getValue());
        out.collect(output);
    }

    private void collectUpdateBefore(
            Collector<RowData> out, RowData rowValue, RowData prevAccValue) {
        output.setRowKind(RowKind.UPDATE_BEFORE);
        output.replace(rowValue, prevAccValue);
        out.collect(output);
    }

    private void collectUpdateAfter(
            Collector<RowData> out, RowData rowValue, RowData currAccValue) {
        output.setRowKind(RowKind.UPDATE_AFTER);
        output.replace(rowValue, currAccValue);
        out.collect(output);
    }

    private void collectDelete(Collector<RowData> out, RowData rowValue, RowData prevAccValue) {
        output.setRowKind(RowKind.DELETE);
        output.replace(rowValue, prevAccValue);
        out.collect(output);
    }

    /**
     * Removes the row matching input from the sortedList. Matching is done based on the sortKeys.
     * Once matched, the actual row is compared for all ids belonging to the same sortKey. After the
     * element is removed, a DELETE event is emitted for the removed element, and for all subsequent
     * elements an UPDATE_BEFORE and UPDATE_AFTER event is emitted based on the previous and newly
     * aggregated values. Updates are skipped if the previously accumulated value is the same as the
     * newly accumulated value to save on network bandwidth.
     *
     * @param input
     * @param out
     * @throws Exception
     */
    private void removeFromSortedList(RowData input, Collector<RowData> out) throws Exception {
        int i;
        input.setRowKind(RowKind.INSERT);
        RowData inputSortKey = sortKeySelector.getKey(input);
        List<Tuple2<RowData, List<Long>>> sortedList = getSortedList();

        for (i = 0; i < sortedList.size(); i++) {
            RowData curSortKey = sortedList.get(i).f0;
            List<Long> ids = new ArrayList<>(sortedList.get(i).f1);
            if (sortKeyEqualiser.equals(curSortKey, inputSortKey)) {
                setAccumulatorOfPrevRow(sortedList, i - 1);
                // Accumulate all ids except id which needs to be removed
                int removeIndex = reAccumulateIdsAndGetRemoveIndex(ids, input);
                if (removeIndex == -1) {
                    // TODO: can this ever happen? may be during/after state cleaning?
                    LOG.info("Could not find id to remove");
                    return;
                }
                emitUpdatesForIds(
                        ids,
                        removeIndex,
                        accMapState.get(curSortKey),
                        currFunction.getAccumulators(),
                        RowKind.DELETE,
                        input,
                        out);
                Long deletedId = ids.remove(removeIndex);
                i = removeIdFromSortedList(sortedList, i, ids, curSortKey);
                removeFromState(sortedList, deletedId, curSortKey);
                break;
            }
        }

        processRemainingElements(sortedList, i, out);
    }

    /**
     * Helper method to re-accumulate the aggregated value for all ids without the id that will be
     * removed.
     *
     * @param ids
     * @param input
     * @return the index position of the id that should be removed
     * @throws Exception
     */
    private int reAccumulateIdsAndGetRemoveIndex(List<Long> ids, RowData input) throws Exception {
        int removeIndex = -1;
        for (int j = 0; j < ids.size(); j++) {
            RowData curValue = valueMapState.get(ids.get(j));
            if (valueEqualiser.equals(curValue, input)) {
                removeIndex = j;
            } else {
                currFunction.accumulate(curValue);
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
    private int removeIdFromSortedList(
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

    private void removeFromState(
            List<Tuple2<RowData, List<Long>>> sortedList, Long id, RowData sortKey)
            throws Exception {
        valueMapState.remove(id);
        accMapState.put(sortKey, currFunction.getAccumulators());
        sortedListState.update(sortedList);
    }

    @Override
    public void close() throws Exception {
        if (null != currFunction) {
            currFunction.close();
        }
    }
}
