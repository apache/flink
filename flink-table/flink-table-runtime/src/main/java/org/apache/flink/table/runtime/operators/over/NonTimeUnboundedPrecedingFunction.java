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
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.functions.KeyedProcessFunctionWithCleanupState;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;

/** A basic implementation to support non-time unbounded over aggregate with retract mode. */
public class NonTimeUnboundedPrecedingFunction<K>
        extends KeyedProcessFunctionWithCleanupState<K, RowData, RowData> {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG =
            LoggerFactory.getLogger(NonTimeUnboundedPrecedingFunction.class);

    private final int keyIdx;
    private final int sortKeyIdx;
    private final FieldGetter sortKeyFieldGetter;

    private final GeneratedAggsHandleFunction generatedAggsHandler;
    private final GeneratedRecordEqualiser generatedRecordEqualiser;
    private final GeneratedRecordComparator generatedRecordComparator;

    // The util to compare two rows based on the sort attribute.
    private transient Comparator<RowData> sortKeyComparator;
    // The record equaliser used to equal RowData.
    private transient RecordEqualiser equaliser;

    private final LogicalType[] accTypes;
    private final LogicalType[] inputFieldTypes;
    protected transient JoinedRowData output;

    // state to hold the Long ID counter
    private transient ValueState<Long> idState;

    // state to hold a list of sorted keys with an artificial id
    // The artificial id acts as the key in the valueMapState
    private transient ValueState<List<RowData>> sortedKeyState;
    // state to hold rows until state ttl expires
    private transient MapState<Long, RowData> valueMapState;
    // state to hold row ID and its associated accumulator
    private transient MapState<Long, RowData> accMapState;

    protected transient AggsHandleFunction currFunction;

    public NonTimeUnboundedPrecedingFunction(
            long minRetentionTime,
            long maxRetentionTime,
            GeneratedAggsHandleFunction genAggsHandler,
            GeneratedRecordEqualiser genRecordEqualiser,
            GeneratedRecordComparator genRecordComparator,
            LogicalType[] accTypes,
            LogicalType[] inputFieldTypes,
            FieldGetter sortKeyFieldGetter,
            int sortKeyIdx) {
        super(minRetentionTime, maxRetentionTime);
        this.generatedAggsHandler = genAggsHandler;
        this.generatedRecordEqualiser = genRecordEqualiser;
        this.generatedRecordComparator = genRecordComparator;
        this.accTypes = accTypes;
        this.inputFieldTypes = inputFieldTypes;
        this.sortKeyFieldGetter = sortKeyFieldGetter;
        this.sortKeyIdx = sortKeyIdx;
        this.keyIdx = 0;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        // Initialize agg functions
        currFunction =
                generatedAggsHandler.newInstance(getRuntimeContext().getUserCodeClassLoader());
        currFunction.open(new PerKeyStateDataViewStore(getRuntimeContext()));

        // Initialize output record
        output = new JoinedRowData();

        // Initialize record equaliser
        equaliser =
                generatedRecordEqualiser.newInstance(getRuntimeContext().getUserCodeClassLoader());

        // Initialize sort comparator
        sortKeyComparator =
                generatedRecordComparator.newInstance(getRuntimeContext().getUserCodeClassLoader());

        // Initialize state to maintain id counter
        ValueStateDescriptor<Long> idStateDescriptor =
                new ValueStateDescriptor<Long>("idState", Long.class);
        idState = getRuntimeContext().getState(idStateDescriptor);

        // Input elements are all binary rows as they came from network
        InternalTypeInfo<RowData> inputType = InternalTypeInfo.ofFields(inputFieldTypes);
        LogicalType idType = DataTypes.BIGINT().getLogicalType();
        LogicalType sortKeyType = inputType.toRowType().getTypeAt(sortKeyIdx);
        RowType sortedRow = RowType.of(idType, sortKeyType);
        LogicalType[] sortedRowTypes = sortedRow.getChildren().toArray(new LogicalType[0]);
        InternalTypeInfo<RowData> sortedKeyRowType = InternalTypeInfo.ofFields(sortedRowTypes);
        ListTypeInfo<RowData> sortedKeyTypeInfo = new ListTypeInfo<>(sortedKeyRowType);
        // Initialize state which maintains a sorted list of pair(ID, sortKey)
        ValueStateDescriptor<List<RowData>> sortedKeyStateDescriptor =
                new ValueStateDescriptor<List<RowData>>("sortedKeyState", sortedKeyTypeInfo);
        sortedKeyState = getRuntimeContext().getState(sortedKeyStateDescriptor);

        // Initialize state which maintains the actual row
        MapStateDescriptor<Long, RowData> valueStateDescriptor =
                new MapStateDescriptor<Long, RowData>("valueState", Types.LONG, inputType);
        valueMapState = getRuntimeContext().getMapState(valueStateDescriptor);

        // Initialize accumulator state per row
        InternalTypeInfo<RowData> accTypeInfo = InternalTypeInfo.ofFields(accTypes);
        MapStateDescriptor<Long, RowData> accStateDescriptor =
                new MapStateDescriptor<Long, RowData>("accMapState", Types.LONG, accTypeInfo);
        accMapState = getRuntimeContext().getMapState(accStateDescriptor);

        initCleanupTimeState("NonTimeUnboundedPrecedingFunctionCleanupTime");
    }

    /**
     * Puts an element from the input stream into state. Emits the aggregated value for the newly
     * inserted element. For append stream emits updates(UB, UA) for all elements which are present
     * after the newly inserted element. For retract stream emits an UB for the element and
     * thereafter emits updates(UB, UA) for all elements which are present after the retracted
     * element.
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

    private void setAccumulatorOfPrevRow(List<RowData> sortedKeyList, int prevIndex)
            throws Exception {
        Long prevKey = sortedKeyList.get(prevIndex).getLong(keyIdx);
        RowData prevAcc = accMapState.get(prevKey);
        currFunction.setAccumulators(prevAcc);
    }

    private void insertRow(
            List<RowData> sortedKeyList,
            ListIterator<RowData> iterator,
            Long id,
            RowData inputKey,
            RowData input)
            throws Exception {
        iterator.add(inputKey);
        // Only accumulate input with currFunction
        currFunction.accumulate(input);
        valueMapState.put(id, input);
        accMapState.put(id, currFunction.getAccumulators());
        sortedKeyState.update(sortedKeyList);
        idState.update(++id);
    }

    private void insertIntoSortedList(RowData input, Collector<RowData> out) throws Exception {
        List<RowData> sortedKeyList = sortedKeyState.value();
        if (sortedKeyList == null) {
            sortedKeyList = new ArrayList<>();
        }
        Long id = idState.value();
        if (id == null) {
            id = 0L;
        }
        boolean isInserted = false;
        RowKind origRowKind = input.getRowKind();
        input.setRowKind(RowKind.INSERT);
        ListIterator<RowData> iterator = sortedKeyList.listIterator();

        RowData inputKey = GenericRowData.of(id, sortKeyFieldGetter.getFieldOrNull(input));

        while (iterator.hasNext()) {
            RowData curKey = iterator.next(); // (ID, sortKey)
            if (sortKeyComparator.compare(curKey, inputKey) > 0) {
                iterator.previous();
                setAccumulatorOfPrevRow(sortedKeyList, iterator.previousIndex());
                insertRow(sortedKeyList, iterator, id, inputKey, input);
                isInserted = true;
                break;
            }
        }

        // Add to the end of the list
        if (!isInserted) {
            if (iterator.hasPrevious()) {
                setAccumulatorOfPrevRow(sortedKeyList, iterator.previousIndex());
            }
            insertRow(sortedKeyList, iterator, id, inputKey, input);
        }

        // prepare output row
        output.setRowKind(origRowKind);
        output.replace(input, currFunction.getValue());
        out.collect(output);

        // Emit updated agg value for all records after newly inserted row
        while (iterator.hasNext()) {
            RowData curKey = iterator.next();
            if (!updateRow(curKey, out)) {
                break;
            }
        }
    }

    private boolean updateRow(RowData curKey, Collector<RowData> out) throws Exception {
        RowData curValue = valueMapState.get(curKey.getLong(keyIdx));
        RowData prevAcc = accMapState.get(curKey.getLong(keyIdx));
        if (prevAcc.equals(currFunction.getAccumulators())) {
            // If previously accumulated value is same as the newly accumulated value
            // then no need to send updates downstream since nothing has changed
            // break;
            return false;
        }
        // Generate UPDATE_BEFORE
        output.setRowKind(RowKind.UPDATE_BEFORE);
        output.replace(curValue, prevAcc);
        out.collect(output);

        // Accumulate first for the UPDATE_AFTER Row
        currFunction.accumulate(curValue);

        // Generate UPDATE_AFTER
        output.setRowKind(RowKind.UPDATE_AFTER);
        output.replace(curValue, currFunction.getValue());
        out.collect(output);

        // accMapState.put(curKey.getLong(keyIdx), currFunction.getValue());
        accMapState.put(curKey.getLong(keyIdx), currFunction.getAccumulators());

        return true;
    }

    private void removeFromSortedList(RowData input, Collector<RowData> out) throws Exception {
        boolean isRetracted = false;
        input.setRowKind(RowKind.INSERT);
        List<RowData> sortedKeyList = sortedKeyState.value();
        ListIterator<RowData> iterator = sortedKeyList.listIterator();

        while (iterator.hasNext()) {
            RowData curKey = iterator.next();
            RowData curValue = valueMapState.get(curKey.getLong(keyIdx));
            if (isRetracted) {
                if (!updateRow(curKey, out)) {
                    break;
                }
            } else if (equaliser.equals(curValue, input)) {
                // Retract record
                RowData curAcc = accMapState.get(curKey.getLong(keyIdx));
                currFunction.setAccumulators(curAcc);
                output.setRowKind(RowKind.UPDATE_BEFORE);
                output.replace(input, currFunction.getValue());
                out.collect(output);

                iterator.remove();

                sortedKeyState.update(sortedKeyList);
                valueMapState.remove(curKey.getLong(keyIdx));
                accMapState.remove(curKey.getLong(keyIdx));
                if (iterator.hasPrevious()) {
                    setAccumulatorOfPrevRow(sortedKeyList, iterator.previousIndex());
                } else {
                    currFunction.setAccumulators(currFunction.createAccumulators());
                }
                isRetracted = true;
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (null != currFunction) {
            currFunction.close();
        }
    }
}
