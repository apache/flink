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

    private final GeneratedAggsHandleFunction genAggsHandler;
    private final GeneratedRecordEqualiser generatedRecordEqualiser;
    private final GeneratedRecordComparator generatedRecordComparator;
    private final GeneratedRecordComparator keyGeneratedRecordComparator;

    // The util to compare two rows based on the sort attribute.
    private transient Comparator<RowData> sortKeyComparator;
    private transient Comparator<RowData> newSortKeyComparator;
    // The record equaliser used to equal RowData.
    private transient RecordEqualiser equaliser;

    private final LogicalType[] accTypes;
    private final LogicalType[] inputFieldTypes;
    protected transient JoinedRowData output;

    // state to hold the accumulators of the aggregations
    private transient ValueState<RowData> accState;
    // state to hold rows until state ttl expires
    private transient ValueState<List<RowData>> inputState;

    // state to hold the Long ID counter
    private transient ValueState<Long> idState;

    // state to hold a list of sorted keys with an artificial id
    // The artificial id acts as the key in the valueMapState
    private transient ValueState<List<RowData>> sortedKeyState;
    // state to hold rows until state ttl expires
    private transient MapState<Long, RowData> valueMapState;

    protected transient AggsHandleFunction currFunction;
    protected transient AggsHandleFunction prevFunction;

    public NonTimeUnboundedPrecedingFunction(
            long minRetentionTime,
            long maxRetentionTime,
            GeneratedAggsHandleFunction genAggsHandler,
            GeneratedRecordEqualiser genRecordEqualiser,
            GeneratedRecordComparator genRecordComparator,
            GeneratedRecordComparator keyGenRecordComparator,
            LogicalType[] accTypes,
            LogicalType[] inputFieldTypes,
            FieldGetter sortKeyFieldGetter,
            int sortKeyIdx) {
        super(minRetentionTime, maxRetentionTime);
        this.genAggsHandler = genAggsHandler;
        this.generatedRecordEqualiser = genRecordEqualiser;
        this.generatedRecordComparator = genRecordComparator;
        this.keyGeneratedRecordComparator = keyGenRecordComparator;
        this.accTypes = accTypes;
        this.inputFieldTypes = inputFieldTypes;
        this.sortKeyFieldGetter = sortKeyFieldGetter;
        this.sortKeyIdx = sortKeyIdx;
        this.keyIdx = 0;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        // Initialize agg functions
        currFunction = genAggsHandler.newInstance(getRuntimeContext().getUserCodeClassLoader());
        currFunction.open(new PerKeyStateDataViewStore(getRuntimeContext()));

        prevFunction = genAggsHandler.newInstance(getRuntimeContext().getUserCodeClassLoader());
        prevFunction.open(new PerKeyStateDataViewStore(getRuntimeContext()));

        // Initialize output record
        output = new JoinedRowData();

        // Initialize record equaliser
        equaliser =
                generatedRecordEqualiser.newInstance(getRuntimeContext().getUserCodeClassLoader());

        // Initialize sort comparator
        sortKeyComparator =
                generatedRecordComparator.newInstance(getRuntimeContext().getUserCodeClassLoader());

        newSortKeyComparator =
                keyGeneratedRecordComparator.newInstance(
                        getRuntimeContext().getUserCodeClassLoader());

        // Initialize accumulator state
        InternalTypeInfo<RowData> accTypeInfo = InternalTypeInfo.ofFields(accTypes);
        ValueStateDescriptor<RowData> accStateDesc =
                new ValueStateDescriptor<RowData>("accState", accTypeInfo);
        accState = getRuntimeContext().getState(accStateDesc);

        // Input elements are all binary rows as they came from network
        InternalTypeInfo<RowData> inputType = InternalTypeInfo.ofFields(inputFieldTypes);
        ListTypeInfo<RowData> rowListTypeInfo = new ListTypeInfo<RowData>(inputType);

        ValueStateDescriptor<List<RowData>> inputStateDescriptor =
                new ValueStateDescriptor<List<RowData>>("inputState", rowListTypeInfo);

        // Initialize state which maintains records in sorted(ASC) order
        inputState = getRuntimeContext().getState(inputStateDescriptor);

        ValueStateDescriptor<Long> idStateDescriptor =
                new ValueStateDescriptor<Long>("idState", Long.class);
        idState = getRuntimeContext().getState(idStateDescriptor);

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

        MapStateDescriptor<Long, RowData> valueStateDescriptor =
                new MapStateDescriptor<Long, RowData>("valueState", Types.LONG, inputType);
        valueMapState = getRuntimeContext().getMapState(valueStateDescriptor);

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

        // get last accumulator
        RowData lastAccumulatorCurr = accState.value();
        if (lastAccumulatorCurr == null) {
            // initialize accumulator
            lastAccumulatorCurr = currFunction.createAccumulators();
        }
        // set accumulator in function context first
        currFunction.setAccumulators(lastAccumulatorCurr);

        // get last accumulator
        RowData lastAccumulatorPrev = accState.value();
        if (lastAccumulatorPrev == null) {
            // initialize accumulator
            lastAccumulatorPrev = prevFunction.createAccumulators();
        }
        // set accumulator in function context first
        prevFunction.setAccumulators(lastAccumulatorPrev);

        RowKind rowKind = input.getRowKind();

        if (rowKind == RowKind.INSERT || rowKind == RowKind.UPDATE_AFTER) {
            // insertIntoSortedList(input, out);
            insertIntoSortedListOptimized(input, out);
        } else if (rowKind == RowKind.DELETE || rowKind == RowKind.UPDATE_BEFORE) {
            // removeFromSortedList(input, out);
            removeFromSortedListOptimized(input, out);
        }

        // Reset acc state since we can have out of order inserts into the ordered list
        currFunction.resetAccumulators();
        prevFunction.resetAccumulators();
        currFunction.cleanup();
        prevFunction.cleanup();
    }

    private void insertIntoSortedList(RowData rowData, Collector<RowData> out) throws Exception {
        List<RowData> rowList = inputState.value();
        if (rowList == null) {
            rowList = new ArrayList<>();
        }
        boolean isInserted = false;
        RowKind origRowKind = rowData.getRowKind();
        rowData.setRowKind(RowKind.INSERT);
        ListIterator<RowData> iterator = rowList.listIterator();

        while (iterator.hasNext()) {
            RowData curRow = iterator.next();
            if (sortKeyComparator.compare(curRow, rowData) > 0) {
                iterator.previous();
                iterator.add(rowData);
                isInserted = true;
                break;
            }
            currFunction.accumulate(curRow);
            prevFunction.accumulate(curRow);
        }

        // Add to the end of the list
        if (!isInserted) {
            iterator.add(rowData);
        }

        // Only accumulate rowData with currFunction
        currFunction.accumulate(rowData);

        // Update state with the newly inserted row
        inputState.update(rowList);

        // prepare output row
        output.setRowKind(origRowKind);
        output.replace(rowData, currFunction.getValue());
        out.collect(output);

        // Emit updated agg value for all records after newly inserted row
        while (iterator.hasNext()) {
            RowData curRow = iterator.next();
            currFunction.accumulate(curRow);
            prevFunction.accumulate(curRow);
            // Generate UPDATE_BEFORE
            output.setRowKind(RowKind.UPDATE_BEFORE);
            output.replace(curRow, prevFunction.getValue());
            out.collect(output);
            // Generate UPDATE_AFTER
            output.setRowKind(RowKind.UPDATE_AFTER);
            output.replace(curRow, currFunction.getValue());
            out.collect(output);
        }
    }

    private void insertIntoSortedListOptimized(RowData input, Collector<RowData> out)
            throws Exception {
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

        while (iterator.hasNext()) {
            RowData curKey = iterator.next(); // (ID, sortKey)
            RowData inputKey = GenericRowData.of(-1L, sortKeyFieldGetter.getFieldOrNull(input));

            if (newSortKeyComparator.compare(curKey, inputKey) > 0) {
                iterator.previous();
                iterator.add(GenericRowData.of(id, sortKeyFieldGetter.getFieldOrNull(input)));
                valueMapState.put(id, input);
                isInserted = true;
                id++;
                break;
            }
            // Can also add the accKey to the sortedKeyList to avoid reading from the valueMapState
            RowData curRow = valueMapState.get(curKey.getLong(keyIdx));
            currFunction.accumulate(curRow);
            prevFunction.accumulate(curRow);
        }

        // Add to the end of the list
        if (!isInserted) {
            iterator.add(GenericRowData.of(id, sortKeyFieldGetter.getFieldOrNull(input)));
            valueMapState.put(id, input);
            id++;
        }

        // Only accumulate rowData with currFunction
        currFunction.accumulate(input);

        // Update sorted key state with the newly inserted row's key
        sortedKeyState.update(sortedKeyList);
        idState.update(id);

        // prepare output row
        output.setRowKind(origRowKind);
        output.replace(input, currFunction.getValue());
        out.collect(output);

        // Emit updated agg value for all records after newly inserted row
        while (iterator.hasNext()) {
            RowData curKey = iterator.next();
            RowData curValue = valueMapState.get(curKey.getLong(keyIdx));
            currFunction.accumulate(curValue);
            prevFunction.accumulate(curValue);
            // Generate UPDATE_BEFORE
            output.setRowKind(RowKind.UPDATE_BEFORE);
            output.replace(curValue, prevFunction.getValue());
            out.collect(output);
            // Generate UPDATE_AFTER
            output.setRowKind(RowKind.UPDATE_AFTER);
            output.replace(curValue, currFunction.getValue());
            out.collect(output);
        }
    }

    private void removeFromSortedList(RowData rowData, Collector<RowData> out) throws Exception {
        boolean isRetracted = false;
        rowData.setRowKind(RowKind.INSERT);
        List<RowData> rowList = inputState.value();
        ListIterator<RowData> iterator = rowList.listIterator();

        while (iterator.hasNext()) {
            RowData curRow = iterator.next();
            currFunction.accumulate(curRow);
            prevFunction.accumulate(curRow);
            if (isRetracted) {
                // Emit updated agg value for all records after retraction
                output.setRowKind(RowKind.UPDATE_BEFORE);
                output.replace(curRow, prevFunction.getValue());
                out.collect(output);

                output.setRowKind(RowKind.UPDATE_AFTER);
                output.replace(curRow, currFunction.getValue());
                out.collect(output);
            } else if (equaliser.equals(curRow, rowData)) {
                // Retract record
                output.setRowKind(RowKind.UPDATE_BEFORE);
                output.replace(rowData, currFunction.getValue());
                out.collect(output);
                iterator.remove();
                currFunction.retract(curRow);
                isRetracted = true;
            }
        }

        // Update state without the retracted row
        inputState.update(rowList);
    }

    private void removeFromSortedListOptimized(RowData input, Collector<RowData> out)
            throws Exception {
        boolean isRetracted = false;
        input.setRowKind(RowKind.INSERT);
        List<RowData> sortedKeyList = sortedKeyState.value();
        ListIterator<RowData> iterator = sortedKeyList.listIterator();

        while (iterator.hasNext()) {
            RowData curKey = iterator.next();
            RowData curValue = valueMapState.get(curKey.getLong(keyIdx));
            currFunction.accumulate(curValue);
            prevFunction.accumulate(curValue);
            if (isRetracted) {
                // Emit updated agg value for all records after retraction
                output.setRowKind(RowKind.UPDATE_BEFORE);
                output.replace(curValue, prevFunction.getValue());
                out.collect(output);

                output.setRowKind(RowKind.UPDATE_AFTER);
                output.replace(curValue, currFunction.getValue());
                out.collect(output);
            } else if (equaliser.equals(curValue, input)) {
                // Retract record
                output.setRowKind(RowKind.UPDATE_BEFORE);
                output.replace(input, currFunction.getValue());
                out.collect(output);
                iterator.remove();
                valueMapState.remove(curKey.getLong(keyIdx));
                currFunction.retract(curValue);
                isRetracted = true;
            }
        }

        // Update sorted key state without the retracted row
        sortedKeyState.update(sortedKeyList);
    }

    @Override
    public void close() throws Exception {
        if (null != prevFunction) {
            prevFunction.close();
        }
        if (null != currFunction) {
            currFunction.close();
        }
    }
}
