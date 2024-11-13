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
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
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
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
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

    private final GeneratedAggsHandleFunction genAggsHandler;
    private final GeneratedRecordEqualiser generatedRecordEqualiser;
    private final GeneratedRecordComparator generatedRecordComparator;

    // The util to compare two rows based on the sort attribute.
    private transient Comparator<RowData> sortKeyComparator;
    // The record equaliser used to equal RowData.
    private transient RecordEqualiser equaliser;

    private final LogicalType[] accTypes;
    private final LogicalType[] inputFieldTypes;
    protected transient JoinedRowData output;

    // state to hold the accumulators of the aggregations
    private transient ValueState<RowData> accState;
    // state to hold rows until state ttl expires
    private transient ValueState<List<RowData>> inputState;

    protected transient AggsHandleFunction currFunction;
    protected transient AggsHandleFunction prevFunction;

    public NonTimeUnboundedPrecedingFunction(
            long minRetentionTime,
            long maxRetentionTime,
            GeneratedAggsHandleFunction genAggsHandler,
            GeneratedRecordEqualiser genRecordEqualiser,
            GeneratedRecordComparator genRecordComparator,
            LogicalType[] accTypes,
            LogicalType[] inputFieldTypes) {
        super(minRetentionTime, maxRetentionTime);
        this.genAggsHandler = genAggsHandler;
        this.generatedRecordEqualiser = genRecordEqualiser;
        this.generatedRecordComparator = genRecordComparator;
        this.accTypes = accTypes;
        this.inputFieldTypes = inputFieldTypes;
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

        // Intialize record equaliser
        equaliser =
                generatedRecordEqualiser.newInstance(getRuntimeContext().getUserCodeClassLoader());

        // Initialize sort comparator
        sortKeyComparator =
                generatedRecordComparator.newInstance(getRuntimeContext().getUserCodeClassLoader());

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

        initCleanupTimeState("NonTimeUnboundedPrecedingFunctionCleanupTime");
    }

    /**
     * Puts an element from the input stream into state if it is not late. Registers a timer for the
     * next watermark.
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
            insertIntoSortedList(input, out);
        } else if (rowKind == RowKind.DELETE || rowKind == RowKind.UPDATE_BEFORE) {
            removeFromSortedList(input, out);
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

    @Override
    public void close() throws Exception {
        if (null != currFunction) {
            currFunction.close();
        }
    }
}
