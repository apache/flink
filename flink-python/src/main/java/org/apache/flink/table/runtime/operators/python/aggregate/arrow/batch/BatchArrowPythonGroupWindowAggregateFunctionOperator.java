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

package org.apache.flink.table.runtime.operators.python.aggregate.arrow.batch;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.grouping.HeapWindowsGrouping;
import org.apache.flink.table.runtime.operators.window.grouping.WindowsGrouping;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.RowIterator;
import org.apache.flink.table.types.logical.RowType;

import java.util.LinkedList;

/** The Batch Arrow Python {@link AggregateFunction} Operator for Group Window Aggregation. */
@Internal
public class BatchArrowPythonGroupWindowAggregateFunctionOperator
        extends AbstractBatchArrowPythonAggregateFunctionOperator {

    private static final long serialVersionUID = 1L;

    /**
     * The Infos of the Window. 0 -> start of the Window. 1 -> end of the Window. 2 -> row time of
     * the Window.
     */
    private final int[] namedProperties;

    /** The row time index of the input data. */
    private final int inputTimeFieldIndex;

    /** The window elements buffer size limit used in group window agg operator. */
    private final int maxLimitSize;

    /** The window size of the window. */
    private final long windowSize;

    /** The sliding size of the sliding window. */
    private final long slideSize;

    private transient WindowsGrouping windowsGrouping;

    /**
     * The GenericRowData reused holding the property of the window, such as window start, window
     * end and window time.
     */
    private transient GenericRowData windowProperty;

    /** The JoinedRowData reused holding the window agg execution result. */
    private transient JoinedRowData windowAggResult;

    /**
     * The queue holding the input groupSet with the TimeWindow for which the execution results have
     * not been received.
     */
    private transient LinkedList<Tuple2<RowData, TimeWindow>> inputKeyAndWindow;

    /** The type serializer for the forwarded fields. */
    private transient RowDataSerializer forwardedInputSerializer;

    public BatchArrowPythonGroupWindowAggregateFunctionOperator(
            Configuration config,
            PythonFunctionInfo[] pandasAggFunctions,
            RowType inputType,
            RowType outputType,
            int inputTimeFieldIndex,
            int maxLimitSize,
            long windowSize,
            long slideSize,
            int[] namedProperties,
            int[] groupKey,
            int[] groupingSet,
            int[] udafInputOffsets) {
        super(
                config,
                pandasAggFunctions,
                inputType,
                outputType,
                groupKey,
                groupingSet,
                udafInputOffsets);
        this.namedProperties = namedProperties;
        this.inputTimeFieldIndex = inputTimeFieldIndex;
        this.maxLimitSize = maxLimitSize;
        this.windowSize = windowSize;
        this.slideSize = slideSize;
    }

    @Override
    public void open() throws Exception {
        userDefinedFunctionOutputType =
                new RowType(
                        outputType
                                .getFields()
                                .subList(
                                        groupingSet.length,
                                        outputType.getFieldCount() - namedProperties.length));
        inputKeyAndWindow = new LinkedList<>();
        windowProperty = new GenericRowData(namedProperties.length);
        windowAggResult = new JoinedRowData();
        windowsGrouping =
                new HeapWindowsGrouping(
                        maxLimitSize, windowSize, slideSize, inputTimeFieldIndex, false);
        forwardedInputSerializer = new RowDataSerializer(inputType);
        super.open();
    }

    @Override
    public void close() throws Exception {
        super.close();
        windowsGrouping.close();
    }

    @Override
    public void bufferInput(RowData input) throws Exception {
        BinaryRowData currentKey = groupKeyProjection.apply(input).copy();
        if (isNewKey(currentKey)) {
            if (lastGroupKey != null) {
                invokeCurrentBatch();
            }
            lastGroupKey = currentKey;
            lastGroupSet = groupSetProjection.apply(input).copy();
        }
    }

    @Override
    protected void invokeCurrentBatch() throws Exception {
        windowsGrouping.advanceWatermarkToTriggerAllWindows();
        triggerWindowProcess();
        windowsGrouping.reset();
    }

    @Override
    public void processElementInternal(RowData value) throws Exception {
        windowsGrouping.addInputToBuffer(forwardedInputSerializer.toBinaryRow(value).copy());
        triggerWindowProcess();
    }

    @Override
    @SuppressWarnings("ConstantConditions")
    public void emitResult(Tuple2<byte[], Integer> resultTuple) throws Exception {
        byte[] udafResult = resultTuple.f0;
        int length = resultTuple.f1;
        bais.setBuffer(udafResult, 0, length);
        int rowCount = arrowSerializer.load();
        for (int i = 0; i < rowCount; i++) {
            Tuple2<RowData, TimeWindow> input = inputKeyAndWindow.poll();
            RowData key = input.f0;
            TimeWindow window = input.f1;
            setWindowProperty(window);
            windowAggResult.replace(key, arrowSerializer.read(i));
            rowDataWrapper.collect(reuseJoinedRow.replace(windowAggResult, windowProperty));
        }
    }

    private void triggerWindowProcess() throws Exception {
        while (windowsGrouping.hasTriggerWindow()) {
            RowIterator<BinaryRowData> elementIterator =
                    windowsGrouping.buildTriggerWindowElementsIterator();
            while (elementIterator.advanceNext()) {
                BinaryRowData winElement = elementIterator.getRow();
                arrowSerializer.write(getFunctionInput(winElement));
                currentBatchCount++;
            }
            if (currentBatchCount > 0) {
                TimeWindow currentWindow = windowsGrouping.getTriggerWindow();
                inputKeyAndWindow.add(Tuple2.of(lastGroupSet, currentWindow));
                arrowSerializer.finishCurrentBatch();
                pythonFunctionRunner.process(baos.toByteArray());
                elementCount += currentBatchCount;
                checkInvokeFinishBundleByCount();
                currentBatchCount = 0;
                baos.reset();
            }
        }
    }

    private void setWindowProperty(TimeWindow currentWindow) {
        for (int i = 0; i < namedProperties.length; i++) {
            switch (namedProperties[i]) {
                case 0:
                    windowProperty.setField(
                            i, TimestampData.fromEpochMillis(currentWindow.getStart()));
                    break;
                case 1:
                    windowProperty.setField(
                            i, TimestampData.fromEpochMillis(currentWindow.getEnd()));
                    break;
                case 2:
                    windowProperty.setField(
                            i, TimestampData.fromEpochMillis(currentWindow.maxTimestamp()));
                    break;
            }
        }
    }
}
