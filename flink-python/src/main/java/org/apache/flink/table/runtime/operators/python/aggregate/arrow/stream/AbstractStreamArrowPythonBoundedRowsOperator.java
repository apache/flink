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

package org.apache.flink.table.runtime.operators.python.aggregate.arrow.stream;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.functions.CleanupState;
import org.apache.flink.table.types.logical.RowType;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

/**
 * The Abstract class of Stream Arrow Python {@link AggregateFunction} Operator for ROWS clause
 * bounded Over Window Aggregation.
 */
@Internal
public abstract class AbstractStreamArrowPythonBoundedRowsOperator<K>
        extends AbstractStreamArrowPythonOverWindowAggregateFunctionOperator<K>
        implements CleanupState {

    private static final long serialVersionUID = 1L;

    private final long minRetentionTime;

    private final long maxRetentionTime;

    private final boolean stateCleaningEnabled;

    /** list to sort timestamps to access rows in timestamp order. */
    transient LinkedList<Long> sortedTimestamps;

    transient LinkedList<RowData> windowData;

    public AbstractStreamArrowPythonBoundedRowsOperator(
            Configuration config,
            long minRetentionTime,
            long maxRetentionTime,
            PythonFunctionInfo[] pandasAggFunctions,
            RowType inputType,
            RowType outputType,
            int inputTimeFieldIndex,
            long lowerBoundary,
            int[] groupingSet,
            int[] udafInputOffsets) {
        super(
                config,
                pandasAggFunctions,
                inputType,
                outputType,
                inputTimeFieldIndex,
                lowerBoundary,
                groupingSet,
                udafInputOffsets);
        this.minRetentionTime = minRetentionTime;
        this.maxRetentionTime = maxRetentionTime;
        this.stateCleaningEnabled = minRetentionTime > 1;
    }

    @Override
    public void open() throws Exception {
        super.open();
        sortedTimestamps = new LinkedList<>();
        windowData = new LinkedList<>();
    }

    @Override
    public void onProcessingTime(InternalTimer<K, VoidNamespace> timer) throws Exception {
        if (stateCleaningEnabled) {

            Iterator<Long> keysIt = inputState.keys().iterator();
            Long lastProcessedTime = lastTriggeringTsState.value();
            if (lastProcessedTime == null) {
                lastProcessedTime = 0L;
            }

            // is data left which has not been processed yet?
            boolean noRecordsToProcess = true;
            while (keysIt.hasNext() && noRecordsToProcess) {
                if (keysIt.next() > lastProcessedTime) {
                    noRecordsToProcess = false;
                }
            }

            if (noRecordsToProcess) {
                inputState.clear();
                cleanupTsState.clear();
            } else {
                // There are records left to process because a watermark has not been received yet.
                // This would only happen if the input stream has stopped. So we don't need to clean
                // up.
                // We leave the state as it is and schedule a new cleanup timer
                registerProcessingCleanupTimer(timerService.currentProcessingTime());
            }
        }
    }

    @Override
    public void onEventTime(InternalTimer<K, VoidNamespace> timer) throws Exception {
        long timestamp = timer.getTimestamp();
        // gets all window data from state for the calculation
        List<RowData> inputs = inputState.get(timestamp);

        Iterable<Long> keyIter = inputState.keys();
        long currentWatermark = timerService.currentWatermark();
        for (Long dataTs : keyIter) {
            if (dataTs <= currentWatermark) {
                insertToSortedList(dataTs);
            }
        }
        int index = sortedTimestamps.indexOf(timestamp);
        for (int i = 0; i < inputs.size(); i++) {
            forwardedInputQueue.add(inputs.get(i));
            triggerWindowProcess(inputs, i, index);
        }
        windowData.clear();
        sortedTimestamps.clear();
    }

    @Override
    @SuppressWarnings("ConstantConditions")
    public void emitResult(Tuple2<byte[], Integer> resultTuple) throws Exception {
        byte[] udafResult = resultTuple.f0;
        int length = resultTuple.f1;
        bais.setBuffer(udafResult, 0, length);
        int rowCount = arrowSerializer.load();
        for (int i = 0; i < rowCount; i++) {
            RowData data = arrowSerializer.read(i);
            RowData key = forwardedInputQueue.poll();
            reuseJoinedRow.setRowKind(key.getRowKind());
            rowDataWrapper.collect(reuseJoinedRow.replace(key, data));
        }
        arrowSerializer.resetReader();
    }

    void registerProcessingCleanupTimer(long currentTime) throws Exception {
        if (stateCleaningEnabled) {
            registerProcessingCleanupTimer(
                    cleanupTsState, currentTime, minRetentionTime, maxRetentionTime, timerService);
        }
    }

    void triggerWindowProcess(List<RowData> inputs, int i, int index) throws Exception {
        if (windowData.isEmpty()) {
            if (i >= lowerBoundary) {
                for (int j = (int) (i - lowerBoundary); j <= i; j++) {
                    windowData.add(inputs.get(j));
                }
                currentBatchCount += lowerBoundary;
            } else {
                for (int j = 0; j <= i; j++) {
                    RowData rowData = inputs.get(j);
                    windowData.add(rowData);
                    currentBatchCount++;
                }
                Long previousTimestamp;
                List<RowData> previousData;
                int length;
                long remainingDataCount = lowerBoundary - i;
                ListIterator<Long> iter = sortedTimestamps.listIterator(index);
                while (remainingDataCount > 0 && iter.hasPrevious()) {
                    previousTimestamp = iter.previous();
                    previousData = inputState.get(previousTimestamp);
                    length = previousData.size();
                    ListIterator<RowData> previousDataIter = previousData.listIterator(length);
                    while (previousDataIter.hasPrevious() && remainingDataCount > 0) {
                        windowData.addFirst(previousDataIter.previous());
                        remainingDataCount--;
                        currentBatchCount++;
                    }
                }
                // clear outdated data.
                while (iter.hasPrevious()) {
                    previousTimestamp = iter.previous();
                    inputState.remove(previousTimestamp);
                }
            }
        } else {
            if (windowData.size() > lowerBoundary) {
                windowData.pop();
            }
            windowData.add(inputs.get(i));
            currentBatchCount += windowData.size();
        }
        for (RowData rowData : windowData) {
            arrowSerializer.write(getFunctionInput(rowData));
        }
        invokeCurrentBatch();
    }

    void insertToSortedList(Long dataTs) {
        ListIterator<Long> listIterator = sortedTimestamps.listIterator(0);
        while (listIterator.hasNext()) {
            Long timestamp = listIterator.next();
            if (dataTs < timestamp) {
                listIterator.previous();
                listIterator.add(dataTs);
                return;
            }
        }
        sortedTimestamps.addLast(dataTs);
    }
}
