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
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.types.logical.RowType;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * The Abstract class of Stream Arrow Python {@link AggregateFunction} Operator for RANGE clause
 * bounded Over Window Aggregation.
 */
@Internal
public abstract class AbstractStreamArrowPythonBoundedRangeOperator<K>
        extends AbstractStreamArrowPythonOverWindowAggregateFunctionOperator<K> {

    private static final long serialVersionUID = 1L;

    private transient LinkedList<List<RowData>> inputData;

    public AbstractStreamArrowPythonBoundedRangeOperator(
            Configuration config,
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
    }

    @Override
    public void open() throws Exception {
        super.open();
        inputData = new LinkedList<>();
    }

    @Override
    public void onEventTime(InternalTimer<K, VoidNamespace> timer) throws Exception {
        long timestamp = timer.getTimestamp();
        Long cleanupTimestamp = cleanupTsState.value();
        // if cleanupTsState has not been updated then it is safe to cleanup states
        if (cleanupTimestamp != null && cleanupTimestamp <= timestamp) {
            inputState.clear();
            lastTriggeringTsState.clear();
            cleanupTsState.clear();
            return;
        }
        // gets all window data from state for the calculation
        List<RowData> inputs = inputState.get(timestamp);
        triggerWindowProcess(timestamp, inputs);
        lastTriggeringTsState.update(timestamp);
    }

    @Override
    public void onProcessingTime(InternalTimer<K, VoidNamespace> timer) throws Exception {
        long timestamp = timer.getTimestamp();
        Long cleanupTimestamp = cleanupTsState.value();
        // if cleanupTsState has not been updated then it is safe to cleanup states
        if (cleanupTimestamp != null && cleanupTimestamp <= timestamp) {
            inputState.clear();
            cleanupTsState.clear();
            return;
        }

        // we consider the original timestamp of events
        // that have registered this time trigger 1 ms ago

        long currentTime = timestamp - 1;

        // get the list of elements of current proctime
        List<RowData> currentElements = inputState.get(currentTime);
        triggerWindowProcess(timestamp, currentElements);
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
            List<RowData> input = inputData.poll();
            for (RowData ele : input) {
                reuseJoinedRow.setRowKind(ele.getRowKind());
                rowDataWrapper.collect(reuseJoinedRow.replace(ele, data));
            }
        }
        arrowSerializer.resetReader();
    }

    void registerCleanupTimer(long timestamp, TimeDomain domain) throws Exception {
        long minCleanupTimestamp = timestamp + lowerBoundary + 1;
        long maxCleanupTimestamp = timestamp + (long) (lowerBoundary * 1.5) + 1;
        // update timestamp and register timer if needed
        Long curCleanupTimestamp = cleanupTsState.value();
        if (curCleanupTimestamp == null || curCleanupTimestamp < minCleanupTimestamp) {
            // we don't delete existing timer since it may delete timer for data processing
            if (domain == TimeDomain.EVENT_TIME) {
                timerService.registerEventTimeTimer(maxCleanupTimestamp);
            } else {
                timerService.registerProcessingTimeTimer(maxCleanupTimestamp);
            }
            cleanupTsState.update(maxCleanupTimestamp);
        }
    }

    private void triggerWindowProcess(long upperLimit, List<RowData> inputs) throws Exception {
        long lowerLimit = upperLimit - lowerBoundary;
        if (inputs != null) {
            Iterator<Map.Entry<Long, List<RowData>>> iter = inputState.iterator();
            while (iter.hasNext()) {
                Map.Entry<Long, List<RowData>> entry = iter.next();
                long dataTs = entry.getKey();
                if (dataTs >= lowerLimit) {
                    if (dataTs <= upperLimit) {
                        List<RowData> dataList = entry.getValue();
                        for (RowData data : dataList) {
                            arrowSerializer.write(getFunctionInput(data));
                            currentBatchCount++;
                        }
                    }
                } else {
                    iter.remove();
                }
            }
            inputData.add(inputs);
            invokeCurrentBatch();
        }
    }
}
