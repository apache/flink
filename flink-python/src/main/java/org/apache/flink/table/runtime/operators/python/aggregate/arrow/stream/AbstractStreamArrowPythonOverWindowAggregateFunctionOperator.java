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
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.operators.python.aggregate.arrow.AbstractArrowPythonAggregateFunctionOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;

/**
 * The Abstract class of Stream Arrow Python {@link AggregateFunction} Operator for Over Window
 * Aggregation.
 */
@Internal
public abstract class AbstractStreamArrowPythonOverWindowAggregateFunctionOperator<K>
        extends AbstractArrowPythonAggregateFunctionOperator
        implements Triggerable<K, VoidNamespace> {

    private static final long serialVersionUID = 1L;

    /** The row time index of the input data. */
    protected final int inputTimeFieldIndex;

    /** Window lower boundary. */
    protected final long lowerBoundary;

    /**
     * the state which keeps all the data that are not expired. The first element (as the mapState
     * key) of the tuple is the time stamp. Per each time stamp, the second element of tuple is a
     * list that contains the entire data of all the rows belonging to this time stamp.
     */
    transient MapState<Long, List<RowData>> inputState;

    /** Interface for working with time and timers. */
    transient TimerService timerService;

    /** the state which keeps the last triggering timestamp. */
    transient ValueState<Long> lastTriggeringTsState;

    /** the state which keeps the safe timestamp to cleanup states. */
    transient ValueState<Long> cleanupTsState;

    public AbstractStreamArrowPythonOverWindowAggregateFunctionOperator(
            Configuration config,
            PythonFunctionInfo[] pandasAggFunctions,
            RowType inputType,
            RowType outputType,
            int inputTimeFieldIndex,
            long lowerBoundary,
            int[] groupingSet,
            int[] udafInputOffsets) {
        super(config, pandasAggFunctions, inputType, outputType, groupingSet, udafInputOffsets);
        this.inputTimeFieldIndex = inputTimeFieldIndex;
        this.lowerBoundary = lowerBoundary;
    }

    @Override
    public void open() throws Exception {
        userDefinedFunctionOutputType =
                new RowType(
                        outputType
                                .getFields()
                                .subList(inputType.getFieldCount(), outputType.getFieldCount()));
        InternalTimerService<VoidNamespace> internalTimerService =
                getInternalTimerService(
                        "python-over-window-timers", VoidNamespaceSerializer.INSTANCE, this);

        timerService = new SimpleTimerService(internalTimerService);

        InternalTypeInfo<RowData> inputTypeInfo = InternalTypeInfo.of(inputType);
        ListTypeInfo<RowData> rowListTypeInfo = new ListTypeInfo<>(inputTypeInfo);

        MapStateDescriptor<Long, List<RowData>> inputStateDesc =
                new MapStateDescriptor<>("inputState", Types.LONG, rowListTypeInfo);
        ValueStateDescriptor<Long> lastTriggeringTsDescriptor =
                new ValueStateDescriptor<>("lastTriggeringTsState", Types.LONG);
        lastTriggeringTsState = getRuntimeContext().getState(lastTriggeringTsDescriptor);
        ValueStateDescriptor<Long> cleanupTsStateDescriptor =
                new ValueStateDescriptor<>("cleanupTsState", Types.LONG);
        cleanupTsState = getRuntimeContext().getState(cleanupTsStateDescriptor);
        inputState = getRuntimeContext().getMapState(inputStateDesc);
        super.open();
    }

    @Override
    public void processElementInternal(RowData value) throws Exception {}

    void invokeCurrentBatch() throws Exception {
        if (currentBatchCount > 0) {
            arrowSerializer.finishCurrentBatch();
            pythonFunctionRunner.process(baos.toByteArray());
            elementCount += currentBatchCount;
            checkInvokeFinishBundleByCount();
            currentBatchCount = 0;
            baos.reset();
        }
    }
}
