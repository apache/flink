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
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.functions.KeyedProcessFunctionWithCleanupState;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.operators.over.AbstractRowTimeUnboundedPrecedingOver.ACCUMULATOR_STATE_NAME;
import static org.apache.flink.table.runtime.operators.over.AbstractRowTimeUnboundedPrecedingOver.CLEANUP_STATE_NAME;
import static org.apache.flink.table.runtime.operators.over.AbstractRowTimeUnboundedPrecedingOver.INPUT_STATE_NAME;
import static org.apache.flink.table.runtime.operators.over.AbstractRowTimeUnboundedPrecedingOver.LATE_ELEMENTS_DROPPED_METRIC_NAME;
import static org.apache.flink.table.runtime.operators.over.RowTimeRangeUnboundedPrecedingFunction.processElementsWithSameTimestampRange;
import static org.apache.flink.table.runtime.operators.over.RowTimeRowsUnboundedPrecedingFunction.processElementsWithSameTimestampRows;

/**
 * A ProcessFunction to support unbounded ROWS and RANGE windows.
 *
 * <p>ROWS E.g.: SELECT rowtime, b, c, min(c) OVER (PARTITION BY b ORDER BY rowtime ROWS BETWEEN
 * UNBOUNDED preceding AND CURRENT ROW), max(c) OVER (PARTITION BY b ORDER BY rowtime ROWS BETWEEN
 * UNBOUNDED preceding AND CURRENT ROW) FROM T.
 *
 * <p>RANGE E.g.: SELECT rowtime, b, c, min(c) OVER (PARTITION BY b ORDER BY rowtime RANGE BETWEEN
 * UNBOUNDED preceding AND CURRENT ROW), max(c) OVER (PARTITION BY b ORDER BY rowtime RANGE BETWEEN
 * UNBOUNDED preceding AND CURRENT ROW) FROM T.
 */
public class RowTimeUnboundedPrecedingOverFunctionV2<K>
        extends KeyedProcessFunctionWithCleanupState<K, RowData, RowData> {
    public static final int SECOND_OVER_VERSION = 2;

    private static final long serialVersionUID = 1L;

    private static final Logger LOG =
            LoggerFactory.getLogger(RowTimeUnboundedPrecedingOverFunctionV2.class);

    // whether this is a ROWS or RANGE operation
    private final boolean isRowsWindow;
    private final GeneratedAggsHandleFunction genAggsHandler;
    private final LogicalType[] accTypes;
    private final LogicalType[] inputFieldTypes;
    private final int rowTimeIdx;

    protected transient JoinedRowData output;
    // state to hold the accumulators of the aggregations
    private transient ValueState<RowData> accState;
    // state to hold rows until the next watermark arrives
    private transient MapState<Long, List<RowData>> inputState;

    protected transient AggsHandleFunction function;

    private transient Counter numLateRecordsDropped;

    @VisibleForTesting
    protected Counter getCounter() {
        return numLateRecordsDropped;
    }

    public RowTimeUnboundedPrecedingOverFunctionV2(
            boolean isRowsWindow,
            long minRetentionTime,
            long maxRetentionTime,
            GeneratedAggsHandleFunction genAggsHandler,
            LogicalType[] accTypes,
            LogicalType[] inputFieldTypes,
            int rowTimeIdx) {
        super(minRetentionTime, maxRetentionTime);
        this.isRowsWindow = isRowsWindow;
        this.genAggsHandler = genAggsHandler;
        this.accTypes = accTypes;
        this.inputFieldTypes = inputFieldTypes;
        this.rowTimeIdx = rowTimeIdx;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        function = genAggsHandler.newInstance(getRuntimeContext().getUserCodeClassLoader());
        function.open(new PerKeyStateDataViewStore(getRuntimeContext()));

        output = new JoinedRowData();

        // initialize accumulator state
        InternalTypeInfo<RowData> accTypeInfo = InternalTypeInfo.ofFields(accTypes);
        ValueStateDescriptor<RowData> accStateDesc =
                new ValueStateDescriptor<>(ACCUMULATOR_STATE_NAME, accTypeInfo);
        accState = getRuntimeContext().getState(accStateDesc);

        // input element are all binary row as they are came from network
        InternalTypeInfo<RowData> inputType = InternalTypeInfo.ofFields(inputFieldTypes);
        ListTypeInfo<RowData> rowListTypeInfo = new ListTypeInfo<>(inputType);
        MapStateDescriptor<Long, List<RowData>> inputStateDesc =
                new MapStateDescriptor<>(INPUT_STATE_NAME, Types.LONG, rowListTypeInfo);
        inputState = getRuntimeContext().getMapState(inputStateDesc);

        initCleanupTimeState(CLEANUP_STATE_NAME);

        // metrics
        this.numLateRecordsDropped =
                getRuntimeContext().getMetricGroup().counter(LATE_ELEMENTS_DROPPED_METRIC_NAME);
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

        long timestamp = input.getLong(rowTimeIdx);
        long curWatermark = ctx.timerService().currentWatermark();

        if (timestamp <= curWatermark) {
            // discard late record
            numLateRecordsDropped.inc();
            return;
        }
        // put row into state
        List<RowData> rowList = inputState.get(timestamp);
        if (rowList == null) {
            rowList = new ArrayList<>();
            // if that's the first timestamp for the given key, register the timer to process
            // those records.
            ctx.timerService().registerEventTimeTimer(timestamp);
        }
        rowList.add(input);
        inputState.put(timestamp, rowList);
    }

    @Override
    public void onTimer(
            long timestamp,
            KeyedProcessFunction<K, RowData, RowData>.OnTimerContext ctx,
            Collector<RowData> out)
            throws Exception {
        if (isProcessingTimeTimer(ctx)) {
            cleanupState(ctx);
            return;
        }

        RowData lastAccumulator = accState.value();
        if (lastAccumulator == null) {
            lastAccumulator = function.createAccumulators();
        }
        function.setAccumulators(lastAccumulator);

        processElementsWithSameTimestamp(timestamp, out);

        lastAccumulator = function.getAccumulators();
        accState.update(lastAccumulator);

        registerProcessingCleanupTimer(ctx, ctx.timerService().currentProcessingTime());
    }

    /**
     * Process the same timestamp datas, the mechanism is different between rows and range window.
     */
    private void processElementsWithSameTimestamp(long timestamp, Collector<RowData> out)
            throws Exception {
        List<RowData> curRowList = inputState.get(timestamp);
        if (curRowList == null) {
            // Ignore the same timestamp datas if the state is cleared already.
            LOG.warn(
                    "The state is cleared because of state ttl. "
                            + "This will result in incorrect result. "
                            + "You can increase the state ttl to avoid this.");
        } else {
            if (isRowsWindow) {
                processElementsWithSameTimestampRows(function, output, curRowList, out);
            } else {
                processElementsWithSameTimestampRange(function, output, curRowList, out);
            }
        }
        inputState.remove(timestamp);
    }

    private void cleanupState(OnTimerContext ctx) throws Exception {
        if (stateCleaningEnabled) {
            // we check whether there are still records which have not been processed yet
            if (inputState.isEmpty()) {
                // we clean the state
                cleanupState(inputState, accState);
                function.cleanup();
            } else {
                // There are records left to process because a watermark has not been received
                // yet.
                // This would only happen if the input stream has stopped. So we don't need to
                // clean up.
                // We leave the state as it is and schedule a new cleanup timer
                registerProcessingCleanupTimer(ctx, ctx.timerService().currentProcessingTime());
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (null != function) {
            function.close();
        }
    }
}
