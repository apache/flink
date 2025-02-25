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

package org.apache.flink.table.runtime.operators.deduplicate;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.runtime.operators.deduplicate.utils.DeduplicateFunctionHelper.checkInsertOnly;
import static org.apache.flink.table.runtime.operators.deduplicate.utils.DeduplicateFunctionHelper.shouldKeepCurrentRow;
import static org.apache.flink.table.runtime.operators.over.AbstractRowTimeUnboundedPrecedingOver.LATE_ELEMENTS_DROPPED_METRIC_NAME;
import static org.apache.flink.table.runtime.util.StateConfigUtil.createTtlConfig;

/**
 * This function is used to deduplicate on keys and keeps only first row on row time. It produces
 * append only stream thanks to emitting results only via firing the timers.
 */
public class RowTimeDeduplicateKeepFirstRowFunction
        extends KeyedProcessFunction<RowData, RowData, RowData> {

    private static final long serialVersionUID = 1L;

    // the TypeInformation of the values in the state.
    private final TypeInformation<RowData> typeInfo;
    private final long stateRetentionTime;
    private final int rowtimeIndex;

    // state stores previous message under the key.
    protected ValueState<RowData> waitingToEmitOnTimerState;
    protected ValueState<Boolean> alreadyEmittedState;

    private transient Counter numLateRecordsDropped;

    public RowTimeDeduplicateKeepFirstRowFunction(
            InternalTypeInfo<RowData> typeInfo, long minRetentionTime, int rowtimeIndex) {
        this.typeInfo = typeInfo;
        this.stateRetentionTime = minRetentionTime;
        this.rowtimeIndex = rowtimeIndex;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);

        // We don't enable TTL on the timer's state, because we rely on the state cleaning up on
        // watermark. Also otherwise TTL clean up before firing the watermark would cause a data
        // loss.
        ValueStateDescriptor<RowData> timerStateDesc =
                new ValueStateDescriptor<>("waiting-to-emit-on-timer", typeInfo);
        waitingToEmitOnTimerState = getRuntimeContext().getState(timerStateDesc);

        ValueStateDescriptor<Boolean> stateDesc =
                new ValueStateDescriptor<>("already-emitted-state-boolean", Types.BOOLEAN);
        StateTtlConfig ttlConfig = createTtlConfig(stateRetentionTime);
        if (ttlConfig.isEnabled()) {
            stateDesc.enableTimeToLive(ttlConfig);
        }
        alreadyEmittedState = getRuntimeContext().getState(stateDesc);
        numLateRecordsDropped =
                getRuntimeContext().getMetricGroup().counter(LATE_ELEMENTS_DROPPED_METRIC_NAME);
    }

    @Override
    public void processElement(RowData input, Context ctx, Collector<RowData> out)
            throws Exception {
        checkInsertOnly(input);
        long rowtime = input.getLong(rowtimeIndex);
        if (rowtime < ctx.timerService().currentWatermark()) {
            // ignore late record
            numLateRecordsDropped.inc();
            return;
        }
        Boolean allreadyEmitted = alreadyEmittedState.value();
        if (allreadyEmitted != null && allreadyEmitted) {
            // result has already been emitted, we can not retract/emit anything different.
            return;
        }
        RowData preRow = waitingToEmitOnTimerState.value();
        if (shouldKeepCurrentRow(preRow, input, rowtimeIndex, false)) {
            if (preRow != null) {
                ctx.timerService().deleteEventTimeTimer(preRow.getLong(rowtimeIndex));
            }
            ctx.timerService().registerEventTimeTimer(rowtime);
            waitingToEmitOnTimerState.update(input);
        }
    }

    @Override
    public void onTimer(
            long timestamp,
            KeyedProcessFunction<RowData, RowData, RowData>.OnTimerContext ctx,
            Collector<RowData> out)
            throws Exception {
        out.collect(waitingToEmitOnTimerState.value());
        waitingToEmitOnTimerState.clear();
        alreadyEmittedState.update(true);
    }

    @VisibleForTesting
    protected Counter getNumLateRecordsDropped() {
        return numLateRecordsDropped;
    }
}
