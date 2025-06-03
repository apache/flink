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

package org.apache.flink.table.runtime.operators.aggregate.async;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.functions.agg.BundledKeySegment;
import org.apache.flink.table.functions.agg.BundledKeySegmentApplied;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.operators.aggregate.RecordCounter;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.table.data.util.RowDataUtil.isRetractMsg;
import static org.apache.flink.table.runtime.util.StateConfigUtil.createTtlConfig;

/** Aggregate Function used for the groupby, which is asynchronous. */
public class BundledAggregateAsyncFunction
        extends KeyedAsyncFunctionCommon<RowData, RowData, RowData> {

    private final GeneratedAggsHandleFunction genAggsHandle;
    private final GeneratedRecordEqualiser genRecordEqualiser;
    private final LogicalType[] accTypes;
    private final RowType inputType;
    private final boolean generateUpdateBefore;
    private final long stateRetentionTime;
    private RecordCounter recordCounter;

    // function used to handle all aggregates
    private transient AggsHandleFunction function = null;

    // stores the accumulators
    private transient ValueState<RowData> accState = null;

    // function used to equal RowData
    private transient RecordEqualiser equaliser = null;
    private transient OpenContext ctx;

    public BundledAggregateAsyncFunction(
            GeneratedAggsHandleFunction genAggsHandle,
            GeneratedRecordEqualiser genRecordEqualiser,
            LogicalType[] accTypes,
            RowType inputType,
            int indexOfCountStar,
            boolean generateUpdateBefore,
            long stateRetentionTime) {
        this.genAggsHandle = genAggsHandle;
        this.genRecordEqualiser = genRecordEqualiser;
        this.accTypes = accTypes;
        this.inputType = inputType;
        this.recordCounter = RecordCounter.of(indexOfCountStar);
        this.generateUpdateBefore = generateUpdateBefore;
        this.stateRetentionTime = stateRetentionTime;
    }

    @Override
    public void open(OpenContext ctx) throws Exception {
        super.open(ctx);
        this.ctx = ctx;
        // instantiate function
        StateTtlConfig ttlConfig = createTtlConfig(stateRetentionTime);
        function = genAggsHandle.newInstance(ctx.getRuntimeContext().getUserCodeClassLoader());
        function.open(new PerKeyStateDataViewStore(ctx.getRuntimeContext(), ttlConfig));

        equaliser =
                genRecordEqualiser.newInstance(ctx.getRuntimeContext().getUserCodeClassLoader());

        InternalTypeInfo<RowData> accTypeInfo = InternalTypeInfo.ofFields(accTypes);
        ValueStateDescriptor<RowData> accDesc = new ValueStateDescriptor<>("accState", accTypeInfo);
        if (ttlConfig.isEnabled()) {
            accDesc.enableTimeToLive(ttlConfig);
        }
        accState = ctx.getRuntimeContext().getState(accDesc);
    }

    public void asyncInvokeProtected(RowData input, ResultFuture<RowData> resultFuture)
            throws Exception {
        RowData acc = accState.value();
        if (acc == null && isRetractMsg(input)) {
            handleResponseForAsyncInvoke(
                    () -> {
                        resultFuture.complete(Collections.emptyList());
                    });
            return;
        }
        BundledKeySegment bundledKeySegment =
                new BundledKeySegment(
                        ctx.currentKey(),
                        Collections.singletonList(input),
                        accState.value(),
                        false);
        CompletableFuture<BundledKeySegmentApplied> result = new CompletableFuture<>();
        function.bundledAccumulateRetract(result, bundledKeySegment);

        handleResponseForAsyncInvoke(
                result,
                resultFuture::completeExceptionally,
                bundledKeySegmentApplied -> {
                    resultFuture.complete(
                            handleResponse(bundledKeySegment, bundledKeySegmentApplied));
                });
    }

    private Collection<RowData> handleResponse(
            BundledKeySegment keySegment, BundledKeySegmentApplied updatedSegment)
            throws Exception {
        final boolean isFirstRow = keySegment.getAccumulator() == null;
        RowData currentKey = ctx.currentKey();

        // get previous aggregate result
        RowData prevAggValue = updatedSegment.getStartingValue();

        RowData acc = updatedSegment.getAccumulator();

        // get new aggregate result
        RowData newAggValue = updatedSegment.getFinalValue();

        List<RowData> output = new ArrayList<>();

        if (!recordCounter.recordCountIsZero(acc)) {
            // we aggregated at least one record for this key
            // update acc to state accState.update(acc);
            accState.update(acc);

            // if this was not the first row and we have to emit retractions
            if (!isFirstRow) {
                if (stateRetentionTime > 0 || !equaliser.equals(prevAggValue, newAggValue)) {
                    // new row is not same with prev row
                    if (generateUpdateBefore) {
                        // prepare UPDATE_BEFORE message for previous row
                        JoinedRowData resultRow =
                                new JoinedRowData(RowKind.UPDATE_BEFORE, currentKey, prevAggValue);
                        output.add(resultRow);
                    }
                    // prepare UPDATE_AFTER message for new row
                    JoinedRowData resultRow =
                            new JoinedRowData(RowKind.UPDATE_AFTER, currentKey, newAggValue);
                    output.add(resultRow);
                }
                // new row is same with prev row, no need to output
            } else {
                // this is the first, output new result
                // prepare INSERT message for new row
                JoinedRowData resultRow =
                        new JoinedRowData(RowKind.INSERT, currentKey, newAggValue);
                output.add(resultRow);
            }

        } else {
            // we retracted the last record for this key
            // if this is not first row sent out a DELETE message
            if (!isFirstRow) {
                // prepare DELETE message for previous row
                JoinedRowData resultRow =
                        new JoinedRowData(RowKind.DELETE, currentKey, prevAggValue);
                output.add(resultRow);
            }
            // and clear all state
            accState.clear();
            // cleanup dataview under current key
            function.cleanup();
        }
        return output;
    }
}
