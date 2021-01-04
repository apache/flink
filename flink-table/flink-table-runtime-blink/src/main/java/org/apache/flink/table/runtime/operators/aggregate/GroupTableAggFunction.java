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

package org.apache.flink.table.runtime.operators.aggregate;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.generated.GeneratedTableAggsHandleFunction;
import org.apache.flink.table.runtime.generated.TableAggsHandleFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.data.util.RowDataUtil.isAccumulateMsg;
import static org.apache.flink.table.runtime.util.StateTtlConfigUtil.createTtlConfig;

/** Aggregate Function used for the groupby (without window) table aggregate. */
public class GroupTableAggFunction extends KeyedProcessFunction<RowData, RowData, RowData> {

    private static final long serialVersionUID = 1L;

    /** The code generated function used to handle table aggregates. */
    private final GeneratedTableAggsHandleFunction genAggsHandler;

    /** The accumulator types. */
    private final LogicalType[] accTypes;

    /** Used to count the number of added and retracted input records. */
    private final RecordCounter recordCounter;

    /** Whether this operator will generate UPDATE_BEFORE messages. */
    private final boolean generateUpdateBefore;

    /** State idle retention time which unit is MILLISECONDS. */
    private final long stateRetentionTime;

    // function used to handle all table aggregates
    private transient TableAggsHandleFunction function = null;

    // stores the accumulators
    private transient ValueState<RowData> accState = null;

    /**
     * Creates a {@link GroupTableAggFunction}.
     *
     * @param genAggsHandler The code generated function used to handle table aggregates.
     * @param accTypes The accumulator types.
     * @param indexOfCountStar The index of COUNT(*) in the aggregates. -1 when the input doesn't
     *     contain COUNT(*), i.e. doesn't contain retraction messages. We make sure there is a
     *     COUNT(*) if input stream contains retraction.
     * @param generateUpdateBefore Whether this operator will generate UPDATE_BEFORE messages.
     * @param stateRetentionTime state idle retention time which unit is MILLISECONDS.
     */
    public GroupTableAggFunction(
            GeneratedTableAggsHandleFunction genAggsHandler,
            LogicalType[] accTypes,
            int indexOfCountStar,
            boolean generateUpdateBefore,
            long stateRetentionTime) {
        this.genAggsHandler = genAggsHandler;
        this.accTypes = accTypes;
        this.recordCounter = RecordCounter.of(indexOfCountStar);
        this.generateUpdateBefore = generateUpdateBefore;
        this.stateRetentionTime = stateRetentionTime;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // instantiate function
        StateTtlConfig ttlConfig = createTtlConfig(stateRetentionTime);
        function = genAggsHandler.newInstance(getRuntimeContext().getUserCodeClassLoader());
        function.open(new PerKeyStateDataViewStore(getRuntimeContext(), ttlConfig));

        InternalTypeInfo<RowData> accTypeInfo = InternalTypeInfo.ofFields(accTypes);
        ValueStateDescriptor<RowData> accDesc = new ValueStateDescriptor<>("accState", accTypeInfo);
        if (ttlConfig.isEnabled()) {
            accDesc.enableTimeToLive(ttlConfig);
        }
        accState = getRuntimeContext().getState(accDesc);
    }

    @Override
    public void processElement(RowData input, Context ctx, Collector<RowData> out)
            throws Exception {
        RowData currentKey = ctx.getCurrentKey();

        boolean firstRow;
        RowData accumulators = accState.value();
        if (null == accumulators) {
            firstRow = true;
            accumulators = function.createAccumulators();
        } else {
            firstRow = false;
        }

        // set accumulators to handler first
        function.setAccumulators(accumulators);

        if (!firstRow && generateUpdateBefore) {
            function.emitValue(out, currentKey, true);
        }

        // update aggregate result and set to the newRow
        if (isAccumulateMsg(input)) {
            // accumulate input
            function.accumulate(input);
        } else {
            // retract input
            function.retract(input);
        }

        // get accumulator
        accumulators = function.getAccumulators();
        if (!recordCounter.recordCountIsZero(accumulators)) {
            function.emitValue(out, currentKey, false);

            // update the state
            accState.update(accumulators);

        } else {
            // and clear all state
            accState.clear();
            // cleanup dataview under current key
            function.cleanup();
        }
    }

    @Override
    public void close() throws Exception {
        if (function != null) {
            function.close();
        }
    }
}
