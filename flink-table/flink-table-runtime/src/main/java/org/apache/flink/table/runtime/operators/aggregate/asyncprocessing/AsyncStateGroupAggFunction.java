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

package org.apache.flink.table.runtime.operators.aggregate.asyncprocessing;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.runtime.state.v2.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.operators.aggregate.GroupAggFunction;
import org.apache.flink.table.runtime.operators.aggregate.RecordCounter;
import org.apache.flink.table.runtime.operators.aggregate.utils.GroupAggHelper;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.runtime.util.StateConfigUtil.createTtlConfig;

/** Aggregate Function used for the groupby (without window) aggregate with async state api. */
public class AsyncStateGroupAggFunction extends KeyedProcessFunction<RowData, RowData, RowData> {

    private static final long serialVersionUID = 1L;

    /** The code generated function used to handle aggregates. */
    private final GeneratedAggsHandleFunction genAggsHandler;

    /** The code generated equaliser used to equal RowData. */
    private final GeneratedRecordEqualiser genRecordEqualiser;

    /** The accumulator types. */
    private final LogicalType[] accTypes;

    /** Used to count the number of added and retracted input records. */
    private final RecordCounter recordCounter;

    /** Whether this operator will generate UPDATE_BEFORE messages. */
    private final boolean generateUpdateBefore;

    /** State idle retention time which unit is MILLISECONDS. */
    private final long stateRetentionTime;

    // function used to handle all aggregates
    private transient AggsHandleFunction function = null;

    // function used to equal RowData
    private transient RecordEqualiser equaliser = null;

    // stores the accumulators
    private transient ValueState<RowData> accState = null;

    private transient AsyncStateGroupAggHelper aggHelper = null;

    /**
     * Creates a {@link GroupAggFunction}.
     *
     * @param genAggsHandler The code generated function used to handle aggregates.
     * @param genRecordEqualiser The code generated equaliser used to equal RowData.
     * @param accTypes The accumulator types.
     * @param indexOfCountStar The index of COUNT(*) in the aggregates. -1 when the input doesn't
     *     contain COUNT(*), i.e. doesn't contain retraction messages. We make sure there is a
     *     COUNT(*) if input stream contains retraction.
     * @param generateUpdateBefore Whether this operator will generate UPDATE_BEFORE messages.
     * @param stateRetentionTime state idle retention time which unit is MILLISECONDS.
     */
    public AsyncStateGroupAggFunction(
            GeneratedAggsHandleFunction genAggsHandler,
            GeneratedRecordEqualiser genRecordEqualiser,
            LogicalType[] accTypes,
            int indexOfCountStar,
            boolean generateUpdateBefore,
            long stateRetentionTime) {
        this.genAggsHandler = genAggsHandler;
        this.genRecordEqualiser = genRecordEqualiser;
        this.accTypes = accTypes;
        this.recordCounter = RecordCounter.of(indexOfCountStar);
        this.generateUpdateBefore = generateUpdateBefore;
        this.stateRetentionTime = stateRetentionTime;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        final StreamingRuntimeContext runtimeContext =
                (StreamingRuntimeContext) getRuntimeContext();

        // instantiate function
        StateTtlConfig ttlConfig = createTtlConfig(stateRetentionTime);
        function = genAggsHandler.newInstance(runtimeContext.getUserCodeClassLoader());
        function.open(new PerKeyStateDataViewStore(runtimeContext, ttlConfig));

        // instantiate equaliser
        equaliser = genRecordEqualiser.newInstance(runtimeContext.getUserCodeClassLoader());

        InternalTypeInfo<RowData> accTypeInfo = InternalTypeInfo.ofFields(accTypes);
        ValueStateDescriptor<RowData> accDesc = new ValueStateDescriptor<>("accState", accTypeInfo);
        if (ttlConfig.isEnabled()) {
            accDesc.enableTimeToLive(ttlConfig);
        }

        accState = runtimeContext.getValueState(accDesc);
        aggHelper = new AsyncStateGroupAggHelper();
    }

    @Override
    public void processElement(RowData input, Context ctx, Collector<RowData> out)
            throws Exception {
        RowData currentKey = ctx.getCurrentKey();
        accState.asyncValue()
                .thenAccept(acc -> aggHelper.processElement(input, currentKey, acc, out));
    }

    @Override
    public void close() throws Exception {
        if (function != null) {
            function.close();
        }
    }

    private class AsyncStateGroupAggHelper extends GroupAggHelper {

        public AsyncStateGroupAggHelper() {
            super(recordCounter, generateUpdateBefore, stateRetentionTime, function, equaliser);
        }

        @Override
        protected void updateAccumulatorsState(RowData accumulators) throws Exception {
            accState.update(accumulators);
        }

        @Override
        protected void clearAccumulatorsState() throws Exception {
            accState.clear();
        }
    }
}
