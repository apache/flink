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

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.state.v2.ValueStateDescriptor;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.operators.aggregate.GroupAggFunctionBase;
import org.apache.flink.table.runtime.operators.aggregate.utils.GroupAggHelper;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Aggregate Function used for the groupby (without window) aggregate with async state api. */
public class AsyncStateGroupAggFunction extends GroupAggFunctionBase {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(AsyncStateGroupAggFunction.class);

    // stores the accumulators
    private transient ValueState<RowData> accState = null;

    private transient AsyncStateGroupAggHelper aggHelper = null;

    /**
     * Creates a {@link AsyncStateGroupAggFunction}.
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
        super(
                genAggsHandler,
                genRecordEqualiser,
                accTypes,
                indexOfCountStar,
                generateUpdateBefore,
                stateRetentionTime);
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);

        LOG.info("Group agg is using async state");

        InternalTypeInfo<RowData> accTypeInfo = InternalTypeInfo.ofFields(accTypes);
        ValueStateDescriptor<RowData> accDesc = new ValueStateDescriptor<>("accState", accTypeInfo);
        if (ttlConfig.isEnabled()) {
            accDesc.enableTimeToLive(ttlConfig);
        }

        accState = getRuntimeContext().getState(accDesc);
        aggHelper = new AsyncStateGroupAggHelper();
    }

    @Override
    public void processElement(RowData input, Context ctx, Collector<RowData> out)
            throws Exception {
        RowData currentKey = ctx.getCurrentKey();
        accState.asyncValue()
                .thenAccept(acc -> aggHelper.processElement(input, currentKey, acc, out));
    }

    private class AsyncStateGroupAggHelper extends GroupAggHelper {

        public AsyncStateGroupAggHelper() {
            super(recordCounter, generateUpdateBefore, ttlConfig, function, equaliser);
        }

        @Override
        protected void updateAccumulatorsState(RowData accumulators) throws Exception {
            accState.asyncUpdate(accumulators);
        }

        @Override
        protected void clearAccumulatorsState() throws Exception {
            accState.asyncClear();
        }
    }
}
