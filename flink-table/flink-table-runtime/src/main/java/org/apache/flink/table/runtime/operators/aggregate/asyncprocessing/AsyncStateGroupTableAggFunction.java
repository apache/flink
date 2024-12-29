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
import org.apache.flink.table.runtime.generated.GeneratedTableAggsHandleFunction;
import org.apache.flink.table.runtime.generated.TableAggsHandleFunction;
import org.apache.flink.table.runtime.operators.aggregate.RecordCounter;
import org.apache.flink.table.runtime.operators.aggregate.utils.GroupTableAggHelper;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.runtime.util.StateConfigUtil.createTtlConfig;

/** Aggregate Function used for the groupby (without window) table aggregate in async state. */
public class AsyncStateGroupTableAggFunction
        extends KeyedProcessFunction<RowData, RowData, RowData> {

    private static final long serialVersionUID = 1L;

    /** The code generated function used to handle table aggregates. */
    private final GeneratedTableAggsHandleFunction genAggsHandler;

    /** The accumulator types. */
    private final LogicalType[] accTypes;

    /** Used to count the number of added and retracted input records. */
    private final RecordCounter recordCounter;

    /** Whether this operator will generate UPDATE_BEFORE messages. */
    private final boolean generateUpdateBefore;

    private final boolean incrementalUpdate;

    /** State idle retention time which unit is MILLISECONDS. */
    private final long stateRetentionTime;

    // function used to handle all table aggregates
    private transient TableAggsHandleFunction function = null;

    // stores the accumulators
    private transient ValueState<RowData> accState = null;

    private transient AsyncStateGroupTableAggHelper aggHelper = null;

    /**
     * Creates a {@link AsyncStateGroupTableAggFunction}.
     *
     * @param genAggsHandler The code generated function used to handle table aggregates.
     * @param accTypes The accumulator types.
     * @param indexOfCountStar The index of COUNT(*) in the aggregates. -1 when the input doesn't
     *     contain COUNT(*), i.e. doesn't contain retraction messages. We make sure there is a
     *     COUNT(*) if input stream contains retraction.
     * @param generateUpdateBefore Whether this operator will generate UPDATE_BEFORE messages.
     * @param incrementalUpdate Whether to update acc result incrementally.
     * @param stateRetentionTime state idle retention time which unit is MILLISECONDS.
     */
    public AsyncStateGroupTableAggFunction(
            GeneratedTableAggsHandleFunction genAggsHandler,
            LogicalType[] accTypes,
            int indexOfCountStar,
            boolean generateUpdateBefore,
            boolean incrementalUpdate,
            long stateRetentionTime) {
        this.genAggsHandler = genAggsHandler;
        this.accTypes = accTypes;
        this.recordCounter = RecordCounter.of(indexOfCountStar);
        this.generateUpdateBefore = generateUpdateBefore;
        this.incrementalUpdate = incrementalUpdate;
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

        InternalTypeInfo<RowData> accTypeInfo = InternalTypeInfo.ofFields(accTypes);
        ValueStateDescriptor<RowData> accDesc = new ValueStateDescriptor<>("accState", accTypeInfo);
        if (ttlConfig.isEnabled()) {
            accDesc.enableTimeToLive(ttlConfig);
        }
        accState = runtimeContext.getValueState(accDesc);
        aggHelper = new AsyncStateGroupTableAggHelper();
    }

    @Override
    public void processElement(RowData input, Context ctx, Collector<RowData> out)
            throws Exception {
        RowData currentKey = ctx.getCurrentKey();

        accState.asyncValue()
                .thenAccept(
                        accumulators -> {
                            accumulators =
                                    aggHelper.processElement(accumulators, currentKey, out, input);

                            if (!recordCounter.recordCountIsZero(accumulators)) {
                                function.emitValue(out, currentKey, false);

                                // update the state
                                accState.asyncUpdate(accumulators);

                            } else {
                                // and clear all state
                                accState.asyncClear();
                                // cleanup dataview under current key
                                function.cleanup();
                            }
                        });
    }

    @Override
    public void close() throws Exception {
        if (function != null) {
            function.close();
        }
    }

    private class AsyncStateGroupTableAggHelper extends GroupTableAggHelper {
        public AsyncStateGroupTableAggHelper() {
            super(generateUpdateBefore, incrementalUpdate, function);
        }
    }
}
