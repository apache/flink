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

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Collector;

/**
 * Process Function for processing-time unbounded OVER window.
 *
 * <p>E.g.: SELECT currtime, b, c, min(c) OVER (PARTITION BY b ORDER BY proctime ROWS BETWEEN
 * UNBOUNDED preceding AND CURRENT ROW), max(c) OVER (PARTITION BY b ORDER BY proctime ROWS BETWEEN
 * UNBOUNDED preceding AND CURRENT ROW) FROM T.
 */
public class ProcTimeUnboundedPrecedingFunction<K>
        extends KeyedProcessFunction<K, RowData, RowData> {
    private static final long serialVersionUID = 2L;

    private final StateTtlConfig ttlConfig;
    private final GeneratedAggsHandleFunction genAggsHandler;
    private final LogicalType[] accTypes;

    private transient AggsHandleFunction function;
    private transient ValueState<RowData> accState;
    private transient JoinedRowData output;

    public ProcTimeUnboundedPrecedingFunction(
            StateTtlConfig ttlConfig,
            GeneratedAggsHandleFunction genAggsHandler,
            LogicalType[] accTypes) {
        this.ttlConfig = ttlConfig;
        this.genAggsHandler = genAggsHandler;
        this.accTypes = accTypes;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        function = genAggsHandler.newInstance(getRuntimeContext().getUserCodeClassLoader());
        function.open(new PerKeyStateDataViewStore(getRuntimeContext()));

        output = new JoinedRowData();

        InternalTypeInfo<RowData> accTypeInfo = InternalTypeInfo.ofFields(accTypes);
        ValueStateDescriptor<RowData> stateDescriptor =
                new ValueStateDescriptor<>("accState", accTypeInfo);
        if (ttlConfig.isEnabled()) {
            stateDescriptor.enableTimeToLive(ttlConfig);
        }
        accState = getRuntimeContext().getState(stateDescriptor);
    }

    @Override
    public void processElement(
            RowData input,
            KeyedProcessFunction<K, RowData, RowData>.Context ctx,
            Collector<RowData> out)
            throws Exception {
        RowData accumulators = accState.value();
        if (null == accumulators) {
            accumulators = function.createAccumulators();
        }
        // set accumulators in context first
        function.setAccumulators(accumulators);

        // accumulate input row
        function.accumulate(input);

        // update the value of accumulators for future incremental computation
        accumulators = function.getAccumulators();
        accState.update(accumulators);

        // prepare output row
        RowData aggValue = function.getValue();
        output.replace(input, aggValue);
        out.collect(output);
    }

    @Override
    public void close() throws Exception {
        if (null != function) {
            function.close();
        }
    }
}
