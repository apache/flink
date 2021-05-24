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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;

/**
 * The Stream Arrow Python {@link AggregateFunction} Operator for RANGE clause event-time bounded
 * OVER window.
 */
@Internal
public class StreamArrowPythonRowTimeBoundedRangeOperator<K>
        extends AbstractStreamArrowPythonBoundedRangeOperator<K> {

    private static final long serialVersionUID = 1L;

    public StreamArrowPythonRowTimeBoundedRangeOperator(
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
    public void bufferInput(RowData input) throws Exception {
        long triggeringTs = input.getLong(inputTimeFieldIndex);
        Long lastTriggeringTs = lastTriggeringTsState.value();
        if (lastTriggeringTs == null) {
            lastTriggeringTs = 0L;
        }
        if (triggeringTs > lastTriggeringTs) {
            List<RowData> data = inputState.get(triggeringTs);
            if (null != data) {
                data.add(input);
                inputState.put(triggeringTs, data);
            } else {
                data = new ArrayList<>();
                data.add(input);
                inputState.put(triggeringTs, data);
                // register event time timer
                timerService.registerEventTimeTimer(triggeringTs);
            }
            registerCleanupTimer(triggeringTs, TimeDomain.EVENT_TIME);
        }
    }
}
