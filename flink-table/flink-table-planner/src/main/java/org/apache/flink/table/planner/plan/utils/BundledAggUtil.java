/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.functions.BundledAggregateFunction;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlAggFunction;

import org.apache.calcite.rel.core.AggregateCall;

/** Utility class for bundled aggregate functions. */
public class BundledAggUtil {
    public static boolean containsBatchAggCall(AggregateCall aggregateCall) {
        if (aggregateCall.getAggregation() instanceof BridgingSqlAggFunction) {
            BridgingSqlAggFunction bridgingSqlAggFunction =
                    (BridgingSqlAggFunction) aggregateCall.getAggregation();
            return bridgingSqlAggFunction.getDefinition().getKind() == FunctionKind.AGGREGATE
                    && (bridgingSqlAggFunction.getDefinition() instanceof BundledAggregateFunction
                            && ((BundledAggregateFunction) bridgingSqlAggFunction.getDefinition())
                                    .canBundle());
        }
        return false;
    }

    /** Returns the batch size from the config. */
    public static int bufferCapacity(ReadableConfig config) {
        int capacity = config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_AGG_BUFFER_CAPACITY);
        if (capacity <= 0) {
            throw new IllegalArgumentException(
                    ExecutionConfigOptions.TABLE_EXEC_ASYNC_AGG_BUFFER_CAPACITY + " must be > 0.");
        }
        return capacity;
    }

    /** Returns the max latency from the config. */
    public static long timeout(ReadableConfig config) {
        return config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_AGG_TIMEOUT).toMillis();
    }
}
