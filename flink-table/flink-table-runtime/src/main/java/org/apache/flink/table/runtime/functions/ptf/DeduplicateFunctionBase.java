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

package org.apache.flink.table.runtime.functions.ptf;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.StateTypeStrategy;
import org.apache.flink.table.types.inference.strategies.ChangelogTypeStrategyUtils;
import org.apache.flink.table.types.inference.utils.CallContextUtils;

import java.time.Duration;
import java.util.LinkedHashMap;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.IDLE_STATE_RETENTION;
import static org.apache.flink.table.types.inference.strategies.DeduplicateTypeStrategy.ARG_STATE_TTL;

/** Base class of the {@link Deduplicate} implementations. */
@Internal
abstract class DeduplicateFunctionBase extends BuiltInProcessTableFunction<RowData> {

    private static final long serialVersionUID = 1L;

    protected final int[] outputIndices;

    protected final boolean isTtlEnabled;

    DeduplicateFunctionBase(
            final SpecializedContext context,
            final LinkedHashMap<String, StateTypeStrategy> stateStrategies) {
        super(BuiltInFunctionDefinitions.DEDUPLICATE, context, stateStrategies);
        final CallContext callContext = context.getCallContext();
        this.outputIndices =
                ChangelogTypeStrategyUtils.computeOutputIndices(
                        DeduplicateUtils.tableSemantics(context));
        final Duration effectiveTtl =
                CallContextUtils.getStateTtl(callContext, ARG_STATE_TTL)
                        .orElseGet(() -> context.getConfiguration().get(IDLE_STATE_RETENTION));
        this.isTtlEnabled = !effectiveTtl.isZero();
    }
}
