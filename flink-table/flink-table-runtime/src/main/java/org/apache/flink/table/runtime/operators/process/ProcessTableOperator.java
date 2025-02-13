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

package org.apache.flink.table.runtime.operators.process;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.runtime.generated.HashFunction;
import org.apache.flink.table.runtime.generated.ProcessTableRunner;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.util.StateConfigUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Operator for {@link ProcessTableFunction}. */
public class ProcessTableOperator extends AbstractStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData> {

    private final @Nullable RuntimeTableSemantics tableSemantics;
    private final List<RuntimeStateInfo> stateInfos;
    private final ProcessTableRunner processTableRunner;
    private final HashFunction[] stateHashCode;
    private final RecordEqualiser[] stateEquals;

    private transient PassThroughCollectorBase collector;
    private transient ValueStateDescriptor<RowData>[] stateDescriptors;
    private transient ValueState<RowData>[] stateHandles;
    private transient RowData[] stateToFunction;
    private transient boolean[] stateCleared;
    private transient RowData[] stateFromFunction;

    public ProcessTableOperator(
            StreamOperatorParameters<RowData> parameters,
            @Nullable RuntimeTableSemantics tableSemantics,
            List<RuntimeStateInfo> stateInfos,
            ProcessTableRunner processTableRunner,
            HashFunction[] stateHashCode,
            RecordEqualiser[] stateEquals) {
        super(parameters);
        this.tableSemantics = tableSemantics;
        this.stateInfos = stateInfos;
        this.processTableRunner = processTableRunner;
        this.stateHashCode = stateHashCode;
        this.stateEquals = stateEquals;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void open() throws Exception {
        super.open();

        // Pass through columns
        if (tableSemantics == null || tableSemantics.passColumnsThrough()) {
            collector = new PassAllCollector(output);
        } else {
            collector = new PassPartitionKeysCollector(output, tableSemantics.partitionByColumns());
        }
        processTableRunner.runnerCollector = this.collector;

        // State
        final KeyedStateStore keyedStateStore = getKeyedStateStore();
        stateDescriptors = new ValueStateDescriptor[stateInfos.size()];
        stateHandles = new ValueState[stateInfos.size()];
        stateToFunction = new RowData[stateInfos.size()];
        stateCleared = new boolean[stateInfos.size()];
        stateFromFunction = new RowData[stateInfos.size()];
        for (int i = 0; i < stateInfos.size(); i++) {
            final RuntimeStateInfo stateInfo = stateInfos.get(i);
            final LogicalType type = stateInfo.getType();
            final ValueStateDescriptor<RowData> descriptor =
                    new ValueStateDescriptor(
                            stateInfo.getStateName(), InternalSerializers.create(type));
            final StateTtlConfig ttlConfig =
                    StateConfigUtil.createTtlConfig(stateInfo.getTimeToLive());
            if (ttlConfig.isEnabled()) {
                descriptor.enableTimeToLive(ttlConfig);
            }
            stateDescriptors[i] = descriptor;
            stateHandles[i] = keyedStateStore.getState(descriptor);
        }

        // Context
        processTableRunner.runnerContext = new ProcessFunctionContext();

        // Prepare runner
        FunctionUtils.setFunctionRuntimeContext(processTableRunner, getRuntimeContext());
        FunctionUtils.openFunction(processTableRunner, DefaultOpenContext.INSTANCE);
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        collector.setInput(element.getValue());

        // For each function call:
        // - the state is read from Flink
        // - converted into external data structure
        // - evaluated
        // - converted into internal data structure (if not cleared)
        // - the state is written into Flink

        moveStateToFunction();
        processTableRunner.processElement(
                stateToFunction, stateCleared, stateFromFunction, 0, element.getValue());
        moveStateFromFunction();
    }

    private void moveStateToFunction() throws IOException {
        Arrays.fill(stateCleared, false);
        for (int i = 0; i < stateHandles.length; i++) {
            final RowData value = stateHandles[i].value();
            stateToFunction[i] = value;
        }
    }

    private void moveStateFromFunction() throws IOException {
        for (int i = 0; i < stateHandles.length; i++) {
            final RowData fromFunction = stateFromFunction[i];
            if (fromFunction == null || isEmpty(fromFunction)) {
                // Reduce state size
                stateHandles[i].clear();
            } else {
                final HashFunction hashCode = this.stateHashCode[i];
                final RecordEqualiser equals = this.stateEquals[i];
                final RowData toFunction = stateToFunction[i];
                // Reduce state updates by checking if something has changed
                if (toFunction == null
                        || hashCode.hashCode(toFunction) != hashCode.hashCode(fromFunction)
                        || !equals.equals(toFunction, fromFunction)) {
                    stateHandles[i].update(fromFunction);
                }
            }
        }
    }

    private static boolean isEmpty(RowData row) {
        for (int i = 0; i < row.getArity(); i++) {
            if (!row.isNullAt(i)) {
                return false;
            }
        }
        return row.getRowKind() == RowKind.INSERT;
    }

    @Internal
    public class ProcessFunctionContext implements ProcessTableFunction.Context {

        private final Map<String, RuntimeTableSemantics> tableSemanticsMap;
        private final Map<String, Integer> stateNameToPosMap;

        ProcessFunctionContext() {
            this.tableSemanticsMap =
                    Optional.ofNullable(tableSemantics)
                            .map(s -> Map.of(tableSemantics.getArgName(), tableSemantics))
                            .orElse(Map.of());
            this.stateNameToPosMap = new HashMap<>();
            for (int i = 0; i < stateInfos.size(); i++) {
                this.stateNameToPosMap.put(stateInfos.get(i).getStateName(), i);
            }
        }

        @Override
        public TableSemantics tableSemanticsFor(String argName) {
            final RuntimeTableSemantics tableSemantics = tableSemanticsMap.get(argName);
            if (tableSemantics == null) {
                throw new TableRuntimeException("Unknown table argument: " + argName);
            }
            return tableSemantics;
        }

        @Override
        public void clearState(String stateName) {
            final Integer statePos = stateNameToPosMap.get(stateName);
            if (statePos == null) {
                throw new TableRuntimeException("Unknown state entry: " + stateName);
            }
            stateCleared[statePos] = true;
        }

        @Override
        public void clearAllState() {
            Arrays.fill(stateCleared, true);
        }

        @VisibleForTesting
        public ValueStateDescriptor<RowData> getValueStateDescriptor(String stateName) {
            final Integer statePos = stateNameToPosMap.get(stateName);
            if (statePos == null) {
                throw new TableRuntimeException("Unknown state entry: " + stateName);
            }
            return stateDescriptors[statePos];
        }
    }
}
