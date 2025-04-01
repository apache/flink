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
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.functions.ProcessTableFunction.TimeContext;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.runtime.generated.HashFunction;
import org.apache.flink.table.runtime.generated.ProcessTableRunner;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.operators.process.TimeConverter.InstantTimeConverter;
import org.apache.flink.table.runtime.operators.process.TimeConverter.LocalDateTimeConverter;
import org.apache.flink.table.runtime.operators.process.TimeConverter.LongTimeConverter;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.StringDataSerializer;
import org.apache.flink.table.runtime.util.StateConfigUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Operator for {@link ProcessTableFunction}. */
public class ProcessTableOperator extends AbstractStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData>, Triggerable<RowData, Object> {

    private final @Nullable RuntimeTableSemantics tableSemantics;
    private final List<RuntimeStateInfo> stateInfos;
    private final ProcessTableRunner processTableRunner;
    private final HashFunction[] stateHashCode;
    private final RecordEqualiser[] stateEquals;

    private transient ReadableInternalTimeContext internalTimeContext;
    private transient PassThroughCollectorBase evalCollector;
    private transient PassAllCollector onTimerCollector;
    private transient ValueStateDescriptor<RowData>[] stateDescriptors;
    private transient ValueState<RowData>[] stateHandles;

    private transient @Nullable MapState<StringData, Long> namedTimersMapState;
    private transient @Nullable InternalTimerService<StringData> namedTimerService;
    private transient @Nullable InternalTimerService<VoidNamespace> unnamedTimerService;

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
    public void open() throws Exception {
        super.open();

        final RunnerContext runnerContext = new RunnerContext();
        final RunnerOnTimerContext runnerOnTimerContext = new RunnerOnTimerContext();

        setTimerServices();
        setTimeContext();
        setCollectors();
        setStateDescriptors();
        setStateHandles();

        processTableRunner.initialize(
                stateHandles,
                stateHashCode,
                stateEquals,
                shouldEmitRowtime(),
                runnerContext,
                runnerOnTimerContext,
                evalCollector,
                onTimerCollector);

        // Open runner
        FunctionUtils.setFunctionRuntimeContext(processTableRunner, getRuntimeContext());
        FunctionUtils.openFunction(processTableRunner, DefaultOpenContext.INSTANCE);
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        // Set table argument
        if (tableSemantics != null) {
            processTableRunner.ingestTableEvent(0, element.getValue(), tableSemantics.timeColumn());
        }
        processTableRunner.processEval();
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        super.processWatermark(mark);
        processTableRunner.ingestWatermarkEvent(mark.getTimestamp());
    }

    @Override
    public void onEventTime(InternalTimer<RowData, Object> timer) throws Exception {
        final Object namedTimer = timer.getNamespace();
        processTableRunner.ingestTimerEvent(
                timer.getKey(),
                namedTimer == VoidNamespace.INSTANCE ? null : (StringData) namedTimer,
                timer.getTimestamp());
        processTableRunner.processOnTimer();
    }

    @Override
    public void onProcessingTime(InternalTimer<RowData, Object> timer) throws Exception {}

    /** Implementation of {@link ProcessTableFunction.Context}. */
    @Internal
    public class RunnerContext implements ProcessTableFunction.Context {

        private final Map<String, RuntimeTableSemantics> tableSemanticsMap;
        private final Map<String, Integer> stateNameToPosMap;

        RunnerContext() {
            this.tableSemanticsMap = createTableSemanticsMap();
            this.stateNameToPosMap = createStateNameToPosMap();
        }

        private Map<String, RuntimeTableSemantics> createTableSemanticsMap() {
            return Optional.ofNullable(tableSemantics)
                    .map(s -> Map.of(tableSemantics.getArgName(), tableSemantics))
                    .orElse(Map.of());
        }

        private Map<String, Integer> createStateNameToPosMap() {
            final Map<String, Integer> stateNameToPosMap = new HashMap<>();
            for (int i = 0; i < stateInfos.size(); i++) {
                stateNameToPosMap.put(stateInfos.get(i).getStateName(), i);
            }
            return stateNameToPosMap;
        }

        @Override
        @SuppressWarnings({"unchecked"})
        public <TimeType> TimeContext<TimeType> timeContext(Class<TimeType> conversionClass) {
            final TimeConverter<?> timeConverter;
            if (conversionClass == Instant.class) {
                timeConverter = InstantTimeConverter.INSTANCE;
            } else if (conversionClass == LocalDateTime.class) {
                timeConverter = LocalDateTimeConverter.INSTANCE;
            } else if (conversionClass == Long.class) {
                timeConverter = LongTimeConverter.INSTANCE;
            } else {
                throw new TableRuntimeException(
                        "Unsupported conversion class for TimeContext: "
                                + conversionClass.getName());
            }

            internalTimeContext.setTime(
                    processTableRunner.getCurrentWatermark(), processTableRunner.getTime());

            return (TimeContext<TimeType>)
                    new ExternalTimeContext<>(internalTimeContext, timeConverter);
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
            processTableRunner.clearState(statePos);
        }

        @Override
        public void clearAllState() {
            processTableRunner.clearAllState();
        }

        @Override
        public void clearAllTimers() {
            internalTimeContext.clearAllTimers();
        }

        @Override
        public void clearAll() {
            clearAllState();
            clearAllTimers();
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

    /** Implementation of {@link ProcessTableFunction.OnTimerContext}. */
    @Internal
    public class RunnerOnTimerContext extends RunnerContext
            implements ProcessTableFunction.OnTimerContext {

        @Override
        public @Nullable String currentTimer() {
            final StringData timerName = processTableRunner.getTimerName();
            return timerName == null ? null : timerName.toString();
        }
    }

    // --------------------------------------------------------------------------------------------
    // Instances from table semantics
    // --------------------------------------------------------------------------------------------

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void setTimerServices() {
        if (shouldEnableTimers()) {
            final KeyedStateStore keyedStateStore = getKeyedStateStore();
            final MapStateDescriptor<StringData, Long> namedTimersDescriptor =
                    new MapStateDescriptor<>(
                            "internal-named-timers-map",
                            StringDataSerializer.INSTANCE,
                            LongSerializer.INSTANCE);
            namedTimersMapState = keyedStateStore.getMapState(namedTimersDescriptor);
            namedTimerService =
                    getInternalTimerService(
                            "user-named-timers", StringDataSerializer.INSTANCE, (Triggerable) this);
            unnamedTimerService =
                    getInternalTimerService(
                            "user-unnamed-timers",
                            VoidNamespaceSerializer.INSTANCE,
                            (Triggerable) this);
        } else {
            namedTimersMapState = null;
            namedTimerService = null;
            unnamedTimerService = null;
        }
    }

    private void setTimeContext() {
        if (shouldEnableTimers()) {
            internalTimeContext =
                    new WritableInternalTimeContext(
                            namedTimersMapState, namedTimerService, unnamedTimerService);
        } else {
            internalTimeContext = new ReadableInternalTimeContext();
        }
    }

    private void setCollectors() {
        if (tableSemantics == null || tableSemantics.passColumnsThrough()) {
            evalCollector = new PassAllCollector(output);
        } else {
            evalCollector =
                    new PassPartitionKeysCollector(output, tableSemantics.partitionByColumns());
        }
        onTimerCollector = new PassAllCollector(output);
    }

    @SuppressWarnings("unchecked")
    private void setStateDescriptors() {
        final ValueStateDescriptor<RowData>[] stateDescriptors =
                new ValueStateDescriptor[stateInfos.size()];
        for (int i = 0; i < stateInfos.size(); i++) {
            final RuntimeStateInfo stateInfo = stateInfos.get(i);
            final LogicalType type = stateInfo.getType();
            final ValueStateDescriptor<RowData> stateDescriptor =
                    new ValueStateDescriptor<>(
                            stateInfo.getStateName(), InternalSerializers.create(type));
            final StateTtlConfig ttlConfig =
                    StateConfigUtil.createTtlConfig(stateInfo.getTimeToLive());
            if (ttlConfig.isEnabled()) {
                stateDescriptor.enableTimeToLive(ttlConfig);
            }
            stateDescriptors[i] = stateDescriptor;
        }
        this.stateDescriptors = stateDescriptors;
    }

    @SuppressWarnings("unchecked")
    private void setStateHandles() {
        final KeyedStateStore keyedStateStore = getKeyedStateStore();
        final ValueState<RowData>[] stateHandles = new ValueState[stateDescriptors.length];
        for (int i = 0; i < stateInfos.size(); i++) {
            stateHandles[i] = keyedStateStore.getState(stateDescriptors[i]);
        }
        this.stateHandles = stateHandles;
    }

    private boolean shouldEmitRowtime() {
        return tableSemantics != null && tableSemantics.timeColumn() != -1;
    }

    private boolean shouldEnableTimers() {
        return tableSemantics != null
                && tableSemantics.hasSetSemantics()
                && !tableSemantics.passColumnsThrough()
                && tableSemantics.getChangelogMode().containsOnly(RowKind.INSERT);
    }
}
