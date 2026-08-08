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

package org.apache.flink.table.runtime.operators.python.process;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.table.functions.python.PythonProcessTableFunction;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.operators.process.RuntimeStateInfo;
import org.apache.flink.table.runtime.operators.process.RuntimeTableSemantics;
import org.apache.flink.table.runtime.operators.process.WritableInternalTimeContext;
import org.apache.flink.table.runtime.operators.python.AbstractOneInputPythonFunctionOperator;
import org.apache.flink.table.runtime.operators.python.utils.StreamRecordRowDataWrappingCollector;
import org.apache.flink.table.runtime.runners.python.beam.BeamProcessTableFunctionRunner;
import org.apache.flink.table.runtime.typeutils.PythonTypeUtils;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.typeutils.StringDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

import static org.apache.flink.python.PythonOptions.MAP_STATE_READ_CACHE_SIZE;
import static org.apache.flink.python.PythonOptions.MAP_STATE_WRITE_CACHE_SIZE;
import static org.apache.flink.python.PythonOptions.PYTHON_METRIC_ENABLED;
import static org.apache.flink.python.PythonOptions.PYTHON_PROFILE_ENABLED;
import static org.apache.flink.python.PythonOptions.STATE_CACHE_SIZE;
import static org.apache.flink.python.util.ProtoUtils.createFlattenRowTypeCoderInfoDescriptorProto;
import static org.apache.flink.python.util.ProtoUtils.createRowTypeCoderInfoDescriptorProto;
import static org.apache.flink.streaming.api.utils.PythonOperatorUtils.setCurrentKeyForStreaming;

/** Runtime operator for a single-input Python {@link ProcessTableFunction}. */
@Internal
public final class PythonProcessTableFunctionOperator
        extends AbstractOneInputPythonFunctionOperator<RowData, RowData>
        implements Triggerable<RowData, Object> {

    private static final long serialVersionUID = 1L;

    private static final String NAMED_TIMER_STATE_NAME = "__flink_internal_python_ptf_named_timers";
    private static final String NAMED_TIMER_SERVICE_NAME =
            "__flink_internal_python_ptf_named_timer_service";
    private static final String ANONYMOUS_TIMER_SERVICE_NAME =
            "__flink_internal_python_ptf_anonymous_timer_service";

    private final PythonProcessTableFunction function;
    private final RuntimeTableSemantics tableSemantics;
    private final List<RuntimeStateInfo> stateInfos;
    private final RowType inputType;
    private final RowType argumentType;
    private final RowType resultType;
    private final RowType keyType;
    private final GeneratedProjection argumentGeneratedProjection;

    private transient Projection<RowData, BinaryRowData> argumentProjection;
    private transient TypeSerializer<RowData> runnerInputSerializer;
    private transient TypeSerializer<RowData> resultSerializer;
    private transient TypeSerializer<RowData> timerDataSerializer;
    private transient ByteArrayInputStreamWithPos bais;
    private transient DataInputViewStreamWrapper baisWrapper;
    private transient ByteArrayOutputStreamWithPos baos;
    private transient DataOutputViewStreamWrapper baosWrapper;
    private transient RowDataSerializer inputSerializer;
    private transient RowDataSerializer keySerializer;
    private transient TypeSerializer<RowData> stateKeySerializer;
    private transient StreamRecordRowDataWrappingCollector outputCollector;
    private transient Queue<Invocation> invocations;
    private transient Invocation currentInvocation;
    private transient JoinedRowData withResult;
    private transient JoinedRowData withRowtime;
    private transient long inputWatermark;
    private transient Object keyForTimerService;

    private transient @Nullable MapState<StringData, Long> namedTimers;
    private transient @Nullable InternalTimerService<StringData> namedTimerService;
    private transient @Nullable InternalTimerService<VoidNamespace> anonymousTimerService;
    private transient @Nullable WritableInternalTimeContext timeContext;
    private transient @Nullable ProcessTableTimerRegistration timerRegistration;
    private transient RowType runnerInputType;
    private transient RowType timerDataType;

    public PythonProcessTableFunctionOperator(
            Configuration config,
            PythonProcessTableFunction function,
            RuntimeTableSemantics tableSemantics,
            List<RuntimeStateInfo> stateInfos,
            RowType inputType,
            RowType argumentType,
            RowType resultType,
            RowType keyType,
            GeneratedProjection argumentGeneratedProjection) {
        super(config);
        this.function = function;
        this.tableSemantics = tableSemantics;
        this.stateInfos = stateInfos;
        this.inputType = inputType;
        this.argumentType = argumentType;
        this.resultType = resultType;
        this.keyType = keyType;
        this.argumentGeneratedProjection = argumentGeneratedProjection;
    }

    @Override
    public void open() throws Exception {
        bais = new ByteArrayInputStreamWithPos();
        baisWrapper = new DataInputViewStreamWrapper(bais);
        baos = new ByteArrayOutputStreamWithPos();
        baosWrapper = new DataOutputViewStreamWrapper(baos);
        runnerInputType = createRunnerInputType();
        timerDataType = createTimerDataType();
        runnerInputSerializer = PythonTypeUtils.toInternalSerializer(runnerInputType);
        resultSerializer = PythonTypeUtils.toInternalSerializer(resultType);
        timerDataSerializer = PythonTypeUtils.toInternalSerializer(timerDataType);
        inputSerializer = new RowDataSerializer(inputType);
        keySerializer =
                tableSemantics.hasSetSemantics()
                        ? (RowDataSerializer)
                                (TypeSerializer<?>) getKeyedStateBackend().getKeySerializer()
                        : new RowDataSerializer(keyType);
        stateKeySerializer = PythonTypeUtils.toInternalSerializer(keyType);
        argumentProjection =
                argumentGeneratedProjection.newInstance(
                        Thread.currentThread().getContextClassLoader());
        outputCollector = new StreamRecordRowDataWrappingCollector(output);
        invocations = new ArrayDeque<>();
        withResult = new JoinedRowData();
        withRowtime = new JoinedRowData();
        inputWatermark = Long.MIN_VALUE;
        setTimerServices();
        super.open();
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        final RowData input = element.getValue();
        final RowData key =
                tableSemantics.hasSetSemantics() ? (RowData) getCurrentKey() : GenericRowData.of();
        final RowData prefix = createPrefix(input, key);
        final Long time = extractTime(input);
        invocations.add(new Invocation(prefix, time));

        final GenericRowData runnerInput = new GenericRowData(5);
        runnerInput.setField(0, key);
        runnerInput.setField(1, argumentProjection.apply(input));
        runnerInput.setField(2, time);
        runnerInput.setField(3, nullableWatermark(inputWatermark));
        runnerInput.setField(4, nullableWatermark(currentWatermark()));
        runnerInputSerializer.serialize(runnerInput, baosWrapper);
        pythonFunctionRunner.process(baos.toByteArray());
        baos.reset();
        elementCount++;
        checkInvokeFinishBundleByCount();
        emitResults();
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        inputWatermark = mark.getTimestamp();
        super.processWatermark(mark);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        super.prepareSnapshotPreBarrier(checkpointId);
        drainUnregisteredTimers();
    }

    @Override
    public void endInput() throws Exception {
        super.endInput();
        drainUnregisteredTimers();
    }

    @Override
    public void onEventTime(InternalTimer<RowData, Object> timer) throws Exception {
        setBackendCurrentKey(timer.getKey());
        final boolean named = timer.getNamespace() != VoidNamespace.INSTANCE;
        if (named) {
            namedTimers.remove((StringData) timer.getNamespace());
        }
        final RowData key = timer.getKey();
        invocations.add(new Invocation(keySerializer.copy(key), timer.getTimestamp()));

        final GenericRowData timerData = new GenericRowData(6);
        timerData.setField(0, ProcessTableTimerRegistration.TRIGGER);
        timerData.setField(1, timer.getTimestamp());
        timerData.setField(2, named ? timer.getNamespace() : null);
        timerData.setField(3, key);
        timerData.setField(4, null);
        timerData.setField(5, nullableWatermark(currentWatermark()));
        timerDataSerializer.serialize(timerData, baosWrapper);
        pythonFunctionRunner.processTimer(baos.toByteArray());
        baos.reset();
        elementCount++;
        checkInvokeFinishBundleByCount();
        emitResults();
    }

    @Override
    public void onProcessingTime(InternalTimer<RowData, Object> timer) {}

    @Override
    public PythonFunctionRunner createPythonFunctionRunner() throws Exception {
        final boolean stateful = tableSemantics.hasSetSemantics();
        return new BeamProcessTableFunctionRunner(
                getContainingTask().getEnvironment(),
                getRuntimeContext().getTaskInfo().getTaskName(),
                createPythonEnvironmentManager(),
                createFunctionProto(),
                getFlinkMetricContainer(),
                stateful ? getKeyedStateBackend() : null,
                stateful ? stateKeySerializer : null,
                timerRegistration,
                getContainingTask().getEnvironment().getMemoryManager(),
                getOperatorConfig()
                        .getManagedMemoryFractionOperatorUseCaseOfSlot(
                                ManagedMemoryUseCase.PYTHON,
                                getContainingTask().getJobConfiguration(),
                                getContainingTask()
                                        .getEnvironment()
                                        .getTaskManagerInfo()
                                        .getConfiguration(),
                                getContainingTask()
                                        .getEnvironment()
                                        .getUserCodeClassLoader()
                                        .asClassLoader()),
                createFlattenRowTypeCoderInfoDescriptorProto(
                        runnerInputType, FlinkFnApi.CoderInfoDescriptor.Mode.MULTIPLE, true),
                createFlattenRowTypeCoderInfoDescriptorProto(
                        resultType, FlinkFnApi.CoderInfoDescriptor.Mode.MULTIPLE, true),
                timerRegistration == null
                        ? null
                        : createRowTypeCoderInfoDescriptorProto(
                                timerDataType, FlinkFnApi.CoderInfoDescriptor.Mode.SINGLE, false));
    }

    @Override
    public PythonEnv getPythonEnv() {
        return function.getPythonEnv();
    }

    @Override
    public void emitResult(Tuple3<String, byte[], Integer> resultTuple) throws Exception {
        if (currentInvocation == null) {
            currentInvocation = invocations.remove();
        }
        while (resultTuple != null) {
            if (isFinishResult(resultTuple)) {
                currentInvocation = null;
                return;
            }
            bais.setBuffer(resultTuple.f1, 0, resultTuple.f2);
            final RowData result = resultSerializer.deserialize(baisWrapper);
            if (result.getRowKind() != RowKind.INSERT) {
                throw new TableRuntimeException(
                        "Python process table functions support append-only output.");
            }
            emitResultRow(currentInvocation, result);
            resultTuple = pythonFunctionRunner.pollResult();
        }
    }

    @Override
    public void setCurrentKey(Object key) {
        keyForTimerService = key;
    }

    @Override
    public Object getCurrentKey() {
        return keyForTimerService;
    }

    private void emitResultRow(Invocation invocation, RowData result) {
        withResult.replace(invocation.prefix, result).setRowKind(RowKind.INSERT);
        if (shouldEmitRowtime()) {
            final GenericRowData rowtime =
                    GenericRowData.of(
                            invocation.time == null
                                    ? null
                                    : TimestampData.fromEpochMillis(invocation.time));
            withRowtime.replace(withResult, rowtime).setRowKind(RowKind.INSERT);
            outputCollector.collect(withRowtime);
        } else {
            outputCollector.collect(withResult);
        }
    }

    private RowData createPrefix(RowData input, RowData key) {
        if (tableSemantics.passColumnsThrough()) {
            return inputSerializer.copy(input);
        }
        if (tableSemantics.hasSetSemantics()) {
            return keySerializer.copy(key);
        }
        return GenericRowData.of();
    }

    private @Nullable Long extractTime(RowData input) {
        final int timeColumn = tableSemantics.timeColumn();
        if (timeColumn < 0 || input.isNullAt(timeColumn)) {
            return null;
        }
        final LogicalType timeType = inputType.getTypeAt(timeColumn);
        final int precision;
        if (timeType instanceof LocalZonedTimestampType) {
            precision = ((LocalZonedTimestampType) timeType).getPrecision();
        } else {
            precision =
                    ((org.apache.flink.table.types.logical.TimestampType) timeType).getPrecision();
        }
        return input.getTimestamp(timeColumn, precision).getMillisecond();
    }

    private long currentWatermark() {
        if (anonymousTimerService != null) {
            return anonymousTimerService.currentWatermark();
        }
        return inputWatermark;
    }

    private static @Nullable Long nullableWatermark(long watermark) {
        return watermark == Long.MIN_VALUE ? null : watermark;
    }

    private boolean shouldEmitRowtime() {
        return tableSemantics.timeColumn() >= 0;
    }

    private static boolean isFinishResult(Tuple3<String, byte[], Integer> resultTuple) {
        return resultTuple.f2 == 1 && resultTuple.f1[0] == 0x00;
    }

    private void setBackendCurrentKey(RowData key) {
        setCurrentKeyForStreaming(
                (KeyedStateBackend<RowData>) (KeyedStateBackend<?>) getKeyedStateBackend(), key);
    }

    private void setTimerServices() throws Exception {
        if (!tableSemantics.hasSetSemantics() || !function.hasOnTimer()) {
            return;
        }
        final MapStateDescriptor<StringData, Long> descriptor =
                new MapStateDescriptor<>(
                        NAMED_TIMER_STATE_NAME,
                        StringDataSerializer.INSTANCE,
                        LongSerializer.INSTANCE);
        namedTimers = getKeyedStateStore().getMapState(descriptor);
        namedTimerService =
                getInternalTimerService(
                        NAMED_TIMER_SERVICE_NAME,
                        StringDataSerializer.INSTANCE,
                        (Triggerable) this);
        anonymousTimerService =
                getInternalTimerService(
                        ANONYMOUS_TIMER_SERVICE_NAME,
                        VoidNamespaceSerializer.INSTANCE,
                        (Triggerable) this);
        timeContext =
                new WritableInternalTimeContext(
                        namedTimers, namedTimerService, anonymousTimerService);
        timerRegistration =
                new ProcessTableTimerRegistration(
                        this,
                        getKeyedStateBackend(),
                        timeContext,
                        timerDataSerializer,
                        keyType.getFieldCount());
    }

    private RowType createRunnerInputType() {
        return RowType.of(
                new LogicalType[] {
                    keyType,
                    argumentType,
                    new BigIntType(true),
                    new BigIntType(true),
                    new BigIntType(true)
                },
                new String[] {"key", "arguments", "time", "table_watermark", "current_watermark"});
    }

    private RowType createTimerDataType() {
        return RowType.of(
                new LogicalType[] {
                    new TinyIntType(false),
                    new BigIntType(true),
                    new VarCharType(true, VarCharType.MAX_LENGTH),
                    keyType,
                    new BigIntType(true),
                    new BigIntType(true)
                },
                new String[] {
                    "operation", "timestamp", "name", "key", "table_watermark", "current_watermark"
                });
    }

    private FlinkFnApi.UserDefinedProcessTableFunction createFunctionProto() {
        final FlinkFnApi.UserDefinedProcessTableFunction.Builder builder =
                FlinkFnApi.UserDefinedProcessTableFunction.newBuilder()
                        .setPayload(
                                com.google.protobuf.ByteString.copyFrom(
                                        function.getSerializedPythonFunction()))
                        .setHasOnTimer(function.hasOnTimer())
                        .setMetricEnabled(config.get(PYTHON_METRIC_ENABLED))
                        .setProfileEnabled(config.get(PYTHON_PROFILE_ENABLED))
                        .setStateCacheSize(config.get(STATE_CACHE_SIZE))
                        .setMapStateReadCacheSize(config.get(MAP_STATE_READ_CACHE_SIZE))
                        .setMapStateWriteCacheSize(config.get(MAP_STATE_WRITE_CACHE_SIZE));
        if (tableSemantics.hasSetSemantics()) {
            builder.setKeyType(PythonTypeUtils.toProtoType(keyType));
        }
        final String[] names = function.getArgumentNames();
        final boolean[] tables = function.getTableArguments();
        final String[] traits = function.getArgumentTraits();
        for (int i = 0; i < names.length; i++) {
            final FlinkFnApi.UserDefinedProcessTableFunction.Argument.Builder argument =
                    FlinkFnApi.UserDefinedProcessTableFunction.Argument.newBuilder()
                            .setName(names[i])
                            .setType(PythonTypeUtils.toProtoType(argumentType.getTypeAt(i)))
                            .setIsTable(tables[i]);
            if (!traits[i].isEmpty()) {
                argument.addAllTraits(Arrays.asList(traits[i].split(",")));
            }
            builder.addArguments(argument);
        }
        for (RuntimeStateInfo stateInfo : stateInfos) {
            builder.addStates(
                    FlinkFnApi.UserDefinedProcessTableFunction.State.newBuilder()
                            .setName(stateInfo.getStateName())
                            .setType(
                                    PythonTypeUtils.toProtoType(
                                            stateInfo.getDataType().getLogicalType()))
                            .setTtlMillis(stateInfo.getTimeToLive()));
        }
        builder.addAllJobParameters(
                getRuntimeContext().getGlobalJobParameters().entrySet().stream()
                        .map(
                                entry ->
                                        FlinkFnApi.JobParameter.newBuilder()
                                                .setKey(entry.getKey())
                                                .setValue(entry.getValue())
                                                .build())
                        .collect(Collectors.toList()));
        builder.setRuntimeContext(
                FlinkFnApi.UserDefinedDataStreamFunction.RuntimeContext.newBuilder()
                        .setTaskName(getRuntimeContext().getTaskInfo().getTaskName())
                        .setTaskNameWithSubtasks(
                                getRuntimeContext().getTaskInfo().getTaskNameWithSubtasks())
                        .setNumberOfParallelSubtasks(
                                getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks())
                        .setMaxNumberOfParallelSubtasks(
                                getRuntimeContext().getTaskInfo().getMaxNumberOfParallelSubtasks())
                        .setIndexOfThisSubtask(
                                getRuntimeContext().getTaskInfo().getIndexOfThisSubtask())
                        .setAttemptNumber(getRuntimeContext().getTaskInfo().getAttemptNumber())
                        .build());
        return builder.build();
    }

    private static final class Invocation {
        private final RowData prefix;
        private final @Nullable Long time;

        private Invocation(RowData prefix, @Nullable Long time) {
            this.prefix = prefix;
            this.time = time;
        }
    }
}
