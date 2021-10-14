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

package org.apache.flink.streaming.api.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.table.functions.python.PythonAggregateFunctionInfo;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.functions.python.PythonFunctionKind;
import org.apache.flink.table.planner.typeutils.DataViewUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.ByteString;
import org.apache.beam.model.pipeline.v1.RunnerApi;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.python.Constants.FLINK_CODER_URN;
import static org.apache.flink.table.runtime.typeutils.PythonTypeUtils.toProtoType;

/** Utilities used to construct protobuf objects or construct objects from protobuf objects. */
@Internal
public enum ProtoUtils {
    ;

    public static FlinkFnApi.UserDefinedFunction getUserDefinedFunctionProto(
            PythonFunctionInfo pythonFunctionInfo) {
        FlinkFnApi.UserDefinedFunction.Builder builder =
                FlinkFnApi.UserDefinedFunction.newBuilder();
        builder.setPayload(
                ByteString.copyFrom(
                        pythonFunctionInfo.getPythonFunction().getSerializedPythonFunction()));
        for (Object input : pythonFunctionInfo.getInputs()) {
            FlinkFnApi.Input.Builder inputProto = FlinkFnApi.Input.newBuilder();
            if (input instanceof PythonFunctionInfo) {
                inputProto.setUdf(getUserDefinedFunctionProto((PythonFunctionInfo) input));
            } else if (input instanceof Integer) {
                inputProto.setInputOffset((Integer) input);
            } else {
                inputProto.setInputConstant(ByteString.copyFrom((byte[]) input));
            }
            builder.addInputs(inputProto);
        }
        builder.setTakesRowAsInput(pythonFunctionInfo.getPythonFunction().takesRowAsInput());
        builder.setIsPandasUdf(
                pythonFunctionInfo.getPythonFunction().getPythonFunctionKind()
                        == PythonFunctionKind.PANDAS);
        return builder.build();
    }

    public static FlinkFnApi.UserDefinedAggregateFunction getUserDefinedAggregateFunctionProto(
            PythonAggregateFunctionInfo pythonFunctionInfo,
            DataViewUtils.DataViewSpec[] dataViewSpecs) {
        FlinkFnApi.UserDefinedAggregateFunction.Builder builder =
                FlinkFnApi.UserDefinedAggregateFunction.newBuilder();
        builder.setPayload(
                ByteString.copyFrom(
                        pythonFunctionInfo.getPythonFunction().getSerializedPythonFunction()));
        builder.setDistinct(pythonFunctionInfo.isDistinct());
        builder.setFilterArg(pythonFunctionInfo.getFilterArg());
        builder.setTakesRowAsInput(pythonFunctionInfo.getPythonFunction().takesRowAsInput());
        for (Object input : pythonFunctionInfo.getInputs()) {
            FlinkFnApi.Input.Builder inputProto = FlinkFnApi.Input.newBuilder();
            if (input instanceof Integer) {
                inputProto.setInputOffset((Integer) input);
            } else {
                inputProto.setInputConstant(ByteString.copyFrom((byte[]) input));
            }
            builder.addInputs(inputProto);
        }
        if (dataViewSpecs != null) {
            for (DataViewUtils.DataViewSpec spec : dataViewSpecs) {
                FlinkFnApi.UserDefinedAggregateFunction.DataViewSpec.Builder specBuilder =
                        FlinkFnApi.UserDefinedAggregateFunction.DataViewSpec.newBuilder();
                specBuilder.setName(spec.getStateId());
                if (spec instanceof DataViewUtils.ListViewSpec) {
                    DataViewUtils.ListViewSpec listViewSpec = (DataViewUtils.ListViewSpec) spec;
                    specBuilder.setListView(
                            FlinkFnApi.UserDefinedAggregateFunction.DataViewSpec.ListView
                                    .newBuilder()
                                    .setElementType(
                                            toProtoType(
                                                    listViewSpec
                                                            .getElementDataType()
                                                            .getLogicalType())));
                } else {
                    DataViewUtils.MapViewSpec mapViewSpec = (DataViewUtils.MapViewSpec) spec;
                    FlinkFnApi.UserDefinedAggregateFunction.DataViewSpec.MapView.Builder
                            mapViewBuilder =
                                    FlinkFnApi.UserDefinedAggregateFunction.DataViewSpec.MapView
                                            .newBuilder();
                    mapViewBuilder.setKeyType(
                            toProtoType(mapViewSpec.getKeyDataType().getLogicalType()));
                    mapViewBuilder.setValueType(
                            toProtoType(mapViewSpec.getValueDataType().getLogicalType()));
                    specBuilder.setMapView(mapViewBuilder.build());
                }
                specBuilder.setFieldIndex(spec.getFieldIndex());
                builder.addSpecs(specBuilder.build());
            }
        }
        return builder.build();
    }

    public static FlinkFnApi.UserDefinedDataStreamFunction createUserDefinedDataStreamFunctionProto(
            DataStreamPythonFunctionInfo dataStreamPythonFunctionInfo,
            RuntimeContext runtimeContext,
            Map<String, String> internalParameters,
            boolean inBatchExecutionMode) {
        FlinkFnApi.UserDefinedDataStreamFunction.Builder builder =
                FlinkFnApi.UserDefinedDataStreamFunction.newBuilder();
        builder.setFunctionType(
                FlinkFnApi.UserDefinedDataStreamFunction.FunctionType.forNumber(
                        dataStreamPythonFunctionInfo.getFunctionType()));
        builder.setRuntimeContext(
                FlinkFnApi.UserDefinedDataStreamFunction.RuntimeContext.newBuilder()
                        .setTaskName(runtimeContext.getTaskName())
                        .setTaskNameWithSubtasks(runtimeContext.getTaskNameWithSubtasks())
                        .setNumberOfParallelSubtasks(runtimeContext.getNumberOfParallelSubtasks())
                        .setMaxNumberOfParallelSubtasks(
                                runtimeContext.getMaxNumberOfParallelSubtasks())
                        .setIndexOfThisSubtask(runtimeContext.getIndexOfThisSubtask())
                        .setAttemptNumber(runtimeContext.getAttemptNumber())
                        .addAllJobParameters(
                                runtimeContext.getExecutionConfig().getGlobalJobParameters().toMap()
                                        .entrySet().stream()
                                        .map(
                                                entry ->
                                                        FlinkFnApi.UserDefinedDataStreamFunction
                                                                .JobParameter.newBuilder()
                                                                .setKey(entry.getKey())
                                                                .setValue(entry.getValue())
                                                                .build())
                                        .collect(Collectors.toList()))
                        .addAllJobParameters(
                                internalParameters.entrySet().stream()
                                        .map(
                                                entry ->
                                                        FlinkFnApi.UserDefinedDataStreamFunction
                                                                .JobParameter.newBuilder()
                                                                .setKey(entry.getKey())
                                                                .setValue(entry.getValue())
                                                                .build())
                                        .collect(Collectors.toList()))
                        .setInBatchExecutionMode(inBatchExecutionMode)
                        .build());
        builder.setPayload(
                ByteString.copyFrom(
                        dataStreamPythonFunctionInfo
                                .getPythonFunction()
                                .getSerializedPythonFunction()));
        builder.setMetricEnabled(true);
        return builder.build();
    }

    public static FlinkFnApi.UserDefinedDataStreamFunction
            createReviseOutputDataStreamFunctionProto() {
        return FlinkFnApi.UserDefinedDataStreamFunction.newBuilder()
                .setFunctionType(
                        FlinkFnApi.UserDefinedDataStreamFunction.FunctionType.REVISE_OUTPUT)
                .build();
    }

    public static List<FlinkFnApi.UserDefinedDataStreamFunction>
            createUserDefinedDataStreamFunctionProtos(
                    DataStreamPythonFunctionInfo dataStreamPythonFunctionInfo,
                    RuntimeContext runtimeContext,
                    Map<String, String> internalParameters,
                    boolean inBatchExecutionMode) {
        List<FlinkFnApi.UserDefinedDataStreamFunction> results = new ArrayList<>();

        Object[] inputs = dataStreamPythonFunctionInfo.getInputs();
        if (inputs != null && inputs.length > 0) {
            Preconditions.checkArgument(inputs.length == 1);
            results.addAll(
                    createUserDefinedDataStreamFunctionProtos(
                            (DataStreamPythonFunctionInfo) inputs[0],
                            runtimeContext,
                            internalParameters,
                            inBatchExecutionMode));
        }

        results.add(
                createUserDefinedDataStreamFunctionProto(
                        dataStreamPythonFunctionInfo,
                        runtimeContext,
                        internalParameters,
                        inBatchExecutionMode));
        return results;
    }

    public static List<FlinkFnApi.UserDefinedDataStreamFunction>
            createUserDefinedDataStreamStatefulFunctionProtos(
                    DataStreamPythonFunctionInfo dataStreamPythonFunctionInfo,
                    RuntimeContext runtimeContext,
                    Map<String, String> internalParameters,
                    TypeInformation<?> keyTypeInfo,
                    boolean inBatchExecutionMode) {
        List<FlinkFnApi.UserDefinedDataStreamFunction> results =
                createUserDefinedDataStreamFunctionProtos(
                        dataStreamPythonFunctionInfo,
                        runtimeContext,
                        internalParameters,
                        inBatchExecutionMode);

        // set the key typeinfo for the head operator
        FlinkFnApi.TypeInfo builtKeyTypeInfo =
                PythonTypeUtils.TypeInfoToProtoConverter.toTypeInfoProto(keyTypeInfo);
        results.set(0, results.get(0).toBuilder().setKeyTypeInfo(builtKeyTypeInfo).build());
        return results;
    }

    public static RunnerApi.Coder createCoderProto(
            FlinkFnApi.CoderInfoDescriptor coderInfoDescriptor) {
        return RunnerApi.Coder.newBuilder()
                .setSpec(
                        RunnerApi.FunctionSpec.newBuilder()
                                .setUrn(FLINK_CODER_URN)
                                .setPayload(
                                        org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf
                                                .ByteString.copyFrom(
                                                coderInfoDescriptor.toByteArray()))
                                .build())
                .build();
    }

    public static FlinkFnApi.CoderInfoDescriptor createFlattenRowTypeCoderInfoDescriptorProto(
            RowType rowType,
            FlinkFnApi.CoderInfoDescriptor.Mode mode,
            boolean separatedWithEndMessage) {
        FlinkFnApi.CoderInfoDescriptor.FlattenRowType flattenRowType =
                FlinkFnApi.CoderInfoDescriptor.FlattenRowType.newBuilder()
                        .setSchema(toProtoType(rowType).getRowSchema())
                        .build();
        return createCoderInfoDescriptorProto(
                flattenRowType, null, null, null, null, mode, separatedWithEndMessage);
    }

    public static FlinkFnApi.CoderInfoDescriptor createRowTypeCoderInfoDescriptorProto(
            RowType rowType,
            FlinkFnApi.CoderInfoDescriptor.Mode mode,
            boolean separatedWithEndMessage) {
        return createCoderInfoDescriptorProto(
                null,
                FlinkFnApi.CoderInfoDescriptor.RowType.newBuilder()
                        .setSchema(toProtoType(rowType).getRowSchema())
                        .build(),
                null,
                null,
                null,
                mode,
                separatedWithEndMessage);
    }

    public static FlinkFnApi.CoderInfoDescriptor createArrowTypeCoderInfoDescriptorProto(
            RowType rowType,
            FlinkFnApi.CoderInfoDescriptor.Mode mode,
            boolean separatedWithEndMessage) {
        return createCoderInfoDescriptorProto(
                null,
                null,
                FlinkFnApi.CoderInfoDescriptor.ArrowType.newBuilder()
                        .setSchema(toProtoType(rowType).getRowSchema())
                        .build(),
                null,
                null,
                mode,
                separatedWithEndMessage);
    }

    public static FlinkFnApi.CoderInfoDescriptor createOverWindowArrowTypeCoderInfoDescriptorProto(
            RowType rowType,
            FlinkFnApi.CoderInfoDescriptor.Mode mode,
            boolean separatedWithEndMessage) {
        return createCoderInfoDescriptorProto(
                null,
                null,
                null,
                FlinkFnApi.CoderInfoDescriptor.OverWindowArrowType.newBuilder()
                        .setSchema(toProtoType(rowType).getRowSchema())
                        .build(),
                null,
                mode,
                separatedWithEndMessage);
    }

    public static FlinkFnApi.CoderInfoDescriptor createRawTypeCoderInfoDescriptorProto(
            TypeInformation<?> typeInformation,
            FlinkFnApi.CoderInfoDescriptor.Mode mode,
            boolean separatedWithEndMessage) {
        FlinkFnApi.TypeInfo typeinfo =
                PythonTypeUtils.TypeInfoToProtoConverter.toTypeInfoProto(typeInformation);
        return createCoderInfoDescriptorProto(
                null,
                null,
                null,
                null,
                FlinkFnApi.CoderInfoDescriptor.RawType.newBuilder().setTypeInfo(typeinfo).build(),
                mode,
                separatedWithEndMessage);
    }

    private static FlinkFnApi.CoderInfoDescriptor createCoderInfoDescriptorProto(
            FlinkFnApi.CoderInfoDescriptor.FlattenRowType flattenRowType,
            FlinkFnApi.CoderInfoDescriptor.RowType rowType,
            FlinkFnApi.CoderInfoDescriptor.ArrowType arrowType,
            FlinkFnApi.CoderInfoDescriptor.OverWindowArrowType overWindowArrowType,
            FlinkFnApi.CoderInfoDescriptor.RawType rawType,
            FlinkFnApi.CoderInfoDescriptor.Mode mode,
            boolean separatedWithEndMessage) {
        FlinkFnApi.CoderInfoDescriptor.Builder builder =
                FlinkFnApi.CoderInfoDescriptor.newBuilder();
        if (flattenRowType != null) {
            builder.setFlattenRowType(flattenRowType);
        } else if (rowType != null) {
            builder.setRowType(rowType);
        } else if (arrowType != null) {
            builder.setArrowType(arrowType);
        } else if (overWindowArrowType != null) {
            builder.setOverWindowArrowType(overWindowArrowType);
        } else if (rawType != null) {
            builder.setRawType(rawType);
        }
        builder.setMode(mode);
        builder.setSeparatedWithEndMessage(separatedWithEndMessage);
        return builder.build();
    }

    public static StateTtlConfig parseStateTtlConfigFromProto(
            FlinkFnApi.StateDescriptor.StateTTLConfig stateTTLConfigProto) {
        StateTtlConfig.Builder builder =
                StateTtlConfig.newBuilder(Time.milliseconds(stateTTLConfigProto.getTtl()))
                        .setUpdateType(
                                parseUpdateTypeFromProto(stateTTLConfigProto.getUpdateType()))
                        .setStateVisibility(
                                parseStateVisibilityFromProto(
                                        stateTTLConfigProto.getStateVisibility()))
                        .setTtlTimeCharacteristic(
                                parseTtlTimeCharacteristicFromProto(
                                        stateTTLConfigProto.getTtlTimeCharacteristic()));

        FlinkFnApi.StateDescriptor.StateTTLConfig.CleanupStrategies cleanupStrategiesProto =
                stateTTLConfigProto.getCleanupStrategies();

        if (!cleanupStrategiesProto.getIsCleanupInBackground()) {
            builder.disableCleanupInBackground();
        }

        for (FlinkFnApi.StateDescriptor.StateTTLConfig.CleanupStrategies.MapStrategiesEntry
                mapStrategiesEntry : cleanupStrategiesProto.getStrategiesList()) {
            FlinkFnApi.StateDescriptor.StateTTLConfig.CleanupStrategies.Strategies strategyProto =
                    mapStrategiesEntry.getStrategy();
            if (strategyProto
                    == FlinkFnApi.StateDescriptor.StateTTLConfig.CleanupStrategies.Strategies
                            .FULL_STATE_SCAN_SNAPSHOT) {
                builder.cleanupFullSnapshot();
            } else if (strategyProto
                    == FlinkFnApi.StateDescriptor.StateTTLConfig.CleanupStrategies.Strategies
                            .INCREMENTAL_CLEANUP) {
                FlinkFnApi.StateDescriptor.StateTTLConfig.CleanupStrategies
                                .IncrementalCleanupStrategy
                        incrementalCleanupStrategyProto =
                                mapStrategiesEntry.getIncrementalCleanupStrategy();
                builder.cleanupIncrementally(
                        incrementalCleanupStrategyProto.getCleanupSize(),
                        incrementalCleanupStrategyProto.getRunCleanupForEveryRecord());
            } else if (strategyProto
                    == FlinkFnApi.StateDescriptor.StateTTLConfig.CleanupStrategies.Strategies
                            .ROCKSDB_COMPACTION_FILTER) {
                FlinkFnApi.StateDescriptor.StateTTLConfig.CleanupStrategies
                                .RocksdbCompactFilterCleanupStrategy
                        rocksdbCompactFilterCleanupStrategyProto =
                                mapStrategiesEntry.getRocksdbCompactFilterCleanupStrategy();
                builder.cleanupInRocksdbCompactFilter(
                        rocksdbCompactFilterCleanupStrategyProto.getQueryTimeAfterNumEntries());
            }
        }

        return builder.build();
    }

    private static StateTtlConfig.UpdateType parseUpdateTypeFromProto(
            FlinkFnApi.StateDescriptor.StateTTLConfig.UpdateType updateType) {
        if (updateType == FlinkFnApi.StateDescriptor.StateTTLConfig.UpdateType.Disabled) {
            return StateTtlConfig.UpdateType.Disabled;
        } else if (updateType
                == FlinkFnApi.StateDescriptor.StateTTLConfig.UpdateType.OnCreateAndWrite) {
            return StateTtlConfig.UpdateType.OnCreateAndWrite;
        } else if (updateType
                == FlinkFnApi.StateDescriptor.StateTTLConfig.UpdateType.OnReadAndWrite) {
            return StateTtlConfig.UpdateType.OnReadAndWrite;
        }
        throw new RuntimeException("Unknown UpdateType " + updateType);
    }

    private static StateTtlConfig.StateVisibility parseStateVisibilityFromProto(
            FlinkFnApi.StateDescriptor.StateTTLConfig.StateVisibility stateVisibility) {
        if (stateVisibility
                == FlinkFnApi.StateDescriptor.StateTTLConfig.StateVisibility
                        .ReturnExpiredIfNotCleanedUp) {
            return StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp;
        } else if (stateVisibility
                == FlinkFnApi.StateDescriptor.StateTTLConfig.StateVisibility.NeverReturnExpired) {
            return StateTtlConfig.StateVisibility.NeverReturnExpired;
        }
        throw new RuntimeException("Unknown StateVisibility " + stateVisibility);
    }

    private static StateTtlConfig.TtlTimeCharacteristic parseTtlTimeCharacteristicFromProto(
            FlinkFnApi.StateDescriptor.StateTTLConfig.TtlTimeCharacteristic ttlTimeCharacteristic) {
        if (ttlTimeCharacteristic
                == FlinkFnApi.StateDescriptor.StateTTLConfig.TtlTimeCharacteristic.ProcessingTime) {
            return StateTtlConfig.TtlTimeCharacteristic.ProcessingTime;
        }
        throw new RuntimeException("Unknown TtlTimeCharacteristic " + ttlTimeCharacteristic);
    }
}
