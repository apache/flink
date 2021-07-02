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

package org.apache.flink.table.runtime.runners.python.beam;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.python.metric.FlinkMetricContainer;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.streaming.api.runners.python.beam.BeamPythonFunctionRunner;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.GeneratedMessageV3;
import org.apache.beam.model.pipeline.v1.RunnerApi;

import java.util.Map;

import static org.apache.flink.python.Constants.FLINK_CODER_URN;
import static org.apache.flink.table.runtime.typeutils.PythonTypeUtils.toProtoType;

/** A {@link BeamTablePythonFunctionRunner} used to execute Python functions in Table API. */
@Internal
public class BeamTablePythonFunctionRunner extends BeamPythonFunctionRunner {

    private final RowType inputType;
    private final RowType outputType;
    private final GeneratedMessageV3 userDefinedFunctionProto;

    public BeamTablePythonFunctionRunner(
            String taskName,
            PythonEnvironmentManager environmentManager,
            RowType inputType,
            RowType outputType,
            String functionUrn,
            GeneratedMessageV3 userDefinedFunctionProto,
            Map<String, String> jobOptions,
            FlinkMetricContainer flinkMetricContainer,
            KeyedStateBackend keyedStateBackend,
            TypeSerializer keySerializer,
            TypeSerializer namespaceSerializer,
            MemoryManager memoryManager,
            double managedMemoryFraction,
            FlinkFnApi.CoderParam.DataType inputDataType,
            FlinkFnApi.CoderParam.DataType outputDataType,
            FlinkFnApi.CoderParam.OutputMode outputMode) {
        super(
                taskName,
                environmentManager,
                functionUrn,
                jobOptions,
                flinkMetricContainer,
                keyedStateBackend,
                keySerializer,
                namespaceSerializer,
                memoryManager,
                managedMemoryFraction,
                inputDataType,
                outputDataType,
                outputMode);
        this.inputType = Preconditions.checkNotNull(inputType);
        this.outputType = Preconditions.checkNotNull(outputType);
        this.userDefinedFunctionProto = Preconditions.checkNotNull(userDefinedFunctionProto);
    }

    @Override
    protected byte[] getUserDefinedFunctionsProtoBytes() {
        return userDefinedFunctionProto.toByteArray();
    }

    @Override
    protected RunnerApi.Coder getInputCoderProto() {
        return getRowCoderProto(inputType, inputDataType, outputMode);
    }

    @Override
    protected RunnerApi.Coder getOutputCoderProto() {
        return getRowCoderProto(outputType, outputDataType, outputMode);
    }

    private static RunnerApi.Coder getRowCoderProto(
            RowType rowType,
            FlinkFnApi.CoderParam.DataType dataType,
            FlinkFnApi.CoderParam.OutputMode outputMode) {
        FlinkFnApi.Schema rowSchema = toProtoType(rowType).getRowSchema();
        FlinkFnApi.CoderParam.Builder coderParamBuilder = FlinkFnApi.CoderParam.newBuilder();
        coderParamBuilder.setDataType(dataType);
        coderParamBuilder.setSchema(rowSchema);
        coderParamBuilder.setOutputMode(outputMode);
        return RunnerApi.Coder.newBuilder()
                .setSpec(
                        RunnerApi.FunctionSpec.newBuilder()
                                .setUrn(FLINK_CODER_URN)
                                .setPayload(
                                        org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf
                                                .ByteString.copyFrom(
                                                coderParamBuilder.build().toByteArray()))
                                .build())
                .build();
    }
}
