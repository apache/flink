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
import org.apache.flink.util.Preconditions;

import com.google.protobuf.GeneratedMessageV3;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.graph.TimerReference;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.python.Constants.INPUT_COLLECTION_ID;
import static org.apache.flink.python.Constants.MAIN_INPUT_NAME;
import static org.apache.flink.python.Constants.MAIN_OUTPUT_NAME;
import static org.apache.flink.python.Constants.OUTPUT_COLLECTION_ID;
import static org.apache.flink.python.Constants.TRANSFORM_ID;

/** A {@link BeamTablePythonFunctionRunner} used to execute Python functions in Table API. */
@Internal
public class BeamTablePythonFunctionRunner extends BeamPythonFunctionRunner {

    /** The urn which represents the function kind to be executed. */
    private final String functionUrn;

    private final GeneratedMessageV3 userDefinedFunctionProto;

    public BeamTablePythonFunctionRunner(
            String taskName,
            PythonEnvironmentManager environmentManager,
            String functionUrn,
            GeneratedMessageV3 userDefinedFunctionProto,
            Map<String, String> jobOptions,
            FlinkMetricContainer flinkMetricContainer,
            KeyedStateBackend<?> keyedStateBackend,
            TypeSerializer<?> keySerializer,
            TypeSerializer<?> namespaceSerializer,
            MemoryManager memoryManager,
            double managedMemoryFraction,
            FlinkFnApi.CoderInfoDescriptor inputCoderDescriptor,
            FlinkFnApi.CoderInfoDescriptor outputCoderDescriptor) {
        super(
                taskName,
                environmentManager,
                jobOptions,
                flinkMetricContainer,
                keyedStateBackend,
                keySerializer,
                namespaceSerializer,
                null,
                memoryManager,
                managedMemoryFraction,
                inputCoderDescriptor,
                outputCoderDescriptor);
        this.functionUrn = Preconditions.checkNotNull(functionUrn);
        this.userDefinedFunctionProto = Preconditions.checkNotNull(userDefinedFunctionProto);
    }

    @Override
    protected void buildTransforms(RunnerApi.Components.Builder componentsBuilder) {
        componentsBuilder.putTransforms(
                TRANSFORM_ID,
                RunnerApi.PTransform.newBuilder()
                        .setUniqueName(TRANSFORM_ID)
                        .setSpec(
                                RunnerApi.FunctionSpec.newBuilder()
                                        .setUrn(functionUrn)
                                        .setPayload(
                                                org.apache.beam.vendor.grpc.v1p26p0.com.google
                                                        .protobuf.ByteString.copyFrom(
                                                        userDefinedFunctionProto.toByteArray()))
                                        .build())
                        .putInputs(MAIN_INPUT_NAME, INPUT_COLLECTION_ID)
                        .putOutputs(MAIN_OUTPUT_NAME, OUTPUT_COLLECTION_ID)
                        .build());
    }

    @Override
    protected List<TimerReference> getTimers(RunnerApi.Components components) {
        return Collections.emptyList();
    }

    @Override
    protected Optional<RunnerApi.Coder> getOptionalTimerCoderProto() {
        return Optional.empty();
    }

    @Override
    public void processTimer(byte[] timerData) throws Exception {
        throw new UnsupportedOperationException();
    }

    public static BeamTablePythonFunctionRunner stateless(
            String taskName,
            PythonEnvironmentManager environmentManager,
            String functionUrn,
            GeneratedMessageV3 userDefinedFunctionProto,
            Map<String, String> jobOptions,
            FlinkMetricContainer flinkMetricContainer,
            MemoryManager memoryManager,
            double managedMemoryFraction,
            FlinkFnApi.CoderInfoDescriptor inputCoderDescriptor,
            FlinkFnApi.CoderInfoDescriptor outputCoderDescriptor) {
        return new BeamTablePythonFunctionRunner(
                taskName,
                environmentManager,
                functionUrn,
                userDefinedFunctionProto,
                jobOptions,
                flinkMetricContainer,
                null,
                null,
                null,
                memoryManager,
                managedMemoryFraction,
                inputCoderDescriptor,
                outputCoderDescriptor);
    }

    public static BeamTablePythonFunctionRunner stateful(
            String taskName,
            PythonEnvironmentManager environmentManager,
            String functionUrn,
            GeneratedMessageV3 userDefinedFunctionProto,
            Map<String, String> jobOptions,
            FlinkMetricContainer flinkMetricContainer,
            KeyedStateBackend<?> keyedStateBackend,
            TypeSerializer<?> keySerializer,
            TypeSerializer<?> namespaceSerializer,
            MemoryManager memoryManager,
            double managedMemoryFraction,
            FlinkFnApi.CoderInfoDescriptor inputCoderDescriptor,
            FlinkFnApi.CoderInfoDescriptor outputCoderDescriptor) {
        return new BeamTablePythonFunctionRunner(
                taskName,
                environmentManager,
                functionUrn,
                userDefinedFunctionProto,
                jobOptions,
                flinkMetricContainer,
                keyedStateBackend,
                keySerializer,
                namespaceSerializer,
                memoryManager,
                managedMemoryFraction,
                inputCoderDescriptor,
                outputCoderDescriptor);
    }
}
