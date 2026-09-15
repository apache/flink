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
import org.apache.flink.python.env.process.ProcessPythonEnvironmentManager;
import org.apache.flink.python.metric.process.FlinkMetricContainer;
import org.apache.flink.python.util.ProtoUtils;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.streaming.api.operators.python.process.timer.TimerRegistrationHandler;
import org.apache.flink.streaming.api.runners.python.beam.BeamPythonFunctionRunner;

import com.google.protobuf.GeneratedMessage;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.BeamUrns;
import org.apache.beam.runners.core.construction.graph.TimerReference;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.python.Constants.INPUT_COLLECTION_ID;
import static org.apache.flink.python.Constants.MAIN_INPUT_NAME;
import static org.apache.flink.python.Constants.MAIN_OUTPUT_NAME;
import static org.apache.flink.python.Constants.OUTPUT_COLLECTION_ID;
import static org.apache.flink.python.Constants.TIMER_ID;
import static org.apache.flink.python.Constants.TRANSFORM_ID;
import static org.apache.flink.python.Constants.WRAPPER_TIMER_CODER_ID;

/** Beam runner adapter for Python process table functions. */
@Internal
public final class BeamProcessTableFunctionRunner extends BeamPythonFunctionRunner {

    public static final String PROCESS_TABLE_FUNCTION_URN =
            "flink:transform:process_table_function:v1";

    private final GeneratedMessage functionProto;
    private final @Nullable FlinkFnApi.CoderInfoDescriptor timerCoderDescriptor;

    public BeamProcessTableFunctionRunner(
            Environment environment,
            String taskName,
            ProcessPythonEnvironmentManager environmentManager,
            GeneratedMessage functionProto,
            @Nullable FlinkMetricContainer flinkMetricContainer,
            @Nullable KeyedStateBackend<?> keyedStateBackend,
            @Nullable TypeSerializer<?> keySerializer,
            @Nullable TimerRegistrationHandler timerRegistration,
            MemoryManager memoryManager,
            double managedMemoryFraction,
            FlinkFnApi.CoderInfoDescriptor inputCoderDescriptor,
            FlinkFnApi.CoderInfoDescriptor outputCoderDescriptor,
            @Nullable FlinkFnApi.CoderInfoDescriptor timerCoderDescriptor) {
        super(
                environment,
                taskName,
                environmentManager,
                flinkMetricContainer,
                keyedStateBackend,
                null,
                keySerializer,
                null,
                timerRegistration,
                memoryManager,
                managedMemoryFraction,
                inputCoderDescriptor,
                outputCoderDescriptor,
                Collections.emptyMap());
        this.functionProto = functionProto;
        this.timerCoderDescriptor = timerCoderDescriptor;
    }

    @Override
    protected void buildTransforms(RunnerApi.Components.Builder componentsBuilder) {
        final RunnerApi.ParDoPayload.Builder payload =
                RunnerApi.ParDoPayload.newBuilder()
                        .setDoFn(
                                RunnerApi.FunctionSpec.newBuilder()
                                        .setUrn(PROCESS_TABLE_FUNCTION_URN)
                                        .setPayload(
                                                org.apache.beam.vendor.grpc.v1p60p1.com.google
                                                        .protobuf.ByteString.copyFrom(
                                                        functionProto.toByteArray()))
                                        .build());
        if (timerCoderDescriptor != null) {
            payload.putTimerFamilySpecs(
                    TIMER_ID,
                    RunnerApi.TimerFamilySpec.newBuilder()
                            .setTimeDomain(RunnerApi.TimeDomain.Enum.EVENT_TIME)
                            .setTimerFamilyCoderId(WRAPPER_TIMER_CODER_ID)
                            .build());
        }

        componentsBuilder.putTransforms(
                TRANSFORM_ID,
                RunnerApi.PTransform.newBuilder()
                        .setUniqueName(TRANSFORM_ID)
                        .setSpec(
                                RunnerApi.FunctionSpec.newBuilder()
                                        .setUrn(
                                                BeamUrns.getUrn(
                                                        RunnerApi.StandardPTransforms.Primitives
                                                                .PAR_DO))
                                        .setPayload(payload.build().toByteString())
                                        .build())
                        .putInputs(MAIN_INPUT_NAME, INPUT_COLLECTION_ID)
                        .putOutputs(MAIN_OUTPUT_NAME, OUTPUT_COLLECTION_ID)
                        .build());
    }

    @Override
    protected List<TimerReference> getTimers(RunnerApi.Components components) {
        if (timerCoderDescriptor == null) {
            return Collections.emptyList();
        }
        final RunnerApi.ExecutableStagePayload.TimerId timerId =
                RunnerApi.ExecutableStagePayload.TimerId.newBuilder()
                        .setTransformId(TRANSFORM_ID)
                        .setLocalName(TIMER_ID)
                        .build();
        return Collections.singletonList(TimerReference.fromTimerId(timerId, components));
    }

    @Override
    protected Optional<RunnerApi.Coder> getOptionalTimerCoderProto() {
        if (timerCoderDescriptor == null) {
            return Optional.empty();
        }
        return Optional.of(ProtoUtils.createCoderProto(timerCoderDescriptor));
    }
}
