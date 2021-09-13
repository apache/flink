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

package org.apache.flink.streaming.api.runners.python.beam;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.python.metric.FlinkMetricContainer;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.streaming.api.operators.python.timer.TimerRegistration;
import org.apache.flink.streaming.api.utils.ProtoUtils;
import org.apache.flink.util.Preconditions;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.BeamUrns;
import org.apache.beam.runners.core.construction.graph.TimerReference;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.python.Constants.INPUT_COLLECTION_ID;
import static org.apache.flink.python.Constants.MAIN_INPUT_NAME;
import static org.apache.flink.python.Constants.MAIN_OUTPUT_NAME;
import static org.apache.flink.python.Constants.OUTPUT_COLLECTION_ID;
import static org.apache.flink.python.Constants.STATELESS_FUNCTION_URN;
import static org.apache.flink.python.Constants.TIMER_ID;
import static org.apache.flink.python.Constants.WINDOW_STRATEGY;
import static org.apache.flink.python.Constants.WRAPPER_TIMER_CODER_ID;
import static org.apache.flink.streaming.api.utils.ProtoUtils.createCoderProto;
import static org.apache.flink.streaming.api.utils.ProtoUtils.createReviseOutputDataStreamFunctionProto;

/**
 * {@link BeamDataStreamPythonFunctionRunner} is responsible for starting a beam python harness to
 * execute user defined python function.
 */
@Internal
public class BeamDataStreamPythonFunctionRunner extends BeamPythonFunctionRunner {

    private static final String TRANSFORM_ID_PREFIX = "transform-";
    private static final String COLLECTION_PREFIX = "collection-";
    private static final String CODER_PREFIX = "coder-";

    @Nullable private final FlinkFnApi.CoderInfoDescriptor timerCoderDescriptor;
    private final String headOperatorFunctionUrn;
    private final List<FlinkFnApi.UserDefinedDataStreamFunction> userDefinedDataStreamFunctions;

    public BeamDataStreamPythonFunctionRunner(
            String taskName,
            PythonEnvironmentManager environmentManager,
            String headOperatorFunctionUrn,
            List<FlinkFnApi.UserDefinedDataStreamFunction> userDefinedDataStreamFunctions,
            Map<String, String> jobOptions,
            @Nullable FlinkMetricContainer flinkMetricContainer,
            KeyedStateBackend<?> stateBackend,
            TypeSerializer<?> keySerializer,
            TypeSerializer<?> namespaceSerializer,
            @Nullable TimerRegistration timerRegistration,
            MemoryManager memoryManager,
            double managedMemoryFraction,
            FlinkFnApi.CoderInfoDescriptor inputCoderDescriptor,
            FlinkFnApi.CoderInfoDescriptor outputCoderDescriptor,
            FlinkFnApi.CoderInfoDescriptor timerCoderDescriptor) {
        super(
                taskName,
                environmentManager,
                jobOptions,
                flinkMetricContainer,
                stateBackend,
                keySerializer,
                namespaceSerializer,
                timerRegistration,
                memoryManager,
                managedMemoryFraction,
                inputCoderDescriptor,
                outputCoderDescriptor);
        this.headOperatorFunctionUrn = Preconditions.checkNotNull(headOperatorFunctionUrn);
        Preconditions.checkArgument(
                userDefinedDataStreamFunctions != null
                        && userDefinedDataStreamFunctions.size() >= 1);
        this.userDefinedDataStreamFunctions = userDefinedDataStreamFunctions;
        this.timerCoderDescriptor = timerCoderDescriptor;
    }

    @Override
    protected void buildTransforms(RunnerApi.Components.Builder componentsBuilder) {
        for (int i = 0; i < userDefinedDataStreamFunctions.size() + 1; i++) {
            String functionUrn;
            if (i == 0) {
                functionUrn = headOperatorFunctionUrn;
            } else {
                functionUrn = STATELESS_FUNCTION_URN;
            }

            FlinkFnApi.UserDefinedDataStreamFunction functionProto;
            if (i < userDefinedDataStreamFunctions.size()) {
                functionProto = userDefinedDataStreamFunctions.get(i);
            } else {
                // the last function in the operation tree is used to prune the watermark column
                functionProto = createReviseOutputDataStreamFunctionProto();
            }

            // Use ParDoPayload as a wrapper of the actual payload as timer is only supported in
            // ParDo
            final RunnerApi.ParDoPayload.Builder payloadBuilder =
                    RunnerApi.ParDoPayload.newBuilder()
                            .setDoFn(
                                    RunnerApi.FunctionSpec.newBuilder()
                                            .setUrn(functionUrn)
                                            .setPayload(
                                                    org.apache.beam.vendor.grpc.v1p26p0.com.google
                                                            .protobuf.ByteString.copyFrom(
                                                            functionProto.toByteArray()))
                                            .build());

            // Timer is only available in the head operator
            if (i == 0 && timerCoderDescriptor != null) {
                payloadBuilder.putTimerFamilySpecs(
                        TIMER_ID,
                        RunnerApi.TimerFamilySpec.newBuilder()
                                // this field is not used, always set it as event time
                                .setTimeDomain(RunnerApi.TimeDomain.Enum.EVENT_TIME)
                                .setTimerFamilyCoderId(WRAPPER_TIMER_CODER_ID)
                                .build());
            }

            final String transformName = TRANSFORM_ID_PREFIX + i;

            final RunnerApi.PTransform.Builder transformBuilder =
                    RunnerApi.PTransform.newBuilder()
                            .setUniqueName(transformName)
                            .setSpec(
                                    RunnerApi.FunctionSpec.newBuilder()
                                            .setUrn(
                                                    BeamUrns.getUrn(
                                                            RunnerApi.StandardPTransforms.Primitives
                                                                    .PAR_DO))
                                            .setPayload(payloadBuilder.build().toByteString())
                                            .build());

            // prepare inputs
            if (i == 0) {
                transformBuilder.putInputs(MAIN_INPUT_NAME, INPUT_COLLECTION_ID);
            } else {
                transformBuilder.putInputs(MAIN_INPUT_NAME, COLLECTION_PREFIX + (i - 1));
            }

            // prepare outputs
            if (i == userDefinedDataStreamFunctions.size()) {
                transformBuilder.putOutputs(MAIN_OUTPUT_NAME, OUTPUT_COLLECTION_ID);
            } else {
                transformBuilder.putOutputs(MAIN_OUTPUT_NAME, COLLECTION_PREFIX + i);

                componentsBuilder
                        .putPcollections(
                                COLLECTION_PREFIX + i,
                                RunnerApi.PCollection.newBuilder()
                                        .setWindowingStrategyId(WINDOW_STRATEGY)
                                        .setCoderId(CODER_PREFIX + i)
                                        .build())
                        .putCoders(CODER_PREFIX + i, createCoderProto(inputCoderDescriptor));
            }

            componentsBuilder.putTransforms(transformName, transformBuilder.build());
        }
    }

    @Override
    protected List<TimerReference> getTimers(RunnerApi.Components components) {
        if (timerCoderDescriptor != null) {
            RunnerApi.ExecutableStagePayload.TimerId timerId =
                    RunnerApi.ExecutableStagePayload.TimerId.newBuilder()
                            .setTransformId(TRANSFORM_ID_PREFIX + 0)
                            .setLocalName(TIMER_ID)
                            .build();
            return Collections.singletonList(TimerReference.fromTimerId(timerId, components));
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    protected Optional<RunnerApi.Coder> getOptionalTimerCoderProto() {
        if (timerCoderDescriptor != null) {
            return Optional.of(ProtoUtils.createCoderProto(timerCoderDescriptor));
        } else {
            return Optional.empty();
        }
    }
}
