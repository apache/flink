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
import org.apache.flink.python.env.process.ProcessPythonEnvironmentManager;
import org.apache.flink.python.metric.process.FlinkMetricContainer;
import org.apache.flink.python.util.ProtoUtils;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.streaming.api.operators.python.process.timer.TimerRegistration;
import org.apache.flink.util.Preconditions;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.BeamUrns;
import org.apache.beam.runners.core.construction.graph.TimerReference;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
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
import static org.apache.flink.python.util.ProtoUtils.createCoderProto;
import static org.apache.flink.python.util.ProtoUtils.createReviseOutputDataStreamFunctionProto;

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
            ProcessPythonEnvironmentManager environmentManager,
            String headOperatorFunctionUrn,
            List<FlinkFnApi.UserDefinedDataStreamFunction> userDefinedDataStreamFunctions,
            @Nullable FlinkMetricContainer flinkMetricContainer,
            @Nullable KeyedStateBackend<?> keyedStateBackend,
            @Nullable OperatorStateBackend operatorStateBackend,
            @Nullable TypeSerializer<?> keySerializer,
            @Nullable TypeSerializer<?> namespaceSerializer,
            @Nullable TimerRegistration timerRegistration,
            MemoryManager memoryManager,
            double managedMemoryFraction,
            FlinkFnApi.CoderInfoDescriptor inputCoderDescriptor,
            FlinkFnApi.CoderInfoDescriptor outputCoderDescriptor,
            @Nullable FlinkFnApi.CoderInfoDescriptor timerCoderDescriptor,
            Map<String, FlinkFnApi.CoderInfoDescriptor> sideOutputCoderDescriptors) {
        super(
                taskName,
                environmentManager,
                flinkMetricContainer,
                keyedStateBackend,
                operatorStateBackend,
                keySerializer,
                namespaceSerializer,
                timerRegistration,
                memoryManager,
                managedMemoryFraction,
                inputCoderDescriptor,
                outputCoderDescriptor,
                sideOutputCoderDescriptors);
        this.headOperatorFunctionUrn = Preconditions.checkNotNull(headOperatorFunctionUrn);
        Preconditions.checkArgument(
                userDefinedDataStreamFunctions != null
                        && userDefinedDataStreamFunctions.size() >= 1);
        this.userDefinedDataStreamFunctions = userDefinedDataStreamFunctions;
        this.timerCoderDescriptor = timerCoderDescriptor;
    }

    @Override
    protected void buildTransforms(RunnerApi.Components.Builder componentsBuilder) {
        for (int i = 0; i < userDefinedDataStreamFunctions.size(); i++) {
            final Map<String, String> outputCollectionMap = new HashMap<>();

            // Prepare side outputs
            if (i == userDefinedDataStreamFunctions.size() - 1) {
                for (Map.Entry<String, FlinkFnApi.CoderInfoDescriptor> entry :
                        sideOutputCoderDescriptors.entrySet()) {
                    final String reviseCollectionId =
                            COLLECTION_PREFIX + "revise-" + entry.getKey();
                    final String reviseCoderId = CODER_PREFIX + "revise-" + entry.getKey();
                    outputCollectionMap.put(entry.getKey(), reviseCollectionId);
                    addCollectionToComponents(componentsBuilder, reviseCollectionId, reviseCoderId);
                }
            }

            // Prepare main outputs
            final String outputCollectionId = COLLECTION_PREFIX + i;
            final String outputCoderId = CODER_PREFIX + i;
            outputCollectionMap.put(MAIN_OUTPUT_NAME, outputCollectionId);
            addCollectionToComponents(componentsBuilder, outputCollectionId, outputCoderId);

            final String transformId = TRANSFORM_ID_PREFIX + i;
            final FlinkFnApi.UserDefinedDataStreamFunction functionProto =
                    userDefinedDataStreamFunctions.get(i);
            if (i == 0) {
                addTransformToComponents(
                        componentsBuilder,
                        transformId,
                        createUdfPayload(functionProto, headOperatorFunctionUrn, true),
                        INPUT_COLLECTION_ID,
                        outputCollectionMap);
            } else {
                addTransformToComponents(
                        componentsBuilder,
                        transformId,
                        createUdfPayload(functionProto, STATELESS_FUNCTION_URN, false),
                        COLLECTION_PREFIX + (i - 1),
                        outputCollectionMap);
            }
        }

        // Add REVISE_OUTPUT transformation for side outputs
        for (Map.Entry<String, FlinkFnApi.CoderInfoDescriptor> entry :
                sideOutputCoderDescriptors.entrySet()) {
            addTransformToComponents(
                    componentsBuilder,
                    TRANSFORM_ID_PREFIX + "revise-" + entry.getKey(),
                    createRevisePayload(),
                    COLLECTION_PREFIX + "revise-" + entry.getKey(),
                    Collections.singletonMap(MAIN_OUTPUT_NAME, entry.getKey()));
        }

        // Add REVISE_OUTPUT transformation for main output
        addTransformToComponents(
                componentsBuilder,
                TRANSFORM_ID_PREFIX + "revise",
                createRevisePayload(),
                COLLECTION_PREFIX + (userDefinedDataStreamFunctions.size() - 1),
                Collections.singletonMap(MAIN_OUTPUT_NAME, OUTPUT_COLLECTION_ID));
    }

    private RunnerApi.ParDoPayload createRevisePayload() {
        final FlinkFnApi.UserDefinedDataStreamFunction proto =
                createReviseOutputDataStreamFunctionProto();
        final RunnerApi.ParDoPayload.Builder payloadBuilder =
                RunnerApi.ParDoPayload.newBuilder()
                        .setDoFn(
                                RunnerApi.FunctionSpec.newBuilder()
                                        .setUrn(STATELESS_FUNCTION_URN)
                                        .setPayload(
                                                org.apache.beam.vendor.grpc.v1p48p1.com.google
                                                        .protobuf.ByteString.copyFrom(
                                                        proto.toByteArray()))
                                        .build());
        return payloadBuilder.build();
    }

    private RunnerApi.ParDoPayload createUdfPayload(
            FlinkFnApi.UserDefinedDataStreamFunction proto, String urn, boolean createTimer) {
        // Use ParDoPayload as a wrapper of the actual payload as timer is only supported in
        // ParDo
        final RunnerApi.ParDoPayload.Builder payloadBuilder =
                RunnerApi.ParDoPayload.newBuilder()
                        .setDoFn(
                                RunnerApi.FunctionSpec.newBuilder()
                                        .setUrn(urn)
                                        .setPayload(
                                                org.apache.beam.vendor.grpc.v1p48p1.com.google
                                                        .protobuf.ByteString.copyFrom(
                                                        proto.toByteArray()))
                                        .build());

        // Timer is only available in the head operator
        if (createTimer && timerCoderDescriptor != null) {
            payloadBuilder.putTimerFamilySpecs(
                    TIMER_ID,
                    RunnerApi.TimerFamilySpec.newBuilder()
                            // this field is not used, always set it as event time
                            .setTimeDomain(RunnerApi.TimeDomain.Enum.EVENT_TIME)
                            .setTimerFamilyCoderId(WRAPPER_TIMER_CODER_ID)
                            .build());
        }

        return payloadBuilder.build();
    }

    private void addTransformToComponents(
            RunnerApi.Components.Builder componentsBuilder,
            String transformId,
            RunnerApi.ParDoPayload payload,
            String inputCollectionId,
            Map<String, String> outputCollectionMap) {
        final RunnerApi.PTransform.Builder transformBuilder =
                RunnerApi.PTransform.newBuilder()
                        .setUniqueName(transformId)
                        .setSpec(
                                RunnerApi.FunctionSpec.newBuilder()
                                        .setUrn(
                                                BeamUrns.getUrn(
                                                        RunnerApi.StandardPTransforms.Primitives
                                                                .PAR_DO))
                                        .setPayload(payload.toByteString())
                                        .build());
        transformBuilder.putInputs(MAIN_INPUT_NAME, inputCollectionId);
        transformBuilder.putAllOutputs(outputCollectionMap);
        componentsBuilder.putTransforms(transformId, transformBuilder.build());
    }

    private void addCollectionToComponents(
            RunnerApi.Components.Builder componentsBuilder, String collectionId, String coderId) {
        componentsBuilder
                .putPcollections(
                        collectionId,
                        RunnerApi.PCollection.newBuilder()
                                .setWindowingStrategyId(WINDOW_STRATEGY)
                                .setCoderId(coderId)
                                .build())
                .putCoders(coderId, createCoderProto(inputCoderDescriptor));
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
