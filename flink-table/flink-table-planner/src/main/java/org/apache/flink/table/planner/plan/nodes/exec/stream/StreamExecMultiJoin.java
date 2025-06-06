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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.StateMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.nodes.exec.utils.TransformationMetadata;
import org.apache.flink.table.planner.plan.utils.JoinUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.generated.MultiJoinCondition;
import org.apache.flink.table.runtime.operators.join.stream.StreamingMultiJoinOperator;
import org.apache.flink.table.runtime.operators.join.stream.StreamingMultiJoinOperator.JoinType;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.JoinKeyExtractor;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Stream {@link StreamExecNode} for N-way Joins. This node handles multi-way joins in streaming
 * mode, supporting different join types and conditions for each input.
 */
@ExecNodeMetadata(
        name = "stream-exec-multi-join",
        version = 1,
        producedTransformations = StreamExecMultiJoin.MULTI_JOIN_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v2_0,
        minStateVersion = FlinkVersion.v2_0)
public class StreamExecMultiJoin extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

    public static final String MULTI_JOIN_TRANSFORMATION = "multi-join";

    // Field names for JSON serialization
    private static final String FIELD_NAME_ID = "id";
    private static final String FIELD_NAME_TYPE = "type";
    private static final String FIELD_NAME_CONFIGURATION = "configuration";
    private static final String FIELD_NAME_INPUT_PROPERTIES = "inputProperties";
    private static final String FIELD_NAME_OUTPUT_TYPE = "outputType";
    private static final String FIELD_NAME_DESCRIPTION = "description";
    private static final String FIELD_NAME_JOIN_TYPES = "joinTypes";
    private static final String FIELD_NAME_JOIN_CONDITIONS = "joinConditions";
    private static final String FIELD_NAME_GENERATED_MULTI_JOIN_CONDITION =
            "generatedMultiJoinCondition";
    private static final String FIELD_NAME_JOIN_ATTRIBUTE_MAP = "joinAttributeMap";
    private static final String FIELD_NAME_INPUT_SIDE_SPECS = "inputSideSpecs";
    private static final String FIELD_NAME_STATE_METADATA_LIST = "stateMetadataList";
    private static final String FIELD_NAME_KEY_SELECTORS = "keySelectorsForTransformation";
    private static final String FIELD_NAME_COMMON_KEY_TYPE = "commonKeyType";

    @JsonProperty(FIELD_NAME_JOIN_TYPES)
    private final List<JoinType> joinTypes;

    @JsonProperty(FIELD_NAME_JOIN_CONDITIONS)
    private final List<? extends @Nullable RexNode> joinConditions;

    @JsonProperty(FIELD_NAME_JOIN_ATTRIBUTE_MAP)
    private final Map<
                    Integer,
                    Map<
                            AttributeBasedJoinKeyExtractor.AttributeRef,
                            AttributeBasedJoinKeyExtractor.AttributeRef>>
            joinAttributeMap;

    @JsonProperty(FIELD_NAME_INPUT_SIDE_SPECS)
    private final List<JoinInputSideSpec> inputSideSpecs;

    @JsonProperty(FIELD_NAME_KEY_SELECTORS)
    private final List<KeySelector<RowData, RowData>> commonJoinKeySelectorsForTransformation;

    @JsonProperty(FIELD_NAME_COMMON_KEY_TYPE)
    private final InternalTypeInfo<RowData> commonJoinKeyType;

    @JsonProperty(FIELD_NAME_STATE_METADATA_LIST)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final List<StateMetadata> stateMetadataList;

    public StreamExecMultiJoin(
            ReadableConfig tableConfig,
            List<JoinType> joinTypes,
            List<? extends @Nullable RexNode> joinConditions,
            @Nullable RexNode generatedMultiJoinCondition,
            Map<
                            Integer,
                            Map<
                                    AttributeBasedJoinKeyExtractor.AttributeRef,
                                    AttributeBasedJoinKeyExtractor.AttributeRef>>
                    joinAttributeMap,
            List<JoinInputSideSpec> inputSideSpecs,
            Map<Integer, Long> stateTtlFromHint,
            List<InputProperty> inputProperties,
            RowType outputType,
            String description,
            List<KeySelector<RowData, RowData>> commonJoinKeySelectorsForTransformation,
            InternalTypeInfo<RowData> commonJoinKeyType) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecMultiJoin.class),
                ExecNodeContext.newPersistedConfig(StreamExecMultiJoin.class, tableConfig),
                joinTypes,
                joinConditions,
                generatedMultiJoinCondition,
                joinAttributeMap,
                inputSideSpecs,
                StateMetadata.getMultiInputOperatorDefaultMeta(
                        stateTtlFromHint, tableConfig, generateStateNames(inputProperties.size())),
                inputProperties,
                outputType,
                description,
                commonJoinKeySelectorsForTransformation,
                commonJoinKeyType);
    }

    @JsonCreator
    public StreamExecMultiJoin(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_JOIN_TYPES) List<JoinType> joinTypes,
            @JsonProperty(FIELD_NAME_JOIN_CONDITIONS)
                    List<? extends @Nullable RexNode> joinConditions,
            @Nullable @JsonProperty(FIELD_NAME_GENERATED_MULTI_JOIN_CONDITION)
                    RexNode generatedMultiJoinCondition,
            @JsonProperty(FIELD_NAME_JOIN_ATTRIBUTE_MAP)
                    Map<
                                    Integer,
                                    Map<
                                            AttributeBasedJoinKeyExtractor.AttributeRef,
                                            AttributeBasedJoinKeyExtractor.AttributeRef>>
                            joinAttributeMap,
            @JsonProperty(FIELD_NAME_INPUT_SIDE_SPECS) List<JoinInputSideSpec> inputSideSpecs,
            @Nullable @JsonProperty(FIELD_NAME_STATE_METADATA_LIST)
                    List<StateMetadata> stateMetadataList,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description,
            @JsonProperty(FIELD_NAME_KEY_SELECTORS)
                    List<KeySelector<RowData, RowData>> commonJoinKeySelectorsForTransformation,
            @JsonProperty(FIELD_NAME_COMMON_KEY_TYPE) InternalTypeInfo<RowData> commonJoinKeyType) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        validateInputs(inputProperties, joinTypes, joinConditions, inputSideSpecs);

        this.joinTypes = checkNotNull(joinTypes);
        this.joinConditions = checkNotNull(joinConditions);
        this.joinAttributeMap = checkNotNull(joinAttributeMap);
        this.inputSideSpecs = checkNotNull(inputSideSpecs);
        this.stateMetadataList = stateMetadataList;
        this.commonJoinKeySelectorsForTransformation =
                checkNotNull(commonJoinKeySelectorsForTransformation);
        this.commonJoinKeyType = checkNotNull(commonJoinKeyType);
    }

    private void validateInputs(
            List<InputProperty> inputProperties,
            List<JoinType> joinTypes,
            List<? extends @Nullable RexNode> joinConditions,
            List<JoinInputSideSpec> inputSideSpecs) {
        checkArgument(
                inputProperties.size() >= 2, "Multi-input join operator needs at least 2 inputs.");
        checkArgument(
                joinTypes.size() == inputProperties.size(),
                "Size of joinTypes must match the number of inputs.");
        checkArgument(
                joinConditions.size() == inputProperties.size(),
                "Size of joinConditions must match the number of inputs.");
        checkArgument(
                inputSideSpecs.size() == inputProperties.size(),
                "Size of inputSideSpecs must match the number of inputs.");
    }

    private static String[] generateStateNames(int numInputs) {
        return IntStream.range(0, numInputs)
                .mapToObj(i -> "inputState-" + i)
                .toArray(String[]::new);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final List<ExecEdge> inputEdges = getInputEdges();
        final int numInputs = inputEdges.size();
        final ClassLoader classLoader = planner.getFlinkContext().getClassLoader();

        // Prepare input transformations and types
        final List<Transformation<RowData>> inputTransforms = new ArrayList<>(numInputs);
        final List<InternalTypeInfo<RowData>> inputTypeInfos = new ArrayList<>(numInputs);
        final List<RowType> inputRowTypes = new ArrayList<>(numInputs);

        for (ExecEdge inputEdge : inputEdges) {
            Transformation<RowData> transform =
                    (Transformation<RowData>) inputEdge.translateToPlan(planner);
            inputTransforms.add(transform);
            RowType inputType = (RowType) inputEdge.getOutputType();
            inputRowTypes.add(inputType);
            inputTypeInfos.add(InternalTypeInfo.of(inputType));
        }

        // Create key extractor and join conditions
        final JoinKeyExtractor keyExtractor =
                new AttributeBasedJoinKeyExtractor(joinAttributeMap, inputRowTypes);
        final JoinCondition[] instantiatedJoinConditions =
                createJoinConditions(config, classLoader, inputRowTypes);

        // Create operator factory
        final StreamOperatorFactory<RowData> operatorFactory =
                createOperatorFactory(
                        config, inputTypeInfos, instantiatedJoinConditions, keyExtractor);

        // Create transformation
        final TransformationMetadata metadata =
                createTransformationMeta(MULTI_JOIN_TRANSFORMATION, config);
        final Transformation<RowData> transform =
                createTransformation(inputTransforms, metadata, operatorFactory);

        // Handle singleton inputs
        if (inputsContainSingleton()) {
            transform.setParallelism(1);
            transform.setMaxParallelism(1);
        }

        return transform;
    }

    private JoinCondition[] createJoinConditions(
            ExecNodeConfig config, ClassLoader classLoader, List<RowType> inputRowTypes) {
        final JoinCondition[] instantiatedJoinConditions = new JoinCondition[joinConditions.size()];
        for (int i = 0; i < joinConditions.size(); i++) {
            RexNode rexCond = joinConditions.get(i);
            if (rexCond != null) {
                GeneratedJoinCondition generatedCondition =
                        generateJoinConditionForInput(
                                config, classLoader, rexCond, inputRowTypes, i);
                try {
                    assert generatedCondition != null;
                    instantiatedJoinConditions[i] = generatedCondition.newInstance(classLoader);
                } catch (Exception e) {
                    throw new RuntimeException(
                            "Failed to instantiate join condition for input " + i, e);
                }
            } else {
                instantiatedJoinConditions[i] = null;
            }
        }
        return instantiatedJoinConditions;
    }

    private StreamOperatorFactory<RowData> createOperatorFactory(
            ExecNodeConfig config,
            List<InternalTypeInfo<RowData>> inputTypeInfos,
            JoinCondition[] joinConditions,
            JoinKeyExtractor keyExtractor) {
        List<Long> stateTtls =
                StateMetadata.getStateTtlForMultiInputOperator(
                        config, inputTypeInfos.size(), stateMetadataList);
        long[] stateRetentionTimes = stateTtls.stream().mapToLong(Long::longValue).toArray();

        return new StreamingMultiJoinOperatorFactoryImpl(
                inputTypeInfos,
                inputSideSpecs,
                joinTypes,
                null, // multiJoinCondition is currently not used
                stateRetentionTimes,
                joinConditions,
                keyExtractor,
                joinAttributeMap);
    }

    private Transformation<RowData> createTransformation(
            List<Transformation<RowData>> inputTransforms,
            TransformationMetadata metadata,
            StreamOperatorFactory<RowData> operatorFactory) {
        if (inputTransforms.isEmpty()) {
            throw new IllegalStateException("StreamExecMultiJoin requires at least two inputs.");
        }

        return ExecNodeUtil.createKeyedMultiInputTransformation(
                inputTransforms,
                commonJoinKeySelectorsForTransformation,
                commonJoinKeyType,
                metadata,
                operatorFactory,
                InternalTypeInfo.of(getOutputType()),
                inputTransforms.get(0).getParallelism(),
                false);
    }

    private GeneratedJoinCondition generateJoinConditionForInput(
            ExecNodeConfig config,
            ClassLoader classLoader,
            RexNode joinCondition,
            List<RowType> inputRowTypes,
            int inputIndex) {
        if (inputIndex == 0) {
            return null;
        }

        RowType leftType = leftTypeForIndex(inputRowTypes, inputIndex);
        RowType rightType = inputRowTypes.get(inputIndex);

        return JoinUtil.generateConditionFunction(
                config, classLoader, joinCondition, leftType, rightType);
    }

    private RowType leftTypeForIndex(List<RowType> inputRowTypes, int inputIndex) {
        if (inputIndex <= 0) {
            throw new IllegalArgumentException(
                    "Input index must be greater than 0 for accumulated left type calculation");
        }

        RowType leftType = inputRowTypes.get(0);
        for (int i = 1; i < inputIndex; i++) {
            leftType = RowType.of(leftType, leftType);
        }

        return leftType;
    }

    /** Serializable factory for creating {@link StreamingMultiJoinOperator}. */
    private static class StreamingMultiJoinOperatorFactoryImpl
            extends AbstractStreamOperatorFactory<RowData> implements Serializable {
        private static final long serialVersionUID = 1L;

        private final List<InternalTypeInfo<RowData>> inputTypeInfos;
        private final List<JoinInputSideSpec> inputSideSpecs;
        private final List<JoinType> joinTypes;
        @Nullable private final MultiJoinCondition multiJoinCondition;
        private final long[] stateRetentionTime;
        private final JoinCondition[] joinConditions;
        private final JoinKeyExtractor keyExtractor;
        private final Map<
                        Integer,
                        Map<
                                AttributeBasedJoinKeyExtractor.AttributeRef,
                                AttributeBasedJoinKeyExtractor.AttributeRef>>
                joinAttributeMap;

        public StreamingMultiJoinOperatorFactoryImpl(
                List<InternalTypeInfo<RowData>> inputTypeInfos,
                List<JoinInputSideSpec> inputSideSpecs,
                List<JoinType> joinTypes,
                @Nullable MultiJoinCondition multiJoinCondition,
                long[] stateRetentionTime,
                JoinCondition[] joinConditions,
                JoinKeyExtractor keyExtractor,
                Map<
                                Integer,
                                Map<
                                        AttributeBasedJoinKeyExtractor.AttributeRef,
                                        AttributeBasedJoinKeyExtractor.AttributeRef>>
                        joinAttributeMap) {
            this.inputTypeInfos = inputTypeInfos;
            this.inputSideSpecs = inputSideSpecs;
            this.joinTypes = joinTypes;
            this.multiJoinCondition = multiJoinCondition;
            this.stateRetentionTime = stateRetentionTime;
            this.joinConditions = joinConditions;
            this.keyExtractor = keyExtractor;
            this.joinAttributeMap = joinAttributeMap;
        }

        @Override
        public <T extends StreamOperator<RowData>> T createStreamOperator(
                StreamOperatorParameters<RowData> parameters) {
            var inputRowTypes =
                    inputTypeInfos.stream()
                            .map(InternalTypeInfo::toRowType)
                            .collect(Collectors.toList());

            StreamingMultiJoinOperator operator =
                    new StreamingMultiJoinOperator(
                            parameters,
                            inputRowTypes,
                            inputSideSpecs,
                            joinTypes,
                            multiJoinCondition,
                            stateRetentionTime,
                            joinConditions,
                            keyExtractor,
                            joinAttributeMap);

            @SuppressWarnings("unchecked")
            T castedOperator = (T) operator;
            return castedOperator;
        }

        @Override
        public Class<? extends StreamOperator<RowData>> getStreamOperatorClass(
                ClassLoader classLoader) {
            return StreamingMultiJoinOperator.class;
        }
    }
}
