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
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
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
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.StreamingMultiJoinOperatorFactory;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor.ConditionAttributeRef;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.JoinKeyExtractor;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
        minPlanVersion = FlinkVersion.v2_1,
        minStateVersion = FlinkVersion.v2_1)
public class StreamExecMultiJoin extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

    public static final String MULTI_JOIN_TRANSFORMATION = "multi-join";
    private static final String FIELD_NAME_JOIN_TYPES = "joinTypes";
    private static final String FIELD_NAME_JOIN_CONDITIONS = "joinConditions";
    private static final String FIELD_NAME_JOIN_ATTRIBUTE_MAP = "joinAttributeMap";
    private static final String FIELD_NAME_INPUT_UNIQUE_KEYS = "inputUniqueKeys";
    private static final String FIELD_NAME_MULTI_JOIN_CONDITION = "multiJoinCondition";
    private static final String FIELD_NAME_LEVELS = "levels";

    @JsonProperty(FIELD_NAME_JOIN_TYPES)
    private final List<FlinkJoinType> joinTypes;

    @JsonProperty(FIELD_NAME_JOIN_CONDITIONS)
    private final List<? extends @Nullable RexNode> joinConditions;

    @SuppressWarnings({"unused", "FieldCanBeLocal"})
    @JsonProperty(FIELD_NAME_MULTI_JOIN_CONDITION)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final RexNode multiJoinCondition;

    @JsonProperty(FIELD_NAME_JOIN_ATTRIBUTE_MAP)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private final Map<Integer, List<ConditionAttributeRef>> joinAttributeMap;

    // Why List<List<int[]>> as a type
    // Each unique key can be also a composite key with multiple fields, thus -> int[].
    // Theoretically, each input can have multiple unique keys, thus -> List<int[]>
    // Since we have multiple inputs -> List<List<int[]>>
    @JsonProperty(FIELD_NAME_INPUT_UNIQUE_KEYS)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private final List<List<int[]>> inputUniqueKeys;

    @JsonProperty(FIELD_NAME_STATE)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final List<StateMetadata> stateMetadataList;

    @JsonProperty(FIELD_NAME_LEVELS)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final List<Integer> levels;

    public StreamExecMultiJoin(
            final ReadableConfig tableConfig,
            final List<FlinkJoinType> joinTypes,
            final List<? extends @Nullable RexNode> joinConditions,
            @Nullable final RexNode multiJoinCondition,
            final Map<Integer, List<ConditionAttributeRef>> joinAttributeMap,
            final List<List<int[]>> inputUniqueKeys,
            final List<Integer> levels,
            final Map<Integer, Long> stateTtlFromHint,
            final List<InputProperty> inputProperties,
            final RowType outputType,
            final String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecMultiJoin.class),
                ExecNodeContext.newPersistedConfig(StreamExecMultiJoin.class, tableConfig),
                joinTypes,
                joinConditions,
                multiJoinCondition,
                joinAttributeMap,
                inputUniqueKeys,
                levels,
                StateMetadata.getMultiInputOperatorDefaultMeta(
                        stateTtlFromHint, tableConfig, generateStateNames(inputProperties.size())),
                inputProperties,
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecMultiJoin(
            @JsonProperty(FIELD_NAME_ID) final int id,
            @JsonProperty(FIELD_NAME_TYPE) final ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) final ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_JOIN_TYPES) final List<FlinkJoinType> joinTypes,
            @JsonProperty(FIELD_NAME_JOIN_CONDITIONS)
                    final List<? extends @Nullable RexNode> joinConditions,
            @Nullable @JsonProperty(FIELD_NAME_MULTI_JOIN_CONDITION)
                    final RexNode multiJoinCondition,
            @JsonProperty(FIELD_NAME_JOIN_ATTRIBUTE_MAP)
                    final Map<Integer, List<ConditionAttributeRef>> joinAttributeMap,
            @JsonProperty(FIELD_NAME_INPUT_UNIQUE_KEYS) final List<List<int[]>> inputUniqueKeys,
            @JsonProperty(FIELD_NAME_LEVELS) final List<Integer> levels,
            @Nullable @JsonProperty(FIELD_NAME_STATE) final List<StateMetadata> stateMetadataList,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) final List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) final RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) final String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        validateInputs(inputProperties, joinTypes, joinConditions, inputUniqueKeys);
        this.joinTypes = checkNotNull(joinTypes);
        this.joinConditions = checkNotNull(joinConditions);
        this.inputUniqueKeys = checkNotNull(inputUniqueKeys);
        this.multiJoinCondition = multiJoinCondition;
        this.joinAttributeMap = Objects.requireNonNullElseGet(joinAttributeMap, Map::of);
        this.stateMetadataList = stateMetadataList;
        this.levels = levels;
    }

    private void validateInputs(
            final List<InputProperty> inputProperties,
            final List<FlinkJoinType> joinTypes,
            final List<? extends @Nullable RexNode> joinConditions,
            final List<List<int[]>> inputUniqueKeys) {
        checkArgument(
                inputProperties.size() >= 2, "Multi-input join operator needs at least 2 inputs.");
        checkArgument(
                joinTypes.size() == inputProperties.size(),
                "Size of joinTypes must match the number of inputs.");
        checkArgument(
                joinConditions.size() == inputProperties.size(),
                "Size of joinConditions must match the number of inputs.");
        checkArgument(
                inputUniqueKeys.size() == inputProperties.size(),
                "Size of inputUniqueKeys must match the number of inputs.");
    }

    private static String[] generateStateNames(int numInputs) {
        return IntStream.range(0, numInputs)
                .mapToObj(i -> "input-state-" + i)
                .toArray(String[]::new);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Transformation<RowData> translateToPlanInternal(
            final PlannerBase planner, final ExecNodeConfig config) {
        final List<ExecEdge> inputEdges = getInputEdges();
        final int numInputs = inputEdges.size();
        final ClassLoader classLoader = planner.getFlinkContext().getClassLoader();

        final List<Transformation<RowData>> inputTransforms = new ArrayList<>(numInputs);
        final List<InternalTypeInfo<RowData>> inputTypeInfos = new ArrayList<>(numInputs);
        final List<RowType> inputRowTypes = new ArrayList<>(numInputs);

        for (final ExecEdge inputEdge : inputEdges) {
            final Transformation<RowData> transform =
                    (Transformation<RowData>) inputEdge.translateToPlan(planner);
            inputTransforms.add(transform);
            final RowType inputType = (RowType) inputEdge.getOutputType();
            inputRowTypes.add(inputType);
            inputTypeInfos.add(InternalTypeInfo.of(inputType));
        }

        final JoinKeyExtractor keyExtractor =
                new AttributeBasedJoinKeyExtractor(joinAttributeMap, inputRowTypes, levels);

        final List<JoinInputSideSpec> inputSideSpecs = new ArrayList<>();
        for (int i = 0; i < numInputs; i++) {
            inputSideSpecs.add(
                    JoinUtil.analyzeJoinInput(
                            planner.getFlinkContext().getClassLoader(),
                            inputTypeInfos.get(i),
                            keyExtractor.getJoinKeyIndices(i),
                            inputUniqueKeys.get(i)));
        }

        final GeneratedJoinCondition[] generatedJoinConditions =
                createJoinConditions(config, classLoader, inputRowTypes);

        final StreamOperatorFactory<RowData> operatorFactory =
                createOperatorFactory(
                        config,
                        inputTypeInfos,
                        inputSideSpecs,
                        generatedJoinConditions,
                        keyExtractor,
                        levels);

        final List<KeySelector<RowData, RowData>> commonJoinKeySelectors =
                createKeySelectors(planner, inputTypeInfos, keyExtractor);

        final TransformationMetadata metadata =
                createTransformationMeta(MULTI_JOIN_TRANSFORMATION, config);

        final Transformation<RowData> transform =
                createTransformation(
                        inputTransforms,
                        metadata,
                        operatorFactory,
                        commonJoinKeySelectors,
                        keyExtractor);

        if (inputsContainSingleton()) {
            transform.setParallelism(1);
            transform.setMaxParallelism(1);
        }

        return transform;
    }

    private GeneratedJoinCondition[] createJoinConditions(
            final ExecNodeConfig config,
            final ClassLoader classLoader,
            final List<RowType> inputRowTypes) {

        final GeneratedJoinCondition[] generatedJoinConditions =
                new GeneratedJoinCondition[joinConditions.size()];

        int leftIdx = 0;
        int rightIdx = levels.size() - 1;
        int shift = 0;
        while (leftIdx < rightIdx) {
            final RowType leftRowType;
            final RowType rightRowType;
            final int leftLevel = levels.get(leftIdx);
            final int rightLevel = levels.get(rightIdx);
            if (leftLevel > rightLevel) {
                RexNode rexCond = joinConditions.get(leftIdx);
                int finalShift = shift;
                assert rexCond != null;
                rexCond =
                        rexCond.accept(
                                new RexShuttle() {
                                    @Override
                                    public RexNode visitInputRef(RexInputRef inputRef) {
                                        int newIdx = inputRef.getIndex() + finalShift;
                                        return new RexInputRef(newIdx, inputRef.getType());
                                    }
                                });
                leftRowType = inputRowTypes.get(leftIdx);
                rightRowType = getTypeFromList(inputRowTypes.subList(leftIdx + 1, rightIdx + 1));
                final GeneratedJoinCondition generatedCondition =
                        generateJoinConditionForInput(
                                config, classLoader, rexCond, leftRowType, rightRowType);
                if (generatedCondition != null) {
                    generatedJoinConditions[leftIdx] = generatedCondition;
                }

                leftIdx += 1;
                shift -= leftRowType.getFieldCount();
            } else {
                RexNode rexCond = joinConditions.get(rightIdx);
                int finalShift = shift;
                assert rexCond != null;
                rexCond =
                        rexCond.accept(
                                new RexShuttle() {
                                    @Override
                                    public RexNode visitInputRef(RexInputRef inputRef) {
                                        int newIdx = inputRef.getIndex() + finalShift;
                                        return new RexInputRef(newIdx, inputRef.getType());
                                    }
                                });
                if (leftLevel == rightLevel) {
                    leftRowType = inputRowTypes.get(leftIdx);
                } else {
                    leftRowType = getTypeFromList(inputRowTypes.subList(leftIdx, rightIdx));
                }
                rightRowType = inputRowTypes.get(rightIdx);
                final GeneratedJoinCondition generatedCondition =
                        generateJoinConditionForInput(
                                config, classLoader, rexCond, leftRowType, rightRowType);
                if (generatedCondition != null) {
                    generatedJoinConditions[rightIdx] = generatedCondition;
                }
                rightIdx -= 1;
            }
        }

        return generatedJoinConditions;
    }

    private StreamOperatorFactory<RowData> createOperatorFactory(
            final ExecNodeConfig config,
            final List<InternalTypeInfo<RowData>> inputTypeInfos,
            final List<JoinInputSideSpec> inputSideSpecs,
            final GeneratedJoinCondition[] joinConditions,
            final JoinKeyExtractor keyExtractor,
            List<Integer> levels) {
        final List<Long> stateTtls =
                StateMetadata.getStateTtlForMultiInputOperator(
                        config, inputTypeInfos.size(), stateMetadataList);
        final long[] stateRetentionTimes = stateTtls.stream().mapToLong(Long::longValue).toArray();

        return new StreamingMultiJoinOperatorFactory(
                inputTypeInfos,
                inputSideSpecs,
                joinTypes,
                null, // multiJoinCondition is currently not used
                stateRetentionTimes,
                joinConditions,
                keyExtractor,
                joinAttributeMap,
                levels);
    }

    private List<KeySelector<RowData, RowData>> createKeySelectors(
            final PlannerBase planner,
            final List<InternalTypeInfo<RowData>> inputTypeInfos,
            final JoinKeyExtractor keyExtractor) {
        return IntStream.range(0, inputTypeInfos.size())
                .mapToObj(
                        i ->
                                KeySelectorUtil.getRowDataSelector(
                                        planner.getFlinkContext().getClassLoader(),
                                        keyExtractor.getCommonJoinKeyIndices(i),
                                        inputTypeInfos.get(i)))
                .collect(Collectors.toList());
    }

    private Transformation<RowData> createTransformation(
            final List<Transformation<RowData>> inputTransforms,
            final TransformationMetadata metadata,
            final StreamOperatorFactory<RowData> operatorFactory,
            final List<KeySelector<RowData, RowData>> keySelectors,
            final JoinKeyExtractor keyExtractor) {
        if (inputTransforms.isEmpty()) {
            throw new IllegalStateException("StreamExecMultiJoin requires at least two inputs.");
        }

        return ExecNodeUtil.createKeyedMultiInputTransformation(
                inputTransforms,
                keySelectors,
                InternalTypeInfo.of(keyExtractor.getCommonJoinKeyType()),
                metadata,
                operatorFactory,
                InternalTypeInfo.of(getOutputType()),
                inputTransforms.get(0).getParallelism(),
                false);
    }

    private GeneratedJoinCondition generateJoinConditionForInput(
            final ExecNodeConfig config,
            final ClassLoader classLoader,
            final RexNode joinCondition,
            final RowType leftRowType,
            final RowType rightRowType) {

        return JoinUtil.generateConditionFunction(
                config, classLoader, joinCondition, leftRowType, rightRowType);
    }

    /**
     * Calculates the accumulated {@link RowType} of all inputs from the list of input row types.
     */
    private RowType getTypeFromList(final List<RowType> inputRowTypes) {

        final LogicalType[] fieldTypes =
                inputRowTypes.stream()
                        .flatMap(rowType -> rowType.getChildren().stream())
                        .toArray(LogicalType[]::new);

        return RowType.of(fieldTypes);
    }
}
