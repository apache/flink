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
import org.apache.flink.table.connector.ChangelogMode;
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
import org.apache.flink.table.planner.plan.utils.MultiJoinUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.StreamingMultiJoinOperatorFactory;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor.ConditionAttributeRef;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.JoinKeyExtractor;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.RuntimeChangelogMode;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
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
    private static final String FIELD_NAME_INPUT_UPSERT_KEYS = "inputUpsertKeys";
    private static final String FIELD_NAME_INPUT_CHANGELOG_MODES = "inputChangelogModes";
    private static final String FIELD_NAME_MULTI_JOIN_CONDITION = "multiJoinCondition";

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
    // Only compiled plans before Flink 2.4 contain the unique keys instead of the upsert keys. The
    // state of their inputs stays keyed as before.
    @Nullable
    @JsonProperty(FIELD_NAME_INPUT_UNIQUE_KEYS)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private final List<List<int[]>> inputUniqueKeys;

    @Nullable
    @JsonProperty(FIELD_NAME_INPUT_UPSERT_KEYS)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final List<List<int[]>> inputUpsertKeys;

    @Nullable
    @JsonProperty(FIELD_NAME_INPUT_CHANGELOG_MODES)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final List<ChangelogMode> inputChangelogModes;

    @JsonProperty(FIELD_NAME_STATE)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final List<StateMetadata> stateMetadataList;

    public StreamExecMultiJoin(
            final ReadableConfig tableConfig,
            final List<FlinkJoinType> joinTypes,
            final List<? extends @Nullable RexNode> joinConditions,
            @Nullable final RexNode multiJoinCondition,
            final Map<Integer, List<ConditionAttributeRef>> joinAttributeMap,
            final List<List<int[]>> inputUpsertKeys,
            final List<ChangelogMode> inputChangelogModes,
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
                null,
                inputUpsertKeys,
                inputChangelogModes,
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
            @Nullable @JsonProperty(FIELD_NAME_INPUT_UNIQUE_KEYS)
                    final List<List<int[]>> inputUniqueKeys,
            @Nullable @JsonProperty(FIELD_NAME_INPUT_UPSERT_KEYS)
                    final List<List<int[]>> inputUpsertKeys,
            @Nullable @JsonProperty(FIELD_NAME_INPUT_CHANGELOG_MODES)
                    final List<ChangelogMode> inputChangelogModes,
            @Nullable @JsonProperty(FIELD_NAME_STATE) final List<StateMetadata> stateMetadataList,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) final List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) final RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) final String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        validateInputs(
                inputProperties,
                joinTypes,
                joinConditions,
                inputUpsertKeys != null ? inputUpsertKeys : inputUniqueKeys);
        this.joinTypes = checkNotNull(joinTypes);
        this.joinConditions = checkNotNull(joinConditions);
        this.inputUniqueKeys = inputUniqueKeys;
        this.inputUpsertKeys = inputUpsertKeys;
        this.inputChangelogModes = inputChangelogModes;
        this.multiJoinCondition = multiJoinCondition;
        this.joinAttributeMap = Objects.requireNonNullElseGet(joinAttributeMap, Map::of);
        this.stateMetadataList = stateMetadataList;
    }

    private void validateInputs(
            final List<InputProperty> inputProperties,
            final List<FlinkJoinType> joinTypes,
            final List<? extends @Nullable RexNode> joinConditions,
            @Nullable final List<List<int[]>> inputKeys) {
        checkArgument(
                inputProperties.size() >= 2, "Multi-input join operator needs at least 2 inputs.");
        checkArgument(
                joinTypes.size() == inputProperties.size(),
                "Size of joinTypes must match the number of inputs.");
        checkArgument(
                joinConditions.size() == inputProperties.size(),
                "Size of joinConditions must match the number of inputs.");
        checkArgument(
                inputKeys != null && inputKeys.size() == inputProperties.size(),
                "Size of inputUpsertKeys must match the number of inputs.");
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
                new AttributeBasedJoinKeyExtractor(joinAttributeMap, inputRowTypes);

        final List<List<int[]>> inputKeys =
                inputUpsertKeys != null ? inputUpsertKeys : checkNotNull(inputUniqueKeys);
        final List<JoinInputSideSpec> inputSideSpecs = new ArrayList<>();
        for (int i = 0; i < numInputs; i++) {
            inputSideSpecs.add(
                    JoinUtil.analyzeJoinInput(
                            planner.getFlinkContext().getClassLoader(),
                            inputTypeInfos.get(i),
                            keyExtractor.getJoinKeyIndices(i),
                            inputKeys.get(i)));
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
                        MultiJoinUtil.createNullFilterFields(
                                inputRowTypes, joinConditions, joinTypes),
                        IntStream.range(0, numInputs)
                                .mapToObj(
                                        i ->
                                                toRuntimeChangelogMode(
                                                        i, inputKeys.get(i), keyExtractor))
                                .collect(Collectors.toList()),
                        MultiJoinUtil.createConditionFields(
                                inputRowTypes, joinConditions, inputKeys));

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
        for (int i = 0; i < joinConditions.size(); i++) {
            final RexNode rexCond = joinConditions.get(i);
            if (rexCond == null) {
                // No condition for this input, which is valid e.g. for the first input.
                continue;
            }

            final GeneratedJoinCondition generatedCondition =
                    generateJoinConditionForInput(config, classLoader, rexCond, inputRowTypes, i);

            // For the first input (i=0), there is no preceding input to join with,
            // so no join condition is generated.
            if (generatedCondition != null) {
                generatedJoinConditions[i] = generatedCondition;
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
            final int[][] nullFilterFields,
            final List<RuntimeChangelogMode> inputChangelogModes,
            final int[][] conditionFields) {
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
                nullFilterFields,
                inputChangelogModes,
                conditionFields);
    }

    /**
     * Returns the changelog mode of the input, or null if it is unknown. Compiled plans before
     * Flink 2.4 don't contain the changelog modes, but the planner only let an input omit
     * UPDATE_BEFORE or delete by key if one of its keys contains the common join key.
     */
    private @Nullable RuntimeChangelogMode toRuntimeChangelogMode(
            final int inputId, final List<int[]> inputKeys, final JoinKeyExtractor keyExtractor) {
        if (inputChangelogModes != null) {
            return RuntimeChangelogMode.serialize(inputChangelogModes.get(inputId));
        }
        final ImmutableBitSet commonJoinKey =
                ImmutableBitSet.of(keyExtractor.getCommonJoinKeyIndices(inputId));
        final boolean mayBeUpsert =
                !commonJoinKey.isEmpty()
                        && inputKeys.stream()
                                .anyMatch(key -> ImmutableBitSet.of(key).contains(commonJoinKey));
        return mayBeUpsert ? null : RuntimeChangelogMode.serialize(ChangelogMode.all());
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
            final List<RowType> inputRowTypes,
            final int inputIndex) {
        // Our join conditions are always associated with the left side (with the inputs to the
        // left). For input 0, there is no input to the left, so there is no join condition.
        if (inputIndex == 0) {
            return null;
        }

        final RowType leftType = leftTypeForIndex(inputRowTypes, inputIndex);
        final RowType rightType = inputRowTypes.get(inputIndex);

        return JoinUtil.generateConditionFunction(
                config, classLoader, joinCondition, leftType, rightType);
    }

    /**
     * Calculates the accumulated {@link RowType} of all inputs to the left of a given input index.
     *
     * <p>For a multi-way join, the condition for input `i` is evaluated against the combined row of
     * all inputs from `0` to `i-1`. This method computes the {@link RowType} for this combined row,
     * which is essential for the code generation of the join condition. The resulting {@link
     * RowType} is a flat structure of all fields from the preceding inputs.
     */
    private RowType leftTypeForIndex(final List<RowType> inputRowTypes, final int inputIndex) {
        if (inputIndex <= 0) {
            throw new IllegalArgumentException(
                    "Input index must be greater than 0 for accumulated left type calculation");
        }

        final LogicalType[] fieldTypes =
                inputRowTypes.stream()
                        .limit(inputIndex)
                        .flatMap(rowType -> rowType.getChildren().stream())
                        .toArray(LogicalType[]::new);

        return RowType.of(fieldTypes);
    }
}
