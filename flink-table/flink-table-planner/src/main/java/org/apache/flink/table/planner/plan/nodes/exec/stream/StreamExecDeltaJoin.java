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
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.codegen.FilterCodeGenerator;
import org.apache.flink.table.planner.codegen.FunctionCallCodeGenerator;
import org.apache.flink.table.planner.codegen.LookupJoinCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DeltaJoinLookupChain;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DeltaJoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DeltaJoinTree;
import org.apache.flink.table.planner.plan.nodes.exec.spec.TemporalTableSourceSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.utils.DeltaJoinUtil;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil.AsyncOptions;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.plan.utils.LookupJoinUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.generated.GeneratedFilterCondition;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.StreamingDeltaJoinOperatorFactory;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.deltajoin.AsyncDeltaJoinRunner;
import org.apache.flink.table.runtime.operators.join.deltajoin.BinaryLookupHandler;
import org.apache.flink.table.runtime.operators.join.deltajoin.DeltaJoinHandlerChain;
import org.apache.flink.table.runtime.operators.join.deltajoin.DeltaJoinRuntimeTree;
import org.apache.flink.table.runtime.operators.join.deltajoin.LookupHandlerBase;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava33.com.google.common.collect.Lists;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDeltaJoin.DELTA_JOIN_TRANSFORMATION;
import static org.apache.flink.table.planner.plan.utils.DeltaJoinUtil.combineOutputRowType;
import static org.apache.flink.table.planner.plan.utils.DeltaJoinUtil.getUnwrappedAsyncLookupFunction;
import static org.apache.flink.table.planner.plan.utils.DeltaJoinUtil.swapJoinType;
import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapDataTypeFactory;

/**
 * {@link StreamExecNode} for delta join.
 *
 * <p>This node has two versions with different capabilities:
 *
 * <h3>Version 1 (Binary Delta Join)</h3>
 *
 * <p>Introduced in Flink v2.1. This version only supports a simple two-table delta join scenario.
 * It uses two {@link DeltaJoinSpec} fields ({@link #lookupRightTableJoinSpec} and {@link
 * #lookupLeftTableJoinSpec}) to describe how each streaming side looks up the other side's
 * dimension table:
 *
 * <p>The {@link DeltaJoinTree} is built internally from these two specs during translation. The
 * operator factory is built via {@link DeltaJoinOperatorFactoryBuilderV1}.
 *
 * <pre>{@code
 * Example (v1 - Binary Delta Join):
 *
 *     Left Stream (A)  ──┐
 *                        ├──  DeltaJoin  (each side looks up the other's dimension table)
 *     Right Stream (B) ──┘
 * }</pre>
 *
 * <h3>Version 2 (Cascaded Delta Join)</h3>
 *
 * <p>Introduced in Flink v2.3. This version extends delta join to support multi-table (cascaded)
 * scenarios where each side may involve multiple dimension tables that need to be looked up in a
 * specific order. It uses the following additional structures:
 *
 * <ul>
 *   <li>{@link DeltaJoinLookupChain}: an ordered chain of lookup operations. Each {@link
 *       DeltaJoinLookupChain.Node} represents a single lookup step, using one or more already
 *       resolved inputs to look up the next dimension table.
 *   <li>{@link DeltaJoinTree}: a tree structure describing the relationships among all joins. Leaf
 *       nodes ({@link DeltaJoinTree.BinaryInputNode}) represent source tables, and non-leaf nodes
 *       ({@link DeltaJoinTree.JoinNode}) represent join operations.
 *   <li>{@link #allBinaryInputTables}: the list of all binary input (dimension) table source specs.
 *   <li>{@link #leftAllBinaryInputOrdinals} / {@link #rightAllBinaryInputOrdinals}: the ordinals
 *       identifying which binary inputs belong to the left side and which to the right side.
 *   <li>{@link #condition}: the overall join condition on this join node.
 * </ul>
 *
 * <p>The operator factory is built via {@code DeltaJoinOperatorFactoryBuilderV2}.
 *
 * <pre>{@code
 * Example (v2 - Cascaded Delta Join):
 *
 *              DeltaJoin
 *           /            \
 *       Calc3             \
 *        /                 \
 *   DeltaJoin           DeltaJoin
 *     /    \             /     \
 *  Calc1    \          /      Calc2
 *   /        \       /           \
 * #0 A     #1 B    #2 C          #3 D
 *
 * Left stream side owns inputs #0, #1; Right stream side owns inputs #2, #3.
 * When the left side receives an update, it looks up #2, then #3 (cascaded).
 * When the right side receives an update, it looks up #0, then #1 (cascaded).
 * }</pre>
 *
 * <p>Conceptually, version 1 (binary two-table delta join) is a special case of version 2 (cascaded
 * multi-table delta join) — it is equivalent to a v2 tree with exactly two leaf inputs and no
 * cascading lookup chain.
 *
 * <p>Version 2 is backward compatible with version 1: a plan serialized with v1 can be deserialized
 * and executed by v2. When v1 fields ({@link #lookupRightTableJoinSpec} and {@link
 * #lookupLeftTableJoinSpec}) are present, the node falls back to the v1 code path via {@link
 * DeltaJoinOperatorFactoryBuilderV1}; otherwise it uses the v2 code path via {@link
 * DeltaJoinOperatorFactoryBuilderV2}.
 */
@ExecNodeMetadata(
        name = "stream-exec-delta-join",
        version = 1,
        producedTransformations = DELTA_JOIN_TRANSFORMATION,
        consumedOptions = {
            "table.exec.async-lookup.buffer-capacity",
            "table.exec.async-lookup.timeout"
        },
        minPlanVersion = FlinkVersion.v2_1,
        minStateVersion = FlinkVersion.v2_1)
@ExecNodeMetadata(
        name = "stream-exec-delta-join",
        version = 2,
        producedTransformations = DELTA_JOIN_TRANSFORMATION,
        consumedOptions = {
            "table.exec.async-lookup.buffer-capacity",
            "table.exec.async-lookup.timeout"
        },
        minPlanVersion = FlinkVersion.v2_3,
        minStateVersion = FlinkVersion.v2_3)
@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamExecDeltaJoin extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

    protected static final Logger LOG = LoggerFactory.getLogger(StreamExecDeltaJoin.class);

    public static final String DELTA_JOIN_TRANSFORMATION = "delta-join";

    private static final String GENERATED_JOIN_CONDITION_CLASS_NAME = "JoinCondition";

    private static final String FIELD_NAME_LEFT_JOIN_KEYS = "leftJoinKeys";
    private static final String FIELD_NAME_RIGHT_JOIN_KEYS = "rightJoinKeys";

    private static final String FIELD_NAME_LEFT_UPSERT_KEY = "leftUpsertKey";
    private static final String FIELD_NAME_RIGHT_UPSERT_KEY = "rightUpsertKey";
    private static final String FIELD_NAME_JOIN_TYPE = "joinType";
    private static final String FIELD_NAME_ASYNC_OPTIONS = "asyncOptions";

    // v1 (binary delta join) field names
    private static final String FIELD_NAME_LOOKUP_RIGHT_TABLE_JOIN_SPEC =
            "lookupRightTableJoinSpec";
    private static final String FIELD_NAME_LOOKUP_LEFT_TABLE_JOIN_SPEC = "lookupLeftTableJoinSpec";

    // v2 (cascaded delta join) field names
    private static final String FIELD_NAME_CONDITION = "condition";
    private static final String FIELD_NAME_LEFT_ALL_BINARY_INPUT_ORDINALS =
            "leftAllBinaryInputOrdinals";
    private static final String FIELD_NAME_RIGHT_ALL_BINARY_INPUT_ORDINALS =
            "rightAllBinaryInputOrdinals";
    private static final String FIELD_NAME_LEFT_2_RIGHT_LOOKUP_CHAIN = "left2RightLookupChain";
    private static final String FIELD_NAME_RIGHT_2_LEFT_LOOKUP_CHAIN = "right2LeftLookupChain";
    private static final String FIELD_NAME_ALL_BINARY_INPUT_TABLES = "allBinaryInputTables";
    private static final String FIELD_NAME_DELTA_JOIN_TREE = "deltaJoinTree";

    @JsonProperty(FIELD_NAME_JOIN_TYPE)
    private final FlinkJoinType flinkJoinType;

    @JsonProperty(FIELD_NAME_ASYNC_OPTIONS)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final AsyncOptions asyncLookupOptions;

    @JsonProperty(FIELD_NAME_LEFT_JOIN_KEYS)
    private final int[] leftJoinKeys;

    @JsonProperty(FIELD_NAME_LEFT_UPSERT_KEY)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    private final int[] leftUpsertKeys;

    @JsonProperty(FIELD_NAME_RIGHT_JOIN_KEYS)
    private final int[] rightJoinKeys;

    @JsonProperty(FIELD_NAME_RIGHT_UPSERT_KEY)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    private final int[] rightUpsertKeys;

    // ===== v1 (binary delta join) fields =====

    // left (streaming) side join right (lookup) side
    @JsonProperty(FIELD_NAME_LOOKUP_RIGHT_TABLE_JOIN_SPEC)
    @Nullable
    private final DeltaJoinSpec lookupRightTableJoinSpec;

    // right (streaming) side join left (lookup) side
    @JsonProperty(FIELD_NAME_LOOKUP_LEFT_TABLE_JOIN_SPEC)
    @Nullable
    private final DeltaJoinSpec lookupLeftTableJoinSpec;

    // ===== v2 (cascaded delta join) fields =====

    @JsonProperty(FIELD_NAME_CONDITION)
    @Nullable
    private final RexNode condition;

    // based on 0
    @JsonProperty(FIELD_NAME_LEFT_ALL_BINARY_INPUT_ORDINALS)
    @Nullable
    private final List<Integer> leftAllBinaryInputOrdinals;

    // based on 0
    @JsonProperty(FIELD_NAME_RIGHT_ALL_BINARY_INPUT_ORDINALS)
    @Nullable
    private final List<Integer> rightAllBinaryInputOrdinals;

    @JsonProperty(FIELD_NAME_LEFT_2_RIGHT_LOOKUP_CHAIN)
    @Nullable
    private final DeltaJoinLookupChain left2RightLookupChain;

    @JsonProperty(FIELD_NAME_RIGHT_2_LEFT_LOOKUP_CHAIN)
    @Nullable
    private final DeltaJoinLookupChain right2LeftLookupChain;

    @JsonProperty(FIELD_NAME_ALL_BINARY_INPUT_TABLES)
    @Nullable
    private final List<TemporalTableSourceSpec> allBinaryInputTables;

    @JsonProperty(FIELD_NAME_DELTA_JOIN_TREE)
    @Nullable
    private final DeltaJoinTree deltaJoinTree;

    public StreamExecDeltaJoin(
            ReadableConfig tableConfig,
            FlinkJoinType flinkJoinType,
            RexNode condition,
            int[] leftJoinKeys,
            @Nullable int[] leftUpsertKeys,
            int[] rightJoinKeys,
            @Nullable int[] rightUpsertKeys,
            List<Integer> leftAllBinaryInputOrdinals,
            List<Integer> rightAllBinaryInputOrdinals,
            DeltaJoinLookupChain left2RightLookupChain,
            DeltaJoinLookupChain right2LeftLookupChain,
            List<TemporalTableSourceSpec> allBinaryInputTables,
            DeltaJoinTree deltaJoinTree,
            InputProperty leftInputProperty,
            InputProperty rightInputProperty,
            RowType outputType,
            String description,
            AsyncOptions asyncLookupOptions) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecDeltaJoin.class),
                ExecNodeContext.newPersistedConfig(StreamExecDeltaJoin.class, tableConfig),
                flinkJoinType,
                leftJoinKeys,
                leftUpsertKeys,
                rightJoinKeys,
                rightUpsertKeys,
                null, // v1 lookupRightTableJoinSpec
                null, // v1 lookupLeftTableJoinSpec
                condition,
                leftAllBinaryInputOrdinals,
                rightAllBinaryInputOrdinals,
                left2RightLookupChain,
                right2LeftLookupChain,
                allBinaryInputTables,
                deltaJoinTree,
                Lists.newArrayList(leftInputProperty, rightInputProperty),
                outputType,
                description,
                asyncLookupOptions);
    }

    @JsonCreator
    public StreamExecDeltaJoin(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_JOIN_TYPE) FlinkJoinType flinkJoinType,
            @JsonProperty(FIELD_NAME_LEFT_JOIN_KEYS) int[] leftJoinKeys,
            @JsonProperty(FIELD_NAME_LEFT_UPSERT_KEY) @Nullable int[] leftUpsertKeys,
            @JsonProperty(FIELD_NAME_RIGHT_JOIN_KEYS) int[] rightJoinKeys,
            @JsonProperty(FIELD_NAME_RIGHT_UPSERT_KEY) @Nullable int[] rightUpsertKeys,
            // v1 (binary delta join) fields
            @JsonProperty(FIELD_NAME_LOOKUP_RIGHT_TABLE_JOIN_SPEC) @Nullable
                    DeltaJoinSpec lookupRightTableJoinSpec,
            @JsonProperty(FIELD_NAME_LOOKUP_LEFT_TABLE_JOIN_SPEC) @Nullable
                    DeltaJoinSpec lookupLeftTableJoinSpec,
            // v2 (cascaded delta join) fields
            @JsonProperty(FIELD_NAME_CONDITION) @Nullable RexNode condition,
            @JsonProperty(FIELD_NAME_LEFT_ALL_BINARY_INPUT_ORDINALS) @Nullable
                    List<Integer> leftAllBinaryInputOrdinals,
            @JsonProperty(FIELD_NAME_RIGHT_ALL_BINARY_INPUT_ORDINALS) @Nullable
                    List<Integer> rightAllBinaryInputOrdinals,
            @JsonProperty(FIELD_NAME_LEFT_2_RIGHT_LOOKUP_CHAIN) @Nullable
                    DeltaJoinLookupChain left2RightLookupChain,
            @JsonProperty(FIELD_NAME_RIGHT_2_LEFT_LOOKUP_CHAIN) @Nullable
                    DeltaJoinLookupChain right2LeftLookupChain,
            @JsonProperty(FIELD_NAME_ALL_BINARY_INPUT_TABLES) @Nullable
                    List<TemporalTableSourceSpec> allBinaryInputTables,
            @JsonProperty(FIELD_NAME_DELTA_JOIN_TREE) @Nullable DeltaJoinTree deltaJoinTree,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description,
            @JsonProperty(FIELD_NAME_ASYNC_OPTIONS) AsyncOptions asyncLookupOptions) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        this.flinkJoinType = flinkJoinType;
        this.leftJoinKeys = leftJoinKeys;
        this.leftUpsertKeys = leftUpsertKeys;
        this.rightJoinKeys = rightJoinKeys;
        this.rightUpsertKeys = rightUpsertKeys;
        this.lookupRightTableJoinSpec = lookupRightTableJoinSpec;
        this.lookupLeftTableJoinSpec = lookupLeftTableJoinSpec;
        this.condition = condition;
        this.leftAllBinaryInputOrdinals = leftAllBinaryInputOrdinals;
        this.rightAllBinaryInputOrdinals = rightAllBinaryInputOrdinals;
        this.left2RightLookupChain = left2RightLookupChain;
        this.right2LeftLookupChain = right2LeftLookupChain;
        this.allBinaryInputTables = allBinaryInputTables;
        this.deltaJoinTree = deltaJoinTree;
        this.asyncLookupOptions = asyncLookupOptions;

        if (isDeltaJoinV1()) {
            Preconditions.checkArgument(leftAllBinaryInputOrdinals == null);
            Preconditions.checkArgument(rightAllBinaryInputOrdinals == null);
            Preconditions.checkArgument(left2RightLookupChain == null);
            Preconditions.checkArgument(right2LeftLookupChain == null);
            Preconditions.checkArgument(deltaJoinTree == null);

        } else {
            Preconditions.checkArgument(lookupRightTableJoinSpec == null);
            Preconditions.checkArgument(lookupLeftTableJoinSpec == null);
            Preconditions.checkArgument(leftAllBinaryInputOrdinals != null);
            Preconditions.checkArgument(rightAllBinaryInputOrdinals != null);
            Preconditions.checkArgument(left2RightLookupChain != null);
            Preconditions.checkArgument(right2LeftLookupChain != null);
            Preconditions.checkArgument(deltaJoinTree != null);
        }
    }

    private boolean isDeltaJoinV1() {
        return lookupRightTableJoinSpec != null && lookupLeftTableJoinSpec != null;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        if (!DeltaJoinUtil.isJoinTypeSupported(flinkJoinType)) {
            throw new IllegalStateException(
                    String.format("Unsupported join type [%s] for delta join.", flinkJoinType));
        }

        final ExecEdge leftInputEdge = getInputEdges().get(0);
        final ExecEdge rightInputEdge = getInputEdges().get(1);
        final RowType leftStreamType = (RowType) leftInputEdge.getOutputType();
        final RowType rightStreamType = (RowType) rightInputEdge.getOutputType();

        Transformation<RowData> leftInputTransformation =
                (Transformation<RowData>) leftInputEdge.translateToPlan(planner);
        Transformation<RowData> rightInputTransformation =
                (Transformation<RowData>) rightInputEdge.translateToPlan(planner);

        ClassLoader classLoader = planner.getFlinkContext().getClassLoader();

        // left side selector
        RowDataKeySelector leftJoinKeySelector =
                KeySelectorUtil.getRowDataSelector(
                        classLoader, leftJoinKeys, InternalTypeInfo.of(leftStreamType));
        RowDataKeySelector leftUpsertKeySelector =
                getUpsertKeySelector(leftUpsertKeys, leftStreamType, classLoader);

        // right side selector
        RowDataKeySelector rightJoinKeySelector =
                KeySelectorUtil.getRowDataSelector(
                        classLoader, rightJoinKeys, InternalTypeInfo.of(rightStreamType));
        RowDataKeySelector rightUpsertKeySelector =
                getUpsertKeySelector(rightUpsertKeys, rightStreamType, classLoader);

        DeltaJoinOperatorFactoryBuilder builder;
        if (isDeltaJoinV1()) {
            builder =
                    new DeltaJoinOperatorFactoryBuilderV1(
                            planner,
                            config,
                            leftStreamType,
                            rightStreamType,
                            leftJoinKeySelector,
                            leftUpsertKeySelector,
                            rightJoinKeySelector,
                            rightUpsertKeySelector,
                            requireNonNull(lookupRightTableJoinSpec),
                            requireNonNull(lookupLeftTableJoinSpec));
        } else {
            builder =
                    new DeltaJoinOperatorFactoryBuilderV2(
                            planner,
                            config,
                            leftStreamType,
                            rightStreamType,
                            leftJoinKeySelector,
                            leftUpsertKeySelector,
                            rightJoinKeySelector,
                            rightUpsertKeySelector,
                            requireNonNull(condition),
                            requireNonNull(leftAllBinaryInputOrdinals),
                            requireNonNull(rightAllBinaryInputOrdinals),
                            requireNonNull(left2RightLookupChain),
                            requireNonNull(right2LeftLookupChain),
                            requireNonNull(allBinaryInputTables),
                            requireNonNull(deltaJoinTree));
        }
        StreamOperatorFactory<RowData> operatorFactory = builder.build();

        final TwoInputTransformation<RowData, RowData, RowData> transform =
                ExecNodeUtil.createTwoInputTransformation(
                        leftInputTransformation,
                        rightInputTransformation,
                        createTransformationMeta(DELTA_JOIN_TRANSFORMATION, config),
                        operatorFactory,
                        InternalTypeInfo.of((RowType) getOutputType()),
                        leftInputTransformation.getParallelism(),
                        0,
                        false);

        transform.setStateKeySelectors(leftJoinKeySelector, rightJoinKeySelector);
        transform.setStateKeyType(leftJoinKeySelector.getProducedType());
        return transform;
    }

    private static LookupHandlerBase generateLookupHandler(
            boolean isBinaryLookup,
            DeltaJoinLookupChain.Node node,
            Map<Integer, GeneratedFunction<AsyncFunction<RowData, Object>>>
                    generatedFetcherCollector,
            DeltaJoinTree deltaJoinTree,
            PlannerBase planner,
            FlinkTypeFactory typeFactory,
            ClassLoader classLoader,
            ExecNodeConfig config) {
        final int[] sourceInputOrdinals = node.inputTableBinaryInputOrdinals;
        final int lookupTableOrdinal = node.lookupTableBinaryInputOrdinal;
        final RowType sourceStreamType =
                deltaJoinTree.getOutputRowTypeOnNode(sourceInputOrdinals, typeFactory);

        final TableSourceTable lookupTable =
                (TableSourceTable)
                        node.deltaJoinSpec
                                .getLookupTable()
                                .getTemporalTable(planner.getFlinkContext(), typeFactory);

        final Map<Integer, LookupJoinUtil.FunctionParam> lookupKeyMap =
                node.deltaJoinSpec.getLookupKeyMap();
        AsyncTableFunction<?> lookupSideAsyncTableFunction =
                getUnwrappedAsyncLookupFunction(lookupTable, lookupKeyMap.keySet(), classLoader);
        UserDefinedFunctionHelper.prepareInstance(config, lookupSideAsyncTableFunction);

        final RowType lookupTableSourceRowType =
                FlinkTypeFactory.toLogicalRowType(lookupTable.getRowType());

        final RowType lookupResultRowType =
                combineOutputRowType(
                        sourceStreamType, lookupTableSourceRowType, node.joinType, typeFactory);

        List<LookupJoinUtil.FunctionParam> lookupKeysOnInputSide =
                Arrays.stream(LookupJoinUtil.getOrderedLookupKeys(lookupKeyMap.keySet()))
                        .mapToObj(lookupKeyMap::get)
                        .collect(Collectors.toList());

        FunctionCallCodeGenerator.GeneratedTableFunctionWithDataType<AsyncFunction<RowData, Object>>
                lookupSideGeneratedFetcherWithType =
                        LookupJoinCodeGenerator.generateAsyncLookupFunction(
                                config,
                                classLoader,
                                unwrapDataTypeFactory(planner.createRelBuilder()),
                                sourceStreamType,
                                lookupTableSourceRowType,
                                lookupResultRowType,
                                lookupKeysOnInputSide,
                                lookupSideAsyncTableFunction,
                                String.join(".", lookupTable.getQualifiedName()));

        final RowType lookupSidePassThroughCalcRowType =
                deltaJoinTree.getOutputRowTypeOnNode(new int[] {lookupTableOrdinal}, typeFactory);

        GeneratedFunction<FlatMapFunction<RowData, RowData>> lookupSideGeneratedCalc = null;
        if (node.deltaJoinSpec.getProjectionOnTemporalTable().isPresent()) {
            // a projection or filter after table source scan
            List<RexNode> projectionOnTemporalTable =
                    node.deltaJoinSpec.getProjectionOnTemporalTable().get();
            RexNode filterOnTemporalTable =
                    node.deltaJoinSpec.getFilterOnTemporalTable().orElse(null);
            lookupSideGeneratedCalc =
                    LookupJoinCodeGenerator.generateCalcMapFunction(
                            config,
                            classLoader,
                            JavaScalaConversionUtil.toScala(projectionOnTemporalTable),
                            filterOnTemporalTable,
                            lookupSidePassThroughCalcRowType,
                            lookupTableSourceRowType);
        }

        Preconditions.checkState(!generatedFetcherCollector.containsKey(lookupTableOrdinal));
        generatedFetcherCollector.put(
                lookupTableOrdinal, lookupSideGeneratedFetcherWithType.tableFunc());

        if (isBinaryLookup) {
            return new BinaryLookupHandler(
                    TypeConversions.fromLogicalToDataType(sourceStreamType),
                    lookupSideGeneratedFetcherWithType.dataType(),
                    TypeConversions.fromLogicalToDataType(lookupSidePassThroughCalcRowType),
                    InternalSerializers.create(lookupSidePassThroughCalcRowType),
                    lookupSideGeneratedCalc,
                    node.inputTableBinaryInputOrdinals,
                    node.lookupTableBinaryInputOrdinal);
        }

        throw new IllegalStateException("Support later");
    }

    private static RowDataKeySelector getUpsertKeySelector(
            @Nullable int[] upsertKey, RowType rowType, ClassLoader classLoader) {
        final int[] finalUpsertKeys;
        if (upsertKey != null && upsertKey.length > 0) {
            finalUpsertKeys = upsertKey;
        } else {
            finalUpsertKeys = IntStream.range(0, rowType.getFields().size()).toArray();
        }
        return KeySelectorUtil.getRowDataSelector(
                classLoader, finalUpsertKeys, InternalTypeInfo.of(rowType));
    }

    private boolean enableCache(ReadableConfig config) {
        return config.get(ExecutionConfigOptions.TABLE_EXEC_DELTA_JOIN_CACHE_ENABLED);
    }

    /** Get the left cache size and right size. */
    private Tuple2<Long, Long> getCacheSize(ReadableConfig config) {
        long leftCacheSize =
                config.get(ExecutionConfigOptions.TABLE_EXEC_DELTA_JOIN_LEFT_CACHE_SIZE);
        long rightCacheSize =
                config.get(ExecutionConfigOptions.TABLE_EXEC_DELTA_JOIN_RIGHT_CACHE_SIZE);
        if ((leftCacheSize <= 0 || rightCacheSize <= 0) && enableCache(config)) {
            throw new IllegalArgumentException(
                    "Cache size in delta join must be positive when enabling cache.");
        }
        return Tuple2.of(leftCacheSize, rightCacheSize);
    }

    private abstract static class DeltaJoinOperatorFactoryBuilder {
        protected final PlannerBase planner;
        protected final ExecNodeConfig config;
        protected final RowType leftStreamType;
        protected final RowType rightStreamType;
        protected final RowDataKeySelector leftJoinKeySelector;
        protected final RowDataKeySelector leftUpsertKeySelector;
        protected final RowDataKeySelector rightJoinKeySelector;
        protected final RowDataKeySelector rightUpsertKeySelector;
        protected final ClassLoader classLoader;
        protected final FlinkTypeFactory typeFactory;

        public DeltaJoinOperatorFactoryBuilder(
                PlannerBase planner,
                ExecNodeConfig config,
                RowType leftStreamType,
                RowType rightStreamType,
                RowDataKeySelector leftJoinKeySelector,
                RowDataKeySelector leftUpsertKeySelector,
                RowDataKeySelector rightJoinKeySelector,
                RowDataKeySelector rightUpsertKeySelector) {
            this.planner = planner;
            this.config = config;
            this.leftStreamType = leftStreamType;
            this.rightStreamType = rightStreamType;
            this.leftJoinKeySelector = leftJoinKeySelector;
            this.leftUpsertKeySelector = leftUpsertKeySelector;
            this.rightJoinKeySelector = rightJoinKeySelector;
            this.rightUpsertKeySelector = rightUpsertKeySelector;
            this.classLoader = planner.getFlinkContext().getClassLoader();
            this.typeFactory = planner.getTypeFactory();
        }

        protected abstract StreamOperatorFactory<RowData> build();
    }

    private class DeltaJoinOperatorFactoryBuilderV1 extends DeltaJoinOperatorFactoryBuilder {

        // left (streaming) side join right (lookup) side
        private final DeltaJoinSpec lookupRightTableJoinSpec;
        // right (streaming) side join left (lookup) side
        private final DeltaJoinSpec lookupLeftTableJoinSpec;

        public DeltaJoinOperatorFactoryBuilderV1(
                PlannerBase planner,
                ExecNodeConfig config,
                RowType leftStreamType,
                RowType rightStreamType,
                RowDataKeySelector leftJoinKeySelector,
                RowDataKeySelector leftUpsertKeySelector,
                RowDataKeySelector rightJoinKeySelector,
                RowDataKeySelector rightUpsertKeySelector,
                DeltaJoinSpec lookupRightTableJoinSpec,
                DeltaJoinSpec lookupLeftTableJoinSpec) {
            super(
                    planner,
                    config,
                    leftStreamType,
                    rightStreamType,
                    leftJoinKeySelector,
                    leftUpsertKeySelector,
                    rightJoinKeySelector,
                    rightUpsertKeySelector);
            this.lookupRightTableJoinSpec = lookupRightTableJoinSpec;
            this.lookupLeftTableJoinSpec = lookupLeftTableJoinSpec;
        }

        @Override
        public StreamOperatorFactory<RowData> build() {
            RelOptTable leftTempTable =
                    lookupLeftTableJoinSpec
                            .getLookupTable()
                            .getTemporalTable(planner.getFlinkContext(), typeFactory);
            RelOptTable rightTempTable =
                    lookupRightTableJoinSpec
                            .getLookupTable()
                            .getTemporalTable(planner.getFlinkContext(), typeFactory);

            int[] eachBinaryInputFieldSize = new int[2];
            eachBinaryInputFieldSize[0] =
                    lookupLeftTableJoinSpec.getProjectionOnTemporalTable().isEmpty()
                            ? leftTempTable.getRowType().getFieldCount()
                            : leftStreamType.getFieldCount();
            eachBinaryInputFieldSize[1] =
                    lookupRightTableJoinSpec.getProjectionOnTemporalTable().isEmpty()
                            ? rightTempTable.getRowType().getFieldCount()
                            : rightStreamType.getFieldCount();

            // collect all lookup functions of each source table
            // <input ordinal, lookup func>
            Map<Integer, GeneratedFunction<AsyncFunction<RowData, Object>>>
                    generatedFetcherCollector = new HashMap<>();

            AsyncDeltaJoinRunner left2RightRunner =
                    createAsyncDeltaJoinRunner(
                            eachBinaryInputFieldSize, generatedFetcherCollector, true);
            AsyncDeltaJoinRunner right2LeftRunner =
                    createAsyncDeltaJoinRunner(
                            eachBinaryInputFieldSize, generatedFetcherCollector, false);

            Tuple2<Long, Long> leftRightCacheSize = getCacheSize(config);

            return new StreamingDeltaJoinOperatorFactory(
                    left2RightRunner,
                    right2LeftRunner,
                    generatedFetcherCollector,
                    leftJoinKeySelector,
                    rightJoinKeySelector,
                    asyncLookupOptions.asyncTimeout,
                    asyncLookupOptions.asyncBufferCapacity,
                    leftRightCacheSize.f0,
                    leftRightCacheSize.f1,
                    leftStreamType,
                    rightStreamType);
        }

        private AsyncDeltaJoinRunner createAsyncDeltaJoinRunner(
                int[] eachBinaryInputFieldSize,
                Map<Integer, GeneratedFunction<AsyncFunction<RowData, Object>>>
                        generatedFetcherCollector,
                boolean treatRightAsLookupTable) {
            RexNode remainingCondition;
            if (treatRightAsLookupTable) {
                remainingCondition = lookupRightTableJoinSpec.getRemainingCondition().orElse(null);
            } else {
                remainingCondition = lookupLeftTableJoinSpec.getRemainingCondition().orElse(null);
            }
            GeneratedFilterCondition generatedRemainingJoinCondition =
                    Optional.ofNullable(remainingCondition)
                            .map(
                                    rexNode ->
                                            FilterCodeGenerator.generateFilterCondition(
                                                    config,
                                                    classLoader,
                                                    rexNode,
                                                    getOutputType(),
                                                    GENERATED_JOIN_CONDITION_CLASS_NAME))
                            .orElse(null);

            DeltaJoinTree deltaJoinTree = buildDeltaJoinTree();
            return new AsyncDeltaJoinRunner(
                    eachBinaryInputFieldSize,
                    generatedRemainingJoinCondition,
                    leftJoinKeySelector,
                    leftUpsertKeySelector,
                    rightJoinKeySelector,
                    rightUpsertKeySelector,
                    buildBinaryLookupHandlerChain(
                            generatedFetcherCollector, deltaJoinTree, treatRightAsLookupTable),
                    deltaJoinTree.convert2RuntimeTree(planner, config),
                    Set.of(Set.of(0), Set.of(1)),
                    treatRightAsLookupTable,
                    asyncLookupOptions.asyncBufferCapacity,
                    enableCache(config));
        }

        private DeltaJoinHandlerChain buildBinaryLookupHandlerChain(
                Map<Integer, GeneratedFunction<AsyncFunction<RowData, Object>>>
                        generatedFetcherCollector,
                DeltaJoinTree deltaJoinTree,
                boolean treatRightAsLookupTable) {
            DeltaJoinLookupChain.Node node;
            if (treatRightAsLookupTable) {
                node =
                        DeltaJoinLookupChain.Node.of(
                                0, // inputTableBinaryInputOrdinal
                                1, // lookupTableBinaryInputOrdinal
                                lookupRightTableJoinSpec,
                                flinkJoinType);
            } else {
                node =
                        DeltaJoinLookupChain.Node.of(
                                1, // inputTableBinaryInputOrdinal
                                0, // lookupTableBinaryInputOrdinal
                                lookupLeftTableJoinSpec,
                                swapJoinType(flinkJoinType));
            }
            return DeltaJoinHandlerChain.build(
                    Collections.singletonList(
                            generateLookupHandler(
                                    true, // isBinaryLookup
                                    node,
                                    generatedFetcherCollector,
                                    deltaJoinTree,
                                    planner,
                                    typeFactory,
                                    classLoader,
                                    config)),
                    new int[] {treatRightAsLookupTable ? 0 : 1});
        }

        private DeltaJoinTree buildDeltaJoinTree() {
            RowType leftTablePassThroughCalcRowType = null;
            if (lookupLeftTableJoinSpec.getProjectionOnTemporalTable().isPresent()) {
                leftTablePassThroughCalcRowType = leftStreamType;
            }
            DeltaJoinTree.BinaryInputNode leftInputNode =
                    new DeltaJoinTree.BinaryInputNode(
                            0, // inputOrdinal
                            lookupLeftTableJoinSpec.getProjectionOnTemporalTable().orElse(null),
                            lookupLeftTableJoinSpec.getFilterOnTemporalTable().orElse(null),
                            leftTablePassThroughCalcRowType,
                            FlinkTypeFactory.toLogicalRowType(
                                    lookupLeftTableJoinSpec.getLookupTable().getOutputType()));

            RowType rightTablePassThroughCalcRowType = null;
            if (lookupRightTableJoinSpec.getProjectionOnTemporalTable().isPresent()) {
                rightTablePassThroughCalcRowType = rightStreamType;
            }
            DeltaJoinTree.BinaryInputNode rightInputNode =
                    new DeltaJoinTree.BinaryInputNode(
                            1, // inputOrdinal
                            lookupRightTableJoinSpec.getProjectionOnTemporalTable().orElse(null),
                            lookupRightTableJoinSpec.getFilterOnTemporalTable().orElse(null),
                            rightTablePassThroughCalcRowType,
                            FlinkTypeFactory.toLogicalRowType(
                                    lookupRightTableJoinSpec.getLookupTable().getOutputType()));

            DeltaJoinTree.JoinNode joinNode =
                    new DeltaJoinTree.JoinNode(
                            flinkJoinType,
                            buildJoinCondition(),
                            leftJoinKeys,
                            rightJoinKeys,
                            leftInputNode,
                            rightInputNode,
                            null // `rexProgram`: calc on this join
                            );
            return new DeltaJoinTree(joinNode);
        }

        private RexNode buildJoinCondition() {
            RexBuilder rexBuilder = planner.createRelBuilder().getRexBuilder();
            int leftFieldCount = leftStreamType.getFieldCount();
            List<RexNode> conditions = new ArrayList<>();

            for (int i = 0; i < leftJoinKeys.length; i++) {
                int leftIdx = leftJoinKeys[i];
                int rightIdx = rightJoinKeys[i];
                RexNode leftRef =
                        rexBuilder.makeInputRef(
                                typeFactory.createFieldTypeFromLogicalType(
                                        leftStreamType.getFields().get(leftIdx).getType()),
                                leftIdx);
                RexNode rightRef =
                        rexBuilder.makeInputRef(
                                typeFactory.createFieldTypeFromLogicalType(
                                        rightStreamType.getFields().get(rightIdx).getType()),
                                rightIdx + leftFieldCount);

                conditions.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, leftRef, rightRef));
            }
            lookupRightTableJoinSpec.getRemainingCondition().ifPresent(conditions::add);

            return RexUtil.composeConjunction(rexBuilder, conditions);
        }
    }

    private class DeltaJoinOperatorFactoryBuilderV2 extends DeltaJoinOperatorFactoryBuilder {

        private final RexNode condition;
        private final List<Integer> leftAllBinaryInputOrdinals;
        private final List<Integer> rightAllBinaryInputOrdinals;
        private final DeltaJoinLookupChain left2RightLookupChain;
        private final DeltaJoinLookupChain right2LeftLookupChain;
        private final List<TemporalTableSourceSpec> allBinaryInputTables;
        private final DeltaJoinTree deltaJoinTree;

        public DeltaJoinOperatorFactoryBuilderV2(
                PlannerBase planner,
                ExecNodeConfig config,
                RowType leftStreamType,
                RowType rightStreamType,
                RowDataKeySelector leftJoinKeySelector,
                RowDataKeySelector leftUpsertKeySelector,
                RowDataKeySelector rightJoinKeySelector,
                RowDataKeySelector rightUpsertKeySelector,
                RexNode condition,
                List<Integer> leftAllBinaryInputOrdinals,
                List<Integer> rightAllBinaryInputOrdinals,
                DeltaJoinLookupChain left2RightLookupChain,
                DeltaJoinLookupChain right2LeftLookupChain,
                List<TemporalTableSourceSpec> allBinaryInputTables,
                DeltaJoinTree deltaJoinTree) {
            super(
                    planner,
                    config,
                    leftStreamType,
                    rightStreamType,
                    leftJoinKeySelector,
                    leftUpsertKeySelector,
                    rightJoinKeySelector,
                    rightUpsertKeySelector);
            this.condition = condition;
            this.leftAllBinaryInputOrdinals = leftAllBinaryInputOrdinals;
            this.rightAllBinaryInputOrdinals = rightAllBinaryInputOrdinals;
            this.left2RightLookupChain = left2RightLookupChain;
            this.right2LeftLookupChain = right2LeftLookupChain;
            this.allBinaryInputTables = allBinaryInputTables;
            this.deltaJoinTree = deltaJoinTree;
        }

        @Override
        public StreamOperatorFactory<RowData> build() {
            // collect all lookup functions of each source table
            // <input ordinal, lookup func>
            Map<Integer, GeneratedFunction<AsyncFunction<RowData, Object>>>
                    generatedFetcherCollector = new HashMap<>();
            DeltaJoinHandlerChain left2RightHandlerChain =
                    generateDeltaJoinHandlerChain(true, generatedFetcherCollector);
            DeltaJoinHandlerChain right2LeftHandlerChain =
                    generateDeltaJoinHandlerChain(false, generatedFetcherCollector);
            Preconditions.checkState(
                    generatedFetcherCollector.size()
                            == leftAllBinaryInputOrdinals.size()
                                    + rightAllBinaryInputOrdinals.size());

            int[] eachBinaryInputFieldSize =
                    IntStream.range(0, allBinaryInputTables.size())
                            .map(
                                    i ->
                                            deltaJoinTree
                                                    .getOutputRowTypeOnNode(
                                                            new int[] {i}, typeFactory)
                                                    .getFieldCount())
                            .toArray();

            RowType remainingJoinConditionInputRowType =
                    combineOutputRowType(
                            leftStreamType, rightStreamType, flinkJoinType, typeFactory);
            GeneratedFilterCondition left2RightGeneratedRemainingJoinCondition =
                    generateRemainingJoinCondition(true, remainingJoinConditionInputRowType);
            GeneratedFilterCondition right2LeftGeneratedRemainingJoinCondition =
                    generateRemainingJoinCondition(false, remainingJoinConditionInputRowType);

            DeltaJoinRuntimeTree joinRuntimeTree =
                    deltaJoinTree.convert2RuntimeTree(planner, config);

            Set<Set<Integer>> left2RightAllDrivenInputsWhenLookup =
                    getAllDrivenInputsWhenLookup(true);
            Set<Set<Integer>> right2LeftAllDrivenInputsWhenLookup =
                    getAllDrivenInputsWhenLookup(false);

            AsyncDeltaJoinRunner left2RightAsyncRunner =
                    new AsyncDeltaJoinRunner(
                            eachBinaryInputFieldSize,
                            left2RightGeneratedRemainingJoinCondition,
                            leftJoinKeySelector,
                            leftUpsertKeySelector,
                            rightJoinKeySelector,
                            rightUpsertKeySelector,
                            left2RightHandlerChain,
                            joinRuntimeTree,
                            left2RightAllDrivenInputsWhenLookup,
                            true,
                            asyncLookupOptions.asyncBufferCapacity,
                            enableCache(config));

            AsyncDeltaJoinRunner right2LeftAsyncRunner =
                    new AsyncDeltaJoinRunner(
                            eachBinaryInputFieldSize,
                            right2LeftGeneratedRemainingJoinCondition,
                            leftJoinKeySelector,
                            leftUpsertKeySelector,
                            rightJoinKeySelector,
                            rightUpsertKeySelector,
                            right2LeftHandlerChain,
                            joinRuntimeTree,
                            right2LeftAllDrivenInputsWhenLookup,
                            false,
                            asyncLookupOptions.asyncBufferCapacity,
                            enableCache(config));

            Tuple2<Long, Long> cacheSize = getCacheSize(config);

            return new StreamingDeltaJoinOperatorFactory(
                    left2RightAsyncRunner,
                    right2LeftAsyncRunner,
                    generatedFetcherCollector,
                    leftJoinKeySelector,
                    rightJoinKeySelector,
                    asyncLookupOptions.asyncTimeout,
                    asyncLookupOptions.asyncBufferCapacity,
                    cacheSize.f0,
                    cacheSize.f1,
                    leftStreamType,
                    rightStreamType);
        }

        private DeltaJoinHandlerChain generateDeltaJoinHandlerChain(
                boolean lookupRight,
                Map<Integer, GeneratedFunction<AsyncFunction<RowData, Object>>>
                        generatedFetcherCollector) {
            int[] streamOwnedSourceOrdinals =
                    lookupRight
                            ? leftAllBinaryInputOrdinals.stream().mapToInt(i -> i).toArray()
                            : rightAllBinaryInputOrdinals.stream()
                                    .mapToInt(i -> i + leftAllBinaryInputOrdinals.size())
                                    .toArray();

            DeltaJoinLookupChain lookupChain =
                    lookupRight ? left2RightLookupChain : right2LeftLookupChain;

            List<DeltaJoinLookupChain.Node> nodes = lookupChain.getNodes();
            Preconditions.checkArgument(!nodes.isEmpty());

            boolean isBinaryLookup = isBinaryLookup(lookupRight);
            if (isBinaryLookup) {
                return DeltaJoinHandlerChain.build(
                        Collections.singletonList(
                                generateLookupHandler(
                                        true, // isBinaryLookup
                                        nodes.get(0),
                                        generatedFetcherCollector,
                                        deltaJoinTree,
                                        planner,
                                        typeFactory,
                                        classLoader,
                                        config)),
                        streamOwnedSourceOrdinals);
            }

            throw new UnsupportedOperationException("Support cascaded delta join operator later");
        }

        private Set<Set<Integer>> getAllDrivenInputsWhenLookup(boolean lookupRight) {
            Set<Set<Integer>> result = new HashSet<>();

            DeltaJoinLookupChain lookupChain =
                    lookupRight ? left2RightLookupChain : right2LeftLookupChain;

            for (DeltaJoinLookupChain.Node node : lookupChain.getNodes()) {
                Set<Integer> drivenInputs =
                        Arrays.stream(node.inputTableBinaryInputOrdinals)
                                .boxed()
                                .collect(Collectors.toSet());

                result.add(drivenInputs);
            }
            return result;
        }

        private boolean isBinaryLookup(boolean lookupRight) {
            if (lookupRight) {
                return requireNonNull(left2RightLookupChain).getNodes().size() == 1;
            } else {
                return requireNonNull(right2LeftLookupChain).getNodes().size() == 1;
            }
        }

        @Nullable
        private GeneratedFilterCondition generateRemainingJoinCondition(
                boolean lookupRight, RowType conditionInputRowType) {
            boolean isBinaryLookup = isBinaryLookup(lookupRight);
            final Optional<RexNode> remainingJoinCondition;
            if (isBinaryLookup) {
                DeltaJoinLookupChain lookupChain =
                        lookupRight ? left2RightLookupChain : right2LeftLookupChain;
                remainingJoinCondition =
                        lookupChain.getNodes().get(0).deltaJoinSpec.getRemainingCondition();
            } else {
                // Even if we push down conditions into multi lookup handlers, the original
                // conditions in the join node must still be applied again to filter the retrieved
                // results to ensure correctness
                remainingJoinCondition = Optional.of(condition);
            }
            return remainingJoinCondition
                    .map(
                            cond ->
                                    FilterCodeGenerator.generateFilterCondition(
                                            config,
                                            classLoader,
                                            cond,
                                            conditionInputRowType,
                                            GENERATED_JOIN_CONDITION_CLASS_NAME))
                    .orElse(null);
        }
    }
}
