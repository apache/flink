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
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
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
import org.apache.flink.table.planner.plan.nodes.exec.spec.DeltaJoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.DeltaJoinUtil;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil.AsyncOptions;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil.FunctionParam;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.plan.utils.LookupJoinUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.runtime.collector.TableFunctionResultFuture;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.generated.GeneratedResultFuture;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.StreamingDeltaJoinOperatorFactory;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.deltajoin.AsyncDeltaJoinRunner;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.guava33.com.google.common.collect.Lists;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.tools.RelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDeltaJoin.DELTA_JOIN_TRANSFORMATION;
import static org.apache.flink.table.planner.plan.utils.DeltaJoinUtil.getUnwrappedAsyncLookupFunction;
import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTypeFactory;

/** {@link StreamExecNode} for delta join. */
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
@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamExecDeltaJoin extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

    protected static final Logger LOG = LoggerFactory.getLogger(StreamExecDeltaJoin.class);

    public static final String DELTA_JOIN_TRANSFORMATION = "delta-join";

    private static final String FIELD_NAME_LEFT_JOIN_KEYS = "leftJoinKeys";
    private static final String FIELD_NAME_RIGHT_JOIN_KEYS = "rightJoinKeys";

    private static final String FIELD_NAME_LOOKUP_RIGHT_TABLE_JOIN_SPEC =
            "lookupRightTableJoinSpec";
    private static final String FIELD_NAME_LOOKUP_LEFT_TABLE_JOIN_SPEC = "lookupLeftTableJoinSpec";

    private static final String FIELD_NAME_LEFT_UPSERT_KEY = "leftUpsertKey";
    private static final String FIELD_NAME_RIGHT_UPSERT_KEY = "rightUpsertKey";

    private static final String FIELD_NAME_JOIN_TYPE = "joinType";

    public static final String FIELD_NAME_ASYNC_OPTIONS = "asyncOptions";

    // ===== common =====

    @JsonProperty(FIELD_NAME_JOIN_TYPE)
    private final FlinkJoinType flinkJoinType;

    @JsonProperty(FIELD_NAME_ASYNC_OPTIONS)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final AsyncOptions asyncLookupOptions;

    // ===== related LEFT side =====

    @JsonProperty(FIELD_NAME_LEFT_JOIN_KEYS)
    private final int[] leftJoinKeys;

    @JsonProperty(FIELD_NAME_LEFT_UPSERT_KEY)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    private final int[] leftUpsertKeys;

    // left (streaming) side join right (lookup) side
    @JsonProperty(FIELD_NAME_LOOKUP_RIGHT_TABLE_JOIN_SPEC)
    private final DeltaJoinSpec lookupRightTableJoinSpec;

    // ===== related RIGHT side =====

    @JsonProperty(FIELD_NAME_RIGHT_JOIN_KEYS)
    private final int[] rightJoinKeys;

    @JsonProperty(FIELD_NAME_RIGHT_UPSERT_KEY)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    private final int[] rightUpsertKeys;

    // right (streaming) side join left (lookup) side
    @JsonProperty(FIELD_NAME_LOOKUP_LEFT_TABLE_JOIN_SPEC)
    private final DeltaJoinSpec lookupLeftTableJoinSpec;

    public StreamExecDeltaJoin(
            ReadableConfig tableConfig,
            FlinkJoinType flinkJoinType,
            // delta join args related with the left side
            int[] leftJoinKeys,
            @Nullable int[] leftUpsertKeys,
            DeltaJoinSpec lookupRightTableJoinSpec,
            // delta join args related with the right side
            int[] rightJoinKeys,
            @Nullable int[] rightUpsertKeys,
            DeltaJoinSpec lookupLeftTableJoinSpec,
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
                lookupRightTableJoinSpec,
                rightJoinKeys,
                rightUpsertKeys,
                lookupLeftTableJoinSpec,
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
            @JsonProperty(FIELD_NAME_LOOKUP_RIGHT_TABLE_JOIN_SPEC)
                    DeltaJoinSpec lookupRightTableJoinSpec,
            @JsonProperty(FIELD_NAME_RIGHT_JOIN_KEYS) int[] rightJoinKeys,
            @JsonProperty(FIELD_NAME_RIGHT_UPSERT_KEY) @Nullable int[] rightUpsertKeys,
            @JsonProperty(FIELD_NAME_LOOKUP_LEFT_TABLE_JOIN_SPEC)
                    DeltaJoinSpec lookupLeftTableJoinSpec,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description,
            @JsonProperty(FIELD_NAME_ASYNC_OPTIONS) AsyncOptions asyncLookupOptions) {
        super(id, context, persistedConfig, inputProperties, outputType, description);

        this.flinkJoinType = flinkJoinType;
        this.leftJoinKeys = leftJoinKeys;
        this.leftUpsertKeys = leftUpsertKeys;
        this.lookupRightTableJoinSpec = lookupRightTableJoinSpec;
        this.rightJoinKeys = rightJoinKeys;
        this.rightUpsertKeys = rightUpsertKeys;
        this.lookupLeftTableJoinSpec = lookupLeftTableJoinSpec;
        this.asyncLookupOptions = asyncLookupOptions;
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

        RelOptTable leftTemporalTable =
                lookupLeftTableJoinSpec
                        .getLookupTable()
                        .getTemporalTable(planner.getFlinkContext(), unwrapTypeFactory(planner));
        RelOptTable rightTemporalTable =
                lookupRightTableJoinSpec
                        .getLookupTable()
                        .getTemporalTable(planner.getFlinkContext(), unwrapTypeFactory(planner));

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

        StreamOperatorFactory<RowData> operatorFactory =
                createAsyncLookupDeltaJoin(
                        planner,
                        config,
                        leftTemporalTable,
                        rightTemporalTable,
                        lookupLeftTableJoinSpec.getLookupKeyMap(),
                        lookupRightTableJoinSpec.getLookupKeyMap(),
                        planner.createRelBuilder(),
                        leftStreamType,
                        rightStreamType,
                        leftJoinKeySelector,
                        leftUpsertKeySelector,
                        rightJoinKeySelector,
                        rightUpsertKeySelector,
                        classLoader);

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

    private StreamOperatorFactory<RowData> createAsyncLookupDeltaJoin(
            PlannerBase planner,
            ExecNodeConfig config,
            RelOptTable leftTempTable,
            RelOptTable rightTempTable,
            Map<Integer, FunctionParam> leftLookupKeys,
            Map<Integer, FunctionParam> rightLookupKeys,
            RelBuilder relBuilder,
            RowType leftStreamType,
            RowType rightStreamType,
            RowDataKeySelector leftJoinKeySelector,
            RowDataKeySelector leftUpsertKeySelector,
            RowDataKeySelector rightJoinKeySelector,
            RowDataKeySelector rightUpsertKeySelector,
            ClassLoader classLoader) {

        DataTypeFactory dataTypeFactory =
                ShortcutUtils.unwrapContext(relBuilder).getCatalogManager().getDataTypeFactory();

        AsyncDeltaJoinRunner leftLookupTableAsyncFunction =
                createAsyncDeltaJoinRunner(
                        planner,
                        config,
                        classLoader,
                        dataTypeFactory,
                        leftTempTable,
                        rightTempTable,
                        leftStreamType,
                        rightStreamType,
                        leftLookupKeys,
                        leftJoinKeySelector,
                        leftUpsertKeySelector,
                        rightJoinKeySelector,
                        rightUpsertKeySelector,
                        false);

        AsyncDeltaJoinRunner rightLookupTableAsyncFunction =
                createAsyncDeltaJoinRunner(
                        planner,
                        config,
                        classLoader,
                        dataTypeFactory,
                        leftTempTable,
                        rightTempTable,
                        leftStreamType,
                        rightStreamType,
                        rightLookupKeys,
                        leftJoinKeySelector,
                        leftUpsertKeySelector,
                        rightJoinKeySelector,
                        rightUpsertKeySelector,
                        true);

        Tuple2<Long, Long> leftRightCacheSize = getCacheSize(config);

        return new StreamingDeltaJoinOperatorFactory(
                rightLookupTableAsyncFunction,
                leftLookupTableAsyncFunction,
                leftJoinKeySelector,
                rightJoinKeySelector,
                asyncLookupOptions.asyncTimeout,
                asyncLookupOptions.asyncBufferCapacity,
                leftRightCacheSize.f0,
                leftRightCacheSize.f1,
                leftStreamType,
                rightStreamType);
    }

    @SuppressWarnings("unchecked")
    private AsyncDeltaJoinRunner createAsyncDeltaJoinRunner(
            PlannerBase planner,
            ExecNodeConfig config,
            ClassLoader classLoader,
            DataTypeFactory dataTypeFactory,
            RelOptTable leftTempTable,
            RelOptTable rightTempTable,
            RowType leftStreamSideType,
            RowType rightStreamSideType,
            Map<Integer, FunctionParam> lookupKeys,
            RowDataKeySelector leftJoinKeySelector,
            RowDataKeySelector leftUpsertKeySelector,
            RowDataKeySelector rightJoinKeySelector,
            RowDataKeySelector rightUpsertKeySelector,
            boolean treatRightAsLookupTable) {
        RelOptTable lookupTable = treatRightAsLookupTable ? rightTempTable : leftTempTable;
        RowType streamSideType = treatRightAsLookupTable ? leftStreamSideType : rightStreamSideType;
        RowType lookupSideType = treatRightAsLookupTable ? rightStreamSideType : leftStreamSideType;

        AsyncTableFunction<?> lookupSideAsyncTableFunction =
                getUnwrappedAsyncLookupFunction(lookupTable, lookupKeys.keySet(), classLoader);
        UserDefinedFunctionHelper.prepareInstance(config, lookupSideAsyncTableFunction);

        RowType lookupTableSourceRowType =
                FlinkTypeFactory.toLogicalRowType(lookupTable.getRowType());

        RowType resultRowType = (RowType) getOutputType();

        List<FunctionCallUtil.FunctionParam> convertedKeys =
                Arrays.stream(LookupJoinUtil.getOrderedLookupKeys(lookupKeys.keySet()))
                        .mapToObj(lookupKeys::get)
                        .collect(Collectors.toList());

        FunctionCallCodeGenerator.GeneratedTableFunctionWithDataType<AsyncFunction<RowData, Object>>
                lookupSideGeneratedFuncWithType =
                        LookupJoinCodeGenerator.generateAsyncLookupFunction(
                                config,
                                classLoader,
                                dataTypeFactory,
                                streamSideType,
                                lookupTableSourceRowType,
                                resultRowType,
                                convertedKeys,
                                lookupSideAsyncTableFunction,
                                String.join(".", lookupTable.getQualifiedName()));

        DataStructureConverter<?, ?> lookupSideFetcherConverter =
                DataStructureConverters.getConverter(lookupSideGeneratedFuncWithType.dataType());

        GeneratedResultFuture<TableFunctionResultFuture<RowData>> lookupSideGeneratedResultFuture;
        if (treatRightAsLookupTable) {
            lookupSideGeneratedResultFuture =
                    LookupJoinCodeGenerator.generateTableAsyncCollector(
                            config,
                            classLoader,
                            "TableFunctionResultFuture",
                            streamSideType,
                            lookupTableSourceRowType,
                            JavaScalaConversionUtil.toScala(
                                    lookupRightTableJoinSpec.getRemainingCondition()));
        } else {
            RexBuilder rexBuilder = new RexBuilder(planner.getTypeFactory());

            Optional<RexNode> newCond =
                    lookupLeftTableJoinSpec
                            .getRemainingCondition()
                            .map(
                                    con ->
                                            swapInputRefsInCondition(
                                                    rexBuilder,
                                                    con,
                                                    leftStreamSideType,
                                                    rightStreamSideType));
            lookupSideGeneratedResultFuture =
                    LookupJoinCodeGenerator.generateTableAsyncCollector(
                            config,
                            classLoader,
                            "TableFunctionResultFuture",
                            streamSideType,
                            lookupTableSourceRowType,
                            JavaScalaConversionUtil.toScala(newCond));
        }

        GeneratedFunction<FlatMapFunction<RowData, RowData>> lookupSideGeneratedCalc = null;
        if ((treatRightAsLookupTable
                        && lookupRightTableJoinSpec.getProjectionOnTemporalTable().isPresent())
                || (!treatRightAsLookupTable
                        && lookupLeftTableJoinSpec.getProjectionOnTemporalTable().isPresent())) {
            // a projection or filter after lookup table
            List<RexNode> projectionOnTemporalTable =
                    treatRightAsLookupTable
                            ? lookupRightTableJoinSpec.getProjectionOnTemporalTable().get()
                            : lookupLeftTableJoinSpec.getProjectionOnTemporalTable().get();
            RexNode filterOnTemporalTable =
                    treatRightAsLookupTable
                            ? lookupRightTableJoinSpec.getFilterOnTemporalTable().orElse(null)
                            : lookupLeftTableJoinSpec.getFilterOnTemporalTable().orElse(null);
            lookupSideGeneratedCalc =
                    LookupJoinCodeGenerator.generateCalcMapFunction(
                            config,
                            planner.getFlinkContext().getClassLoader(),
                            JavaScalaConversionUtil.toScala(projectionOnTemporalTable),
                            filterOnTemporalTable,
                            lookupSideType,
                            lookupTableSourceRowType);
        }

        return new AsyncDeltaJoinRunner(
                lookupSideGeneratedFuncWithType.tableFunc(),
                (DataStructureConverter<RowData, Object>) lookupSideFetcherConverter,
                lookupSideGeneratedCalc,
                lookupSideGeneratedResultFuture,
                InternalSerializers.create(lookupSideType),
                leftJoinKeySelector,
                leftUpsertKeySelector,
                rightJoinKeySelector,
                rightUpsertKeySelector,
                asyncLookupOptions.asyncBufferCapacity,
                treatRightAsLookupTable,
                enableCache(config));
    }

    /**
     * When swapping the left and right row type, all input references in the condition should be
     * shifted accordingly. Input references that originally pointed to the left will now point to
     * the right, and those that originally pointed to the right will point to the left.
     *
     * <p>For example, origin left type: [int, double]; origin right type: [double, int]; origin
     * condition: [$1 = $2]. After this shifting, the condition will be [$0 = $3].
     *
     * <p>Mainly inspired by {@link RelOptUtil.RexInputConverter}.
     */
    private RexNode swapInputRefsInCondition(
            RexBuilder rexBuilder, RexNode condition, RowType leftType, RowType rightType) {
        int leftFieldCount = leftType.getFieldCount();
        int rightFieldCount = rightType.getFieldCount();
        int[] adjustments = new int[leftFieldCount + rightFieldCount];
        // all input references on the left will be shifted to the right by `rightFieldCount`
        Arrays.fill(adjustments, 0, leftFieldCount, rightFieldCount);
        // all input references on the right will be shifted to the left by `leftFieldCount`
        Arrays.fill(
                adjustments, leftFieldCount, leftFieldCount + rightFieldCount, leftFieldCount * -1);

        RexShuttle converter =
                new RexShuttle() {

                    @Override
                    public RexNode visitInputRef(RexInputRef inputRef) {
                        int srcIndex = inputRef.getIndex();
                        int destIndex = srcIndex + adjustments[srcIndex];
                        RelDataType type = inputRef.getType();

                        return rexBuilder.makeInputRef(type, destIndex);
                    }
                };

        return condition.accept(converter);
    }

    private RowDataKeySelector getUpsertKeySelector(
            @Nullable int[] upsertKey, RowType rowType, ClassLoader classLoader) {
        final int[] rightUpsertKeys;
        if (upsertKey != null && upsertKey.length > 0) {
            rightUpsertKeys = upsertKey;
        } else {
            rightUpsertKeys = IntStream.range(0, rowType.getFields().size()).toArray();
        }
        return KeySelectorUtil.getRowDataSelector(
                classLoader, rightUpsertKeys, InternalTypeInfo.of(rowType));
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
}
