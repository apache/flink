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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.MultipleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.StateMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecLookupJoin;
import org.apache.flink.table.planner.plan.nodes.exec.spec.TemporalTableSourceSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil.AsyncOptions;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil.FunctionParam;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.plan.utils.LookupJoinUtil.RetryLookupOptions;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.lookup.KeyedLookupJoinWrapper;
import org.apache.flink.table.runtime.operators.join.lookup.LookupJoinRunner;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.StateConfigUtil;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/** {@link StreamExecNode} for temporal table join that implemented by lookup. */
@ExecNodeMetadata(
        name = "stream-exec-lookup-join",
        version = 1,
        producedTransformations = CommonExecLookupJoin.LOOKUP_JOIN_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v1_15,
        minStateVersion = FlinkVersion.v1_15)
public class StreamExecLookupJoin extends CommonExecLookupJoin
        implements StreamExecNode<RowData>, MultipleTransformationTranslator<RowData> {
    public static final String FIELD_NAME_REQUIRE_UPSERT_MATERIALIZE = "requireUpsertMaterialize";

    public static final String PARTITIONER_TRANSFORMATION_LOOKUP_JOIN = "partitioner";

    public static final String FIELD_NAME_LOOKUP_KEY_CONTAINS_PRIMARY_KEY =
            "lookupKeyContainsPrimaryKey";

    public static final String STATE_NAME = "lookupJoinState";
    public static final String FIELD_NAME_INPUT_UPSERT_KEY = "inputUpsertKey";

    @JsonProperty(FIELD_NAME_LOOKUP_KEY_CONTAINS_PRIMARY_KEY)
    private final boolean lookupKeyContainsPrimaryKey;

    @JsonProperty(FIELD_NAME_REQUIRE_UPSERT_MATERIALIZE)
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private final boolean upsertMaterialize;

    @Nullable
    @JsonProperty(FIELD_NAME_STATE)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final List<StateMetadata> stateMetadataList;

    @Nullable
    @JsonProperty(FIELD_NAME_INPUT_UPSERT_KEY)
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private final int[] inputUpsertKey;

    public StreamExecLookupJoin(
            ReadableConfig tableConfig,
            FlinkJoinType joinType,
            @Nullable RexNode preFilterCondition,
            @Nullable RexNode remainingJoinCondition,
            TemporalTableSourceSpec temporalTableSourceSpec,
            Map<Integer, FunctionParam> lookupKeys,
            @Nullable List<RexNode> projectionOnTemporalTable,
            @Nullable RexNode filterOnTemporalTable,
            boolean lookupKeyContainsPrimaryKey,
            boolean upsertMaterialize,
            @Nullable AsyncOptions asyncLookupOptions,
            @Nullable RetryLookupOptions retryOptions,
            ChangelogMode inputChangelogMode,
            @Nullable int[] inputUpsertKey,
            InputProperty inputProperty,
            RowType outputType,
            String description,
            boolean preferCustomShuffle) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecLookupJoin.class),
                ExecNodeContext.newPersistedConfig(StreamExecLookupJoin.class, tableConfig),
                joinType,
                preFilterCondition,
                remainingJoinCondition,
                temporalTableSourceSpec,
                lookupKeys,
                projectionOnTemporalTable,
                filterOnTemporalTable,
                lookupKeyContainsPrimaryKey,
                upsertMaterialize,
                asyncLookupOptions,
                retryOptions,
                inputChangelogMode,
                inputUpsertKey,
                // serialize state meta only when upsert materialize is enabled
                upsertMaterialize
                        ? StateMetadata.getOneInputOperatorDefaultMeta(tableConfig, STATE_NAME)
                        : null,
                Collections.singletonList(inputProperty),
                outputType,
                description,
                preferCustomShuffle);
    }

    @JsonCreator
    public StreamExecLookupJoin(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_JOIN_TYPE) FlinkJoinType joinType,
            @JsonProperty(FIELD_NAME_PRE_FILTER_CONDITION) @Nullable RexNode preFilterCondition,
            @JsonProperty(FIELD_NAME_REMAINING_JOIN_CONDITION) @Nullable
                    RexNode remainingJoinCondition,
            @JsonProperty(FIELD_NAME_TEMPORAL_TABLE)
                    TemporalTableSourceSpec temporalTableSourceSpec,
            @JsonProperty(FIELD_NAME_LOOKUP_KEYS) Map<Integer, FunctionParam> lookupKeys,
            @JsonProperty(FIELD_NAME_PROJECTION_ON_TEMPORAL_TABLE) @Nullable
                    List<RexNode> projectionOnTemporalTable,
            @JsonProperty(FIELD_NAME_FILTER_ON_TEMPORAL_TABLE) @Nullable
                    RexNode filterOnTemporalTable,
            @JsonProperty(FIELD_NAME_LOOKUP_KEY_CONTAINS_PRIMARY_KEY)
                    boolean lookupKeyContainsPrimaryKey,
            @JsonProperty(FIELD_NAME_REQUIRE_UPSERT_MATERIALIZE) boolean upsertMaterialize,
            @JsonProperty(FIELD_NAME_ASYNC_OPTIONS) @Nullable AsyncOptions asyncLookupOptions,
            @JsonProperty(FIELD_NAME_RETRY_OPTIONS) @Nullable RetryLookupOptions retryOptions,
            @JsonProperty(FIELD_NAME_INPUT_CHANGELOG_MODE) @Nullable
                    ChangelogMode inputChangelogMode,
            @JsonProperty(FIELD_NAME_INPUT_UPSERT_KEY) @Nullable int[] inputUpsertKey,
            @JsonProperty(FIELD_NAME_STATE) @Nullable List<StateMetadata> stateMetadataList,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description,
            @JsonProperty(FIELD_NAME_PREFER_CUSTOM_SHUFFLE) boolean preferCustomShuffle) {
        super(
                id,
                context,
                persistedConfig,
                joinType,
                preFilterCondition,
                remainingJoinCondition,
                temporalTableSourceSpec,
                lookupKeys,
                projectionOnTemporalTable,
                filterOnTemporalTable,
                asyncLookupOptions,
                retryOptions,
                inputChangelogMode,
                inputProperties,
                outputType,
                description,
                preferCustomShuffle);
        this.lookupKeyContainsPrimaryKey = lookupKeyContainsPrimaryKey;
        this.upsertMaterialize = upsertMaterialize;
        this.inputUpsertKey = inputUpsertKey;
        this.stateMetadataList = stateMetadataList;
    }

    @Override
    public Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        return createJoinTransformation(
                planner, config, upsertMaterialize, lookupKeyContainsPrimaryKey);
    }

    @Override
    protected Transformation<RowData> createKeyOrderedAsyncLookupJoin(
            Transformation<RowData> inputTransformation,
            RelOptTable temporalTable,
            ExecNodeConfig config,
            ClassLoader classLoader,
            Map<Integer, FunctionCallUtil.FunctionParam> allLookupKeys,
            AsyncTableFunction<Object> asyncLookupFunction,
            RelBuilder relBuilder,
            RowType inputRowType,
            RowType tableSourceRowType,
            RowType resultRowType,
            boolean isLeftOuterJoin,
            FunctionCallUtil.AsyncOptions asyncLookupOptions) {
        RowDataKeySelector keySelector = getKeySelector(classLoader, inputRowType);

        Transformation<RowData> partitionedTransform =
                createPartitionTransformation(keySelector, inputTransformation, config);

        StreamOperatorFactory<RowData> operatorFactory;

        operatorFactory =
                createAsyncLookupJoin(
                        temporalTable,
                        config,
                        classLoader,
                        allLookupKeys,
                        asyncLookupFunction,
                        relBuilder,
                        inputRowType,
                        tableSourceRowType,
                        resultRowType,
                        isLeftOuterJoin,
                        asyncLookupOptions,
                        keySelector);

        OneInputTransformation<RowData, RowData> transform =
                ExecNodeUtil.createOneInputTransformation(
                        partitionedTransform,
                        createTransformationMeta(LOOKUP_JOIN_KEY_ORDERED_TRANSFORMATION, config),
                        operatorFactory,
                        InternalTypeInfo.of(resultRowType),
                        partitionedTransform.getParallelism(),
                        false);

        transform.setStateKeySelector(keySelector);
        transform.setStateKeyType(keySelector.getProducedType());
        return transform;
    }

    @Override
    protected Transformation<RowData> createSyncLookupJoinWithState(
            Transformation<RowData> inputTransformation,
            RelOptTable temporalTable,
            ExecNodeConfig config,
            ClassLoader classLoader,
            Map<Integer, FunctionParam> allLookupKeys,
            TableFunction<?> syncLookupFunction,
            RelBuilder relBuilder,
            RowType inputRowType,
            RowType tableSourceRowType,
            RowType resultRowType,
            boolean isLeftOuterJoin,
            boolean isObjectReuseEnabled,
            boolean lookupKeyContainsPrimaryKey) {

        final long stateRetentionTime =
                StateMetadata.getStateTtlForOneInputOperator(config, stateMetadataList);

        // create lookup function first
        ProcessFunction<RowData, RowData> processFunction =
                createSyncLookupJoinFunction(
                        temporalTable,
                        config,
                        classLoader,
                        allLookupKeys,
                        syncLookupFunction,
                        relBuilder,
                        inputRowType,
                        tableSourceRowType,
                        resultRowType,
                        isLeftOuterJoin,
                        isObjectReuseEnabled);

        RowType rightRowType =
                getRightOutputRowType(
                        getProjectionOutputRelDataType(relBuilder), tableSourceRowType);

        KeyedLookupJoinWrapper keyedLookupJoinWrapper =
                new KeyedLookupJoinWrapper(
                        (LookupJoinRunner) processFunction,
                        StateConfigUtil.createTtlConfig(stateRetentionTime),
                        InternalSerializers.create(rightRowType),
                        lookupKeyContainsPrimaryKey);

        KeyedProcessOperator<RowData, RowData, RowData> operator =
                new KeyedProcessOperator<>(keyedLookupJoinWrapper);

        RowDataKeySelector keySelector = getKeySelector(classLoader, inputRowType);

        Transformation<RowData> partitionedTransform =
                createPartitionTransformation(keySelector, inputTransformation, config);

        OneInputTransformation<RowData, RowData> transform =
                ExecNodeUtil.createOneInputTransformation(
                        partitionedTransform,
                        createTransformationMeta(LOOKUP_JOIN_MATERIALIZE_TRANSFORMATION, config),
                        operator,
                        InternalTypeInfo.of(resultRowType),
                        partitionedTransform.getParallelism(),
                        false);
        transform.setStateKeySelector(keySelector);
        transform.setStateKeyType(keySelector.getProducedType());
        return transform;
    }

    private RowDataKeySelector getKeySelector(ClassLoader classLoader, RowType inputRowType) {
        RowDataKeySelector keySelector;
        int[] shuffleKeys = inputUpsertKey;
        // Two cases the upsertKeys could be null
        // 1. If the job is restored from exec plan then upsertKeys could be null
        // 2. If planner could not deduce the upsertKeys then upsertKeys could also be null.
        if (shuffleKeys == null || shuffleKeys.length == 0) {
            shuffleKeys = IntStream.range(0, inputRowType.getFieldCount()).toArray();
        } else {
            // make it a deterministic asc order
            Arrays.sort(shuffleKeys);
        }
        keySelector =
                KeySelectorUtil.getRowDataSelector(
                        classLoader, shuffleKeys, InternalTypeInfo.of(inputRowType));

        return keySelector;
    }

    private Transformation<RowData> createPartitionTransformation(
            RowDataKeySelector keySelector,
            Transformation<RowData> inputTransformation,
            ExecNodeConfig config) {

        final KeyGroupStreamPartitioner<RowData, RowData> partitioner =
                new KeyGroupStreamPartitioner<>(
                        keySelector, KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM);
        Transformation<RowData> partitionedTransform =
                new PartitionTransformation<>(inputTransformation, partitioner);

        createTransformationMeta(
                        PARTITIONER_TRANSFORMATION_LOOKUP_JOIN,
                        "Partitioner",
                        "Partitioner",
                        config)
                .fill(partitionedTransform);

        partitionedTransform.setParallelism(inputTransformation.getParallelism(), false);

        return partitionedTransform;
    }
}
