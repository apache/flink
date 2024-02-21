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
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.RowData;
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
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.plan.utils.LookupJoinUtil;
import org.apache.flink.table.runtime.keyselector.EmptyRowDataKeySelector;
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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecSink.PARTITIONER_TRANSFORMATION;

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

    public static final String FIELD_NAME_LOOKUP_KEY_CONTAINS_PRIMARY_KEY =
            "lookupKeyContainsPrimaryKey";

    public static final String STATE_NAME = "lookupJoinState";

    @JsonProperty(FIELD_NAME_LOOKUP_KEY_CONTAINS_PRIMARY_KEY)
    private final boolean lookupKeyContainsPrimaryKey;

    @JsonProperty(FIELD_NAME_REQUIRE_UPSERT_MATERIALIZE)
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private final boolean upsertMaterialize;

    @Nullable
    @JsonProperty(FIELD_NAME_STATE)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final List<StateMetadata> stateMetadataList;

    public StreamExecLookupJoin(
            ReadableConfig tableConfig,
            FlinkJoinType joinType,
            @Nullable RexNode preFilterCondition,
            @Nullable RexNode remainingJoinCondition,
            TemporalTableSourceSpec temporalTableSourceSpec,
            Map<Integer, LookupJoinUtil.LookupKey> lookupKeys,
            @Nullable List<RexNode> projectionOnTemporalTable,
            @Nullable RexNode filterOnTemporalTable,
            boolean lookupKeyContainsPrimaryKey,
            boolean upsertMaterialize,
            @Nullable LookupJoinUtil.AsyncLookupOptions asyncLookupOptions,
            @Nullable LookupJoinUtil.RetryLookupOptions retryOptions,
            ChangelogMode inputChangelogMode,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
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
                // serialize state meta only when upsert materialize is enabled
                upsertMaterialize
                        ? StateMetadata.getOneInputOperatorDefaultMeta(tableConfig, STATE_NAME)
                        : null,
                Collections.singletonList(inputProperty),
                outputType,
                description);
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
            @JsonProperty(FIELD_NAME_LOOKUP_KEYS) Map<Integer, LookupJoinUtil.LookupKey> lookupKeys,
            @JsonProperty(FIELD_NAME_PROJECTION_ON_TEMPORAL_TABLE) @Nullable
                    List<RexNode> projectionOnTemporalTable,
            @JsonProperty(FIELD_NAME_FILTER_ON_TEMPORAL_TABLE) @Nullable
                    RexNode filterOnTemporalTable,
            @JsonProperty(FIELD_NAME_LOOKUP_KEY_CONTAINS_PRIMARY_KEY)
                    boolean lookupKeyContainsPrimaryKey,
            @JsonProperty(FIELD_NAME_REQUIRE_UPSERT_MATERIALIZE) boolean upsertMaterialize,
            @JsonProperty(FIELD_NAME_ASYNC_OPTIONS) @Nullable
                    LookupJoinUtil.AsyncLookupOptions asyncLookupOptions,
            @JsonProperty(FIELD_NAME_RETRY_OPTIONS) @Nullable
                    LookupJoinUtil.RetryLookupOptions retryOptions,
            @JsonProperty(FIELD_NAME_INPUT_CHANGELOG_MODE) @Nullable
                    ChangelogMode inputChangelogMode,
            @JsonProperty(FIELD_NAME_STATE) @Nullable List<StateMetadata> stateMetadataList,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
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
                description);
        this.lookupKeyContainsPrimaryKey = lookupKeyContainsPrimaryKey;
        this.upsertMaterialize = upsertMaterialize;
        this.stateMetadataList = stateMetadataList;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        return createJoinTransformation(
                planner, config, upsertMaterialize, lookupKeyContainsPrimaryKey);
    }

    @Override
    protected Transformation<RowData> createSyncLookupJoinWithState(
            Transformation<RowData> inputTransformation,
            RelOptTable temporalTable,
            ExecNodeConfig config,
            ClassLoader classLoader,
            Map<Integer, LookupJoinUtil.LookupKey> allLookupKeys,
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

        List<Integer> refKeys =
                allLookupKeys.values().stream()
                        .filter(key -> key instanceof LookupJoinUtil.FieldRefLookupKey)
                        .map(key -> ((LookupJoinUtil.FieldRefLookupKey) key).index)
                        .collect(Collectors.toList());
        RowDataKeySelector keySelector;

        // use single parallelism for empty key shuffle
        boolean singleParallelism = refKeys.isEmpty();
        if (singleParallelism) {
            // all lookup keys are constants, then use an empty key selector
            keySelector = EmptyRowDataKeySelector.INSTANCE;
        } else {
            // make it a deterministic asc order
            Collections.sort(refKeys);
            keySelector =
                    KeySelectorUtil.getRowDataSelector(
                            classLoader,
                            refKeys.stream().mapToInt(Integer::intValue).toArray(),
                            InternalTypeInfo.of(inputRowType));
        }
        final KeyGroupStreamPartitioner<RowData, RowData> partitioner =
                new KeyGroupStreamPartitioner<>(
                        keySelector, KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM);
        Transformation<RowData> partitionedTransform =
                new PartitionTransformation<>(inputTransformation, partitioner);
        createTransformationMeta(PARTITIONER_TRANSFORMATION, "Partitioner", "Partitioner", config)
                .fill(partitionedTransform);
        if (singleParallelism) {
            setSingletonParallelism(partitionedTransform);
        } else {
            partitionedTransform.setParallelism(inputTransformation.getParallelism(), false);
        }

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
        if (singleParallelism) {
            setSingletonParallelism(transform);
        }
        return transform;
    }

    private void setSingletonParallelism(Transformation<RowData> transformation) {
        transformation.setParallelism(1);
        transformation.setMaxParallelism(1);
    }
}
