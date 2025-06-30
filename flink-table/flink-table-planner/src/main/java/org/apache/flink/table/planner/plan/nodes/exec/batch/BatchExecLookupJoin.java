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

package org.apache.flink.table.planner.plan.nodes.exec.batch;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecLookupJoin;
import org.apache.flink.table.planner.plan.nodes.exec.spec.TemporalTableSourceSpec;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/** {@link BatchExecNode} for temporal table join that implemented by lookup. */
@ExecNodeMetadata(
        name = "batch-exec-lookup-join",
        version = 1,
        producedTransformations = CommonExecLookupJoin.LOOKUP_JOIN_TRANSFORMATION,
        consumedOptions = {
            "table.exec.async-lookup.buffer-capacity",
            "table.exec.async-lookup.timeout",
            "table.exec.async-lookup.output-mode"
        },
        minPlanVersion = FlinkVersion.v2_0,
        minStateVersion = FlinkVersion.v2_0)
public class BatchExecLookupJoin extends CommonExecLookupJoin
        implements BatchExecNode<RowData>, SingleTransformationTranslator<RowData> {

    public BatchExecLookupJoin(
            ReadableConfig tableConfig,
            FlinkJoinType joinType,
            @Nullable RexNode preFilterCondition,
            @Nullable RexNode remainingJoinCondition,
            TemporalTableSourceSpec temporalTableSourceSpec,
            Map<Integer, FunctionCallUtil.FunctionParam> lookupKeys,
            @Nullable List<RexNode> projectionOnTemporalTable,
            @Nullable RexNode filterOnTemporalTable,
            @Nullable FunctionCallUtil.AsyncOptions asyncLookupOptions,
            InputProperty inputProperty,
            RowType outputType,
            String description,
            boolean preferCustomShuffle) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecLookupJoin.class),
                ExecNodeContext.newPersistedConfig(BatchExecLookupJoin.class, tableConfig),
                joinType,
                preFilterCondition,
                remainingJoinCondition,
                temporalTableSourceSpec,
                lookupKeys,
                projectionOnTemporalTable,
                filterOnTemporalTable,
                asyncLookupOptions,
                // batch lookup join does not support retry hint currently
                null,
                ChangelogMode.insertOnly(),
                Collections.singletonList(inputProperty),
                outputType,
                description,
                preferCustomShuffle);
    }

    @JsonCreator
    public BatchExecLookupJoin(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_JOIN_TYPE) FlinkJoinType joinType,
            @JsonProperty(FIELD_NAME_PRE_FILTER_CONDITION) @Nullable RexNode preFilterCondition,
            @JsonProperty(FIELD_NAME_REMAINING_JOIN_CONDITION) @Nullable
                    RexNode remainingJoinCondition,
            @JsonProperty(FIELD_NAME_TEMPORAL_TABLE)
                    TemporalTableSourceSpec temporalTableSourceSpec,
            @JsonProperty(FIELD_NAME_LOOKUP_KEYS)
                    Map<Integer, FunctionCallUtil.FunctionParam> lookupKeys,
            @JsonProperty(FIELD_NAME_PROJECTION_ON_TEMPORAL_TABLE) @Nullable
                    List<RexNode> projectionOnTemporalTable,
            @JsonProperty(FIELD_NAME_FILTER_ON_TEMPORAL_TABLE) @Nullable
                    RexNode filterOnTemporalTable,
            @JsonProperty(FIELD_NAME_ASYNC_OPTIONS) @Nullable
                    FunctionCallUtil.AsyncOptions asyncLookupOptions,
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
                // batch lookup join does not support retry hint currently
                null,
                ChangelogMode.insertOnly(),
                inputProperties,
                outputType,
                description,
                preferCustomShuffle);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        // There's no optimization when lookupKeyContainsPrimaryKey is true for batch, so set it to
        // false for now. We can add it to CommonExecLookupJoin when needed.
        return createJoinTransformation(planner, config, false, false);
    }

    @Override
    protected Transformation<RowData> createSyncLookupJoinWithState(
            Transformation<RowData> inputTransformation,
            RelOptTable temporalTable,
            ExecNodeConfig config,
            ClassLoader classLoader,
            Map<Integer, FunctionCallUtil.FunctionParam> allLookupKeys,
            TableFunction<?> syncLookupFunction,
            RelBuilder relBuilder,
            RowType inputRowType,
            RowType tableSourceRowType,
            RowType resultRowType,
            boolean isLeftOuterJoin,
            boolean isObjectReuseEnabled,
            boolean lookupKeyContainsPrimaryKey) {
        return inputTransformation;
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
        throw new IllegalStateException(
                "Batch mode should not use key-ordered async lookup joins. This is a bug. Please file an issue.");
    }
}
