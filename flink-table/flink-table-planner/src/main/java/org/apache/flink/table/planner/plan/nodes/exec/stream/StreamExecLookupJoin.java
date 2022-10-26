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
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecLookupJoin;
import org.apache.flink.table.planner.plan.nodes.exec.spec.TemporalTableSourceSpec;
import org.apache.flink.table.planner.plan.utils.LookupJoinUtil;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/** {@link StreamExecNode} for temporal table join that implemented by lookup. */
@ExecNodeMetadata(
        name = "stream-exec-lookup-join",
        version = 1,
        producedTransformations = CommonExecLookupJoin.LOOKUP_JOIN_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v1_15,
        minStateVersion = FlinkVersion.v1_15)
public class StreamExecLookupJoin extends CommonExecLookupJoin implements StreamExecNode<RowData> {
    public static final String FIELD_NAME_REQUIRE_UPSERT_MATERIALIZE = "requireUpsertMaterialize";

    public static final String FIELD_NAME_LOOKUP_KEY_CONTAINS_PRIMARY_KEY =
            "lookupKeyContainsPrimaryKey";

    @JsonProperty(FIELD_NAME_LOOKUP_KEY_CONTAINS_PRIMARY_KEY)
    private final boolean lookupKeyContainsPrimaryKey;

    @JsonProperty(FIELD_NAME_REQUIRE_UPSERT_MATERIALIZE)
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private final boolean upsertMaterialize;

    public StreamExecLookupJoin(
            ReadableConfig tableConfig,
            FlinkJoinType joinType,
            @Nullable RexNode joinCondition,
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
                joinCondition,
                temporalTableSourceSpec,
                lookupKeys,
                projectionOnTemporalTable,
                filterOnTemporalTable,
                lookupKeyContainsPrimaryKey,
                upsertMaterialize,
                asyncLookupOptions,
                retryOptions,
                inputChangelogMode,
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
            @JsonProperty(FIELD_NAME_JOIN_CONDITION) @Nullable RexNode joinCondition,
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
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(
                id,
                context,
                persistedConfig,
                joinType,
                joinCondition,
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
    }

    @Override
    @SuppressWarnings("unchecked")
    public Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        return createJoinTransformation(
                planner, config, upsertMaterialize, lookupKeyContainsPrimaryKey);
    }
}
