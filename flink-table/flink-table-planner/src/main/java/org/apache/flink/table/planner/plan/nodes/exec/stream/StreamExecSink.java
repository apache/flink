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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.connectors.CollectDynamicSink;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecSink;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSinkSpec;
import org.apache.flink.table.runtime.typeutils.TypeCheckUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Stream {@link ExecNode} to to write data into an external sink defined by a {@link
 * DynamicTableSink}.
 */
@ExecNodeMetadata(
        name = "stream-exec-sink",
        version = 1,
        consumedOptions = {
            "table.exec.sink.not-null-enforcer",
            "table.exec.sink.type-length-enforcer",
            "table.exec.sink.upsert-materialize",
            "table.exec.sink.keyed-shuffle"
        },
        producedTransformations = {
            CommonExecSink.CONSTRAINT_VALIDATOR_TRANSFORMATION,
            CommonExecSink.PARTITIONER_TRANSFORMATION,
            CommonExecSink.UPSERT_MATERIALIZE_TRANSFORMATION,
            CommonExecSink.TIMESTAMP_INSERTER_TRANSFORMATION,
            CommonExecSink.SINK_TRANSFORMATION
        },
        minPlanVersion = FlinkVersion.v1_15,
        minStateVersion = FlinkVersion.v1_15)
public class StreamExecSink extends CommonExecSink implements StreamExecNode<Object> {

    public static final String FIELD_NAME_INPUT_CHANGELOG_MODE = "inputChangelogMode";
    public static final String FIELD_NAME_REQUIRE_UPSERT_MATERIALIZE = "requireUpsertMaterialize";
    public static final String FIELD_NAME_INPUT_UPSERT_KEY = "inputUpsertKey";

    @JsonProperty(FIELD_NAME_INPUT_CHANGELOG_MODE)
    private final ChangelogMode inputChangelogMode;

    @JsonProperty(FIELD_NAME_REQUIRE_UPSERT_MATERIALIZE)
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private final boolean upsertMaterialize;

    @JsonProperty(FIELD_NAME_INPUT_UPSERT_KEY)
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private final int[] inputUpsertKey;

    public StreamExecSink(
            ReadableConfig tableConfig,
            DynamicTableSinkSpec tableSinkSpec,
            ChangelogMode inputChangelogMode,
            InputProperty inputProperty,
            LogicalType outputType,
            boolean upsertMaterialize,
            int[] inputUpsertKey,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecSink.class),
                ExecNodeContext.newPersistedConfig(StreamExecSink.class, tableConfig),
                tableSinkSpec,
                inputChangelogMode,
                Collections.singletonList(inputProperty),
                outputType,
                upsertMaterialize,
                inputUpsertKey,
                description);
    }

    @JsonCreator
    public StreamExecSink(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_DYNAMIC_TABLE_SINK) DynamicTableSinkSpec tableSinkSpec,
            @JsonProperty(FIELD_NAME_INPUT_CHANGELOG_MODE) ChangelogMode inputChangelogMode,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) LogicalType outputType,
            @JsonProperty(FIELD_NAME_REQUIRE_UPSERT_MATERIALIZE) boolean upsertMaterialize,
            @JsonProperty(FIELD_NAME_INPUT_UPSERT_KEY) int[] inputUpsertKey,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(
                id,
                context,
                persistedConfig,
                tableSinkSpec,
                inputChangelogMode,
                false, // isBounded
                inputProperties,
                outputType,
                description);
        this.inputChangelogMode = inputChangelogMode;
        this.upsertMaterialize = upsertMaterialize;
        this.inputUpsertKey = inputUpsertKey;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<Object> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        final RowType inputRowType = (RowType) inputEdge.getOutputType();
        final DynamicTableSink tableSink = tableSinkSpec.getTableSink(planner.getFlinkContext());
        final boolean isCollectSink = tableSink instanceof CollectDynamicSink;

        final List<Integer> rowtimeFieldIndices = new ArrayList<>();
        for (int i = 0; i < inputRowType.getFieldCount(); ++i) {
            if (TypeCheckUtils.isRowTime(inputRowType.getTypeAt(i))) {
                rowtimeFieldIndices.add(i);
            }
        }
        final int rowtimeFieldIndex;
        if (rowtimeFieldIndices.size() > 1 && !isCollectSink) {
            throw new TableException(
                    String.format(
                            "The query contains more than one rowtime attribute column [%s] for writing into table '%s'.\n"
                                    + "Please select the column that should be used as the event-time timestamp "
                                    + "for the table sink by casting all other columns to regular TIMESTAMP or TIMESTAMP_LTZ.",
                            rowtimeFieldIndices.stream()
                                    .map(i -> inputRowType.getFieldNames().get(i))
                                    .collect(Collectors.joining(", ")),
                            tableSinkSpec
                                    .getContextResolvedTable()
                                    .getIdentifier()
                                    .asSummaryString()));
        } else if (rowtimeFieldIndices.size() == 1) {
            rowtimeFieldIndex = rowtimeFieldIndices.get(0);
        } else {
            rowtimeFieldIndex = -1;
        }

        return createSinkTransformation(
                planner.getExecEnv(),
                config,
                planner.getFlinkContext().getClassLoader(),
                inputTransform,
                tableSink,
                rowtimeFieldIndex,
                upsertMaterialize,
                inputUpsertKey);
    }
}
