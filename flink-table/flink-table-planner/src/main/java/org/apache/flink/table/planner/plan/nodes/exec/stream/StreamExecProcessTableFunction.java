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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * {@link StreamExecNode} for {@link ProcessTableFunction}.
 *
 * <p>A process table function (PTF) maps zero, one, or multiple tables to zero, one, or multiple
 * rows. PTFs enable implementing user-defined operators that can be as feature-rich as built-in
 * operations. PTFs have access to Flink's managed state, event-time and timer services, underlying
 * table changelogs, and can take multiple ordered and/or partitioned tables to produce a new table.
 */
@ExecNodeMetadata(
        name = "stream-exec-process-table-function",
        version = 1,
        producedTransformations = StreamExecProcessTableFunction.PROCESS_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v2_0,
        minStateVersion = FlinkVersion.v2_0)
public class StreamExecProcessTableFunction extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

    public static final String PROCESS_TRANSFORMATION = "process";

    public static final String FIELD_NAME_UID = "uid";
    public static final String FIELD_NAME_FUNCTION_CALL = "functionCall";
    public static final String FIELD_NAME_INPUT_CHANGELOG_MODES = "inputChangelogModes";

    @JsonProperty(FIELD_NAME_UID)
    private final String uid;

    @JsonProperty(FIELD_NAME_FUNCTION_CALL)
    private final RexCall invocation;

    @JsonProperty(FIELD_NAME_INPUT_CHANGELOG_MODES)
    private final List<ChangelogMode> inputChangelogModes;

    public StreamExecProcessTableFunction(
            ReadableConfig tableConfig,
            List<InputProperty> inputProperties,
            RowType outputType,
            String description,
            String uid,
            RexCall invocation,
            List<ChangelogMode> inputChangelogModes) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecProcessTableFunction.class),
                ExecNodeContext.newPersistedConfig(
                        StreamExecProcessTableFunction.class, tableConfig),
                inputProperties,
                outputType,
                description,
                uid,
                invocation,
                inputChangelogModes);
    }

    @JsonCreator
    public StreamExecProcessTableFunction(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description,
            @JsonProperty(FIELD_NAME_UID) String uid,
            @JsonProperty(FIELD_NAME_FUNCTION_CALL) RexNode invocation,
            @JsonProperty(FIELD_NAME_INPUT_CHANGELOG_MODES)
                    List<ChangelogMode> inputChangelogModes) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        this.uid = uid;
        this.invocation = (RexCall) invocation;
        this.inputChangelogModes = inputChangelogModes;
    }

    public String getUid() {
        return uid;
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        throw new TableException("Process table function is not fully supported yet.");
    }
}
