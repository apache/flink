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
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.runtime.operators.sort.LimitOperator;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

/** Batch {@link ExecNode} for Limit. */
@ExecNodeMetadata(
        name = "batch-exec-limit",
        version = 1,
        producedTransformations = BatchExecLimit.LIMIT_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v2_0,
        minStateVersion = FlinkVersion.v2_0)
public class BatchExecLimit extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData>, SingleTransformationTranslator<RowData> {

    public static final String LIMIT_TRANSFORMATION = "limit";
    public static final String FIELD_NAME_LIMIT_START = "limitStart";
    public static final String FIELD_NAME_LIMIT_END = "limitEnd";
    public static final String FIELD_NAME_IS_GLOBAL = "isGlobal";

    @JsonProperty(FIELD_NAME_LIMIT_START)
    private final long limitStart;

    @JsonProperty(FIELD_NAME_LIMIT_END)
    private final long limitEnd;

    @JsonProperty(FIELD_NAME_IS_GLOBAL)
    private final boolean isGlobal;

    public BatchExecLimit(
            ReadableConfig tableConfig,
            long limitStart,
            long limitEnd,
            boolean isGlobal,
            InputProperty inputProperty,
            LogicalType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecLimit.class),
                ExecNodeContext.newPersistedConfig(BatchExecLimit.class, tableConfig),
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.isGlobal = isGlobal;
        this.limitStart = limitStart;
        this.limitEnd = limitEnd;
    }

    @JsonCreator
    public BatchExecLimit(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_LIMIT_START) long limitStart,
            @JsonProperty(FIELD_NAME_LIMIT_END) long limitEnd,
            @JsonProperty(FIELD_NAME_IS_GLOBAL) boolean isGlobal,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) LogicalType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        this.isGlobal = isGlobal;
        this.limitStart = limitStart;
        this.limitEnd = limitEnd;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        Transformation<RowData> inputTransform =
                (Transformation<RowData>) getInputEdges().get(0).translateToPlan(planner);
        LimitOperator operator = new LimitOperator(isGlobal, limitStart, limitEnd);
        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                createTransformationMeta(LIMIT_TRANSFORMATION, config),
                SimpleOperatorFactory.of(operator),
                inputTransform.getOutputType(),
                inputTransform.getParallelism(),
                false);
    }
}
