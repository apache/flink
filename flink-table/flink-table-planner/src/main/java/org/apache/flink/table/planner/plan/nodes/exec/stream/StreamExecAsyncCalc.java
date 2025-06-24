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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecAsyncCalc;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexNode;

import java.util.Collections;
import java.util.List;

/** Stream {@link ExecNode} for {@link org.apache.flink.table.functions.AsyncScalarFunction}. */
@ExecNodeMetadata(
        name = "stream-exec-async-calc",
        version = 1,
        producedTransformations = CommonExecAsyncCalc.ASYNC_CALC_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v1_19,
        minStateVersion = FlinkVersion.v1_19,
        consumedOptions = {
            "table.exec.async-scalar.buffer-capacity",
            "table.exec.async-scalar.timeout",
            "table.exec.async-scalar.retry-strategy",
            "table.exec.async-scalar.retry-delay",
            "table.exec.async-scalar.max-attempts",
        })
public class StreamExecAsyncCalc extends CommonExecAsyncCalc implements StreamExecNode<RowData> {

    public StreamExecAsyncCalc(
            ReadableConfig tableConfig,
            List<RexNode> projection,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecAsyncCalc.class),
                ExecNodeContext.newPersistedConfig(StreamExecAsyncCalc.class, tableConfig),
                projection,
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecAsyncCalc(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_PROJECTION) List<RexNode> projection,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, context, persistedConfig, projection, inputProperties, outputType, description);
    }
}
