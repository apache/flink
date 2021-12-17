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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecPythonCalc;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexNode;

import java.util.Collections;
import java.util.List;

/** Batch {@link ExecNode} for Python ScalarFunctions. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class BatchExecPythonCalc extends CommonExecPythonCalc implements BatchExecNode<RowData> {

    public BatchExecPythonCalc(
            List<RexNode> projection,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        this(
                projection,
                getNewNodeId(),
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public BatchExecPythonCalc(
            @JsonProperty(FIELD_NAME_PROJECTION) List<RexNode> projection,
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(projection, id, inputProperties, outputType, description);
    }
}
