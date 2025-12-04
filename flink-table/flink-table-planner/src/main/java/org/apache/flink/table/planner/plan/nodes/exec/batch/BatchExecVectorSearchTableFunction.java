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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecVectorSearchTableFunction;
import org.apache.flink.table.planner.plan.nodes.exec.spec.VectorSearchSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.VectorSearchTableSourceSpec;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;

/** Batch ExecNode for {@code VECTOR_SEARCH}. */
@ExecNodeMetadata(
        name = "batch-exec-vector-search-table-function",
        version = 1,
        consumedOptions = {
            "table.exec.async-vector-search.max-concurrent-operations",
            "table.exec.async-vector-search.timeout",
            "table.exec.async-vector-search.output-mode"
        },
        producedTransformations = CommonExecVectorSearchTableFunction.VECTOR_SEARCH_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v2_2,
        minStateVersion = FlinkVersion.v2_2)
public class BatchExecVectorSearchTableFunction extends CommonExecVectorSearchTableFunction
        implements BatchExecNode<RowData>, SingleTransformationTranslator<RowData> {

    public BatchExecVectorSearchTableFunction(
            ReadableConfig tableConfig,
            VectorSearchTableSourceSpec tableSourceSpec,
            VectorSearchSpec vectorSearchSpec,
            @Nullable FunctionCallUtil.AsyncOptions asyncOptions,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecVectorSearchTableFunction.class),
                ExecNodeContext.newPersistedConfig(
                        BatchExecVectorSearchTableFunction.class, tableConfig),
                tableSourceSpec,
                vectorSearchSpec,
                asyncOptions,
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public BatchExecVectorSearchTableFunction(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_TABLE_SOURCE_SPEC) VectorSearchTableSourceSpec tableSourceSpec,
            @JsonProperty(FIELD_NAME_VECTOR_SEARCH_SPEC) VectorSearchSpec vectorSearchSpec,
            @JsonProperty(FIELD_NAME_ASYNC_OPTIONS) @Nullable
                    FunctionCallUtil.AsyncOptions asyncOptions,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(
                id,
                context,
                persistedConfig,
                tableSourceSpec,
                vectorSearchSpec,
                asyncOptions,
                inputProperties,
                outputType,
                description);
    }
}
