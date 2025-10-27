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

package org.apache.flink.table.planner.plan.nodes.exec.spec;

import org.apache.flink.table.planner.plan.utils.FunctionCallUtil.FunctionParam;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rel.core.JoinRelType;

import javax.annotation.Nullable;

import java.util.Map;

/** VectorSearchSpec describes how vector search is performed. */
public class VectorSearchSpec {

    public static final String FIELD_NAME_JOIN_TYPE = "joinType";
    public static final String FIELD_NAME_SEARCH_COLUMNS = "searchColumns";
    public static final String FIELD_NAME_TOP_K = "topK";
    public static final String FIELD_NAME_RUNTIME_CONFIG = "runtimeConfig";
    public static final String FIELD_NAME_OUTPUT_TYPE = "outputType";

    @JsonProperty(FIELD_NAME_JOIN_TYPE)
    private final JoinRelType joinRelType;

    /** KV: column_to_search -> column_to_query. */
    @JsonProperty(FIELD_NAME_SEARCH_COLUMNS)
    private final Map<Integer, FunctionParam> searchColumns;

    @JsonProperty(FIELD_NAME_TOP_K)
    private final FunctionParam topK;

    @JsonProperty(FIELD_NAME_RUNTIME_CONFIG)
    private final @Nullable Map<String, String> runtimeConfig;

    @JsonProperty(FIELD_NAME_OUTPUT_TYPE)
    private final RowType outputType;

    @JsonCreator
    public VectorSearchSpec(
            @JsonProperty(FIELD_NAME_JOIN_TYPE) JoinRelType joinRelType,
            @JsonProperty(FIELD_NAME_SEARCH_COLUMNS) Map<Integer, FunctionParam> searchColumns,
            @JsonProperty(FIELD_NAME_TOP_K) FunctionParam topK,
            @Nullable @JsonProperty(FIELD_NAME_RUNTIME_CONFIG) Map<String, String> runtimeConfig,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType) {
        this.joinRelType = joinRelType;
        this.searchColumns = searchColumns;
        this.topK = topK;
        this.runtimeConfig = runtimeConfig;
        this.outputType = outputType;
    }

    @JsonIgnore
    public JoinRelType getJoinType() {
        return joinRelType;
    }

    @JsonIgnore
    public Map<Integer, FunctionParam> getSearchColumns() {
        return searchColumns;
    }

    @JsonIgnore
    public FunctionParam getTopK() {
        return topK;
    }

    @JsonIgnore
    public @Nullable Map<String, String> getRuntimeConfig() {
        return runtimeConfig;
    }

    @JsonIgnore
    public RowType getOutputType() {
        return outputType;
    }
}
