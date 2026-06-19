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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.source.VectorSearchTableSource;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilitySpec;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;

import javax.annotation.Nullable;

import java.util.Arrays;

/**
 * Spec describes how the right table of search functions ser/des.
 *
 * <p>This class corresponds to {@link RelOptTable} rel node.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class VectorSearchTableSourceSpec {

    public static final String FIELD_NAME_VECTOR_SEARCH_TABLE_SOURCE = "vectorSearchTableSource";
    public static final String FIELD_NAME_OUTPUT_TYPE = "outputType";

    @JsonProperty(FIELD_NAME_VECTOR_SEARCH_TABLE_SOURCE)
    private final DynamicTableSourceSpec tableSourceSpec;

    @JsonProperty(FIELD_NAME_OUTPUT_TYPE)
    private final RelDataType outputType;

    @JsonIgnore private @Nullable TableSourceTable searchTable;

    public VectorSearchTableSourceSpec(TableSourceTable searchTable) {
        this.searchTable = searchTable;
        this.outputType = searchTable.getRowType();
        this.tableSourceSpec =
                new DynamicTableSourceSpec(
                        searchTable.contextResolvedTable(),
                        Arrays.asList(searchTable.abilitySpecs()));
    }

    @JsonCreator
    public VectorSearchTableSourceSpec(
            @JsonProperty(FIELD_NAME_VECTOR_SEARCH_TABLE_SOURCE)
                    DynamicTableSourceSpec tableSourceSpec,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RelDataType outputType) {
        this.tableSourceSpec = tableSourceSpec;
        this.outputType = outputType;
    }

    @JsonIgnore
    public TableSourceTable getSearchTable(FlinkContext context, FlinkTypeFactory typeFactory) {
        if (null != searchTable) {
            return searchTable;
        }
        if (null != tableSourceSpec && null != outputType) {
            VectorSearchTableSource vectorSearchTableSource =
                    tableSourceSpec.getVectorSearchTableSource(context, typeFactory);
            SourceAbilitySpec[] sourceAbilitySpecs = null;
            if (null != tableSourceSpec.getSourceAbilities()) {
                sourceAbilitySpecs =
                        tableSourceSpec.getSourceAbilities().toArray(new SourceAbilitySpec[0]);
            }
            return new TableSourceTable(
                    null,
                    outputType,
                    FlinkStatistic.UNKNOWN(),
                    vectorSearchTableSource,
                    true,
                    tableSourceSpec.getContextResolvedTable(),
                    context,
                    typeFactory,
                    sourceAbilitySpecs);
        }
        throw new TableException("Can not obtain searchTable correctly!");
    }

    public DynamicTableSourceSpec getTableSourceSpec() {
        return tableSourceSpec;
    }

    public RelDataType getOutputType() {
        return outputType;
    }
}
