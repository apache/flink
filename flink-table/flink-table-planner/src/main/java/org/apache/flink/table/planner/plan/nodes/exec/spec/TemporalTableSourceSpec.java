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

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.planner.delegation.PlannerBase;
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
 * TemporalTableSpec describes how the right tale of lookupJoin ser/des.
 *
 * <p>This class corresponds to {@link org.apache.calcite.plan.RelOptTable} rel node.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TemporalTableSourceSpec {
    public static final String FIELD_NAME_LOOK_UP_TABLE_SOURCE = "lookupTableSource";
    public static final String FIELD_NAME_OUTPUT_TYPE = "outputType";

    @JsonProperty(FIELD_NAME_LOOK_UP_TABLE_SOURCE)
    @Nullable
    private DynamicTableSourceSpec tableSourceSpec;

    @JsonProperty(FIELD_NAME_OUTPUT_TYPE)
    @Nullable
    private RelDataType outputType;

    @JsonIgnore private RelOptTable temporalTable;

    public TemporalTableSourceSpec(RelOptTable temporalTable, TableConfig tableConfig) {
        this.temporalTable = temporalTable;
        if (temporalTable instanceof TableSourceTable) {
            TableSourceTable tableSourceTable = (TableSourceTable) temporalTable;
            outputType = tableSourceTable.getRowType();
            this.tableSourceSpec =
                    new DynamicTableSourceSpec(
                            tableSourceTable.tableIdentifier(),
                            tableSourceTable.catalogTable(),
                            Arrays.asList(tableSourceTable.abilitySpecs()));
            tableSourceSpec.setTableSource(tableSourceTable.tableSource());
            tableSourceSpec.setReadableConfig(tableConfig.getConfiguration());
        }
    }

    @JsonCreator
    public TemporalTableSourceSpec(
            @JsonProperty(FIELD_NAME_LOOK_UP_TABLE_SOURCE) @Nullable
                    DynamicTableSourceSpec dynamicTableSourceSpec,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) @Nullable RelDataType outputType) {
        this.tableSourceSpec = dynamicTableSourceSpec;
        this.outputType = outputType;
    }

    @JsonIgnore
    public RelOptTable getTemporalTable(PlannerBase planner) {
        if (null != temporalTable) {
            return temporalTable;
        }
        if (null != tableSourceSpec && null != outputType) {
            LookupTableSource lookupTableSource = tableSourceSpec.getLookupTableSource(planner);
            ObjectIdentifier objectIdentifier = tableSourceSpec.getObjectIdentifier();
            ResolvedCatalogTable catalogTable = tableSourceSpec.getCatalogTable();
            SourceAbilitySpec[] sourceAbilitySpecs = null;
            if (null != tableSourceSpec.getSourceAbilitySpecs()) {
                sourceAbilitySpecs =
                        tableSourceSpec.getSourceAbilitySpecs().toArray(new SourceAbilitySpec[0]);
            }
            return new TableSourceTable(
                    null,
                    objectIdentifier,
                    outputType,
                    FlinkStatistic.UNKNOWN(),
                    lookupTableSource,
                    true,
                    catalogTable,
                    new String[] {},
                    sourceAbilitySpecs);
        }
        throw new TableException("Can not obtain temporalTable correctly!");
    }

    @JsonIgnore
    @Nullable
    public DynamicTableSourceSpec getTableSourceSpec() {
        return tableSourceSpec;
    }

    @JsonIgnore
    @Nullable
    public RelDataType getOutputType() {
        return outputType;
    }
}
