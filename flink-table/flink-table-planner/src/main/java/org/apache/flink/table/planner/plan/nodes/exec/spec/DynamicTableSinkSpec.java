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

import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.plan.abilities.sink.SinkAbilitySpec;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * {@link DynamicTableSourceSpec} describes how to serialize/deserialize dynamic table sink table
 * and create {@link DynamicTableSink} from the deserialization result.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DynamicTableSinkSpec extends DynamicTableSpecBase {

    public static final String FIELD_NAME_CATALOG_TABLE = "table";
    public static final String FIELD_NAME_SINK_ABILITIES = "abilities";

    public static final String FIELD_NAME_TARGET_COLUMNS = "targetColumns";

    private final ContextResolvedTable contextResolvedTable;
    private final @Nullable List<SinkAbilitySpec> sinkAbilities;

    private final @Nullable int[][] targetColumns;

    private DynamicTableSink tableSink;

    @JsonCreator
    public DynamicTableSinkSpec(
            @JsonProperty(FIELD_NAME_CATALOG_TABLE) ContextResolvedTable contextResolvedTable,
            @Nullable @JsonProperty(FIELD_NAME_SINK_ABILITIES) List<SinkAbilitySpec> sinkAbilities,
            @Nullable @JsonProperty(FIELD_NAME_TARGET_COLUMNS) int[][] targetColumns) {
        this.contextResolvedTable = contextResolvedTable;
        this.sinkAbilities = sinkAbilities;
        this.targetColumns = targetColumns;
    }

    @JsonGetter(FIELD_NAME_CATALOG_TABLE)
    public ContextResolvedTable getContextResolvedTable() {
        return contextResolvedTable;
    }

    @JsonGetter(FIELD_NAME_SINK_ABILITIES)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Nullable
    public List<SinkAbilitySpec> getSinkAbilities() {
        return sinkAbilities;
    }

    public DynamicTableSink getTableSink(FlinkContext context) {
        if (tableSink == null) {
            final DynamicTableSinkFactory factory =
                    context.getModuleManager().getFactory(Module::getTableSinkFactory).orElse(null);

            tableSink =
                    FactoryUtil.createDynamicTableSink(
                            factory,
                            contextResolvedTable.getIdentifier(),
                            contextResolvedTable.getResolvedTable(),
                            loadOptionsFromCatalogTable(contextResolvedTable, context),
                            context.getTableConfig(),
                            context.getClassLoader(),
                            contextResolvedTable.isTemporary());
            if (sinkAbilities != null) {
                sinkAbilities.forEach(spec -> spec.apply(tableSink));
            }
        }
        return tableSink;
    }

    @JsonGetter(FIELD_NAME_TARGET_COLUMNS)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Nullable
    public int[][] getTargetColumns() {
        return targetColumns;
    }

    public void setTableSink(DynamicTableSink tableSink) {
        this.tableSink = tableSink;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DynamicTableSinkSpec that = (DynamicTableSinkSpec) o;
        return Objects.equals(contextResolvedTable, that.contextResolvedTable)
                && Objects.equals(sinkAbilities, that.sinkAbilities)
                && Objects.equals(tableSink, that.tableSink)
                && Objects.equals(targetColumns, that.targetColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(contextResolvedTable, sinkAbilities, targetColumns, tableSink);
    }

    @Override
    public String toString() {
        return "DynamicTableSinkSpec{"
                + "contextResolvedTable="
                + contextResolvedTable
                + ", sinkAbilities="
                + sinkAbilities
                + ", targetColumns="
                + targetColumns
                + ", tableSink="
                + tableSink
                + '}';
    }
}
