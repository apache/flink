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
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilityContext;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilitySpec;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * {@link DynamicTableSourceSpec} describes how to serialize/deserialize dynamic table source table
 * and create {@link DynamicTableSource} from the deserialization result.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DynamicTableSourceSpec extends DynamicTableSpecBase {

    public static final String FIELD_NAME_CATALOG_TABLE = "table";
    public static final String FIELD_NAME_SOURCE_ABILITIES = "abilities";

    private final ContextResolvedTable contextResolvedTable;
    private final @Nullable List<SourceAbilitySpec> sourceAbilities;

    private DynamicTableSource tableSource;

    @JsonCreator
    public DynamicTableSourceSpec(
            @JsonProperty(FIELD_NAME_CATALOG_TABLE) ContextResolvedTable contextResolvedTable,
            @Nullable @JsonProperty(FIELD_NAME_SOURCE_ABILITIES)
                    List<SourceAbilitySpec> sourceAbilities) {
        this.contextResolvedTable = contextResolvedTable;
        this.sourceAbilities = sourceAbilities;
    }

    private DynamicTableSource getTableSource(FlinkContext context, FlinkTypeFactory typeFactory) {
        if (tableSource == null) {
            DynamicTableSourceFactory factory =
                    context.getModuleManager()
                            .getFactory(Module::getTableSourceFactory)
                            .orElse(null);

            if (factory == null) {
                // try to get DynamicTableSourceFactory from Catalog,
                // we need it for we can only get DynamicTableSourceFactory from catalog in Hive
                // table
                Catalog catalog =
                        context.getCatalogManager()
                                .getCatalog(contextResolvedTable.getIdentifier().getCatalogName())
                                .orElse(null);
                factory =
                        FactoryUtil.getDynamicTableFactory(DynamicTableSourceFactory.class, catalog)
                                .orElse(null);
            }

            tableSource =
                    FactoryUtil.createDynamicTableSource(
                            factory,
                            contextResolvedTable.getIdentifier(),
                            contextResolvedTable.getResolvedTable(),
                            loadOptionsFromCatalogTable(contextResolvedTable, context),
                            context.getTableConfig(),
                            context.getClassLoader(),
                            contextResolvedTable.isTemporary());

            if (sourceAbilities != null) {
                RowType newProducedType =
                        (RowType)
                                contextResolvedTable
                                        .getResolvedSchema()
                                        .toPhysicalRowDataType()
                                        .getLogicalType();
                for (SourceAbilitySpec spec : sourceAbilities) {
                    SourceAbilityContext sourceAbilityContext =
                            new SourceAbilityContext(context, typeFactory, newProducedType);
                    spec.apply(tableSource, sourceAbilityContext);
                    if (spec.getProducedType().isPresent()) {
                        newProducedType = spec.getProducedType().get();
                    }
                }
            }
        }
        return tableSource;
    }

    public ScanTableSource getScanTableSource(FlinkContext context, FlinkTypeFactory typeFactory) {
        DynamicTableSource tableSource = getTableSource(context, typeFactory);
        if (tableSource instanceof ScanTableSource) {
            return (ScanTableSource) tableSource;
        } else {
            throw new TableException(
                    String.format(
                            "%s is not a ScanTableSource.\nPlease check it.",
                            tableSource.getClass().getName()));
        }
    }

    public LookupTableSource getLookupTableSource(
            FlinkContext context, FlinkTypeFactory typeFactory) {
        DynamicTableSource tableSource = getTableSource(context, typeFactory);
        if (tableSource instanceof LookupTableSource) {
            return (LookupTableSource) tableSource;
        } else {
            throw new TableException(
                    String.format(
                            "%s is not a LookupTableSource.\nPlease check it.",
                            tableSource.getClass().getName()));
        }
    }

    @JsonGetter(FIELD_NAME_CATALOG_TABLE)
    public ContextResolvedTable getContextResolvedTable() {
        return contextResolvedTable;
    }

    @JsonGetter(FIELD_NAME_SOURCE_ABILITIES)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Nullable
    public List<SourceAbilitySpec> getSourceAbilities() {
        return sourceAbilities;
    }

    public void setTableSource(DynamicTableSource tableSource) {
        this.tableSource = tableSource;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DynamicTableSourceSpec that = (DynamicTableSourceSpec) o;
        return Objects.equals(contextResolvedTable, that.contextResolvedTable)
                && Objects.equals(sourceAbilities, that.sourceAbilities)
                && Objects.equals(tableSource, that.tableSource);
    }

    @Override
    public int hashCode() {
        return Objects.hash(contextResolvedTable, sourceAbilities, tableSource);
    }

    @Override
    public String toString() {
        return "DynamicTableSourceSpec{"
                + "contextResolvedTable="
                + contextResolvedTable
                + ", sourceAbilities="
                + sourceAbilities
                + ", tableSource="
                + tableSource
                + '}';
    }
}
