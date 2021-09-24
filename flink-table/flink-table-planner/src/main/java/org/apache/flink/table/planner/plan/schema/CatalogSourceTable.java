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

package org.apache.flink.table.planner.plan.schema;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.catalog.CatalogSchemaTable;
import org.apache.flink.table.planner.connectors.DynamicSourceUtils;
import org.apache.flink.table.planner.hint.FlinkHints;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;
import java.util.Map;

/**
 * A {@link FlinkPreparingTableBase} implementation which defines the interfaces required to
 * translate the Calcite {@link RelOptTable} to the Flink specific {@link TableSourceTable}.
 *
 * <p>This table is only used to translate the {@link CatalogTable} into {@link TableSourceTable}
 * during the last phase of the SQL-to-rel conversion, it is not necessary anymore once the SQL node
 * was converted to a relational expression.
 */
public final class CatalogSourceTable extends FlinkPreparingTableBase {

    private final CatalogSchemaTable schemaTable;

    private final ResolvedCatalogTable catalogTable;

    public CatalogSourceTable(
            RelOptSchema relOptSchema,
            List<String> names,
            RelDataType rowType,
            CatalogSchemaTable schemaTable,
            ResolvedCatalogTable catalogTable) {
        super(relOptSchema, rowType, names, schemaTable.getStatistic());
        this.schemaTable = schemaTable;
        this.catalogTable = catalogTable;
    }

    @Override
    public RelNode toRel(ToRelContext toRelContext) {
        final RelOptCluster cluster = toRelContext.getCluster();
        final List<RelHint> hints = toRelContext.getTableHints();
        final FlinkContext context = ShortcutUtils.unwrapContext(cluster);
        final FlinkRelBuilder relBuilder = FlinkRelBuilder.of(cluster, relOptSchema);

        // finalize catalog table
        final Map<String, String> hintedOptions = FlinkHints.getHintedOptions(hints);
        final ResolvedCatalogTable catalogTable = createFinalCatalogTable(context, hintedOptions);

        // create table source
        final DynamicTableSource tableSource = createDynamicTableSource(context, catalogTable);

        // prepare table source and convert to RelNode
        return DynamicSourceUtils.convertSourceToRel(
                !schemaTable.isStreamingMode(),
                context.getTableConfig().getConfiguration(),
                relBuilder,
                schemaTable.getTableIdentifier(),
                catalogTable,
                schemaTable.getStatistic(),
                hints,
                tableSource);
    }

    private ResolvedCatalogTable createFinalCatalogTable(
            FlinkContext context, Map<String, String> hintedOptions) {
        if (hintedOptions.isEmpty()) {
            return catalogTable;
        }
        final ReadableConfig config = context.getTableConfig().getConfiguration();
        if (!config.get(TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED)) {
            throw new ValidationException(
                    String.format(
                            "The '%s' hint is allowed only when the config option '%s' is set to true.",
                            FlinkHints.HINT_NAME_OPTIONS,
                            TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED.key()));
        }
        return catalogTable.copy(
                FlinkHints.mergeTableOptions(hintedOptions, catalogTable.getOptions()));
    }

    private DynamicTableSource createDynamicTableSource(
            FlinkContext context, ResolvedCatalogTable catalogTable) {
        final ReadableConfig config = context.getTableConfig().getConfiguration();
        return FactoryUtil.createTableSource(
                schemaTable.getCatalog().orElse(null),
                schemaTable.getTableIdentifier(),
                catalogTable,
                config,
                Thread.currentThread().getContextClassLoader(),
                schemaTable.isTemporary());
    }

    public CatalogTable getCatalogTable() {
        return catalogTable;
    }
}
