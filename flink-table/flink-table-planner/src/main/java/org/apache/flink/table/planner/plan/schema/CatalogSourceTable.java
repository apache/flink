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

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable.TableKind;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.catalog.CatalogSchemaTable;
import org.apache.flink.table.planner.connectors.DynamicSourceUtils;
import org.apache.flink.table.planner.hint.FlinkHints;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.util.OptionalUtils.firstPresent;

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

    public CatalogSourceTable(
            RelOptSchema relOptSchema,
            List<String> names,
            RelDataType rowType,
            CatalogSchemaTable schemaTable) {
        super(relOptSchema, rowType, names, schemaTable.getStatistic());
        this.schemaTable = schemaTable;
    }

    /**
     * Create a {@link CatalogSourceTable} from an anonymous {@link ContextResolvedTable}. This is
     * required to manually create a preparing table skipping the calcite catalog resolution.
     */
    public static CatalogSourceTable createAnonymous(
            FlinkRelBuilder relBuilder,
            ContextResolvedTable contextResolvedTable,
            boolean isBatchMode) {
        Preconditions.checkArgument(
                contextResolvedTable.isAnonymous(), "ContextResolvedTable must be anonymous");

        // Statistics are unknown for anonymous tables
        // Look at DatabaseCalciteSchema#getStatistic for more details
        FlinkStatistic flinkStatistic =
                FlinkStatistic.unknown(contextResolvedTable.getResolvedSchema()).build();

        CatalogSchemaTable catalogSchemaTable =
                new CatalogSchemaTable(contextResolvedTable, flinkStatistic, !isBatchMode);

        return new CatalogSourceTable(
                relBuilder.getRelOptSchema(),
                contextResolvedTable.getIdentifier().toList(),
                catalogSchemaTable.getRowType(relBuilder.getTypeFactory()),
                catalogSchemaTable);
    }

    @Override
    public RelNode toRel(ToRelContext toRelContext) {
        final RelOptCluster cluster = toRelContext.getCluster();
        final List<RelHint> hints = toRelContext.getTableHints();
        final FlinkContext context = ShortcutUtils.unwrapContext(cluster);
        final FlinkRelBuilder relBuilder = FlinkRelBuilder.of(cluster, relOptSchema);

        // finalize catalog table with option hints
        final Map<String, String> hintedOptions = FlinkHints.getHintedOptions(hints);
        final ContextResolvedTable contextTableWithHints =
                computeContextResolvedTable(context, hintedOptions);

        // create table source
        final DynamicTableSource tableSource =
                createDynamicTableSource(context, contextTableWithHints.getResolvedTable());

        // prepare table source and convert to RelNode
        return DynamicSourceUtils.convertSourceToRel(
                !schemaTable.isStreamingMode(),
                context.getTableConfig(),
                relBuilder,
                contextTableWithHints,
                schemaTable.getStatistic(),
                hints,
                tableSource);
    }

    private ContextResolvedTable computeContextResolvedTable(
            FlinkContext context, Map<String, String> hintedOptions) {
        ContextResolvedTable contextResolvedTable = schemaTable.getContextResolvedTable();
        if (hintedOptions.isEmpty()) {
            return contextResolvedTable;
        }
        if (!context.getTableConfig().get(TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED)) {
            throw new ValidationException(
                    String.format(
                            "The '%s' hint is allowed only when the config option '%s' is set to true.",
                            FlinkHints.HINT_NAME_OPTIONS,
                            TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED.key()));
        }
        if (contextResolvedTable.getResolvedTable().getTableKind() == TableKind.VIEW) {
            throw new ValidationException(
                    String.format(
                            "View '%s' cannot be enriched with new options. "
                                    + "Hints can only be applied to tables.",
                            contextResolvedTable.getIdentifier()));
        }
        return contextResolvedTable.copy(
                FlinkHints.mergeTableOptions(
                        hintedOptions, contextResolvedTable.getResolvedTable().getOptions()));
    }

    private DynamicTableSource createDynamicTableSource(
            FlinkContext context, ResolvedCatalogTable catalogTable) {

        final Optional<DynamicTableSourceFactory> factoryFromCatalog =
                schemaTable
                        .getContextResolvedTable()
                        .getCatalog()
                        .flatMap(Catalog::getFactory)
                        .map(
                                f ->
                                        f instanceof DynamicTableSourceFactory
                                                ? (DynamicTableSourceFactory) f
                                                : null);

        final Optional<DynamicTableSourceFactory> factoryFromModule =
                context.getModuleManager().getFactory(Module::getTableSourceFactory);

        // Since the catalog is more specific, we give it precedence over a factory provided by any
        // modules.
        final DynamicTableSourceFactory factory =
                firstPresent(factoryFromCatalog, factoryFromModule).orElse(null);

        return FactoryUtil.createDynamicTableSource(
                factory,
                schemaTable.getContextResolvedTable().getIdentifier(),
                catalogTable,
                Collections.emptyMap(),
                context.getTableConfig(),
                context.getClassLoader(),
                schemaTable.isTemporary());
    }

    public CatalogTable getCatalogTable() {
        return schemaTable.getContextResolvedTable().getResolvedTable();
    }
}
