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

package org.apache.flink.table.planner.plan;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.QueryOperationCatalogView;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.TableFactoryUtil;
import org.apache.flink.table.planner.calcite.FlinkSqlNameMatcher;
import org.apache.flink.table.planner.catalog.CatalogSchemaTable;
import org.apache.flink.table.planner.catalog.QueryOperationCatalogViewTable;
import org.apache.flink.table.planner.catalog.SqlCatalogViewTable;
import org.apache.flink.table.planner.plan.schema.CatalogSourceTable;
import org.apache.flink.table.planner.plan.schema.FlinkPreparingTableBase;
import org.apache.flink.table.planner.plan.schema.LegacyCatalogSourceTable;
import org.apache.flink.table.planner.plan.schema.LegacyTableSourceTable;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.utils.TableSchemaUtils;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.validate.SqlNameMatchers;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Flink specific {@link CalciteCatalogReader} that changes the RelOptTable which wrapped a {@link
 * CatalogSchemaTable} to a {@link FlinkPreparingTableBase}.
 */
public class FlinkCalciteCatalogReader extends CalciteCatalogReader {

    public FlinkCalciteCatalogReader(
            CalciteSchema rootSchema,
            List<List<String>> defaultSchemas,
            RelDataTypeFactory typeFactory,
            CalciteConnectionConfig config) {

        super(
                rootSchema,
                new FlinkSqlNameMatcher(
                        SqlNameMatchers.withCaseSensitive(config != null && config.caseSensitive()),
                        typeFactory),
                Stream.concat(defaultSchemas.stream(), Stream.of(Collections.<String>emptyList()))
                        .collect(Collectors.toList()),
                typeFactory,
                config);
    }

    @Override
    public Prepare.PreparingTable getTable(List<String> names) {
        Prepare.PreparingTable originRelOptTable = super.getTable(names);
        if (originRelOptTable == null) {
            return null;
        } else {
            // Wrap as FlinkPreparingTableBase to use in query optimization.
            CatalogSchemaTable table = originRelOptTable.unwrap(CatalogSchemaTable.class);
            if (table != null) {
                return toPreparingTable(
                        originRelOptTable.getRelOptSchema(),
                        originRelOptTable.getQualifiedName(),
                        originRelOptTable.getRowType(),
                        table);
            } else {
                return originRelOptTable;
            }
        }
    }

    /** Translate this {@link CatalogSchemaTable} into Flink source table. */
    private static FlinkPreparingTableBase toPreparingTable(
            RelOptSchema relOptSchema,
            List<String> names,
            RelDataType rowType,
            CatalogSchemaTable schemaTable) {
        final ResolvedCatalogBaseTable<?> resolvedBaseTable =
                schemaTable.getContextResolvedTable().getResolvedTable();
        final CatalogBaseTable originTable = resolvedBaseTable.getOrigin();
        if (originTable instanceof QueryOperationCatalogView) {
            return convertQueryOperationView(
                    relOptSchema, names, rowType, (QueryOperationCatalogView) originTable);
        } else if (originTable instanceof ConnectorCatalogTable) {
            ConnectorCatalogTable<?, ?> connectorTable = (ConnectorCatalogTable<?, ?>) originTable;
            if ((connectorTable).getTableSource().isPresent()) {
                return convertLegacyTableSource(
                        relOptSchema,
                        rowType,
                        schemaTable.getContextResolvedTable().getIdentifier(),
                        connectorTable,
                        schemaTable.getStatistic(),
                        schemaTable.isStreamingMode());
            } else {
                throw new ValidationException(
                        "Cannot convert a connector table " + "without source.");
            }
        } else if (originTable instanceof CatalogView) {
            return convertCatalogView(
                    relOptSchema,
                    names,
                    rowType,
                    schemaTable.getStatistic(),
                    (CatalogView) originTable);
        } else if (originTable instanceof CatalogTable) {
            return convertCatalogTable(relOptSchema, names, rowType, schemaTable);
        } else {
            throw new ValidationException("Unsupported table type: " + originTable);
        }
    }

    private static FlinkPreparingTableBase convertQueryOperationView(
            RelOptSchema relOptSchema,
            List<String> names,
            RelDataType rowType,
            QueryOperationCatalogView view) {
        return QueryOperationCatalogViewTable.create(relOptSchema, names, rowType, view);
    }

    private static FlinkPreparingTableBase convertCatalogView(
            RelOptSchema relOptSchema,
            List<String> names,
            RelDataType rowType,
            FlinkStatistic statistic,
            CatalogView view) {
        return new SqlCatalogViewTable(
                relOptSchema, rowType, names, statistic, view, names.subList(0, 2));
    }

    private static FlinkPreparingTableBase convertLegacyTableSource(
            RelOptSchema relOptSchema,
            RelDataType rowType,
            ObjectIdentifier tableIdentifier,
            ConnectorCatalogTable<?, ?> table,
            FlinkStatistic statistic,
            boolean isStreamingMode) {
        TableSource<?> tableSource = table.getTableSource().get();
        if (!(tableSource instanceof StreamTableSource
                || tableSource instanceof LookupableTableSource)) {
            throw new ValidationException(
                    "Only StreamTableSource and LookupableTableSource can be used in planner.");
        }
        if (!isStreamingMode
                && tableSource instanceof StreamTableSource
                && !((StreamTableSource<?>) tableSource).isBounded()) {
            throw new ValidationException(
                    "Only bounded StreamTableSource can be used in batch mode.");
        }

        return new LegacyTableSourceTable<>(
                relOptSchema,
                tableIdentifier,
                rowType,
                statistic,
                tableSource,
                isStreamingMode,
                table);
    }

    private static FlinkPreparingTableBase convertCatalogTable(
            RelOptSchema relOptSchema,
            List<String> names,
            RelDataType rowType,
            CatalogSchemaTable schemaTable) {
        if (isLegacySourceOptions(schemaTable)) {
            return new LegacyCatalogSourceTable<>(
                    relOptSchema,
                    names,
                    rowType,
                    schemaTable,
                    schemaTable.getContextResolvedTable().getResolvedTable());
        } else {
            return new CatalogSourceTable(relOptSchema, names, rowType, schemaTable);
        }
    }

    /** Checks whether the {@link CatalogTable} uses legacy connector source options. */
    private static boolean isLegacySourceOptions(CatalogSchemaTable schemaTable) {
        // normalize option keys
        DescriptorProperties properties = new DescriptorProperties(true);
        properties.putProperties(
                schemaTable.getContextResolvedTable().getResolvedTable().getOptions());
        if (properties.containsKey(ConnectorDescriptorValidator.CONNECTOR_TYPE)) {
            return true;
        } else {
            // try to create legacy table source using the options,
            // some legacy factories uses the new 'connector' key
            try {
                // The input table is ResolvedCatalogTable that the
                // rowtime/proctime contains {@link TimestampKind}. However, rowtime
                // is the concept defined by the WatermarkGenerator and the
                // WatermarkGenerator is responsible to convert the rowtime column
                // to Long. For source, it only treats the rowtime column as regular
                // timestamp. So, we erase the rowtime indicator here. Please take a
                // look at the usage of the {@link
                // DataTypeUtils#removeTimeAttribute}
                ResolvedCatalogTable originTable =
                        schemaTable.getContextResolvedTable().getResolvedTable();
                ResolvedSchema resolvedSchemaWithRemovedTimeAttribute =
                        TableSchemaUtils.removeTimeAttributeFromResolvedSchema(
                                originTable.getResolvedSchema());
                TableFactoryUtil.findAndCreateTableSource(
                        schemaTable.getContextResolvedTable().getCatalog().orElse(null),
                        schemaTable.getContextResolvedTable().getIdentifier(),
                        new ResolvedCatalogTable(
                                CatalogTable.of(
                                        Schema.newBuilder()
                                                .fromResolvedSchema(
                                                        resolvedSchemaWithRemovedTimeAttribute)
                                                .build(),
                                        originTable.getComment(),
                                        originTable.getPartitionKeys(),
                                        originTable.getOptions()),
                                resolvedSchemaWithRemovedTimeAttribute),
                        new Configuration(),
                        schemaTable.isTemporary());
                // success, then we will use the legacy factories
                return true;
            } catch (Throwable e) {
                // fail, then we will use new factories
                return false;
            }
        }
    }
}
