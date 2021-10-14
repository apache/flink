/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.catalog;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogManager.TableLookupResult;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.factories.TableFactoryUtil;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.factories.TableSourceFactoryContextImpl;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.planner.sources.TableSourceUtil;
import org.apache.flink.table.runtime.types.PlannerTypeUtils;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.TableSourceValidation;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TemporalTable;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Represents a wrapper for {@link CatalogBaseTable} in {@link org.apache.calcite.schema.Schema}.
 *
 * <p>This table would be converted to {@link
 * org.apache.flink.table.planner.plan.schema.FlinkPreparingTableBase} based on its internal source
 * type during sql-to-rel conversion.
 *
 * <p>See {@link org.apache.flink.table.planner.plan.FlinkCalciteCatalogReader#getTable(List)} for
 * details.
 */
public class CatalogSchemaTable extends AbstractTable implements TemporalTable {
    // ~ Instance fields --------------------------------------------------------

    private final ObjectIdentifier tableIdentifier;
    private final TableLookupResult lookupResult;
    private final FlinkStatistic statistic;
    private final boolean isStreamingMode;

    // ~ Constructors -----------------------------------------------------------

    /**
     * Create a CatalogSchemaTable instance.
     *
     * @param tableIdentifier Table identifier
     * @param lookupResult A result of catalog lookup
     * @param statistic Table statistics
     * @param isStreaming If the table is for streaming mode
     */
    public CatalogSchemaTable(
            ObjectIdentifier tableIdentifier,
            TableLookupResult lookupResult,
            FlinkStatistic statistic,
            boolean isStreaming) {
        this.tableIdentifier = tableIdentifier;
        this.lookupResult = lookupResult;
        this.statistic = statistic;
        this.isStreamingMode = isStreaming;
    }

    // ~ Methods ----------------------------------------------------------------

    public Optional<Catalog> getCatalog() {
        return lookupResult.getCatalog();
    }

    public ObjectIdentifier getTableIdentifier() {
        return tableIdentifier;
    }

    public ResolvedCatalogBaseTable<?> getResolvedCatalogTable() {
        return lookupResult.getResolvedTable();
    }

    public boolean isTemporary() {
        return lookupResult.isTemporary();
    }

    public boolean isStreamingMode() {
        return isStreamingMode;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        final FlinkTypeFactory flinkTypeFactory = (FlinkTypeFactory) typeFactory;
        final ResolvedSchema schema = lookupResult.getResolvedSchema();

        // The following block is a workaround to support tables defined by
        // TableEnvironment.connect() and
        // the actual table sources implement DefinedProctimeAttribute/DefinedRowtimeAttributes.
        // It should be removed after we remove DefinedProctimeAttribute/DefinedRowtimeAttributes.
        Optional<TableSource<?>> sourceOpt = findAndCreateTableSource();
        if (isStreamingMode
                && schema.getColumns().stream().allMatch(Column::isPhysical)
                && schema.getWatermarkSpecs().isEmpty()
                && sourceOpt.isPresent()) {
            TableSchema tableSchema = TableSchema.fromResolvedSchema(schema);
            TableSource<?> source = sourceOpt.get();
            if (TableSourceValidation.hasProctimeAttribute(source)
                    || TableSourceValidation.hasRowtimeAttribute(source)) {
                // If the table is defined by TableEnvironment.connect(), and use the legacy
                // proctime and rowtime
                // descriptors, the TableSchema should fallback to
                // ConnectorCatalogTable#calculateSourceSchema
                tableSchema = ConnectorCatalogTable.calculateSourceSchema(source, false);
            }
            return TableSourceUtil.getSourceRowType(
                    flinkTypeFactory, tableSchema, scala.Option.empty(), true);
        }

        final List<String> fieldNames = schema.getColumnNames();
        final List<LogicalType> fieldTypes =
                schema.getColumnDataTypes().stream()
                        .map(DataType::getLogicalType)
                        .map(PlannerTypeUtils::removeLegacyTypes)
                        .collect(Collectors.toList());
        return flinkTypeFactory.buildRelNodeRowType(fieldNames, fieldTypes);
    }

    @Override
    public FlinkStatistic getStatistic() {
        return statistic;
    }

    @Override
    public String getSysStartFieldName() {
        return "sys_start";
    }

    @Override
    public String getSysEndFieldName() {
        return "sys_end";
    }

    private Optional<TableSource<?>> findAndCreateTableSource() {
        Optional<TableSource<?>> tableSource = Optional.empty();
        try {
            if (lookupResult.getTable() instanceof CatalogTable) {
                // Use an empty config for TableSourceFactoryContextImpl since we can't fetch the
                // actual TableConfig here. And currently the empty config do not affect the logic.
                ReadableConfig config = new Configuration();
                TableSourceFactory.Context context =
                        new TableSourceFactoryContextImpl(
                                tableIdentifier,
                                (CatalogTable) lookupResult.getTable(),
                                config,
                                lookupResult.isTemporary());
                TableSource<?> source = TableFactoryUtil.findAndCreateTableSource(context);
                if (source instanceof StreamTableSource) {
                    if (!isStreamingMode && !((StreamTableSource<?>) source).isBounded()) {
                        throw new ValidationException(
                                "Cannot query on an unbounded source in batch mode, but "
                                        + tableIdentifier.asSummaryString()
                                        + " is unbounded.");
                    }
                    tableSource = Optional.of(source);
                } else {
                    throw new ValidationException(
                            "Catalog tables only support "
                                    + "StreamTableSource and InputFormatTableSource.");
                }
            }
        } catch (Exception e) {
            tableSource = Optional.empty();
        }
        return tableSource;
    }
}
