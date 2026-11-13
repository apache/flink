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

package org.apache.flink.table.api.internal.materializedtable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.MaterializedTableDescriptor;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedMetadataColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.api.Schema.UnresolvedWatermarkSpec;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogMaterializedTable.LogicalRefreshMode;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.StartMode;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.materializedtable.MaterializedTableMergeContext;
import org.apache.flink.table.operations.utils.AsQuerySchemaMerger;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.operations.materializedtable.MaterializedTableSchemaUtils.METADATA_PERSISTED_COLUMN_KIND;
import static org.apache.flink.table.operations.materializedtable.MaterializedTableSchemaUtils.PHYSICAL_COLUMN_KIND;
import static org.apache.flink.table.operations.materializedtable.MaterializedTableSchemaUtils.checkPersistedColumnProducedByQuery;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/**
 * The {@link MaterializedTableMergeContext} of {@code Table#materializeInto}, with the semantics of
 * the equivalent {@code CREATE [OR ALTER] MATERIALIZED TABLE} statement.
 */
@Internal
final class DescriptorMaterializedTableMergeContext implements MaterializedTableMergeContext {

    private static final String OPERATION_NAME = "CREATE OR ALTER MATERIALIZED TABLE";

    private final MaterializedTableDescriptor descriptor;
    private final QueryOperation queryOperation;
    private final String serializedQuery;
    private final DataTypeFactory dataTypeFactory;
    private final ResolvedSchema querySchema;

    DescriptorMaterializedTableMergeContext(
            MaterializedTableDescriptor descriptor,
            QueryOperation queryOperation,
            String serializedQuery,
            CatalogManager catalogManager) {
        this.descriptor = descriptor;
        this.queryOperation = queryOperation;
        this.serializedQuery = serializedQuery;
        this.dataTypeFactory = catalogManager.getDataTypeFactory();
        this.querySchema = toPhysicalSchema(queryOperation.getResolvedSchema());
    }

    @Override
    public boolean hasSchemaDefinition() {
        // A column list defines the schema only if it starts with a physical column.
        return descriptor
                .getSchema()
                .map(
                        schema ->
                                !schema.getColumns().isEmpty()
                                        && schema.getColumns().get(0)
                                                instanceof UnresolvedPhysicalColumn)
                .orElse(false);
    }

    @Override
    public boolean hasConstraintDefinition() {
        return descriptor.getSchema().flatMap(Schema::getPrimaryKey).isPresent()
                || hasSchemaDefinition();
    }

    @Override
    public Schema getMergedSchema() {
        final Schema querySchemaDeclaration =
                Schema.newBuilder().fromResolvedSchema(querySchema).build();
        final Schema declaredSchema = descriptor.getSchema().orElse(null);
        if (declaredSchema == null) {
            return querySchemaDeclaration;
        }
        rejectUnsupportedSchemaParts(declaredSchema);
        checkPersistedColumnsProducedByQuery(declaredSchema);

        final AsQuerySchemaMerger merger =
                new AsQuerySchemaMerger(querySchemaDeclaration.getColumns(), dataTypeFactory);
        final List<UnresolvedColumn> declaredColumns = declaredSchema.getColumns();
        // Schema.Builder accepts repeated names, and the merger only rejects a repeat of a column
        // the query doesn't produce.
        final Set<String> declaredNames = new HashSet<>();
        for (int position = 0; position < declaredColumns.size(); position++) {
            final UnresolvedColumn column = declaredColumns.get(position);
            if (!declaredNames.add(column.getName())) {
                throw new ValidationException(
                        String.format(
                                "A column named '%s' already exists in the schema.",
                                column.getName()));
            }
            merger.addDeclaredColumn(withDefaultConversionClass(column), position);
        }

        final Schema.Builder mergedSchema =
                Schema.newBuilder().fromColumns(merger.getMergedColumns());
        for (UnresolvedWatermarkSpec watermark : declaredSchema.getWatermarkSpecs()) {
            mergedSchema.watermark(watermark.getColumnName(), watermark.getWatermarkExpression());
        }
        declaredSchema
                .getPrimaryKey()
                .ifPresent(
                        primaryKey ->
                                mergedSchema.primaryKeyNamed(
                                        primaryKey.getConstraintName(),
                                        primaryKey.getColumnNames()));
        return mergedSchema.build();
    }

    @Override
    public ResolvedSchema getQuerySchema() {
        return querySchema;
    }

    @Override
    public Map<String, String> getOptions() {
        return descriptor.getOptions();
    }

    @Override
    public List<String> getPartitionKeys() {
        return descriptor.getPartitionKeys();
    }

    @Override
    public Optional<TableDistribution> getDistribution() {
        return descriptor.getDistribution();
    }

    @Override
    public @Nullable String getComment() {
        return descriptor.getComment().orElse(null);
    }

    @Override
    public @Nullable IntervalFreshness getFreshness() {
        return descriptor.getFreshness().orElse(null);
    }

    @Override
    public LogicalRefreshMode getLogicalRefreshMode() {
        return LogicalRefreshMode.valueOf(descriptor.getRefreshStrategy().getKind().name());
    }

    @Override
    public @Nullable StartMode getStartMode() {
        return null;
    }

    @Override
    public String getOriginalQuery() {
        return serializedQuery;
    }

    @Override
    public String getExpandedQuery() {
        return serializedQuery;
    }

    @Override
    public QueryOperation getQueryOperation() {
        return queryOperation;
    }

    @Override
    public QueryOperation alignQuery(ResolvedCatalogMaterializedTable table) {
        // The merge keeps every persisted column at its query position and only adds
        // non-persisted columns, so the query already lines up with the sink.
        final List<String> persistedColumns =
                table.getResolvedSchema().getColumns().stream()
                        .filter(Column::isPersisted)
                        .map(Column::getName)
                        .collect(Collectors.toList());
        if (!persistedColumns.equals(querySchema.getColumnNames())) {
            throw new TableException(
                    String.format(
                            "The persisted columns %s of the materialized table do not match the query columns %s.",
                            persistedColumns, querySchema.getColumnNames()));
        }
        return queryOperation;
    }

    /** Produces physical columns with default conversion classes. */
    private static ResolvedSchema toPhysicalSchema(ResolvedSchema schema) {
        final List<DataType> dataTypes =
                schema.getColumnDataTypes().stream()
                        .map(dataType -> fromLogicalToDataType(dataType.getLogicalType()))
                        .collect(Collectors.toList());
        return ResolvedSchema.physical(schema.getColumnNames(), dataTypes);
    }

    private UnresolvedColumn withDefaultConversionClass(UnresolvedColumn column) {
        final String comment = column.getComment().orElse(null);
        if (column instanceof UnresolvedPhysicalColumn) {
            return new UnresolvedPhysicalColumn(
                    column.getName(),
                    withDefaultConversionClass(((UnresolvedPhysicalColumn) column).getDataType()),
                    comment);
        }
        if (column instanceof UnresolvedMetadataColumn) {
            final UnresolvedMetadataColumn metadataColumn = (UnresolvedMetadataColumn) column;
            return new UnresolvedMetadataColumn(
                    column.getName(),
                    withDefaultConversionClass(metadataColumn.getDataType()),
                    metadataColumn.getMetadataKey(),
                    metadataColumn.isVirtual(),
                    comment);
        }
        return column;
    }

    private DataType withDefaultConversionClass(AbstractDataType<?> dataType) {
        return fromLogicalToDataType(dataTypeFactory.createDataType(dataType).getLogicalType());
    }

    private static void rejectUnsupportedSchemaParts(Schema declaredSchema) {
        if (!declaredSchema.getIndexes().isEmpty()) {
            throw new ValidationException("Indexes are not supported for materialized tables.");
        }
        if (declaredSchema.getImmutableColumns().isPresent()) {
            throw new ValidationException(
                    "Immutable columns are not supported for materialized tables.");
        }
    }

    private void checkPersistedColumnsProducedByQuery(Schema declaredSchema) {
        final Set<String> queryColumns = new HashSet<>(querySchema.getColumnNames());
        for (UnresolvedColumn column : declaredSchema.getColumns()) {
            if (column instanceof UnresolvedPhysicalColumn) {
                checkPersistedColumnProducedByQuery(
                        column.getName(), PHYSICAL_COLUMN_KIND, queryColumns, OPERATION_NAME);
            } else if (column instanceof UnresolvedMetadataColumn
                    && !((UnresolvedMetadataColumn) column).isVirtual()) {
                checkPersistedColumnProducedByQuery(
                        column.getName(),
                        METADATA_PERSISTED_COLUMN_KIND,
                        queryColumns,
                        OPERATION_NAME);
            }
        }
    }
}
