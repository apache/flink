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

package org.apache.flink.table.planner.operations.converters;

import org.apache.flink.sql.parser.ddl.materializedtable.SqlAlterMaterializedTableSchema;
import org.apache.flink.sql.parser.ddl.materializedtable.SqlAlterMaterializedTableSchema.SqlAlterMaterializedTableAddSchema;
import org.apache.flink.sql.parser.ddl.materializedtable.SqlAlterMaterializedTableSchema.SqlAlterMaterializedTableModifySchema;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedComputedColumn;
import org.apache.flink.table.api.Schema.UnresolvedMetadataColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.Column.ComputedColumn;
import org.apache.flink.table.catalog.Column.MetadataColumn;
import org.apache.flink.table.catalog.Column.PhysicalColumn;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableChangeOperation;
import org.apache.flink.table.planner.utils.MaterializedTableUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Abstract class for converting {@link SqlAlterMaterializedTableSchema} and its children for alter
 * schema materialized table operations.
 */
public abstract class SqlAlterMaterializedTableSchemaConverter<
                T extends SqlAlterMaterializedTableSchema>
        extends AbstractAlterMaterializedTableConverter<T> {
    @Override
    protected Operation convertToOperation(
            T alterTableSchema, ResolvedCatalogMaterializedTable oldTable, ConvertContext context) {
        MaterializedTableUtils.validatePersistedColumnsUsedByQuery(
                oldTable, alterTableSchema, context);

        SchemaConverter converter = createSchemaConverter(oldTable, context);
        converter.updateColumn(alterTableSchema.getColumnPositions().getList());
        alterTableSchema.getWatermark().ifPresent(converter::updateWatermark);
        alterTableSchema.getFullConstraint().ifPresent(converter::updatePrimaryKey);
        Schema schema = converter.convert();

        validateChanges(oldTable.getResolvedSchema(), schema, context);

        CatalogMaterializedTable mtWithUpdatedSchemaAndQuery =
                buildUpdatedMaterializedTable(oldTable, builder -> builder.schema(schema));

        return new AlterMaterializedTableChangeOperation(
                resolveIdentifier(alterTableSchema, context),
                converter.changesCollector,
                mtWithUpdatedSchemaAndQuery);
    }

    protected abstract SchemaConverter createSchemaConverter(
            ResolvedCatalogMaterializedTable oldTable, ConvertContext context);

    protected abstract void validateChanges(
            ResolvedSchema oldSchema, Schema newSchema, ConvertContext context);

    /** A converter for {@link SqlAlterMaterializedTableAddSchema}. */
    public static class SqlAlterMaterializedTableAddSchemaConverter
            extends SqlAlterMaterializedTableSchemaConverter<SqlAlterMaterializedTableAddSchema> {
        @Override
        protected SchemaConverter createSchemaConverter(
                ResolvedCatalogMaterializedTable oldTable, ConvertContext context) {
            return new SchemaAddConverter(oldTable, context);
        }

        @Override
        protected void validateChanges(
                ResolvedSchema oldSchema, Schema newSchema, ConvertContext context) {}
    }

    /** A converter for {@link SqlAlterMaterializedTableModifySchema}. */
    public static class SqlAlterMaterializedTableModifySchemaConverter
            extends SqlAlterMaterializedTableSchemaConverter<
                    SqlAlterMaterializedTableModifySchema> {
        @Override
        protected SchemaConverter createSchemaConverter(
                ResolvedCatalogMaterializedTable oldMaterializedTable, ConvertContext context) {
            return new SchemaModifyConverter(oldMaterializedTable, context);
        }

        @Override
        protected void validateChanges(
                ResolvedSchema oldSchema, Schema newSchema, ConvertContext context) {
            Map<String, Column> map = new HashMap<>();
            for (int i = 0; i < oldSchema.getColumnCount(); i++) {
                final Column column = oldSchema.getColumn(i).get();
                map.put(column.getName(), column);
            }

            List<UnresolvedColumn> columns = newSchema.getColumns();
            for (int i = 0; i < columns.size(); i++) {
                UnresolvedColumn col = columns.get(i);
                final String name = col.getName();
                if (map.containsKey(name) && !columnTypeKept(col, map.get(name))) {
                    throw new ValidationException(
                            String.format(
                                    "Failed to execute ALTER MATERIALIZED TABLE statement.\n"
                                            + "Changing of %s column '%s' to %s column is not supported.",
                                    getColumnKind(map.get(name).getClass()),
                                    name,
                                    getColumnKind(col.getClass())));
                }
                if (col instanceof UnresolvedComputedColumn) {
                    continue;
                }

                LogicalType dataType =
                        createDataType(context.getCatalogManager().getDataTypeFactory(), col);
                LogicalType oldDataType = map.get(col.getName()).getDataType().getLogicalType();
                // The check is similar to the one in
                // SchemaBuilderUtil#validateImplicitCastCompatibility
                // which is used while merging schemas (CREATE [MATERIALIZED ]TABLE operation)
                if (!LogicalTypeCasts.supportsImplicitCast(oldDataType, dataType)) {
                    throw new ValidationException(
                            String.format(
                                    "Failed to execute ALTER MATERIALIZED TABLE statement.\n"
                                            + "Column '%s' with type %s can not be changed to type %s.",
                                    col.getName(), oldDataType, dataType));
                }
            }
        }

        private LogicalType createDataType(DataTypeFactory dataTypeFactory, UnresolvedColumn col) {
            if (col instanceof UnresolvedMetadataColumn) {
                return dataTypeFactory
                        .createDataType(((UnresolvedMetadataColumn) col).getDataType())
                        .getLogicalType();
            } else if (col instanceof UnresolvedPhysicalColumn) {
                return dataTypeFactory
                        .createDataType(((UnresolvedPhysicalColumn) col).getDataType())
                        .getLogicalType();
            } else {
                throw new ValidationException("Unexpected column type " + col.getClass());
            }
        }

        private boolean columnTypeKept(UnresolvedColumn col, Column oldColumn) {
            return col instanceof UnresolvedPhysicalColumn && oldColumn instanceof PhysicalColumn
                    || col instanceof UnresolvedComputedColumn
                            && oldColumn instanceof ComputedColumn
                    || col instanceof UnresolvedMetadataColumn
                            && oldColumn instanceof MetadataColumn;
        }

        private String getColumnKind(Class<?> clazz) {
            return clazz.getSimpleName()
                    .toLowerCase(Locale.ROOT)
                    .replace("column", "")
                    .replace("unresolved", "");
        }
    }
}
