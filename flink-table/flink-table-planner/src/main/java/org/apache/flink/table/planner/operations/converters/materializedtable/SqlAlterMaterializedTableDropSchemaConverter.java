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

package org.apache.flink.table.planner.operations.converters.materializedtable;

import org.apache.flink.sql.parser.ddl.materializedtable.SqlAlterMaterializedTableSchema.SqlAlterMaterializedTableDropColumn;
import org.apache.flink.sql.parser.ddl.materializedtable.SqlAlterMaterializedTableSchema.SqlAlterMaterializedTableDropConstraint;
import org.apache.flink.sql.parser.ddl.materializedtable.SqlAlterMaterializedTableSchema.SqlAlterMaterializedTableDropPrimaryKey;
import org.apache.flink.sql.parser.ddl.materializedtable.SqlAlterMaterializedTableSchema.SqlAlterMaterializedTableDropSchema;
import org.apache.flink.sql.parser.ddl.materializedtable.SqlAlterMaterializedTableSchema.SqlAlterMaterializedTableDropWatermark;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableChangeOperation;
import org.apache.flink.table.planner.utils.MaterializedTableUtils;
import org.apache.flink.table.planner.utils.OperationConverterUtils;

import org.apache.calcite.sql.SqlIdentifier;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Abstract class for converters to convert {@code ALTER MATERIALIZED TABLE DROP <schema_component>}
 * to generate an updated Schema.
 */
public abstract class SqlAlterMaterializedTableDropSchemaConverter<
                T extends SqlAlterMaterializedTableDropSchema>
        extends AbstractAlterMaterializedTableConverter<T> {
    @Override
    protected Operation convertToOperation(
            T alterTableSchema, ResolvedCatalogMaterializedTable oldTable, ConvertContext context) {
        Set<String> columnsToDrop = getColumnsToDrop(alterTableSchema);
        List<TableChange> tableChanges =
                validateAndGatherDropChanges(alterTableSchema, oldTable, columnsToDrop, context);

        return new AlterMaterializedTableChangeOperation(
                resolveIdentifier(alterTableSchema, context), tableChanges, oldTable);
    }

    protected abstract List<TableChange> validateAndGatherDropChanges(
            T alterTableSchema,
            ResolvedCatalogMaterializedTable oldTable,
            Set<String> columnsToDrop,
            ConvertContext context);

    protected abstract Set<String> getColumnsToDrop(T alterTableSchema);

    /**
     * Convert {@code ALTER TABLE MATERIALIZED TABLE DROP PRIMARY KEY} to generate an updated
     * Schema.
     */
    public static class SqlAlterMaterializedTableDropPrimaryKeyConverter
            extends SqlAlterMaterializedTableDropSchemaConverter<
                    SqlAlterMaterializedTableDropPrimaryKey> {
        @Override
        protected List<TableChange> validateAndGatherDropChanges(
                SqlAlterMaterializedTableDropPrimaryKey alterTableSchema,
                ResolvedCatalogMaterializedTable oldTable,
                Set<String> columnsToDrop,
                ConvertContext context) {
            return OperationConverterUtils.validateAndGatherDropPrimaryKey(
                    oldTable, EX_MSG_PREFIX, "materialized table");
        }

        @Override
        protected Set<String> getColumnsToDrop(
                SqlAlterMaterializedTableDropPrimaryKey alterTableSchema) {
            return Set.of();
        }
    }

    /**
     * Convert {@code ALTER TABLE MATERIALIZED TABLE DROP CONSTRAINT constraint_name} to generate an
     * updated Schema.
     */
    public static class SqlAlterMaterializedTableDropConstraintConverter
            extends SqlAlterMaterializedTableDropSchemaConverter<
                    SqlAlterMaterializedTableDropConstraint> {

        @Override
        protected List<TableChange> validateAndGatherDropChanges(
                SqlAlterMaterializedTableDropConstraint dropConstraint,
                ResolvedCatalogMaterializedTable oldTable,
                Set<String> columnsToDrop,
                ConvertContext context) {
            return OperationConverterUtils.validateAndGatherDropConstraintChanges(
                    oldTable,
                    dropConstraint.getConstraintName(),
                    EX_MSG_PREFIX,
                    "materialized table");
        }

        @Override
        protected Set<String> getColumnsToDrop(
                SqlAlterMaterializedTableDropConstraint alterTableSchema) {
            return Set.of();
        }
    }

    /**
     * Convert {@code ALTER TABLE MATERIALIZED TABLE DROP WATERMARK} to generate an updated Schema.
     */
    public static class SqlAlterMaterializedTableDropWatermarkConverter
            extends SqlAlterMaterializedTableDropSchemaConverter<
                    SqlAlterMaterializedTableDropWatermark> {

        @Override
        protected List<TableChange> validateAndGatherDropChanges(
                SqlAlterMaterializedTableDropWatermark alterTableSchema,
                ResolvedCatalogMaterializedTable oldTable,
                Set<String> columnsToDrop,
                ConvertContext context) {
            return OperationConverterUtils.validateAndGatherDropWatermarkChanges(
                    oldTable, EX_MSG_PREFIX, "materialized table");
        }

        @Override
        protected Set<String> getColumnsToDrop(
                SqlAlterMaterializedTableDropWatermark alterTableSchema) {
            return Set.of();
        }
    }

    /**
     * Convert {@code ALTER TABLE MATERIALIZED TABLE DROP column_name} in case of one column and
     * {@code ALTER TABLE MATERIALIZED TABLE DROP (column_name [, column_name2])} in case of several
     * columns to generate an updated Schema.
     */
    public static class SqlAlterMaterializedTableSchemaDropColumnConverter
            extends SqlAlterMaterializedTableDropSchemaConverter<
                    SqlAlterMaterializedTableDropColumn> {

        @Override
        protected List<TableChange> validateAndGatherDropChanges(
                SqlAlterMaterializedTableDropColumn alterTableSchema,
                ResolvedCatalogMaterializedTable oldTable,
                Set<String> columnsToDrop,
                ConvertContext context) {
            List<TableChange> tableChanges =
                    OperationConverterUtils.validateAndGatherDropColumn(
                            oldTable, columnsToDrop, EX_MSG_PREFIX);
            validateColumnsUsedInQuery(oldTable, alterTableSchema, context);
            for (Column column : oldTable.getResolvedSchema().getColumns()) {
                if (column.isPersisted() && columnsToDrop.contains(column.getName())) {
                    throw new ValidationException(
                            String.format(
                                    "%sThe column `%s` is a persisted column. Dropping of persisted columns is not supported.",
                                    EX_MSG_PREFIX, column.getName()));
                }
            }
            return tableChanges;
        }

        @Override
        protected Set<String> getColumnsToDrop(
                SqlAlterMaterializedTableDropColumn alterTableSchema) {
            return OperationConverterUtils.getColumnNames(
                    alterTableSchema.getColumnList(), EX_MSG_PREFIX);
        }

        private static void validateColumnsUsedInQuery(
                ResolvedCatalogMaterializedTable oldTable,
                SqlAlterMaterializedTableDropColumn alterTableSchema,
                ConvertContext context) {
            final ResolvedSchema querySchema =
                    MaterializedTableUtils.getQueryOperationResolvedSchema(oldTable, context);

            Set<String> querySchemaColumnNames = new HashSet<>(querySchema.getColumnNames());
            querySchemaColumnNames.retainAll(
                    alterTableSchema.getColumnList().stream()
                            .map(c -> ((SqlIdentifier) c).getSimple())
                            .collect(Collectors.toList()));
            if (!querySchemaColumnNames.isEmpty()) {
                throw new ValidationException(
                        String.format(
                                "%sColumn(s) (%s) are used in query.",
                                EX_MSG_PREFIX,
                                querySchemaColumnNames.stream()
                                        .map(c -> "'" + c + "'")
                                        .collect(Collectors.joining(", "))));
            }
        }
    }
}
