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

package org.apache.flink.table.planner.operations.converters.table;

import org.apache.flink.sql.parser.ddl.table.SqlAlterTableAdd;
import org.apache.flink.sql.parser.ddl.table.SqlAlterTableDrop.SqlAlterTableDropColumn;
import org.apache.flink.sql.parser.ddl.table.SqlAlterTableDrop.SqlAlterTableDropConstraint;
import org.apache.flink.sql.parser.ddl.table.SqlAlterTableDrop.SqlAlterTableDropPrimaryKey;
import org.apache.flink.sql.parser.ddl.table.SqlAlterTableDrop.SqlAlterTableDropWatermark;
import org.apache.flink.sql.parser.ddl.table.SqlAlterTableModify;
import org.apache.flink.sql.parser.ddl.table.SqlAlterTableSchema;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.operations.converters.SchemaAddConverter;
import org.apache.flink.table.planner.operations.converters.SchemaConverter;
import org.apache.flink.table.planner.operations.converters.SchemaModifyConverter;
import org.apache.flink.table.planner.operations.converters.SchemaReferencesManager;
import org.apache.flink.table.planner.utils.OperationConverterUtils;

import java.util.List;
import java.util.Set;

/**
 * Abstract class for converters to convert {@code ALTER TABLE ADD | MODIFY
 * (&lt;schema_component&gt; [, &lt;schema_component&gt;, ...])} to generate an updated Schema.
 */
public abstract class SqlAlterTableSchemaConverter<T extends SqlAlterTableSchema>
        extends AbstractAlterTableConverter<T> {
    @Override
    protected Operation convertToOperation(
            T alterTableSchema, ResolvedCatalogTable oldTable, ConvertContext context) {
        SchemaConverter converter = createSchemaConverter(oldTable, context);
        converter.updateColumn(alterTableSchema.getColumnPositions().getList());
        alterTableSchema.getWatermark().ifPresent(converter::updateWatermark);
        alterTableSchema.getFullConstraint().ifPresent(converter::updatePrimaryKey);

        return buildAlterTableChangeOperation(
                alterTableSchema,
                converter.getChangesCollector(),
                converter.convert(),
                oldTable,
                context.getCatalogManager());
    }

    protected abstract SchemaConverter createSchemaConverter(
            ResolvedCatalogTable oldTable, ConvertContext context);

    /**
     * Convert {@code ALTER TABLE ADD (<schema_component> [, <schema_component>])} to generate an
     * updated Schema.
     */
    public static class SqlAlterTableSchemaAddConverter
            extends SqlAlterTableSchemaConverter<SqlAlterTableAdd> {
        @Override
        protected SchemaConverter createSchemaConverter(
                ResolvedCatalogTable oldTable, ConvertContext context) {
            return new SchemaAddConverter(oldTable, context);
        }
    }

    /**
     * Convert {@code ALTER TABLE MODIFY (<schema_component> [, <schema_component>])} to generate an
     * updated Schema.
     */
    public static class SqlAlterTableSchemaModifyConverter
            extends SqlAlterTableSchemaConverter<SqlAlterTableModify> {
        @Override
        protected SchemaModifyConverter createSchemaConverter(
                ResolvedCatalogTable oldTable, ConvertContext context) {
            return new SchemaModifyConverter(oldTable, context);
        }
    }

    /** Convert {@code ALTER TABLE DROP} to generate an updated Schema. */
    public abstract static class SqlAlterTableSchemaDropConverter<T extends SqlAlterTableSchema>
            extends AbstractAlterTableConverter<T> {
        @Override
        protected Operation convertToOperation(
                T alterTableSchema, ResolvedCatalogTable oldTable, ConvertContext context) {
            Set<String> columnsToDrop = getColumnsToDrop(alterTableSchema);
            List<TableChange> tableChanges =
                    validateAndGatherDropChanges(alterTableSchema, oldTable, columnsToDrop);
            Schema schema =
                    SchemaReferencesManager.buildSchemaForAlterSchemaDrop(
                            oldTable, tableChanges, columnsToDrop);

            return buildAlterTableChangeOperation(
                    alterTableSchema, tableChanges, schema, oldTable, context.getCatalogManager());
        }

        protected abstract List<TableChange> validateAndGatherDropChanges(
                T alterTableSchema, ResolvedCatalogTable oldTable, Set<String> columnsToDrop);

        protected abstract Set<String> getColumnsToDrop(T alterTableSchema);
    }

    /** Convert {@code ALTER TABLE DROP PRIMARY KEY} to generate an updated Schema. */
    public static class SqlAlterTableSchemaDropPrimaryKeyConverter
            extends SqlAlterTableSchemaDropConverter<SqlAlterTableDropPrimaryKey> {

        @Override
        protected List<TableChange> validateAndGatherDropChanges(
                SqlAlterTableDropPrimaryKey alterTableSchema,
                ResolvedCatalogTable oldTable,
                Set<String> columnsToDrop) {
            return OperationConverterUtils.validateAndGatherDropPrimaryKey(
                    oldTable, EX_MSG_PREFIX, "table");
        }

        @Override
        protected Set<String> getColumnsToDrop(SqlAlterTableDropPrimaryKey alterTableSchema) {
            return Set.of();
        }
    }

    /** Convert {@code ALTER TABLE DROP WATERMARK} to generate an updated Schema. */
    public static class SqlAlterTableSchemaDropWatermarkConverter
            extends SqlAlterTableSchemaDropConverter<SqlAlterTableDropWatermark> {

        @Override
        protected List<TableChange> validateAndGatherDropChanges(
                SqlAlterTableDropWatermark alterTableSchema,
                ResolvedCatalogTable oldTable,
                Set<String> columnsToDrop) {
            return OperationConverterUtils.validateAndGatherDropWatermarkChanges(
                    oldTable, EX_MSG_PREFIX, "table");
        }

        @Override
        protected Set<String> getColumnsToDrop(SqlAlterTableDropWatermark alterTableSchema) {
            return Set.of();
        }
    }

    /**
     * Convert {@code ALTER TABLE DROP column_name} in case of one column and {@code ALTER TABLE
     * DROP (column_name1 [, column_name2])} in case of multiple columns to generate an updated
     * Schema.
     */
    public static class SqlAlterTableSchemaDropColumnConverter
            extends SqlAlterTableSchemaDropConverter<SqlAlterTableDropColumn> {

        @Override
        protected List<TableChange> validateAndGatherDropChanges(
                SqlAlterTableDropColumn alterTableSchema,
                ResolvedCatalogTable oldTable,
                Set<String> columnsToDrop) {
            return OperationConverterUtils.validateAndGatherDropColumn(
                    oldTable, columnsToDrop, EX_MSG_PREFIX);
        }

        @Override
        protected Set<String> getColumnsToDrop(SqlAlterTableDropColumn dropColumn) {
            return OperationConverterUtils.getColumnNames(
                    dropColumn.getColumnList(), EX_MSG_PREFIX);
        }
    }

    /**
     * Convert {@code ALTER TABLE DROP CONSTRAINT constraint_name} to generate an updated Schema.
     */
    public static class SqlAlterTableSchemaDropConstraintConverter
            extends SqlAlterTableSchemaDropConverter<SqlAlterTableDropConstraint> {

        @Override
        protected List<TableChange> validateAndGatherDropChanges(
                SqlAlterTableDropConstraint dropConstraint,
                ResolvedCatalogTable oldTable,
                Set<String> columnsToDrop) {
            return OperationConverterUtils.validateAndGatherDropConstraintChanges(
                    oldTable, dropConstraint.getConstraintName(), EX_MSG_PREFIX, "table");
        }

        @Override
        protected Set<String> getColumnsToDrop(SqlAlterTableDropConstraint alterTableSchema) {
            return Set.of();
        }
    }
}
