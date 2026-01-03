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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.operations.converters.SchemaAddConverter;
import org.apache.flink.table.planner.operations.converters.SchemaConverter;
import org.apache.flink.table.planner.operations.converters.SchemaModifyConverter;
import org.apache.flink.table.planner.operations.converters.SchemaReferencesManager;

import org.apache.calcite.sql.SqlIdentifier;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Abstract class for converters to convert ALTER TABLE ADD | MODIFY (&lt;schema_component&gt; [,
 * &lt;schema_component&gt;, ...]) to generate an updated Schema.
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
     * Convert ALTER TABLE ADD (&lt;schema_component&gt; [, &lt;schema_component&gt;, ...]) to
     * generate an updated Schema.
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
     * Convert ALTER TABLE MODIFY (&lt;schema_component&gt; [, &lt;schema_component&gt;, ...]) to
     * generate an updated Schema.
     */
    public static class SqlAlterTableSchemaModifyConverter
            extends SqlAlterTableSchemaConverter<SqlAlterTableModify> {
        @Override
        protected SchemaModifyConverter createSchemaConverter(
                ResolvedCatalogTable oldTable, ConvertContext context) {
            return new SchemaModifyConverter(oldTable, context);
        }
    }

    /** Convert ALTER TABLE DROP ... to generate an updated Schema. */
    public abstract static class SqlAlterTableSchemaDropConverter<T extends SqlAlterTableSchema>
            extends AbstractAlterTableConverter<T> {
        @Override
        protected Operation convertToOperation(
                T alterTableSchema, ResolvedCatalogTable oldTable, ConvertContext context) {
            Set<String> columnsToDrop = getColumnsToDrop(alterTableSchema);
            List<TableChange> tableChanges =
                    validateAndGatherDropChanges(alterTableSchema, oldTable, columnsToDrop);

            Schema.Builder schemaBuilder = Schema.newBuilder();
            SchemaReferencesManager.buildUpdatedColumn(
                    schemaBuilder,
                    oldTable,
                    (builder, column) -> {
                        if (!columnsToDrop.contains(column.getName())) {
                            builder.fromColumns(List.of(column));
                        }
                    });
            if (tableChanges.stream().noneMatch(c -> c instanceof TableChange.DropConstraint)) {
                SchemaReferencesManager.buildUpdatedPrimaryKey(
                        schemaBuilder, oldTable, Function.identity());
            }

            if (tableChanges.stream().noneMatch(c -> c instanceof TableChange.DropWatermark)) {
                SchemaReferencesManager.buildUpdatedWatermark(schemaBuilder, oldTable);
            }
            Schema schema = schemaBuilder.build();

            return buildAlterTableChangeOperation(
                    alterTableSchema, tableChanges, schema, oldTable, context.getCatalogManager());
        }

        protected abstract List<TableChange> validateAndGatherDropChanges(
                T alterTableSchema, ResolvedCatalogTable oldTable, Set<String> columnsToDrop);

        protected abstract Set<String> getColumnsToDrop(T alterTableSchema);
    }

    public static class SqlAlterTableSchemaDropPrimaryKeyConverter
            extends SqlAlterTableSchemaDropConverter<SqlAlterTableDropPrimaryKey> {

        @Override
        protected List<TableChange> validateAndGatherDropChanges(
                SqlAlterTableDropPrimaryKey alterTableSchema,
                ResolvedCatalogTable oldTable,
                Set<String> columnsToDrop) {
            Optional<UniqueConstraint> pkConstraint = oldTable.getResolvedSchema().getPrimaryKey();

            if (pkConstraint.isEmpty()) {
                throw new ValidationException(
                        String.format(
                                "%sThe base table does not define any primary key.",
                                EX_MSG_PREFIX));
            }

            return List.of(TableChange.dropConstraint(pkConstraint.get().getName()));
        }

        @Override
        protected Set<String> getColumnsToDrop(SqlAlterTableDropPrimaryKey alterTableSchema) {
            return Set.of();
        }
    }

    public static class SqlAlterTableSchemaDropWatermarkConverter
            extends SqlAlterTableSchemaDropConverter<SqlAlterTableDropWatermark> {

        @Override
        protected List<TableChange> validateAndGatherDropChanges(
                SqlAlterTableDropWatermark alterTableSchema,
                ResolvedCatalogTable oldTable,
                Set<String> columnsToDrop) {
            if (oldTable.getResolvedSchema().getWatermarkSpecs().isEmpty()) {
                throw new ValidationException(
                        String.format(
                                "%sThe base table does not define any watermark strategy.",
                                EX_MSG_PREFIX));
            }

            return List.of(TableChange.dropWatermark());
        }

        @Override
        protected Set<String> getColumnsToDrop(SqlAlterTableDropWatermark alterTableSchema) {
            return Set.of();
        }
    }

    public static class SqlAlterTableSchemaDropColumnConverter
            extends SqlAlterTableSchemaDropConverter<SqlAlterTableDropColumn> {

        @Override
        protected List<TableChange> validateAndGatherDropChanges(
                SqlAlterTableDropColumn alterTableSchema,
                ResolvedCatalogTable oldTable,
                Set<String> columnsToDrop) {

            SchemaReferencesManager referencesManager = SchemaReferencesManager.create(oldTable);
            // Sort by dependencies count from smallest to largest. For example, when dropping
            // column a,
            // b(b as a+1), the order should be: [b, a] after sort.
            Comparator<Object> comparator =
                    Comparator.comparingInt(
                                    col -> referencesManager.getColumnDependencyCount((String) col))
                            .reversed();
            List<String> sortedColumnsToDrop =
                    columnsToDrop.stream().sorted(comparator).collect(Collectors.toList());
            List<TableChange> tableChanges = new ArrayList<>(sortedColumnsToDrop.size());
            for (String columnToDrop : sortedColumnsToDrop) {
                referencesManager.dropColumn(columnToDrop, () -> EX_MSG_PREFIX);
                tableChanges.add(TableChange.dropColumn(columnToDrop));
            }

            return tableChanges;
        }

        @Override
        protected Set<String> getColumnsToDrop(SqlAlterTableDropColumn dropColumn) {
            return getColumnNames(dropColumn.getColumnList());
        }
    }

    public static class SqlAlterTableSchemaDropConstraintConverter
            extends SqlAlterTableSchemaDropConverter<SqlAlterTableDropConstraint> {

        @Override
        protected List<TableChange> validateAndGatherDropChanges(
                SqlAlterTableDropConstraint dropConstraint,
                ResolvedCatalogTable oldTable,
                Set<String> columnsToDrop) {

            Optional<UniqueConstraint> pkConstraint = oldTable.getResolvedSchema().getPrimaryKey();
            if (pkConstraint.isEmpty()) {
                throw new ValidationException(
                        String.format(
                                "%sThe base table does not define any primary key.",
                                EX_MSG_PREFIX));
            }
            SqlIdentifier constraintIdentifier = dropConstraint.getConstraintName();
            String constraintName = pkConstraint.get().getName();
            if (constraintIdentifier != null
                    && !constraintIdentifier.getSimple().equals(constraintName)) {
                throw new ValidationException(
                        String.format(
                                "%sThe base table does not define a primary key constraint named '%s'. "
                                        + "Available constraint name: ['%s'].",
                                EX_MSG_PREFIX, constraintIdentifier.getSimple(), constraintName));
            }

            return List.of(TableChange.dropConstraint(constraintName));
        }

        @Override
        protected Set<String> getColumnsToDrop(SqlAlterTableDropConstraint alterTableSchema) {
            return Set.of();
        }
    }
}
