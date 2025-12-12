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

import org.apache.flink.sql.parser.ddl.table.SqlAlterTableRenameColumn;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.operations.converters.SchemaReferencesManager;

import java.util.List;

/** Convert ALTER TABLE RENAME col_name to new_col_name to generate an updated Schema. */
public class SqlAlterTableRenameColumnConverter
        extends AbstractAlterTableConverter<SqlAlterTableRenameColumn> {
    @Override
    protected Operation convertToOperation(
            SqlAlterTableRenameColumn renameColumn,
            ResolvedCatalogTable oldTable,
            ConvertContext context) {
        String oldColumnName = getColumnName(renameColumn.getOldColumnIdentifier());
        String newColumnName = getColumnName(renameColumn.getNewColumnIdentifier());

        SchemaReferencesManager.create(oldTable)
                .checkReferences(oldColumnName, () -> EX_MSG_PREFIX);
        if (oldTable.getResolvedSchema().getColumn(newColumnName).isPresent()) {
            throw new ValidationException(
                    String.format(
                            "%sThe column `%s` already existed in table schema.",
                            EX_MSG_PREFIX, newColumnName));
        }

        // generate new schema
        Schema.Builder schemaBuilder = Schema.newBuilder();
        SchemaReferencesManager.buildUpdatedColumn(
                schemaBuilder,
                oldTable,
                (builder, column) -> {
                    if (column.getName().equals(oldColumnName)) {
                        buildNewColumnFromOldColumn(builder, column, newColumnName);
                    } else {
                        builder.fromColumns(List.of(column));
                    }
                });
        SchemaReferencesManager.buildUpdatedPrimaryKey(
                schemaBuilder, oldTable, (pk) -> pk.equals(oldColumnName) ? newColumnName : pk);
        SchemaReferencesManager.buildUpdatedWatermark(schemaBuilder, oldTable);

        return buildAlterTableChangeOperation(
                renameColumn,
                List.of(
                        TableChange.modifyColumnName(
                                oldTable.getResolvedSchema()
                                        .getColumn(oldColumnName)
                                        .orElseThrow(
                                                () ->
                                                        new TableException(
                                                                "The value should never be empty.")),
                                newColumnName)),
                schemaBuilder.build(),
                oldTable,
                context.getCatalogManager());
    }

    private void buildNewColumnFromOldColumn(
            Schema.Builder builder, Schema.UnresolvedColumn oldColumn, String columnName) {
        if (oldColumn instanceof Schema.UnresolvedComputedColumn) {
            builder.columnByExpression(
                    columnName, ((Schema.UnresolvedComputedColumn) oldColumn).getExpression());
        } else if (oldColumn instanceof Schema.UnresolvedPhysicalColumn) {
            builder.column(columnName, ((Schema.UnresolvedPhysicalColumn) oldColumn).getDataType());
        } else if (oldColumn instanceof Schema.UnresolvedMetadataColumn) {
            Schema.UnresolvedMetadataColumn metadataColumn =
                    (Schema.UnresolvedMetadataColumn) oldColumn;
            builder.columnByMetadata(
                    columnName,
                    metadataColumn.getDataType(),
                    metadataColumn.getMetadataKey(),
                    metadataColumn.isVirtual());
        }
        oldColumn.getComment().ifPresent(builder::withComment);
    }
}
