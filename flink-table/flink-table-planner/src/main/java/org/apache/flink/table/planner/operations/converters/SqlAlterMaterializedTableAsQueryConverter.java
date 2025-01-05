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

import org.apache.flink.sql.parser.ddl.SqlAlterMaterializedTableAsQuery;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.TableChange.MaterializedTableChange;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableAsQueryOperation;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;

import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.catalog.CatalogBaseTable.TableKind.MATERIALIZED_TABLE;

/** A converter for {@link SqlAlterMaterializedTableAsQuery}. */
public class SqlAlterMaterializedTableAsQueryConverter
        implements SqlNodeConverter<SqlAlterMaterializedTableAsQuery> {

    @Override
    public Operation convertSqlNode(
            SqlAlterMaterializedTableAsQuery sqlAlterMaterializedTableAsQuery,
            ConvertContext context) {
        ObjectIdentifier identifier = resolveIdentifier(sqlAlterMaterializedTableAsQuery, context);

        // Validate and extract schema from query
        String originalQuery =
                context.toQuotedSqlString(sqlAlterMaterializedTableAsQuery.getAsQuery());
        SqlNode validatedQuery =
                context.getSqlValidator().validate(sqlAlterMaterializedTableAsQuery.getAsQuery());
        // The LATERAL operator was eliminated during sql validation, thus the unparsed SQL
        // does not contain LATERAL which is problematic,
        // the issue was resolved in CALCITE-4077
        // (always treat the table function as implicitly LATERAL).
        String definitionQuery = context.expandSqlIdentifiers(originalQuery);
        PlannerQueryOperation queryOperation =
                new PlannerQueryOperation(
                        context.toRelRoot(validatedQuery).project(), () -> originalQuery);

        ResolvedCatalogMaterializedTable oldTable =
                getResolvedMaterializedTable(context, identifier);
        List<Column> addedColumns =
                validateAndExtractNewColumns(
                        oldTable.getResolvedSchema(), queryOperation.getResolvedSchema());

        // Build new materialized table and apply changes
        CatalogMaterializedTable updatedTable =
                buildUpdatedMaterializedTable(oldTable, addedColumns, definitionQuery);
        List<MaterializedTableChange> tableChanges = new ArrayList<>();
        addedColumns.forEach(column -> tableChanges.add(TableChange.add(column)));
        tableChanges.add(TableChange.modifyDefinitionQuery(definitionQuery));

        return new AlterMaterializedTableAsQueryOperation(identifier, tableChanges, updatedTable);
    }

    private ObjectIdentifier resolveIdentifier(
            SqlAlterMaterializedTableAsQuery sqlAlterTableAsQuery, ConvertContext context) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlAlterTableAsQuery.fullTableName());
        return context.getCatalogManager().qualifyIdentifier(unresolvedIdentifier);
    }

    private ResolvedCatalogMaterializedTable getResolvedMaterializedTable(
            ConvertContext context, ObjectIdentifier identifier) {
        ResolvedCatalogBaseTable<?> baseTable =
                context.getCatalogManager().getTableOrError(identifier).getResolvedTable();
        if (MATERIALIZED_TABLE != baseTable.getTableKind()) {
            throw new ValidationException(
                    "Only materialized table support modify definition query.");
        }
        return (ResolvedCatalogMaterializedTable) baseTable;
    }

    private CatalogMaterializedTable buildUpdatedMaterializedTable(
            ResolvedCatalogMaterializedTable oldTable,
            List<Column> addedColumns,
            String definitionQuery) {

        Schema.Builder newSchemaBuilder =
                Schema.newBuilder().fromResolvedSchema(oldTable.getResolvedSchema());
        addedColumns.forEach(col -> newSchemaBuilder.column(col.getName(), col.getDataType()));

        return CatalogMaterializedTable.newBuilder()
                .schema(newSchemaBuilder.build())
                .comment(oldTable.getComment())
                .partitionKeys(oldTable.getPartitionKeys())
                .options(oldTable.getOptions())
                .definitionQuery(definitionQuery)
                .freshness(oldTable.getDefinitionFreshness())
                .logicalRefreshMode(oldTable.getLogicalRefreshMode())
                .refreshMode(oldTable.getRefreshMode())
                .refreshStatus(oldTable.getRefreshStatus())
                .refreshHandlerDescription(oldTable.getRefreshHandlerDescription().orElse(null))
                .serializedRefreshHandler(oldTable.getSerializedRefreshHandler())
                .build();
    }

    private List<Column> validateAndExtractNewColumns(
            ResolvedSchema oldSchema, ResolvedSchema newSchema) {
        List<Column> newAddedColumns = new ArrayList<>();
        int originalColumnSize = oldSchema.getColumns().size();
        int newColumnSize = newSchema.getColumns().size();

        if (originalColumnSize > newColumnSize) {
            throw new ValidationException(
                    String.format(
                            "Failed to modify query because drop column is unsupported. "
                                    + "When modifying a query, you can only append new columns at the end of original schema. "
                                    + "The original schema has %d columns, but the newly derived schema from the query has %d columns.",
                            originalColumnSize, newColumnSize));
        }

        for (int i = 0; i < oldSchema.getColumns().size(); i++) {
            Column oldColumn = oldSchema.getColumns().get(i);
            Column newColumn = newSchema.getColumns().get(i);
            if (!oldColumn.equals(newColumn)) {
                throw new ValidationException(
                        String.format(
                                "When modifying the query of a materialized table, "
                                        + "currently only support appending columns at the end of original schema, dropping, renaming, and reordering columns are not supported.\n"
                                        + "Column mismatch at position %d: Original column is [%s], but new column is [%s].",
                                i, oldColumn, newColumn));
            }
        }

        for (int i = oldSchema.getColumns().size(); i < newSchema.getColumns().size(); i++) {
            Column newColumn = newSchema.getColumns().get(i);
            newAddedColumns.add(newColumn.copy(newColumn.getDataType().nullable()));
        }

        return newAddedColumns;
    }
}
