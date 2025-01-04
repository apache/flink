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

package org.apache.flink.table.operations.materializedtable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.TableResultImpl;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.OperationUtils;
import org.apache.flink.table.operations.QueryOperation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.catalog.CatalogBaseTable.TableKind.MATERIALIZED_TABLE;

/** Operation to describe an ALTER MATERIALIZED TABLE AS query operation. */
@Internal
public class AlterMaterializedTableAsQueryOperation extends AlterMaterializedTableOperation {

    protected final ObjectIdentifier tableIdentifier;

    private final QueryOperation queryOperation;

    public AlterMaterializedTableAsQueryOperation(
            ObjectIdentifier tableIdentifier, QueryOperation queryOperation) {
        super(tableIdentifier);
        this.tableIdentifier = tableIdentifier;
        this.queryOperation = queryOperation;
    }

    public QueryOperation getQueryOperation() {
        return queryOperation;
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        ResolvedCatalogBaseTable<?> resolvedCatalogBaseTable =
                ctx.getCatalogManager().getTableOrError(tableIdentifier).getResolvedTable();
        if (MATERIALIZED_TABLE != resolvedCatalogBaseTable.getTableKind()) {
            throw new ValidationException(
                    String.format(
                            "Table %s is not a materialized table, does not support materialized table related operation.",
                            tableIdentifier));
        }

        ResolvedCatalogMaterializedTable oldResolvedMaterializedTable =
                (ResolvedCatalogMaterializedTable) resolvedCatalogBaseTable;

        // validate new schema and derived origin primary key and watermark spec
        ResolvedSchema resolvedQuerySchema = queryOperation.getResolvedSchema();
        ResolvedSchema oldResolvedSchema = oldResolvedMaterializedTable.getResolvedSchema();
        List<TableChange> tableChanges =
                validateAndExtractNewColumns(oldResolvedSchema, resolvedQuerySchema).stream()
                        .map(TableChange::add)
                        .collect(Collectors.toList());
        ResolvedSchema newResolvedSchema =
                new ResolvedSchema(
                        resolvedQuerySchema.getColumns(),
                        oldResolvedSchema.getWatermarkSpecs(),
                        oldResolvedSchema.getPrimaryKey().orElse(null));
        Schema newSchema = Schema.newBuilder().fromResolvedSchema(newResolvedSchema).build();

        // update schema and definition query
        String definitionQuery = queryOperation.asSerializableString();
        CatalogMaterializedTable catalogMaterializedTable =
                oldResolvedMaterializedTable.getOrigin().copy(newSchema, definitionQuery);

        ResolvedCatalogMaterializedTable newResolvedMaterializedTable =
                new ResolvedCatalogMaterializedTable(catalogMaterializedTable, newResolvedSchema);
        ctx.getCatalogManager()
                .alterTable(newResolvedMaterializedTable, tableChanges, tableIdentifier, false);

        return TableResultImpl.TABLE_RESULT_OK;
    }

    @Override
    public String asSummaryString() {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("identifier", tableIdentifier);
        return OperationUtils.formatWithChildren(
                "ALTER MATERIALIZED TABLE",
                params,
                Collections.singletonList(queryOperation),
                Operation::asSummaryString);
    }

    private List<Column> validateAndExtractNewColumns(
            ResolvedSchema oldSchema, ResolvedSchema newSchema) {
        List<Column> newAddedColumns = new ArrayList<>();

        if (oldSchema.getColumns().size() > newSchema.getColumns().size()) {
            throw new ValidationException(
                    "Cannot alter table. The new schema has fewer columns than the original schema.");
        }

        for (int i = 0; i < oldSchema.getColumns().size(); i++) {
            Column oldColumn = oldSchema.getColumns().get(i);
            Column newColumn = newSchema.getColumns().get(i);
            if (!oldColumn.equals(newColumn)) {
                throw new ValidationException(
                        String.format(
                                "Cannot alter table. The schema of existing column %s has changed.",
                                oldColumn.getName()));
            }
        }
        for (int i = oldSchema.getColumns().size(); i < newSchema.getColumns().size(); i++) {
            Column newColumn = newSchema.getColumns().get(i);
            newAddedColumns.add(newColumn.copy(newColumn.getDataType().nullable()));
        }
        return newAddedColumns;
    }
}
