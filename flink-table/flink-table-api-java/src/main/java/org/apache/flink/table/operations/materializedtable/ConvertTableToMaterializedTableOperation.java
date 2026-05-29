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
import org.apache.flink.table.api.internal.TableResultImpl;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.operations.ExecutableOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.OperationUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Operation to describe an in-place conversion of an existing regular table to a materialized table
 * via CREATE OR ALTER MATERIALIZED TABLE.
 */
@Internal
public class ConvertTableToMaterializedTableOperation
        implements MaterializedTableOperation, ExecutableOperation {

    private final ObjectIdentifier tableIdentifier;
    private final ResolvedCatalogTable originalTable;
    private final ResolvedCatalogMaterializedTable materializedTable;
    private final Function<ResolvedCatalogMaterializedTable, List<TableChange>>
            tableChangesForTable;
    private List<TableChange> tableChanges;

    public ConvertTableToMaterializedTableOperation(
            ObjectIdentifier tableIdentifier,
            ResolvedCatalogTable originalTable,
            ResolvedCatalogMaterializedTable materializedTable,
            Function<ResolvedCatalogMaterializedTable, List<TableChange>> tableChangesBuilder) {
        this.tableIdentifier = tableIdentifier;
        this.originalTable = originalTable;
        this.materializedTable = materializedTable;
        this.tableChangesForTable = tableChangesBuilder;
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        ctx.getCatalogManager()
                .convertTableToMaterializedTable(
                        originalTable, materializedTable, getTableChanges(), tableIdentifier);
        return TableResultImpl.TABLE_RESULT_OK;
    }

    public ObjectIdentifier getTableIdentifier() {
        return tableIdentifier;
    }

    public ResolvedCatalogTable getOriginalTable() {
        return originalTable;
    }

    public ResolvedCatalogMaterializedTable getMaterializedTable() {
        return materializedTable;
    }

    public List<TableChange> getTableChanges() {
        if (tableChanges == null) {
            tableChanges = tableChangesForTable.apply(materializedTable);
        }
        return tableChanges;
    }

    @Override
    public String asSummaryString() {
        final Map<String, Object> params = new LinkedHashMap<>();
        params.put("identifier", tableIdentifier);
        params.put("materializedTable", materializedTable);
        params.put(
                "tableChanges",
                getTableChanges().stream()
                        .map(AlterMaterializedTableChangeOperation::toString)
                        .collect(Collectors.joining(",\n")));
        return OperationUtils.formatWithChildren(
                "CREATE OR ALTER MATERIALIZED TABLE",
                params,
                List.of(),
                Operation::asSummaryString);
    }
}
