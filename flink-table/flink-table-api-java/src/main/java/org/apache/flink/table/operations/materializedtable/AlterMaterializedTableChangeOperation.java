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
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.TableChange.ModifyDefinitionQuery;
import org.apache.flink.table.catalog.TableChange.ModifyRefreshHandler;
import org.apache.flink.table.catalog.TableChange.ModifyRefreshStatus;
import org.apache.flink.table.operations.ddl.AlterTableChangeOperation;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Alter materialized table with new table definition and table changes represents the modification.
 */
@Internal
public class AlterMaterializedTableChangeOperation extends AlterMaterializedTableOperation {

    private final Function<ResolvedCatalogMaterializedTable, List<TableChange>> tableChangeForTable;
    private final ResolvedCatalogMaterializedTable oldTable;
    private MaterializedTableChangeHandler handler;
    private CatalogMaterializedTable newTable;
    private List<TableChange> tableChanges;

    public AlterMaterializedTableChangeOperation(
            ObjectIdentifier tableIdentifier,
            Function<ResolvedCatalogMaterializedTable, List<TableChange>> tableChangeForTable,
            ResolvedCatalogMaterializedTable oldTable) {
        super(tableIdentifier);
        this.tableChangeForTable = tableChangeForTable;
        this.oldTable = oldTable;
    }

    public List<TableChange> getTableChanges() {
        if (tableChanges == null) {
            tableChanges = tableChangeForTable.apply(oldTable);
        }
        return tableChanges;
    }

    public AlterMaterializedTableChangeOperation copyAsTableChangeOperation() {
        return new AlterMaterializedTableChangeOperation(
                tableIdentifier, tableChangeForTable, oldTable);
    }

    public CatalogMaterializedTable getNewTable() {
        if (newTable == null) {
            newTable =
                    MaterializedTableChangeHandler.buildNewMaterializedTable(
                            getHandlerWithChanges());
        }
        return newTable;
    }

    protected MaterializedTableChangeHandler getHandlerWithChanges() {
        if (handler == null) {
            handler =
                    MaterializedTableChangeHandler.getHandlerWithChanges(
                            oldTable, getTableChanges());
        }
        return handler;
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        ctx.getCatalogManager()
                .alterTable(getNewTable(), getTableChanges(), getTableIdentifier(), false);
        return TableResultImpl.TABLE_RESULT_OK;
    }

    @Override
    public String asSummaryString() {
        String changes =
                getTableChanges().stream()
                        .map(AlterMaterializedTableChangeOperation::toString)
                        .collect(Collectors.joining(",\n"));
        return String.format(
                "%s %s\n%s", getOperationName(), tableIdentifier.asSummaryString(), changes);
    }

    protected String getOperationName() {
        return "ALTER MATERIALIZED TABLE";
    }

    private static String toString(TableChange tableChange) {
        if (tableChange instanceof ModifyRefreshStatus) {
            ModifyRefreshStatus refreshStatus = (ModifyRefreshStatus) tableChange;
            return String.format(
                    "  MODIFY REFRESH STATUS TO '%s'", refreshStatus.getRefreshStatus());
        } else if (tableChange instanceof ModifyRefreshHandler) {
            ModifyRefreshHandler refreshHandler = (ModifyRefreshHandler) tableChange;
            return String.format(
                    "  MODIFY REFRESH HANDLER DESCRIPTION TO '%s'",
                    refreshHandler.getRefreshHandlerDesc());
        } else if (tableChange instanceof ModifyDefinitionQuery) {
            ModifyDefinitionQuery definitionQuery = (ModifyDefinitionQuery) tableChange;
            return String.format(
                    "  MODIFY DEFINITION QUERY TO '%s'", definitionQuery.getDefinitionQuery());
        } else {
            return AlterTableChangeOperation.toString(tableChange);
        }
    }
}
