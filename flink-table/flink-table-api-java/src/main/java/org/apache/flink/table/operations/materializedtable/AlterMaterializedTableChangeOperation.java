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
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.operations.ddl.AlterTableChangeOperation;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Alter materialized table with new table definition and table changes represents the modification.
 */
@Internal
public class AlterMaterializedTableChangeOperation extends AlterMaterializedTableOperation {

    private final List<TableChange> tableChanges;
    private final CatalogMaterializedTable catalogMaterializedTable;

    public AlterMaterializedTableChangeOperation(
            ObjectIdentifier tableIdentifier,
            List<TableChange> tableChanges,
            CatalogMaterializedTable catalogMaterializedTable) {
        super(tableIdentifier);
        this.tableChanges = tableChanges;
        this.catalogMaterializedTable = catalogMaterializedTable;
    }

    public List<TableChange> getTableChanges() {
        return tableChanges;
    }

    public CatalogMaterializedTable getCatalogMaterializedTable() {
        return catalogMaterializedTable;
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        ctx.getCatalogManager()
                .alterTable(
                        getCatalogMaterializedTable(),
                        getTableChanges().stream()
                                .map(TableChange.class::cast)
                                .collect(Collectors.toList()),
                        getTableIdentifier(),
                        false);
        return TableResultImpl.TABLE_RESULT_OK;
    }

    @Override
    public String asSummaryString() {
        String changes =
                tableChanges.stream()
                        .map(
                                tableChange -> {
                                    if (tableChange
                                            instanceof TableChange.MaterializedTableChange) {
                                        return toString(
                                                (TableChange.MaterializedTableChange) tableChange);
                                    } else {
                                        return AlterTableChangeOperation.toString(tableChange);
                                    }
                                })
                        .collect(Collectors.joining(",\n"));
        return String.format(
                "ALTER MATERIALIZED TABLE %s\n%s", tableIdentifier.asSummaryString(), changes);
    }

    private String toString(TableChange.MaterializedTableChange tableChange) {
        if (tableChange instanceof TableChange.ModifyRefreshStatus) {
            TableChange.ModifyRefreshStatus refreshStatus =
                    (TableChange.ModifyRefreshStatus) tableChange;
            return String.format(
                    "  MODIFY REFRESH STATUS TO '%s'", refreshStatus.getRefreshStatus());
        } else if (tableChange instanceof TableChange.ModifyRefreshHandler) {
            TableChange.ModifyRefreshHandler refreshHandler =
                    (TableChange.ModifyRefreshHandler) tableChange;
            return String.format(
                    "  MODIFY REFRESH HANDLER DESCRIPTION TO '%s'",
                    refreshHandler.getRefreshHandlerDesc());
        } else {
            throw new UnsupportedOperationException(
                    String.format("Unknown materialized table change: %s.", tableChange));
        }
    }
}
