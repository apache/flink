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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedMetadataColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.TableResultImpl;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.TableChange.AddColumn;
import org.apache.flink.table.catalog.TableChange.DropColumn;
import org.apache.flink.table.catalog.TableChange.ModifyColumn;
import org.apache.flink.table.catalog.TableChange.ModifyDefinitionQuery;
import org.apache.flink.table.catalog.TableChange.ModifyPhysicalColumnType;
import org.apache.flink.table.catalog.TableChange.ModifyRefreshHandler;
import org.apache.flink.table.catalog.TableChange.ModifyRefreshStatus;
import org.apache.flink.table.catalog.TableChange.ModifyStartMode;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.ModifyOperationVisitor;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.ddl.AlterTableChangeOperation;
import org.apache.flink.table.utils.EncodingUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Alter materialized table with new table definition and table changes represents the modification.
 */
@Internal
public class AlterMaterializedTableChangeOperation extends AlterMaterializedTableOperation
        implements ModifyOperation {

    private final Function<ResolvedCatalogMaterializedTable, List<TableChange>> tableChangeForTable;
    private final QueryOperation asQueryOperation;
    private ResolvedCatalogMaterializedTable oldTable;
    private MaterializedTableChangeHandler handler;
    private CatalogMaterializedTable newTable;
    private List<TableChange> tableChanges;

    public AlterMaterializedTableChangeOperation(
            ObjectIdentifier tableIdentifier,
            Function<ResolvedCatalogMaterializedTable, List<TableChange>> tableChangeForTable,
            ResolvedCatalogMaterializedTable oldTable) {
        // Metadata-only changes (options, schema, distribution, ...) carry no sink-modifying query.
        this(tableIdentifier, tableChangeForTable, oldTable, null);
    }

    public AlterMaterializedTableChangeOperation(
            ObjectIdentifier tableIdentifier,
            Function<ResolvedCatalogMaterializedTable, List<TableChange>> tableChangeForTable,
            ResolvedCatalogMaterializedTable oldTable,
            QueryOperation asQueryOperation) {
        super(tableIdentifier);
        this.tableChangeForTable = tableChangeForTable;
        this.oldTable = oldTable;
        this.asQueryOperation = asQueryOperation;
    }

    public QueryOperation getAsQueryOperation() {
        return asQueryOperation;
    }

    public List<TableChange> getTableChanges() {
        if (tableChanges == null) {
            tableChanges = tableChangeForTable.apply(oldTable);
        }
        return tableChanges;
    }

    public AlterMaterializedTableChangeOperation copyAsTableChangeOperation() {
        return new AlterMaterializedTableChangeOperation(
                tableIdentifier, tableChangeForTable, oldTable, asQueryOperation);
    }

    public CatalogMaterializedTable getNewTable() {
        if (newTable == null) {
            newTable = computeNewTable();
        }
        return newTable;
    }

    public ResolvedCatalogMaterializedTable getOldTable() {
        return oldTable;
    }

    public void setOldTable(final ResolvedCatalogMaterializedTable oldTable) {
        this.oldTable = oldTable;
        // All caches are derived from oldTable; invalidate them together.
        this.tableChanges = null;
        this.handler = null;
        this.newTable = null;
    }

    @VisibleForTesting
    public void validateChanges() {
        final List<TableChange> changes = getTableChanges();
        // CoA and ALTER ... AS carry the defining query, so the append-only column rules apply even
        // when the query text is unchanged; metadata-only DDL alters carry no query.
        final boolean isQueryChange =
                asQueryOperation != null
                        || changes.stream().anyMatch(ModifyDefinitionQuery.class::isInstance);
        final List<Column> oldColumns = oldTable.getResolvedSchema().getColumns();
        final Map<String, Integer> columnIndex =
                IntStream.range(0, oldColumns.size())
                        .boxed()
                        .collect(
                                Collectors.toMap(
                                        i -> oldColumns.get(i).getName(), Function.identity()));
        final List<String> errors = new ArrayList<>();
        for (final TableChange change : changes) {
            if (change instanceof DropColumn) {
                checkDroppedColumn((DropColumn) change, oldColumns, columnIndex, errors);
            } else if (isQueryChange && change instanceof ModifyPhysicalColumnType) {
                checkPhysicalTypeChange(
                        (ModifyPhysicalColumnType) change, oldColumns, columnIndex, errors);
            }
        }
        if (isQueryChange) {
            checkPersistedColumnOrder(oldColumns, columnIndex, changes, errors);
        }
        if (!errors.isEmpty()) {
            throw new ValidationException(String.join("\n", errors));
        }
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        validateChanges();
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

    @Override
    public QueryOperation getChild() {
        return this.asQueryOperation;
    }

    @Override
    public <T> T accept(final ModifyOperationVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** Hook for subclasses to provide a different new-table builder. */
    protected CatalogMaterializedTable computeNewTable() {
        return MaterializedTableChangeHandler.buildNewMaterializedTable(getHandlerWithChanges());
    }

    protected MaterializedTableChangeHandler getHandlerWithChanges() {
        if (handler == null) {
            handler =
                    MaterializedTableChangeHandler.getHandlerWithChanges(
                            oldTable, getTableChanges());
        }
        return handler;
    }

    protected String getOperationName() {
        return "ALTER MATERIALIZED TABLE";
    }

    private void checkDroppedColumn(
            DropColumn change,
            List<Column> oldColumns,
            Map<String, Integer> columnIndex,
            List<String> errors) {
        final int idx = columnIndex.getOrDefault(change.getColumnName(), -1);
        if (idx < 0 || !oldColumns.get(idx).isPersisted()) {
            return;
        }
        errors.add(
                String.format(
                        "Dropping of persisted column `%s` is not supported.",
                        change.getColumnName()));
    }

    private void checkPhysicalTypeChange(
            ModifyPhysicalColumnType change,
            List<Column> oldColumns,
            Map<String, Integer> columnIndex,
            List<String> errors) {
        final int idx = columnIndex.getOrDefault(change.getOldColumn().getName(), -1);
        if (idx < 0) {
            return;
        }
        errors.add(
                positionChangeError(
                        change.getOldColumn().asSummaryString(),
                        change.getNewColumn().asSummaryString(),
                        idx));
    }

    /**
     * The sink writes by position, so the new table must start with the old persisted columns in
     * their old order. Individually harmless position changes can still move persisted columns, so
     * the built table's order is checked. Reports the first displaced column only.
     */
    private void checkPersistedColumnOrder(
            List<Column> oldColumns,
            Map<String, Integer> columnIndex,
            List<TableChange> changes,
            List<String> errors) {
        final Schema newSchema = getNewTable().getUnresolvedSchema();
        final List<String> newOrder = persistedColumnNames(newSchema);
        final Set<String> newNames =
                newSchema.getColumns().stream()
                        .map(UnresolvedColumn::getName)
                        .collect(Collectors.toSet());
        final List<Column> oldPersisted =
                oldColumns.stream()
                        .filter(Column::isPersisted)
                        .filter(column -> newNames.contains(column.getName()))
                        .collect(Collectors.toList());
        for (int i = 0; i < oldPersisted.size(); i++) {
            final String oldName = oldPersisted.get(i).getName();
            if (i < newOrder.size() && oldName.equals(newOrder.get(i))) {
                continue;
            }
            // Describe the old column's own new form if it is no longer persisted.
            final String displacingName = newOrder.contains(oldName) ? newOrder.get(i) : oldName;
            final Column newColumn = latestColumnsByName(oldColumns, changes).get(displacingName);
            final String newSummary =
                    newColumn != null
                            ? newColumn.asSummaryString()
                            : EncodingUtils.escapeIdentifier(displacingName);
            errors.add(
                    positionChangeError(
                            oldPersisted.get(i).asSummaryString(),
                            newSummary,
                            columnIndex.get(oldName)));
            return;
        }
    }

    private static List<String> persistedColumnNames(Schema schema) {
        return schema.getColumns().stream()
                .filter(
                        column ->
                                column instanceof UnresolvedPhysicalColumn
                                        || (column instanceof UnresolvedMetadataColumn
                                                && !((UnresolvedMetadataColumn) column)
                                                        .isVirtual()))
                .map(UnresolvedColumn::getName)
                .collect(Collectors.toList());
    }

    /** Old columns as last added or modified by the changes, for describing the new column. */
    private static Map<String, Column> latestColumnsByName(
            List<Column> oldColumns, List<TableChange> changes) {
        final Map<String, Column> columnsByName = new HashMap<>();
        oldColumns.forEach(column -> columnsByName.put(column.getName(), column));
        for (final TableChange change : changes) {
            if (change instanceof AddColumn) {
                final Column added = ((AddColumn) change).getColumn();
                columnsByName.put(added.getName(), added);
            } else if (change instanceof ModifyColumn) {
                final Column modified = ((ModifyColumn) change).getNewColumn();
                columnsByName.put(modified.getName(), modified);
            }
        }
        return columnsByName;
    }

    private static String positionChangeError(String oldColumn, String newColumn, int position) {
        return String.format(
                "When modifying the query of a materialized table, "
                        + "currently only support appending columns at the end of original schema, dropping, renaming, and reordering columns are not supported.\n"
                        + "Column mismatch at position %d: Original column is [%s], but new column is [%s].",
                position + 1, oldColumn, newColumn);
    }

    static String toString(TableChange tableChange) {
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
        } else if (tableChange instanceof ModifyStartMode) {
            ModifyStartMode startMode = (ModifyStartMode) tableChange;
            return String.format("  MODIFY START_MODE TO '%s'", startMode.getStartMode());
        } else {
            return AlterTableChangeOperation.toString(tableChange);
        }
    }
}
