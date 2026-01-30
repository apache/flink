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
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedComputedColumn;
import org.apache.flink.table.api.Schema.UnresolvedMetadataColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.api.Schema.UnresolvedWatermarkSpec;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.TableResultImpl;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshStatus;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.Column.ComputedColumn;
import org.apache.flink.table.catalog.Column.MetadataColumn;
import org.apache.flink.table.catalog.Column.PhysicalColumn;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.TableChange.AddColumn;
import org.apache.flink.table.catalog.TableChange.AddDistribution;
import org.apache.flink.table.catalog.TableChange.AddUniqueConstraint;
import org.apache.flink.table.catalog.TableChange.AddWatermark;
import org.apache.flink.table.catalog.TableChange.After;
import org.apache.flink.table.catalog.TableChange.ColumnPosition;
import org.apache.flink.table.catalog.TableChange.DropColumn;
import org.apache.flink.table.catalog.TableChange.DropConstraint;
import org.apache.flink.table.catalog.TableChange.DropDistribution;
import org.apache.flink.table.catalog.TableChange.DropWatermark;
import org.apache.flink.table.catalog.TableChange.ModifyColumn;
import org.apache.flink.table.catalog.TableChange.ModifyColumnComment;
import org.apache.flink.table.catalog.TableChange.ModifyColumnPosition;
import org.apache.flink.table.catalog.TableChange.ModifyDefinitionQuery;
import org.apache.flink.table.catalog.TableChange.ModifyDistribution;
import org.apache.flink.table.catalog.TableChange.ModifyPhysicalColumnType;
import org.apache.flink.table.catalog.TableChange.ModifyRefreshHandler;
import org.apache.flink.table.catalog.TableChange.ModifyRefreshStatus;
import org.apache.flink.table.catalog.TableChange.ModifyUniqueConstraint;
import org.apache.flink.table.catalog.TableChange.ModifyWatermark;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.operations.ddl.AlterTableChangeOperation;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Alter materialized table with new table definition and table changes represents the modification.
 */
@Internal
public class AlterMaterializedTableChangeOperation extends AlterMaterializedTableOperation {

    private final List<TableChange> tableChanges;
    private final CatalogMaterializedTable oldTable;
    private CatalogMaterializedTable materializedTableWithAppliedChanges;

    /**
     * The original order of table changes should be kept as is in some situations different order
     * might lead to different results.
     */
    public AlterMaterializedTableChangeOperation(
            ObjectIdentifier tableIdentifier,
            List<TableChange> tableChanges,
            CatalogMaterializedTable oldTable) {
        this(tableIdentifier, tableChanges, oldTable, null);
    }

    private AlterMaterializedTableChangeOperation(
            ObjectIdentifier tableIdentifier,
            List<TableChange> tableChanges,
            CatalogMaterializedTable oldTable,
            CatalogMaterializedTable catalogMaterializedTable) {
        super(tableIdentifier);
        this.tableChanges = tableChanges;
        this.oldTable = oldTable;
        this.materializedTableWithAppliedChanges = catalogMaterializedTable;
    }

    public List<TableChange> getTableChanges() {
        return tableChanges;
    }

    public AlterMaterializedTableChangeOperation copyAsTableChangeOperation() {
        return new AlterMaterializedTableChangeOperation(
                tableIdentifier, tableChanges, oldTable, materializedTableWithAppliedChanges);
    }

    public CatalogMaterializedTable getMaterializedTableWithAppliedChanges() {
        // The only case when materializedTableWithAppliedChanges is not null from the beginning
        // is copyAsTableChangeOperation where it copies already evaluated materialized table
        if (oldTable == null || materializedTableWithAppliedChanges != null) {
            return materializedTableWithAppliedChanges;
        }

        ChangeContext changeContext = new ChangeContext(oldTable);
        changeContext.applyTableChanges(tableChanges);

        materializedTableWithAppliedChanges =
                CatalogMaterializedTable.newBuilder()
                        .schema(changeContext.retrieveSchema())
                        .comment(oldTable.getComment())
                        .partitionKeys(oldTable.getPartitionKeys())
                        .options(oldTable.getOptions())
                        .originalQuery(changeContext.originalQuery)
                        .expandedQuery(changeContext.expandedQuery)
                        .distribution(changeContext.distribution)
                        .freshness(oldTable.getDefinitionFreshness())
                        .logicalRefreshMode(oldTable.getLogicalRefreshMode())
                        .refreshMode(oldTable.getRefreshMode())
                        .refreshStatus(changeContext.refreshStatus)
                        .refreshHandlerDescription(changeContext.refreshHandlerDesc)
                        .serializedRefreshHandler(changeContext.refreshHandlerBytes)
                        .build();

        return materializedTableWithAppliedChanges;
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        ctx.getCatalogManager()
                .alterTable(
                        getMaterializedTableWithAppliedChanges(),
                        getTableChanges(),
                        getTableIdentifier(),
                        false);
        return TableResultImpl.TABLE_RESULT_OK;
    }

    @Override
    public String asSummaryString() {
        String changes =
                tableChanges.stream()
                        .map(AlterMaterializedTableChangeOperation::toString)
                        .collect(Collectors.joining(",\n"));
        return String.format(
                "ALTER MATERIALIZED TABLE %s\n%s", tableIdentifier.asSummaryString(), changes);
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
                    " MODIFY DEFINITION QUERY TO '%s'", definitionQuery.getDefinitionQuery());
        } else {
            return AlterTableChangeOperation.toString(tableChange);
        }
    }

    private static class ChangeContext {
        private static final HandlerRegistry HANDLER_REGISTRY = createHandlerRegistry();

        private final List<UnresolvedColumn> columns;
        private final CatalogMaterializedTable oldTable;
        private boolean isQueryChange;
        private @Nullable TableDistribution distribution;
        private RefreshStatus refreshStatus;
        private @Nullable String refreshHandlerDesc;
        private byte[] refreshHandlerBytes;
        private List<UnresolvedWatermarkSpec> watermarkSpecs;
        private String primaryKeyName = null;
        private List<String> primaryKeyColumns = null;
        private int droppedPersistedCnt = 0;
        private String originalQuery;
        private String expandedQuery;

        public ChangeContext(CatalogMaterializedTable oldTable) {
            this.distribution = oldTable.getDistribution().orElse(null);
            this.refreshStatus = oldTable.getRefreshStatus();
            this.refreshHandlerDesc = oldTable.getRefreshHandlerDescription().orElse(null);
            this.refreshHandlerBytes = oldTable.getSerializedRefreshHandler();
            this.watermarkSpecs = oldTable.getUnresolvedSchema().getWatermarkSpecs();
            this.columns = new LinkedList<>(oldTable.getUnresolvedSchema().getColumns());
            Schema.UnresolvedPrimaryKey primaryKey =
                    oldTable.getUnresolvedSchema().getPrimaryKey().orElse(null);
            if (primaryKey != null) {
                this.primaryKeyName = primaryKey.getConstraintName();
                this.primaryKeyColumns = primaryKey.getColumnNames();
            }
            originalQuery = oldTable.getOriginalQuery();
            expandedQuery = oldTable.getExpandedQuery();
            this.oldTable = oldTable;
        }

        private static final class HandlerRegistry {
            private static final Map<Class<? extends TableChange>, HandlerWrapper<?>> HANDLERS =
                    new IdentityHashMap<>();

            private <T extends TableChange> void register(
                    Class<T> type, BiConsumer<ChangeContext, T> handler) {
                HANDLERS.put(type, new HandlerWrapper<>(handler));
            }

            private void apply(ChangeContext context, TableChange change) {
                HandlerWrapper<?> wrapper = HANDLERS.get(change.getClass());
                if (wrapper == null) {
                    throw new ValidationException("Unknown table change " + change.getClass());
                }
                wrapper.accept(context, change);
            }

            private static final class HandlerWrapper<T extends TableChange> {
                private final BiConsumer<ChangeContext, T> handler;

                private HandlerWrapper(BiConsumer<ChangeContext, T> handler) {
                    this.handler = handler;
                }

                private void accept(ChangeContext context, TableChange change) {
                    handler.accept(context, (T) change);
                }
            }
        }

        private static HandlerRegistry createHandlerRegistry() {
            HandlerRegistry registry = new HandlerRegistry();

            // Column operations
            registry.register(AddColumn.class, ChangeContext::addColumn);
            registry.register(ModifyColumn.class, ChangeContext::modifyColumn);
            registry.register(DropColumn.class, ChangeContext::dropColumn);
            registry.register(
                    ModifyPhysicalColumnType.class, ChangeContext::modifyPhysicalColumnType);
            registry.register(ModifyColumnComment.class, ChangeContext::modifyColumnComment);
            registry.register(ModifyColumnPosition.class, ChangeContext::modifyColumnPosition);

            // Query operations
            registry.register(ModifyDefinitionQuery.class, ChangeContext::modifyDefinitionQuery);

            // Constraint operations
            registry.register(AddUniqueConstraint.class, ChangeContext::addUniqueConstraint);
            registry.register(ModifyUniqueConstraint.class, ChangeContext::modifyUniqueConstraint);
            registry.register(DropConstraint.class, ChangeContext::dropConstraint);

            // Watermark operations
            registry.register(AddWatermark.class, ChangeContext::addWatermark);
            registry.register(ModifyWatermark.class, ChangeContext::modifyWatermark);
            registry.register(DropWatermark.class, ChangeContext::dropWatermark);

            // Refresh operations
            registry.register(ModifyRefreshHandler.class, ChangeContext::modifyRefreshHandler);
            registry.register(ModifyRefreshStatus.class, ChangeContext::modifyRefreshStatus);

            // Distribution operations
            registry.register(AddDistribution.class, ChangeContext::addDistribution);
            registry.register(ModifyDistribution.class, ChangeContext::modifyDistribution);
            registry.register(DropDistribution.class, ChangeContext::dropDistribution);

            return registry;
        }

        private void applyTableChanges(List<TableChange> tableChanges) {
            isQueryChange = tableChanges.stream().anyMatch(t -> t instanceof ModifyDefinitionQuery);
            Schema oldSchema = oldTable.getUnresolvedSchema();
            if (isQueryChange) {
                checkForChangedPositionByQuery(tableChanges, oldSchema);
            }

            for (TableChange tableChange : tableChanges) {
                HANDLER_REGISTRY.apply(this, tableChange);
            }

            if (droppedPersistedCnt > 0 && isQueryChange) {
                final int schemaSize = oldSchema.getColumns().size();
                throw new ValidationException(
                        String.format(
                                "Failed to modify query because drop column is unsupported. "
                                        + "When modifying a query, you can only append new columns at the end of original schema. "
                                        + "The original schema has %d columns, but the newly derived schema from the query has %d columns.",
                                schemaSize, schemaSize - droppedPersistedCnt));
            }
        }

        private Schema retrieveSchema() {
            Schema.Builder schemaToApply = Schema.newBuilder().fromColumns(columns);
            if (primaryKeyColumns != null) {
                if (primaryKeyName == null) {
                    schemaToApply.primaryKey(primaryKeyColumns);
                } else {
                    schemaToApply.primaryKeyNamed(primaryKeyName, primaryKeyColumns);
                }
            }

            for (UnresolvedWatermarkSpec spec : watermarkSpecs) {
                schemaToApply.watermark(spec.getColumnName(), spec.getWatermarkExpression());
            }
            return schemaToApply.build();
        }

        private void addColumn(AddColumn addColumn) {
            Column column = addColumn.getColumn();
            ColumnPosition position = addColumn.getPosition();
            UnresolvedColumn columnToAdd = toUnresolvedColumn(column);
            setColumnAtPosition(columnToAdd, position);
        }

        private void modifyColumn(ModifyColumn modifyColumn) {
            Column column = modifyColumn.getOldColumn();
            Column newColumn = modifyColumn.getNewColumn();
            int index = getColumnIndex(column.getName());
            UnresolvedColumn newColumn1 = toUnresolvedColumn(newColumn);
            columns.set(index, newColumn1);
        }

        private void dropColumn(DropColumn dropColumn) {
            String droppedColumnName = dropColumn.getColumnName();
            int index = getColumnIndex(droppedColumnName);
            UnresolvedColumn column = columns.get(index);
            if (isQueryChange && isNonPersistedColumn(column)) {
                // noop
            } else {
                columns.remove(index);
                droppedPersistedCnt++;
            }
        }

        private void modifyPhysicalColumnType(ModifyPhysicalColumnType modifyPhysicalColumnType) {
            Column column = modifyPhysicalColumnType.getOldColumn();
            int position = getColumnIndex(column.getName());
            columns.set(position, toUnresolvedColumn(modifyPhysicalColumnType.getNewColumn()));
        }

        private void modifyColumnComment(ModifyColumnComment modifyColumnComment) {
            Column column = modifyColumnComment.getOldColumn();
            int position = getColumnIndex(column.getName());
            columns.set(position, toUnresolvedColumn(modifyColumnComment.getNewColumn()));
        }

        private void modifyColumnPosition(ModifyColumnPosition columnWithChangedPosition) {
            Column column = columnWithChangedPosition.getOldColumn();
            int oldPosition = getColumnIndex(column.getName());
            if (isQueryChange) {
                throw new ValidationException(
                        String.format(
                                "When modifying the query of a materialized table, "
                                        + "currently only support appending columns at the end of original schema, dropping, renaming, and reordering columns are not supported.\n"
                                        + "Column mismatch at position %d: Original column is [%s], but new column is [%s].",
                                oldPosition, column, column));
            }

            ColumnPosition position = columnWithChangedPosition.getNewPosition();
            UnresolvedColumn changedPositionColumn = columns.get(oldPosition);
            columns.remove(oldPosition);
            setColumnAtPosition(changedPositionColumn, position);
        }

        private void modifyDefinitionQuery(ModifyDefinitionQuery queryChange) {
            expandedQuery = queryChange.getDefinitionQuery();
            originalQuery = queryChange.getOriginalQuery();
        }

        private boolean isNonPersistedColumn(UnresolvedColumn column) {
            return column instanceof UnresolvedComputedColumn
                    || column instanceof UnresolvedMetadataColumn
                            && ((UnresolvedMetadataColumn) column).isVirtual();
        }

        private void addUniqueConstraint(AddUniqueConstraint addUniqueConstraint) {
            final UniqueConstraint constraint = addUniqueConstraint.getConstraint();
            primaryKeyName = constraint.getName();
            primaryKeyColumns = constraint.getColumns();
        }

        private void modifyUniqueConstraint(ModifyUniqueConstraint modifyUniqueConstraint) {
            final UniqueConstraint constraint = modifyUniqueConstraint.getNewConstraint();
            primaryKeyName = constraint.getName();
            primaryKeyColumns = constraint.getColumns();
        }

        private void dropConstraint(DropConstraint dropConstraint) {
            primaryKeyName = null;
            primaryKeyColumns = null;
        }

        private void addWatermark(AddWatermark addWatermark) {
            final WatermarkSpec spec = addWatermark.getWatermark();
            String rowTimeAttribute = spec.getRowtimeAttribute();
            ResolvedExpression expression = spec.getWatermarkExpression();
            watermarkSpecs = List.of(new UnresolvedWatermarkSpec(rowTimeAttribute, expression));
        }

        private void modifyWatermark(ModifyWatermark modifyWatermark) {
            final WatermarkSpec spec = modifyWatermark.getNewWatermark();
            String rowTimeAttribute = spec.getRowtimeAttribute();
            ResolvedExpression expression = spec.getWatermarkExpression();
            watermarkSpecs = List.of(new UnresolvedWatermarkSpec(rowTimeAttribute, expression));
        }

        private void dropWatermark(DropWatermark dropWatermark) {
            watermarkSpecs = List.of();
        }

        private void modifyRefreshHandler(ModifyRefreshHandler refreshHandler) {
            refreshHandlerDesc = refreshHandler.getRefreshHandlerDesc();
            refreshHandlerBytes = refreshHandler.getRefreshHandlerBytes();
        }

        private void modifyRefreshStatus(ModifyRefreshStatus modifyRefreshStatus) {
            refreshStatus = modifyRefreshStatus.getRefreshStatus();
        }

        private void addDistribution(AddDistribution addDistribution) {
            distribution = addDistribution.getDistribution();
        }

        private void modifyDistribution(ModifyDistribution modifyDistribution) {
            distribution = modifyDistribution.getDistribution();
        }

        private void dropDistribution(DropDistribution dropDistribution) {
            distribution = null;
        }

        private UnresolvedColumn toUnresolvedColumn(Column column) {
            final String name = column.getName();
            final String comment = column.getComment().orElse(null);
            final DataType type = column.getDataType();
            if (column instanceof PhysicalColumn) {
                return new UnresolvedPhysicalColumn(name, type, comment);
            } else if (column instanceof MetadataColumn) {
                final MetadataColumn metadataColumn = (MetadataColumn) column;
                final String metadataKey = metadataColumn.getMetadataKey().orElse(null);
                return new UnresolvedMetadataColumn(
                        name, type, metadataKey, metadataColumn.isVirtual(), comment);
            } else {
                return new UnresolvedComputedColumn(
                        name, ((ComputedColumn) column).getExpression(), comment);
            }
        }

        private void checkForChangedPositionByQuery(
                List<TableChange> tableChanges, Schema oldSchema) {
            List<ModifyColumnPosition> positionChanges =
                    tableChanges.stream()
                            .filter(t -> t instanceof ModifyColumnPosition)
                            .map(t -> (ModifyColumnPosition) t)
                            .collect(Collectors.toList());

            List<ModifyPhysicalColumnType> physicalTypeChanges =
                    tableChanges.stream()
                            .filter(t -> t instanceof ModifyPhysicalColumnType)
                            .map(t -> (ModifyPhysicalColumnType) t)
                            .collect(Collectors.toList());

            if (positionChanges.isEmpty() && physicalTypeChanges.isEmpty()) {
                return;
            }

            int persistedColumnOffset = 0;
            List<UnresolvedColumn> oldColumns = oldSchema.getColumns();
            for (UnresolvedColumn column : oldColumns) {
                if (!isNonPersistedColumn(column)) {
                    persistedColumnOffset++;
                }
            }

            Map<String, Column> afterToColumnName = new HashMap<>();
            for (ModifyColumnPosition change : positionChanges) {
                final ColumnPosition position = change.getNewPosition();
                final Column newColumn = change.getNewColumn();
                if (position == ColumnPosition.first()) {
                    if (persistedColumnOffset == 0) {
                        throwPositionChangeError(
                                newColumn.asSummaryString(),
                                oldColumns.get(persistedColumnOffset).toString(),
                                persistedColumnOffset);
                    } else {
                        afterToColumnName.put(
                                oldColumns.get(persistedColumnOffset).getName(), newColumn);
                    }
                } else {
                    afterToColumnName.put(((After) position).column(), newColumn);
                }
            }

            for (int i = 1; i < oldColumns.size(); i++) {
                Column newColumn = afterToColumnName.get(oldColumns.get(i).getName());
                if (newColumn != null) {
                    throwPositionChangeError(oldColumns.get(i + 1), newColumn, i + 1);
                }
            }

            Map<String, Integer> nameToIndex = new HashMap<>();
            for (int i = 0; i < oldColumns.size(); i++) {
                UnresolvedColumn column = oldColumns.get(i);
                if (!isNonPersistedColumn(column)) {
                    nameToIndex.put(column.getName(), i);
                }
            }

            for (ModifyPhysicalColumnType change : physicalTypeChanges) {
                final int index = nameToIndex.get(change.getOldColumn().getName());
                throwPositionChangeError(change.getOldColumn(), change.getNewColumn(), index);
            }
        }

        private static void throwPositionChangeError(
                UnresolvedColumn oldColumn, Column newColumn, int position) {
            throwPositionChangeError(oldColumn.toString(), newColumn.asSummaryString(), position);
        }

        private static void throwPositionChangeError(
                Column oldColumn, Column newColumn, int position) {
            throwPositionChangeError(
                    oldColumn.asSummaryString(), newColumn.asSummaryString(), position);
        }

        private static void throwPositionChangeError(
                String oldColumn, String newColumn, int position) {
            throw new ValidationException(
                    String.format(
                            "When modifying the query of a materialized table, "
                                    + "currently only support appending columns at the end of original schema, dropping, renaming, and reordering columns are not supported.\n"
                                    + "Column mismatch at position %d: Original column is [%s], but new column is [%s].",
                            position, oldColumn, newColumn));
        }

        private void setColumnAtPosition(UnresolvedColumn column, ColumnPosition position) {
            if (position == null) {
                columns.add(column);
            } else if (position == ColumnPosition.first()) {
                columns.add(0, column);
            } else {
                String after = ((After) position).column();
                int index = getColumnIndex(after);

                columns.add(index + 1, column);
            }
        }

        private int getColumnIndex(String name) {
            for (int i = 0; i < columns.size(); i++) {
                if (Objects.equals(name, columns.get(i).getName())) {
                    return i;
                }
            }
            return -1;
        }
    }
}
