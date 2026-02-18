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
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/** Applying table changes to old materialized table and gathering validation errors. */
@Internal
public class MaterializedTableChangeHandler {
    private static final HandlerRegistry HANDLER_REGISTRY = createHandlerRegistry();

    private final List<Schema.UnresolvedColumn> columns;
    private final CatalogMaterializedTable oldTable;
    private boolean isQueryChange;
    private @Nullable TableDistribution distribution;
    private CatalogMaterializedTable.RefreshStatus refreshStatus;
    private @Nullable String refreshHandlerDesc;
    private byte[] refreshHandlerBytes;
    private List<Schema.UnresolvedWatermarkSpec> watermarkSpecs;
    private String primaryKeyName = null;
    private List<String> primaryKeyColumns = null;
    private int droppedPersistedCnt = 0;
    private String originalQuery;
    private String expandedQuery;
    private final List<String> validationErrors = new ArrayList<>();

    public MaterializedTableChangeHandler(CatalogMaterializedTable oldTable) {
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
        private static final Map<Class<? extends TableChange>, HandlerRegistry.HandlerWrapper<?>>
                HANDLERS = new IdentityHashMap<>();

        private <T extends TableChange> void register(
                Class<T> type, BiConsumer<MaterializedTableChangeHandler, T> handler) {
            HANDLERS.put(type, new HandlerRegistry.HandlerWrapper<>(handler));
        }

        private void apply(MaterializedTableChangeHandler context, TableChange change) {
            HandlerRegistry.HandlerWrapper<?> wrapper = HANDLERS.get(change.getClass());
            if (wrapper == null) {
                context.validationErrors.add("Unknown table change " + change.getClass());
            } else {
                wrapper.accept(context, change);
            }
        }

        private static final class HandlerWrapper<T extends TableChange> {
            private final BiConsumer<MaterializedTableChangeHandler, T> handler;

            private HandlerWrapper(BiConsumer<MaterializedTableChangeHandler, T> handler) {
                this.handler = handler;
            }

            private void accept(MaterializedTableChangeHandler context, TableChange change) {
                handler.accept(context, (T) change);
            }
        }
    }

    public static MaterializedTableChangeResult buildNewMaterializedTable(
            CatalogMaterializedTable oldTable, List<TableChange> tableChanges) {
        MaterializedTableChangeHandler context = new MaterializedTableChangeHandler(oldTable);
        context.applyTableChanges(tableChanges);
        CatalogMaterializedTable materializedTable =
                CatalogMaterializedTable.newBuilder()
                        .schema(context.retrieveSchema())
                        .comment(oldTable.getComment())
                        .partitionKeys(oldTable.getPartitionKeys())
                        .options(oldTable.getOptions())
                        .originalQuery(context.originalQuery)
                        .expandedQuery(context.expandedQuery)
                        .distribution(context.distribution)
                        .freshness(oldTable.getDefinitionFreshness())
                        .logicalRefreshMode(oldTable.getLogicalRefreshMode())
                        .refreshMode(oldTable.getRefreshMode())
                        .refreshStatus(context.refreshStatus)
                        .refreshHandlerDescription(context.refreshHandlerDesc)
                        .serializedRefreshHandler(context.refreshHandlerBytes)
                        .build();
        return new MaterializedTableChangeResult(materializedTable, context.validationErrors);
    }

    private static HandlerRegistry createHandlerRegistry() {
        HandlerRegistry registry = new HandlerRegistry();

        // Column operations
        registry.register(TableChange.AddColumn.class, MaterializedTableChangeHandler::addColumn);
        registry.register(
                TableChange.ModifyColumn.class, MaterializedTableChangeHandler::modifyColumn);
        registry.register(TableChange.DropColumn.class, MaterializedTableChangeHandler::dropColumn);
        registry.register(
                TableChange.ModifyPhysicalColumnType.class,
                MaterializedTableChangeHandler::modifyPhysicalColumnType);
        registry.register(
                TableChange.ModifyColumnComment.class,
                MaterializedTableChangeHandler::modifyColumnComment);
        registry.register(
                TableChange.ModifyColumnPosition.class,
                MaterializedTableChangeHandler::modifyColumnPosition);

        // Query operations
        registry.register(
                TableChange.ModifyDefinitionQuery.class,
                MaterializedTableChangeHandler::modifyDefinitionQuery);

        // Constraint operations
        registry.register(
                TableChange.AddUniqueConstraint.class,
                MaterializedTableChangeHandler::addUniqueConstraint);
        registry.register(
                TableChange.ModifyUniqueConstraint.class,
                MaterializedTableChangeHandler::modifyUniqueConstraint);
        registry.register(
                TableChange.DropConstraint.class, MaterializedTableChangeHandler::dropConstraint);

        // Watermark operations
        registry.register(
                TableChange.AddWatermark.class, MaterializedTableChangeHandler::addWatermark);
        registry.register(
                TableChange.ModifyWatermark.class, MaterializedTableChangeHandler::modifyWatermark);
        registry.register(
                TableChange.DropWatermark.class, MaterializedTableChangeHandler::dropWatermark);

        // Refresh operations
        registry.register(
                TableChange.ModifyRefreshHandler.class,
                MaterializedTableChangeHandler::modifyRefreshHandler);
        registry.register(
                TableChange.ModifyRefreshStatus.class,
                MaterializedTableChangeHandler::modifyRefreshStatus);

        // Distribution operations
        registry.register(
                TableChange.AddDistribution.class, MaterializedTableChangeHandler::addDistribution);
        registry.register(
                TableChange.ModifyDistribution.class,
                MaterializedTableChangeHandler::modifyDistribution);
        registry.register(
                TableChange.DropDistribution.class,
                MaterializedTableChangeHandler::dropDistribution);

        return registry;
    }

    void applyTableChanges(List<TableChange> tableChanges) {
        isQueryChange =
                tableChanges.stream().anyMatch(t -> t instanceof TableChange.ModifyDefinitionQuery);
        Schema oldSchema = oldTable.getUnresolvedSchema();
        if (isQueryChange) {
            checkForChangedPositionByQuery(tableChanges, oldSchema);
        }

        for (TableChange tableChange : tableChanges) {
            HANDLER_REGISTRY.apply(this, tableChange);
        }

        if (droppedPersistedCnt > 0 && isQueryChange) {
            final int schemaSize = oldSchema.getColumns().size();
            validationErrors.add(
                    String.format(
                            "Failed to modify query because drop column is unsupported. "
                                    + "When modifying a query, you can only append new columns at the end of original schema. "
                                    + "The original schema has %d columns, but the newly derived schema from the query has %d columns.",
                            schemaSize, schemaSize - droppedPersistedCnt));
        }
    }

    Schema retrieveSchema() {
        Schema.Builder schemaToApply = Schema.newBuilder().fromColumns(columns);
        if (primaryKeyColumns != null) {
            if (primaryKeyName == null) {
                schemaToApply.primaryKey(primaryKeyColumns);
            } else {
                schemaToApply.primaryKeyNamed(primaryKeyName, primaryKeyColumns);
            }
        }

        for (Schema.UnresolvedWatermarkSpec spec : watermarkSpecs) {
            schemaToApply.watermark(spec.getColumnName(), spec.getWatermarkExpression());
        }
        return schemaToApply.build();
    }

    private void addColumn(TableChange.AddColumn addColumn) {
        Column column = addColumn.getColumn();
        TableChange.ColumnPosition position = addColumn.getPosition();
        Schema.UnresolvedColumn columnToAdd = toUnresolvedColumn(column);
        setColumnAtPosition(columnToAdd, position);
    }

    private void modifyColumn(TableChange.ModifyColumn modifyColumn) {
        Column column = modifyColumn.getOldColumn();
        Column newColumn = modifyColumn.getNewColumn();
        int index = getColumnIndex(column.getName());
        Schema.UnresolvedColumn newColumn1 = toUnresolvedColumn(newColumn);
        columns.set(index, newColumn1);
    }

    private void dropColumn(TableChange.DropColumn dropColumn) {
        String droppedColumnName = dropColumn.getColumnName();
        int index = getColumnIndex(droppedColumnName);
        Schema.UnresolvedColumn column = columns.get(index);
        if (isQueryChange && isNonPersistedColumn(column)) {
            // noop
        } else {
            columns.remove(index);
            droppedPersistedCnt++;
        }
    }

    private void modifyPhysicalColumnType(
            TableChange.ModifyPhysicalColumnType modifyPhysicalColumnType) {
        Column column = modifyPhysicalColumnType.getOldColumn();
        int position = getColumnIndex(column.getName());
        columns.set(position, toUnresolvedColumn(modifyPhysicalColumnType.getNewColumn()));
    }

    private void modifyColumnComment(TableChange.ModifyColumnComment modifyColumnComment) {
        Column column = modifyColumnComment.getOldColumn();
        int position = getColumnIndex(column.getName());
        columns.set(position, toUnresolvedColumn(modifyColumnComment.getNewColumn()));
    }

    private void modifyColumnPosition(TableChange.ModifyColumnPosition columnWithChangedPosition) {
        Column column = columnWithChangedPosition.getOldColumn();
        int oldPosition = getColumnIndex(column.getName());
        if (isQueryChange) {
            validationErrors.add(
                    String.format(
                            "When modifying the query of a materialized table, "
                                    + "currently only support appending columns at the end of original schema, dropping, renaming, and reordering columns are not supported.\n"
                                    + "Column mismatch at position %d: Original column is [%s], but new column is [%s].",
                            oldPosition, column, column));
        }

        TableChange.ColumnPosition position = columnWithChangedPosition.getNewPosition();
        Schema.UnresolvedColumn changedPositionColumn = columns.get(oldPosition);
        columns.remove(oldPosition);
        setColumnAtPosition(changedPositionColumn, position);
    }

    private void modifyDefinitionQuery(TableChange.ModifyDefinitionQuery queryChange) {
        expandedQuery = queryChange.getDefinitionQuery();
        originalQuery = queryChange.getOriginalQuery();
    }

    private boolean isNonPersistedColumn(Schema.UnresolvedColumn column) {
        return column instanceof Schema.UnresolvedComputedColumn
                || column instanceof Schema.UnresolvedMetadataColumn
                        && ((Schema.UnresolvedMetadataColumn) column).isVirtual();
    }

    private void addUniqueConstraint(TableChange.AddUniqueConstraint addUniqueConstraint) {
        final UniqueConstraint constraint = addUniqueConstraint.getConstraint();
        primaryKeyName = constraint.getName();
        primaryKeyColumns = constraint.getColumns();
    }

    private void modifyUniqueConstraint(TableChange.ModifyUniqueConstraint modifyUniqueConstraint) {
        final UniqueConstraint constraint = modifyUniqueConstraint.getNewConstraint();
        primaryKeyName = constraint.getName();
        primaryKeyColumns = constraint.getColumns();
    }

    private void dropConstraint(TableChange.DropConstraint dropConstraint) {
        primaryKeyName = null;
        primaryKeyColumns = null;
    }

    private void addWatermark(TableChange.AddWatermark addWatermark) {
        final WatermarkSpec spec = addWatermark.getWatermark();
        String rowTimeAttribute = spec.getRowtimeAttribute();
        ResolvedExpression expression = spec.getWatermarkExpression();
        watermarkSpecs = List.of(new Schema.UnresolvedWatermarkSpec(rowTimeAttribute, expression));
    }

    private void modifyWatermark(TableChange.ModifyWatermark modifyWatermark) {
        final WatermarkSpec spec = modifyWatermark.getNewWatermark();
        String rowTimeAttribute = spec.getRowtimeAttribute();
        ResolvedExpression expression = spec.getWatermarkExpression();
        watermarkSpecs = List.of(new Schema.UnresolvedWatermarkSpec(rowTimeAttribute, expression));
    }

    private void dropWatermark(TableChange.DropWatermark dropWatermark) {
        watermarkSpecs = List.of();
    }

    private void modifyRefreshHandler(TableChange.ModifyRefreshHandler refreshHandler) {
        refreshHandlerDesc = refreshHandler.getRefreshHandlerDesc();
        refreshHandlerBytes = refreshHandler.getRefreshHandlerBytes();
    }

    private void modifyRefreshStatus(TableChange.ModifyRefreshStatus modifyRefreshStatus) {
        refreshStatus = modifyRefreshStatus.getRefreshStatus();
    }

    private void addDistribution(TableChange.AddDistribution addDistribution) {
        distribution = addDistribution.getDistribution();
    }

    private void modifyDistribution(TableChange.ModifyDistribution modifyDistribution) {
        distribution = modifyDistribution.getDistribution();
    }

    private void dropDistribution(TableChange.DropDistribution dropDistribution) {
        distribution = null;
    }

    private Schema.UnresolvedColumn toUnresolvedColumn(Column column) {
        final String name = column.getName();
        final String comment = column.getComment().orElse(null);
        final DataType type = column.getDataType();
        if (column instanceof Column.PhysicalColumn) {
            return new Schema.UnresolvedPhysicalColumn(name, type, comment);
        } else if (column instanceof Column.MetadataColumn) {
            final Column.MetadataColumn metadataColumn = (Column.MetadataColumn) column;
            final String metadataKey = metadataColumn.getMetadataKey().orElse(null);
            return new Schema.UnresolvedMetadataColumn(
                    name, type, metadataKey, metadataColumn.isVirtual(), comment);
        } else {
            return new Schema.UnresolvedComputedColumn(
                    name, ((Column.ComputedColumn) column).getExpression(), comment);
        }
    }

    private void checkForChangedPositionByQuery(List<TableChange> tableChanges, Schema oldSchema) {
        List<TableChange.ModifyColumnPosition> positionChanges =
                tableChanges.stream()
                        .filter(t -> t instanceof TableChange.ModifyColumnPosition)
                        .map(t -> (TableChange.ModifyColumnPosition) t)
                        .collect(Collectors.toList());

        List<TableChange.ModifyPhysicalColumnType> physicalTypeChanges =
                tableChanges.stream()
                        .filter(t -> t instanceof TableChange.ModifyPhysicalColumnType)
                        .map(t -> (TableChange.ModifyPhysicalColumnType) t)
                        .collect(Collectors.toList());

        if (positionChanges.isEmpty() && physicalTypeChanges.isEmpty()) {
            return;
        }

        int persistedColumnOffset = 0;
        List<Schema.UnresolvedColumn> oldColumns = oldSchema.getColumns();
        for (Schema.UnresolvedColumn column : oldColumns) {
            if (!isNonPersistedColumn(column)) {
                persistedColumnOffset++;
            }
        }

        Map<String, Column> afterToColumnName = new HashMap<>();
        for (TableChange.ModifyColumnPosition change : positionChanges) {
            final TableChange.ColumnPosition position = change.getNewPosition();
            final Column newColumn = change.getNewColumn();
            if (position == TableChange.ColumnPosition.first()) {
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
                afterToColumnName.put(((TableChange.After) position).column(), newColumn);
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
            Schema.UnresolvedColumn column = oldColumns.get(i);
            if (!isNonPersistedColumn(column)) {
                nameToIndex.put(column.getName(), i);
            }
        }

        for (TableChange.ModifyPhysicalColumnType change : physicalTypeChanges) {
            final int index = nameToIndex.get(change.getOldColumn().getName());
            throwPositionChangeError(change.getOldColumn(), change.getNewColumn(), index);
        }
    }

    private static void throwPositionChangeError(
            Schema.UnresolvedColumn oldColumn, Column newColumn, int position) {
        throwPositionChangeError(oldColumn.toString(), newColumn.asSummaryString(), position);
    }

    private static void throwPositionChangeError(Column oldColumn, Column newColumn, int position) {
        throwPositionChangeError(
                oldColumn.asSummaryString(), newColumn.asSummaryString(), position);
    }

    private static void throwPositionChangeError(String oldColumn, String newColumn, int position) {
        throw new ValidationException(
                String.format(
                        "When modifying the query of a materialized table, "
                                + "currently only support appending columns at the end of original schema, dropping, renaming, and reordering columns are not supported.\n"
                                + "Column mismatch at position %d: Original column is [%s], but new column is [%s].",
                        position, oldColumn, newColumn));
    }

    private void setColumnAtPosition(
            Schema.UnresolvedColumn column, TableChange.ColumnPosition position) {
        if (position == null) {
            columns.add(column);
        } else if (position == TableChange.ColumnPosition.first()) {
            columns.add(0, column);
        } else {
            String after = ((TableChange.After) position).column();
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

    @Internal
    public static class MaterializedTableChangeResult {
        private final CatalogMaterializedTable newMaterializedTable;
        private final List<String> validationErrors;

        public MaterializedTableChangeResult(
                CatalogMaterializedTable newTable, List<String> validationErrors) {
            this.newMaterializedTable = newTable;
            this.validationErrors = validationErrors;
        }

        public CatalogMaterializedTable getNewMaterializedTable() {
            return newMaterializedTable;
        }

        public List<String> getValidationErrors() {
            return validationErrors;
        }
    }
}
