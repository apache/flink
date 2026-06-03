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
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.api.Schema.UnresolvedWatermarkSpec;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.Column.MetadataColumn;
import org.apache.flink.table.catalog.StartMode;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.TableChange.AddColumn;
import org.apache.flink.table.catalog.TableChange.AddDistribution;
import org.apache.flink.table.catalog.TableChange.AddUniqueConstraint;
import org.apache.flink.table.catalog.TableChange.AddWatermark;
import org.apache.flink.table.catalog.TableChange.After;
import org.apache.flink.table.catalog.TableChange.ColumnPosition;
import org.apache.flink.table.catalog.TableChange.DropConstraint;
import org.apache.flink.table.catalog.TableChange.DropDistribution;
import org.apache.flink.table.catalog.TableChange.ModifyColumn;
import org.apache.flink.table.catalog.TableChange.ModifyColumnComment;
import org.apache.flink.table.catalog.TableChange.ModifyColumnPosition;
import org.apache.flink.table.catalog.TableChange.ModifyDefinitionQuery;
import org.apache.flink.table.catalog.TableChange.ModifyDistribution;
import org.apache.flink.table.catalog.TableChange.ModifyPhysicalColumnType;
import org.apache.flink.table.catalog.TableChange.ModifyRefreshHandler;
import org.apache.flink.table.catalog.TableChange.ModifyRefreshStatus;
import org.apache.flink.table.catalog.TableChange.ModifyStartMode;
import org.apache.flink.table.catalog.TableChange.ModifyUniqueConstraint;
import org.apache.flink.table.catalog.TableChange.ModifyWatermark;
import org.apache.flink.table.catalog.TableChange.ResetOption;
import org.apache.flink.table.catalog.TableChange.SetOption;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

/** Applies table changes to a materialized table and builds the resulting definition. */
@Internal
public class MaterializedTableChangeHandler {
    private static final HandlerRegistry HANDLER_REGISTRY = createHandlerRegistry();

    private final List<UnresolvedColumn> columns;
    private final CatalogMaterializedTable oldTable;
    private @Nullable TableDistribution distribution;
    private CatalogMaterializedTable.RefreshStatus refreshStatus;
    private @Nullable String refreshHandlerDesc;
    private byte[] refreshHandlerBytes;
    private List<UnresolvedWatermarkSpec> watermarkSpecs;
    private String primaryKeyName = null;
    private List<String> primaryKeyColumns = null;
    private String originalQuery;
    private String expandedQuery;
    private StartMode startMode;
    private final Map<String, String> options;

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
        startMode = oldTable.getStartMode().orElse(null);
        this.oldTable = oldTable;
        this.options = new HashMap<>(oldTable.getOptions());
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
                throw new ValidationException(
                        "Unsupported table change: " + change.getClass().getName());
            }
            wrapper.accept(context, change);
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

    public static MaterializedTableChangeHandler getHandlerWithChanges(
            CatalogMaterializedTable oldTable, List<TableChange> tableChanges) {
        final MaterializedTableChangeHandler handler = new MaterializedTableChangeHandler(oldTable);
        handler.applyTableChanges(tableChanges);
        return handler;
    }

    public static CatalogMaterializedTable buildNewMaterializedTable(
            MaterializedTableChangeHandler context) {
        final CatalogMaterializedTable oldTable = context.getOldTable();
        return CatalogMaterializedTable.newBuilder()
                .schema(context.retrieveSchema())
                .comment(oldTable.getComment())
                .partitionKeys(oldTable.getPartitionKeys())
                .options(context.getOptions())
                .originalQuery(context.getOriginalQuery())
                .expandedQuery(context.getExpandedQuery())
                .distribution(context.getDistribution())
                .freshness(oldTable.getDefinitionFreshness())
                .logicalRefreshMode(oldTable.getLogicalRefreshMode())
                .refreshMode(oldTable.getRefreshMode())
                .refreshStatus(context.getRefreshStatus())
                .refreshHandlerDescription(context.getRefreshHandlerDesc())
                .serializedRefreshHandler(context.getRefreshHandlerBytes())
                .startMode(context.getStartMode())
                .build();
    }

    private static HandlerRegistry createHandlerRegistry() {
        HandlerRegistry registry = new HandlerRegistry();

        // Column operations
        registry.register(AddColumn.class, MaterializedTableChangeHandler::addColumn);
        registry.register(ModifyColumn.class, MaterializedTableChangeHandler::modifyColumn);
        registry.register(TableChange.DropColumn.class, MaterializedTableChangeHandler::dropColumn);
        registry.register(
                ModifyPhysicalColumnType.class,
                MaterializedTableChangeHandler::modifyPhysicalColumnType);
        registry.register(
                ModifyColumnComment.class, MaterializedTableChangeHandler::modifyColumnComment);
        registry.register(
                ModifyColumnPosition.class, MaterializedTableChangeHandler::modifyColumnPosition);

        // Query operations
        registry.register(
                ModifyDefinitionQuery.class, MaterializedTableChangeHandler::modifyDefinitionQuery);

        // Constraint operations
        registry.register(
                AddUniqueConstraint.class, MaterializedTableChangeHandler::addUniqueConstraint);
        registry.register(
                ModifyUniqueConstraint.class,
                MaterializedTableChangeHandler::modifyUniqueConstraint);
        registry.register(DropConstraint.class, MaterializedTableChangeHandler::dropConstraint);

        // Watermark operations
        registry.register(AddWatermark.class, MaterializedTableChangeHandler::addWatermark);
        registry.register(ModifyWatermark.class, MaterializedTableChangeHandler::modifyWatermark);
        registry.register(
                TableChange.DropWatermark.class, MaterializedTableChangeHandler::dropWatermark);

        // Refresh operations
        registry.register(
                ModifyRefreshHandler.class, MaterializedTableChangeHandler::modifyRefreshHandler);
        registry.register(
                ModifyRefreshStatus.class, MaterializedTableChangeHandler::modifyRefreshStatus);

        // Start mode
        registry.register(ModifyStartMode.class, MaterializedTableChangeHandler::modifyStartMode);

        // Distribution operations
        registry.register(AddDistribution.class, MaterializedTableChangeHandler::addDistribution);
        registry.register(
                ModifyDistribution.class, MaterializedTableChangeHandler::modifyDistribution);
        registry.register(DropDistribution.class, MaterializedTableChangeHandler::dropDistribution);

        // Options
        registry.register(SetOption.class, MaterializedTableChangeHandler::setTableOption);
        registry.register(ResetOption.class, MaterializedTableChangeHandler::resetTableOption);

        registry.register(ModifyStartMode.class, MaterializedTableChangeHandler::modifyStartMode);

        return registry;
    }

    void applyTableChanges(List<TableChange> tableChanges) {
        for (TableChange tableChange : tableChanges) {
            HANDLER_REGISTRY.apply(this, tableChange);
        }
    }

    public Schema retrieveSchema() {
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

    public String getExpandedQuery() {
        return expandedQuery;
    }

    public String getOriginalQuery() {
        return originalQuery;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    @Nullable
    public TableDistribution getDistribution() {
        return distribution;
    }

    public byte[] getRefreshHandlerBytes() {
        return refreshHandlerBytes;
    }

    public StartMode getStartMode() {
        return startMode;
    }

    @Nullable
    public String getRefreshHandlerDesc() {
        return refreshHandlerDesc;
    }

    public CatalogMaterializedTable.RefreshStatus getRefreshStatus() {
        return refreshStatus;
    }

    public CatalogMaterializedTable getOldTable() {
        return oldTable;
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
        UnresolvedColumn newUnresolvedColumn = toUnresolvedColumn(newColumn);
        columns.set(index, newUnresolvedColumn);
    }

    private void dropColumn(TableChange.DropColumn dropColumn) {
        columns.remove(getColumnIndex(dropColumn.getColumnName()));
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
        ColumnPosition position = columnWithChangedPosition.getNewPosition();
        UnresolvedColumn changedPositionColumn = columns.get(oldPosition);
        columns.remove(oldPosition);
        setColumnAtPosition(changedPositionColumn, position);
    }

    private void modifyDefinitionQuery(ModifyDefinitionQuery queryChange) {
        expandedQuery = queryChange.getDefinitionQuery();
        originalQuery = queryChange.getOriginalQuery();
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

    private void dropWatermark(TableChange.DropWatermark dropWatermark) {
        watermarkSpecs = List.of();
    }

    private void modifyRefreshHandler(ModifyRefreshHandler refreshHandler) {
        refreshHandlerDesc = refreshHandler.getRefreshHandlerDesc();
        refreshHandlerBytes = refreshHandler.getRefreshHandlerBytes();
    }

    private void modifyRefreshStatus(ModifyRefreshStatus modifyRefreshStatus) {
        refreshStatus = modifyRefreshStatus.getRefreshStatus();
    }

    private void modifyStartMode(ModifyStartMode modifyStartMode) {
        startMode = modifyStartMode.getStartMode();
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

    private void setTableOption(SetOption option) {
        options.put(option.getKey(), option.getValue());
    }

    private void resetTableOption(ResetOption option) {
        options.remove(option.getKey());
    }

    private UnresolvedColumn toUnresolvedColumn(Column column) {
        final String name = column.getName();
        final String comment = column.getComment().orElse(null);
        final DataType type = column.getDataType();
        if (column instanceof Column.PhysicalColumn) {
            return new UnresolvedPhysicalColumn(name, type, comment);
        } else if (column instanceof MetadataColumn) {
            final MetadataColumn metadataColumn = (MetadataColumn) column;
            final String metadataKey = metadataColumn.getMetadataKey().orElse(null);
            return new Schema.UnresolvedMetadataColumn(
                    name, type, metadataKey, metadataColumn.isVirtual(), comment);
        } else {
            return new Schema.UnresolvedComputedColumn(
                    name, ((Column.ComputedColumn) column).getExpression(), comment);
        }
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
