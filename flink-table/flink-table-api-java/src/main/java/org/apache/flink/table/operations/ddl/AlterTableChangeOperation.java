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

package org.apache.flink.table.operations.ddl;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.internal.TableResultImpl;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.utils.EncodingUtils;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** Alter table with new table definition and table changes represents the modification. */
@Internal
public class AlterTableChangeOperation extends AlterTableOperation {

    private final List<TableChange> tableChanges;
    private final CatalogTable newTable;

    public AlterTableChangeOperation(
            ObjectIdentifier tableIdentifier,
            List<TableChange> tableChanges,
            CatalogTable newTable,
            boolean ignoreIfNotExists) {
        super(tableIdentifier, ignoreIfNotExists);
        this.tableChanges = Collections.unmodifiableList(tableChanges);
        this.newTable = newTable;
    }

    public List<TableChange> getTableChanges() {
        return tableChanges;
    }

    public CatalogTable getNewTable() {
        return newTable;
    }

    @Override
    public String asSummaryString() {
        String changes =
                tableChanges.stream().map(this::toString).collect(Collectors.joining(",\n"));
        return String.format(
                "ALTER TABLE %s%s\n%s",
                ignoreIfTableNotExists ? "IF EXISTS " : "",
                tableIdentifier.asSummaryString(),
                changes);
    }

    private String toString(TableChange tableChange) {
        if (tableChange instanceof TableChange.SetOption) {
            TableChange.SetOption setChange = (TableChange.SetOption) tableChange;
            return String.format("  SET '%s' = '%s'", setChange.getKey(), setChange.getValue());
        } else if (tableChange instanceof TableChange.ResetOption) {
            TableChange.ResetOption resetChange = (TableChange.ResetOption) tableChange;
            return String.format("  RESET '%s'", resetChange.getKey());
        } else if (tableChange instanceof TableChange.AddColumn) {
            TableChange.AddColumn addColumn = (TableChange.AddColumn) tableChange;
            return String.format(
                    "  ADD %s %s",
                    addColumn.getColumn(),
                    addColumn.getPosition() == null ? "" : addColumn.getPosition());
        } else if (tableChange instanceof TableChange.AddWatermark) {
            TableChange.AddWatermark addWatermark = (TableChange.AddWatermark) tableChange;
            return String.format("  ADD %s", addWatermark.getWatermark());
        } else if (tableChange instanceof TableChange.AddUniqueConstraint) {
            TableChange.AddUniqueConstraint addUniqueConstraint =
                    (TableChange.AddUniqueConstraint) tableChange;
            return String.format("  ADD %s", addUniqueConstraint.getConstraint());
        } else if (tableChange instanceof TableChange.ModifyColumnComment) {
            TableChange.ModifyColumnComment modifyColumnComment =
                    (TableChange.ModifyColumnComment) tableChange;
            return String.format(
                    "  MODIFY %s COMMENT '%s'",
                    EncodingUtils.escapeIdentifier(modifyColumnComment.getNewColumn().getName()),
                    modifyColumnComment.getNewComment());
        } else if (tableChange instanceof TableChange.ModifyPhysicalColumnType) {
            TableChange.ModifyPhysicalColumnType modifyPhysicalColumnType =
                    (TableChange.ModifyPhysicalColumnType) tableChange;
            return String.format(
                    "  MODIFY %s %s",
                    EncodingUtils.escapeIdentifier(
                            modifyPhysicalColumnType.getNewColumn().getName()),
                    modifyPhysicalColumnType.getNewType());
        } else if (tableChange instanceof TableChange.ModifyColumnPosition) {
            TableChange.ModifyColumnPosition modifyColumnPosition =
                    (TableChange.ModifyColumnPosition) tableChange;
            return String.format(
                    "  MODIFY %s %s",
                    EncodingUtils.escapeIdentifier(modifyColumnPosition.getNewColumn().getName()),
                    modifyColumnPosition.getNewPosition());
        } else if (tableChange instanceof TableChange.ModifyColumnName) {
            TableChange.ModifyColumnName modifyColumnName =
                    (TableChange.ModifyColumnName) tableChange;
            return String.format(
                    "  MODIFY %s TO %s",
                    EncodingUtils.escapeIdentifier(modifyColumnName.getOldColumnName()),
                    EncodingUtils.escapeIdentifier(modifyColumnName.getNewColumnName()));
        } else if (tableChange instanceof TableChange.ModifyColumn) {
            TableChange.ModifyColumn modifyColumn = (TableChange.ModifyColumn) tableChange;
            return String.format(
                    "  MODIFY %s %s",
                    modifyColumn.getNewColumn(),
                    modifyColumn.getNewPosition() == null ? "" : modifyColumn.getNewPosition());
        } else if (tableChange instanceof TableChange.ModifyWatermark) {
            TableChange.ModifyWatermark modifyWatermark = (TableChange.ModifyWatermark) tableChange;
            return String.format("  MODIFY %s", modifyWatermark.getNewWatermark());
        } else if (tableChange instanceof TableChange.ModifyUniqueConstraint) {
            TableChange.ModifyUniqueConstraint modifyUniqueConstraint =
                    (TableChange.ModifyUniqueConstraint) tableChange;
            return String.format("  MODIFY %s", modifyUniqueConstraint.getNewConstraint());
        } else if (tableChange instanceof TableChange.DropColumn) {
            TableChange.DropColumn dropColumn = (TableChange.DropColumn) tableChange;
            return String.format(
                    "  DROP %s", EncodingUtils.escapeIdentifier(dropColumn.getColumnName()));
        } else if (tableChange instanceof TableChange.DropConstraint) {
            TableChange.DropConstraint dropConstraint = (TableChange.DropConstraint) tableChange;
            return String.format("  DROP CONSTRAINT %s", dropConstraint.getConstraintName());
        } else if (tableChange instanceof TableChange.DropWatermark) {
            return "  DROP WATERMARK";
        } else {
            throw new UnsupportedOperationException(
                    String.format("Unknown table change: %s.", tableChange));
        }
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        ctx.getCatalogManager()
                .alterTable(
                        getNewTable(),
                        getTableChanges(),
                        getTableIdentifier(),
                        ignoreIfTableNotExists());
        return TableResultImpl.TABLE_RESULT_OK;
    }
}
