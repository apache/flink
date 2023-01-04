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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Objects;

/** {@link TableChange} represents the modification of the table. */
@PublicEvolving
public interface TableChange {

    /**
     * A table change to add the column at last.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; ADD &lt;column_definition&gt;
     * </pre>
     *
     * @param column the added column definition.
     * @return a TableChange represents the modification.
     */
    static AddColumn add(Column column) {
        return new AddColumn(column, null);
    }

    /**
     * A table change to add the column with specified position.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; ADD &lt;column_definition&gt; &lt;column_position&gt;
     * </pre>
     *
     * @param column the added column definition.
     * @param position added column position.
     * @return a TableChange represents the modification.
     */
    static AddColumn add(Column column, @Nullable ColumnPosition position) {
        return new AddColumn(column, position);
    }

    /**
     * A table change to add a unique constraint.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; ADD PRIMARY KEY (&lt;column_name&gt;...) NOT ENFORCED
     * </pre>
     *
     * @param constraint the added constraint definition.
     * @return a TableChange represents the modification.
     */
    static AddUniqueConstraint add(UniqueConstraint constraint) {
        return new AddUniqueConstraint(constraint);
    }

    /**
     * A table change to add a watermark.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; ADD WATERMARK FOR &lt;row_time&gt; AS &lt;row_time_expression&gt;
     * </pre>
     *
     * @param watermarkSpec the added watermark definition.
     * @return a TableChange represents the modification.
     */
    static AddWatermark add(WatermarkSpec watermarkSpec) {
        return new AddWatermark(watermarkSpec);
    }

    /**
     * A table change to modify a column. The modification includes:
     *
     * <ul>
     *   <li>change column data type
     *   <li>reorder column position
     *   <li>modify column comment
     *   <li>rename column name
     *   <li>change the computed expression
     *   <li>change the metadata column expression
     * </ul>
     *
     * <p>Some fine-grained column changes are represented by the {@link
     * TableChange#modifyPhysicalColumnType}, {@link TableChange#modifyColumnName}, {@link
     * TableChange#modifyColumnComment} and {@link TableChange#modifyColumnPosition}.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; MODIFY &lt;column_definition&gt; COMMENT '&lt;column_comment&gt;' &lt;column_position&gt;
     * </pre>
     *
     * @param oldColumn the definition of the old column.
     * @param newColumn the definition of the new column.
     * @param columnPosition the new position of the column.
     * @return a TableChange represents the modification.
     */
    static ModifyColumn modify(
            Column oldColumn, Column newColumn, @Nullable ColumnPosition columnPosition) {
        return new ModifyColumn(oldColumn, newColumn, columnPosition);
    }

    /**
     * A table change that modify the physical column data type.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; MODIFY &lt;column_name&gt; &lt;new_column_type&gt;
     * </pre>
     *
     * @param oldColumn the definition of the old column.
     * @param newType the type of the new column.
     * @return a TableChange represents the modification.
     */
    static ModifyPhysicalColumnType modifyPhysicalColumnType(Column oldColumn, DataType newType) {
        return new ModifyPhysicalColumnType(oldColumn, newType);
    }

    /**
     * A table change to modify the column name.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; RENAME &lt;old_column_name&gt; TO &lt;new_column_name&gt;
     * </pre>
     *
     * @param oldColumn the definition of the old column.
     * @param newName the name of the new column.
     * @return a TableChange represents the modification.
     */
    static ModifyColumnName modifyColumnName(Column oldColumn, String newName) {
        return new ModifyColumnName(oldColumn, newName);
    }

    /**
     * A table change to modify the column comment.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; MODIFY &lt;column_name&gt; &lt;original_column_type&gt; COMMENT '&lt;new_column_comment&gt;'
     * </pre>
     *
     * @param oldColumn the definition of the old column.
     * @param newComment the modified comment.
     * @return a TableChange represents the modification.
     */
    static ModifyColumnComment modifyColumnComment(Column oldColumn, String newComment) {
        return new ModifyColumnComment(oldColumn, newComment);
    }

    /**
     * A table change to modify the column position.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; MODIFY &lt;column_name&gt; &lt;original_column_type&gt; &lt;column_position&gt;
     * </pre>
     *
     * @param oldColumn the definition of the old column.
     * @param columnPosition the new position of the column.
     * @return a TableChange represents the modification.
     */
    static ModifyColumnPosition modifyColumnPosition(
            Column oldColumn, ColumnPosition columnPosition) {
        return new ModifyColumnPosition(oldColumn, columnPosition);
    }

    /**
     * A table change to modify a unique constraint.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; MODIFY PRIMARY KEY (&lt;column_name&gt;...) NOT ENFORCED;
     * </pre>
     *
     * @param newConstraint the modified constraint definition.
     * @return a TableChange represents the modification.
     */
    static ModifyUniqueConstraint modify(UniqueConstraint newConstraint) {
        return new ModifyUniqueConstraint(newConstraint);
    }

    /**
     * A table change to modify a watermark.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; MODIFY WATERMARK FOR &lt;row_time&gt; AS &lt;row_time_expression&gt;
     * </pre>
     *
     * @param newWatermarkSpec the modified watermark definition.
     * @return a TableChange represents the modification.
     */
    static ModifyWatermark modify(WatermarkSpec newWatermarkSpec) {
        return new ModifyWatermark(newWatermarkSpec);
    }

    /**
     * A table change to drop column.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; DROP COLUMN &lt;column_name&gt;
     * </pre>
     *
     * @param columnName the column to drop.
     * @return a TableChange represents the modification.
     */
    static DropColumn dropColumn(String columnName) {
        return new DropColumn(columnName);
    }

    /**
     * A table change to drop watermark.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; DROP WATERMARK
     * </pre>
     *
     * @return a TableChange represents the modification.
     */
    static DropWatermark dropWatermark() {
        return DropWatermark.INSTANCE;
    }

    /**
     * A table change to drop constraint.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; DROP CONSTRAINT &lt;constraint_name&gt;
     * </pre>
     *
     * @param constraintName the constraint to drop.
     * @return a TableChange represents the modification.
     */
    static DropConstraint dropConstraint(String constraintName) {
        return new DropConstraint(constraintName);
    }

    /**
     * A table change to set the table option.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; SET '&lt;key&gt;' = '&lt;value&gt;';
     * </pre>
     *
     * @param key the option name to set.
     * @param value the option value to set.
     * @return a TableChange represents the modification.
     */
    static SetOption set(String key, String value) {
        return new SetOption(key, value);
    }

    /**
     * A table change to reset the table option.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; RESET '&lt;key&gt;'
     * </pre>
     *
     * @param key the option name to reset.
     * @return a TableChange represents the modification.
     */
    static ResetOption reset(String key) {
        return new ResetOption(key);
    }

    // --------------------------------------------------------------------------------------------
    // Add Change
    // --------------------------------------------------------------------------------------------

    /**
     * A table change to add a column.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; ADD &lt;column_definition&gt; &lt;column_position&gt;
     * </pre>
     */
    @PublicEvolving
    class AddColumn implements TableChange {

        private final Column column;
        private final ColumnPosition position;

        private AddColumn(Column column, ColumnPosition position) {
            this.column = column;
            this.position = position;
        }

        public Column getColumn() {
            return column;
        }

        @Nullable
        public ColumnPosition getPosition() {
            return position;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof AddColumn)) {
                return false;
            }
            AddColumn addColumn = (AddColumn) o;
            return Objects.equals(column, addColumn.column)
                    && Objects.equals(position, addColumn.position);
        }

        @Override
        public int hashCode() {
            return Objects.hash(column, position);
        }

        @Override
        public String toString() {
            return "AddColumn{" + "column=" + column + ", position=" + position + '}';
        }
    }

    /**
     * A table change to add a unique constraint.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; ADD PRIMARY KEY (&lt;column_name&gt;...) NOT ENFORCED;
     * </pre>
     */
    @PublicEvolving
    class AddUniqueConstraint implements TableChange {

        private final UniqueConstraint constraint;

        private AddUniqueConstraint(UniqueConstraint constraint) {
            this.constraint = constraint;
        }

        /** Returns the unique constraint to add. */
        public UniqueConstraint getConstraint() {
            return constraint;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof AddUniqueConstraint)) {
                return false;
            }
            AddUniqueConstraint that = (AddUniqueConstraint) o;
            return Objects.equals(constraint, that.constraint);
        }

        @Override
        public int hashCode() {
            return Objects.hash(constraint);
        }

        @Override
        public String toString() {
            return "AddUniqueConstraint{" + "constraint=" + constraint + '}';
        }
    }

    /**
     * A table change to add a watermark.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; ADD WATERMARK FOR &lt;row_time&gt; AS &lt;row_time_expression&gt;
     * </pre>
     */
    @PublicEvolving
    class AddWatermark implements TableChange {

        private final WatermarkSpec watermarkSpec;

        private AddWatermark(WatermarkSpec watermarkSpec) {
            this.watermarkSpec = watermarkSpec;
        }

        /** Returns the watermark to add. */
        public WatermarkSpec getWatermark() {
            return watermarkSpec;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof AddWatermark)) {
                return false;
            }
            AddWatermark that = (AddWatermark) o;
            return Objects.equals(watermarkSpec, that.watermarkSpec);
        }

        @Override
        public int hashCode() {
            return Objects.hash(watermarkSpec);
        }

        @Override
        public String toString() {
            return "AddWatermark{" + "watermarkSpec=" + watermarkSpec + '}';
        }
    }

    // --------------------------------------------------------------------------------------------
    // Modify Change
    // --------------------------------------------------------------------------------------------

    /**
     * A base schema change to modify a column. The modification includes:
     *
     * <ul>
     *   <li>change column data type
     *   <li>reorder column position
     *   <li>modify column comment
     *   <li>rename column name
     *   <li>change the computed expression
     *   <li>change the metadata column expression
     * </ul>
     *
     * <p>Some fine-grained column changes are defined in the {@link ModifyPhysicalColumnType},
     * {@link ModifyColumnComment}, {@link ModifyColumnPosition} and {@link ModifyColumnName}.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; MODIFY &lt;column_definition&gt; COMMENT '&lt;column_comment&gt;' &lt;column_position&gt;
     * </pre>
     */
    @PublicEvolving
    class ModifyColumn implements TableChange {

        protected final Column oldColumn;
        protected final Column newColumn;

        protected final @Nullable ColumnPosition newPosition;

        public ModifyColumn(
                Column oldColumn, Column newColumn, @Nullable ColumnPosition newPosition) {
            this.oldColumn = oldColumn;
            this.newColumn = newColumn;
            this.newPosition = newPosition;
        }

        /** Returns the original {@link Column} instance. */
        public Column getOldColumn() {
            return oldColumn;
        }

        /** Returns the modified {@link Column} instance. */
        public Column getNewColumn() {
            return newColumn;
        }

        /**
         * Returns the position of the modified {@link Column} instance. When the return value is
         * null, it means modify the column at the original position. When the return value is
         * FIRST, it means move the modified column to the first. When the return value is AFTER, it
         * means move the column after the referred column.
         */
        public @Nullable ColumnPosition getNewPosition() {
            return newPosition;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ModifyColumn)) {
                return false;
            }
            ModifyColumn that = (ModifyColumn) o;
            return Objects.equals(oldColumn, that.oldColumn)
                    && Objects.equals(newColumn, that.newColumn)
                    && Objects.equals(newPosition, that.newPosition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(oldColumn, newColumn, newPosition);
        }

        @Override
        public String toString() {
            return "ModifyColumn{"
                    + "oldColumn="
                    + oldColumn
                    + ", newColumn="
                    + newColumn
                    + ", newPosition="
                    + newPosition
                    + '}';
        }
    }

    /**
     * A table change to modify the column comment.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; MODIFY &lt;column_name&gt; &lt;original_column_type&gt; COMMENT '&lt;new_column_comment&gt;'
     * </pre>
     */
    @PublicEvolving
    class ModifyColumnComment extends ModifyColumn {

        private final String newComment;

        private ModifyColumnComment(Column oldColumn, String newComment) {
            super(oldColumn, oldColumn.withComment(newComment), null);
            this.newComment = newComment;
        }

        /** Get the new comment for the column. */
        public String getNewComment() {
            return newComment;
        }

        @Override
        public boolean equals(Object o) {
            return (o instanceof ModifyColumnComment) && super.equals(o);
        }

        @Override
        public String toString() {
            return "ModifyColumnComment{"
                    + "Column="
                    + oldColumn
                    + ", newComment='"
                    + newComment
                    + '\''
                    + '}';
        }
    }

    /**
     * A table change to modify the column position.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; MODIFY &lt;column_name&gt; &lt;original_column_type&gt; &lt;column_position&gt;
     * </pre>
     */
    @PublicEvolving
    class ModifyColumnPosition extends ModifyColumn {

        public ModifyColumnPosition(Column oldColumn, ColumnPosition newPosition) {
            super(oldColumn, oldColumn, newPosition);
        }

        @Override
        public boolean equals(Object o) {
            return (o instanceof ModifyColumnPosition) && super.equals(o);
        }

        @Override
        public String toString() {
            return "ModifyColumnPosition{"
                    + "Column="
                    + oldColumn
                    + ", newPosition="
                    + newPosition
                    + '}';
        }
    }

    /**
     * A table change that modify the physical column data type.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; MODIFY &lt;column_name&gt; &lt;new_column_type&gt;
     * </pre>
     */
    @PublicEvolving
    class ModifyPhysicalColumnType extends ModifyColumn {

        private ModifyPhysicalColumnType(Column oldColumn, DataType newType) {
            super(oldColumn, oldColumn.copy(newType), null);
            Preconditions.checkArgument(oldColumn.isPhysical());
        }

        /** Get the column type for the new column. */
        public DataType getNewType() {
            return newColumn.getDataType();
        }

        @Override
        public boolean equals(Object o) {
            return (o instanceof ModifyPhysicalColumnType) && super.equals(o);
        }

        @Override
        public String toString() {
            return "ModifyPhysicalColumnType{"
                    + "Column="
                    + oldColumn
                    + ", newType="
                    + getNewType()
                    + '}';
        }
    }

    /**
     * A table change to modify the column name.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; RENAME &lt;old_column_name&gt; TO &lt;new_column_name&gt;
     * </pre>
     */
    @PublicEvolving
    class ModifyColumnName extends ModifyColumn {

        private ModifyColumnName(Column oldColumn, String newName) {
            super(oldColumn, createNewColumn(oldColumn, newName), null);
        }

        private static Column createNewColumn(Column oldColumn, String newName) {
            if (oldColumn instanceof Column.PhysicalColumn) {
                return Column.physical(newName, oldColumn.getDataType())
                        .withComment(oldColumn.comment);
            } else if (oldColumn instanceof Column.MetadataColumn) {
                Column.MetadataColumn metadataColumn = (Column.MetadataColumn) oldColumn;
                return Column.metadata(
                                newName,
                                oldColumn.getDataType(),
                                metadataColumn.getMetadataKey().orElse(null),
                                metadataColumn.isVirtual())
                        .withComment(oldColumn.comment);
            } else {
                return Column.computed(newName, ((Column.ComputedColumn) oldColumn).getExpression())
                        .withComment(oldColumn.comment);
            }
        }

        /** Returns the origin column name. */
        public String getOldColumnName() {
            return oldColumn.getName();
        }

        /** Returns the new column name after renaming the column name. */
        public String getNewColumnName() {
            return newColumn.getName();
        }

        @Override
        public boolean equals(Object o) {
            return (o instanceof ModifyColumnName) && super.equals(o);
        }

        @Override
        public String toString() {
            return "ModifyColumnName{"
                    + "Column="
                    + oldColumn
                    + ", newName="
                    + getNewColumnName()
                    + '}';
        }
    }

    /**
     * A table change to modify a unique constraint.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; MODIFY PRIMARY KEY (&lt;column_name&gt; ...) NOT ENFORCED
     * </pre>
     */
    @PublicEvolving
    class ModifyUniqueConstraint implements TableChange {

        private final UniqueConstraint newConstraint;

        public ModifyUniqueConstraint(UniqueConstraint newConstraint) {
            this.newConstraint = newConstraint;
        }

        /** Returns the modified unique constraint. */
        public UniqueConstraint getNewConstraint() {
            return newConstraint;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ModifyUniqueConstraint)) {
                return false;
            }
            ModifyUniqueConstraint that = (ModifyUniqueConstraint) o;
            return Objects.equals(newConstraint, that.newConstraint);
        }

        @Override
        public int hashCode() {
            return Objects.hash(newConstraint);
        }

        @Override
        public String toString() {
            return "ModifyUniqueConstraint{" + "newConstraint=" + newConstraint + '}';
        }
    }

    /**
     * A table change to modify the watermark.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; MODIFY WATERMARK FOR &lt;row_time_column_name&gt; AS &lt;watermark_expression&gt;
     * </pre>
     */
    @PublicEvolving
    class ModifyWatermark implements TableChange {

        private final WatermarkSpec newWatermark;

        public ModifyWatermark(WatermarkSpec newWatermark) {
            this.newWatermark = newWatermark;
        }

        /** Returns the modified watermark. */
        public WatermarkSpec getNewWatermark() {
            return newWatermark;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ModifyWatermark)) {
                return false;
            }
            ModifyWatermark that = (ModifyWatermark) o;
            return Objects.equals(newWatermark, that.newWatermark);
        }

        @Override
        public int hashCode() {
            return Objects.hash(newWatermark);
        }

        @Override
        public String toString() {
            return "ModifyWatermark{" + "newWatermark=" + newWatermark + '}';
        }
    }

    // --------------------------------------------------------------------------------------------
    // Drop Change
    // --------------------------------------------------------------------------------------------

    /**
     * A table change to drop the column.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; DROP COLUMN &lt;column_name&gt;
     * </pre>
     */
    @PublicEvolving
    class DropColumn implements TableChange {

        private final String columnName;

        private DropColumn(String columnName) {
            this.columnName = columnName;
        }

        /** Returns the column name. */
        public String getColumnName() {
            return columnName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof DropColumn)) {
                return false;
            }
            DropColumn that = (DropColumn) o;
            return Objects.equals(columnName, that.columnName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(columnName);
        }

        @Override
        public String toString() {
            return "DropColumn{" + "columnName='" + columnName + '\'' + '}';
        }
    }

    /**
     * A table change to drop the watermark.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; DROP WATERMARK
     * </pre>
     */
    @PublicEvolving
    class DropWatermark implements TableChange {
        static final DropWatermark INSTANCE = new DropWatermark();

        @Override
        public String toString() {
            return "DropWatermark";
        }
    }

    /**
     * A table change to drop the constraints.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; DROP CONSTRAINT &lt;constraint_name&gt;
     * </pre>
     */
    @PublicEvolving
    class DropConstraint implements TableChange {

        private final String constraintName;

        private DropConstraint(String constraintName) {
            this.constraintName = constraintName;
        }

        /** Returns the constraint name. */
        public String getConstraintName() {
            return constraintName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof DropConstraint)) {
                return false;
            }
            DropConstraint that = (DropConstraint) o;
            return Objects.equals(constraintName, that.constraintName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(constraintName);
        }

        @Override
        public String toString() {
            return "DropConstraint{" + "constraintName='" + constraintName + '\'' + '}';
        }
    }

    // --------------------------------------------------------------------------------------------
    // Property change
    // --------------------------------------------------------------------------------------------

    /**
     * A table change to set the table option.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; SET '&lt;key&gt;' = '&lt;value&gt;';
     * </pre>
     */
    @PublicEvolving
    class SetOption implements TableChange {

        private final String key;
        private final String value;

        private SetOption(String key, String value) {
            this.key = key;
            this.value = value;
        }

        /** Returns the Option key to set. */
        public String getKey() {
            return key;
        }

        /** Returns the Option value to set. */
        public String getValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof SetOption)) {
                return false;
            }
            SetOption setOption = (SetOption) o;
            return Objects.equals(key, setOption.key) && Objects.equals(value, setOption.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }

        @Override
        public String toString() {
            return "SetOption{" + "key='" + key + '\'' + ", value='" + value + '\'' + '}';
        }
    }

    /**
     * A table change to reset the table option.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER TABLE &lt;table_name&gt; RESET '&lt;key&gt;'
     * </pre>
     */
    @PublicEvolving
    class ResetOption implements TableChange {

        private final String key;

        public ResetOption(String key) {
            this.key = key;
        }

        /** Returns the Option key to reset. */
        public String getKey() {
            return key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ResetOption)) {
                return false;
            }
            ResetOption that = (ResetOption) o;
            return Objects.equals(key, that.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key);
        }

        @Override
        public String toString() {
            return "ResetOption{" + "key='" + key + '\'' + '}';
        }
    }

    // --------------------------------------------------------------------------------------------

    /** The position of the modified or added column. */
    @PublicEvolving
    interface ColumnPosition {

        /** Get the position to place the column at the first. */
        static ColumnPosition first() {
            return First.INSTANCE;
        }

        /** Get the position to place the column after the specified column. */
        static ColumnPosition after(String column) {
            return new After(column);
        }
    }

    /** Column position FIRST means the specified column should be the first column. */
    @PublicEvolving
    final class First implements ColumnPosition {
        private static final First INSTANCE = new First();

        private First() {}

        @Override
        public String toString() {
            return "FIRST";
        }
    }

    /** Column position AFTER means the specified column should be put after the given `column`. */
    @PublicEvolving
    final class After implements ColumnPosition {
        private final String column;

        private After(String column) {
            this.column = column;
        }

        public String column() {
            return column;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof After)) {
                return false;
            }
            After after = (After) o;
            return Objects.equals(column, after.column);
        }

        @Override
        public int hashCode() {
            return Objects.hash(column);
        }

        @Override
        public String toString() {
            return String.format("AFTER %s", EncodingUtils.escapeIdentifier(column));
        }
    }
}
