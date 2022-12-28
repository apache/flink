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
import org.apache.flink.table.utils.EncodingUtils;

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
     *    ALTER TABLE &lt;table_name&gt; ADD &lt;column_definition&gt; &lt;position&gt;
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
     *    ALTER TABLE &lt;table_name&gt; ADD PRIMARY KEY (&lt;column_name&gt;...) NOT ENFORCED;
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
     *    ALTER TABLE &lt;table_name&gt; ADD &lt;column_definition&gt; &lt;position&gt;
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
