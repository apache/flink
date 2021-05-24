/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

/**
 * Representation of a column in a {@link ResolvedSchema}.
 *
 * <p>A table column describes either a {@link PhysicalColumn}, {@link ComputedColumn}, or {@link
 * MetadataColumn}.
 *
 * <p>Every column is fully resolved. The enclosed {@link DataType} indicates whether the column is
 * a time attribute and thus might differ from the original data type.
 */
@PublicEvolving
public abstract class Column {

    protected final String name;

    protected final DataType dataType;

    private Column(String name, DataType dataType) {
        this.name = name;
        this.dataType = dataType;
    }

    /** Creates a regular table column that represents physical data. */
    public static PhysicalColumn physical(String name, DataType dataType) {
        Preconditions.checkNotNull(name, "Column name can not be null.");
        Preconditions.checkNotNull(dataType, "Column data type can not be null.");
        return new PhysicalColumn(name, dataType);
    }

    /** Creates a computed column that is computed from the given {@link ResolvedExpression}. */
    public static ComputedColumn computed(String name, ResolvedExpression expression) {
        Preconditions.checkNotNull(name, "Column name can not be null.");
        Preconditions.checkNotNull(expression, "Column expression can not be null.");
        return new ComputedColumn(name, expression.getOutputDataType(), expression);
    }

    /**
     * Creates a metadata column from metadata of the given column name or from metadata of the
     * given key (if not null).
     *
     * <p>Allows to specify whether the column is virtual or not.
     */
    public static MetadataColumn metadata(
            String name, DataType dataType, @Nullable String metadataKey, boolean isVirtual) {
        Preconditions.checkNotNull(name, "Column name can not be null.");
        Preconditions.checkNotNull(dataType, "Column data type can not be null.");
        return new MetadataColumn(name, dataType, metadataKey, isVirtual);
    }

    /**
     * Returns whether the given column is a physical column of a table; neither computed nor
     * metadata.
     */
    public abstract boolean isPhysical();

    /** Returns whether the given column is persisted in a sink operation. */
    public abstract boolean isPersisted();

    /** Returns the data type of this column. */
    public DataType getDataType() {
        return this.dataType;
    }

    /** Returns the name of this column. */
    public String getName() {
        return name;
    }

    /** Returns a string that summarizes this column for printing to a console. */
    public String asSummaryString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(EncodingUtils.escapeIdentifier(name));
        sb.append(" ");
        sb.append(dataType);
        explainExtras()
                .ifPresent(
                        e -> {
                            sb.append(" ");
                            sb.append(e);
                        });
        return sb.toString();
    }

    /** Returns an explanation of specific column extras next to name and type. */
    public abstract Optional<String> explainExtras();

    /** Returns a copy of the column with a replaced {@link DataType}. */
    public abstract Column copy(DataType newType);

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Column that = (Column) o;
        return Objects.equals(this.name, that.name) && Objects.equals(this.dataType, that.dataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.name, this.dataType);
    }

    @Override
    public String toString() {
        return asSummaryString();
    }

    // --------------------------------------------------------------------------------------------
    // Specific kinds of columns
    // --------------------------------------------------------------------------------------------

    /** Representation of a physical column. */
    public static final class PhysicalColumn extends Column {

        private PhysicalColumn(String name, DataType dataType) {
            super(name, dataType);
        }

        @Override
        public boolean isPhysical() {
            return true;
        }

        @Override
        public boolean isPersisted() {
            return true;
        }

        @Override
        public Optional<String> explainExtras() {
            return Optional.empty();
        }

        @Override
        public Column copy(DataType newDataType) {
            return new PhysicalColumn(name, newDataType);
        }
    }

    /** Representation of a computed column. */
    public static final class ComputedColumn extends Column {

        private final ResolvedExpression expression;

        private ComputedColumn(String name, DataType dataType, ResolvedExpression expression) {
            super(name, dataType);
            this.expression = expression;
        }

        @Override
        public boolean isPhysical() {
            return false;
        }

        @Override
        public boolean isPersisted() {
            return false;
        }

        public ResolvedExpression getExpression() {
            return expression;
        }

        @Override
        public Optional<String> explainExtras() {
            return Optional.of("AS " + expression.asSummaryString());
        }

        @Override
        public Column copy(DataType newDataType) {
            return new ComputedColumn(name, newDataType, expression);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            ComputedColumn that = (ComputedColumn) o;
            return expression.equals(that.expression);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), expression);
        }
    }

    /** Representation of a metadata column. */
    public static final class MetadataColumn extends Column {

        private final @Nullable String metadataKey;

        private final boolean isVirtual;

        private MetadataColumn(
                String name, DataType dataType, @Nullable String metadataKey, boolean isVirtual) {
            super(name, dataType);
            this.metadataKey = metadataKey;
            this.isVirtual = isVirtual;
        }

        public boolean isVirtual() {
            return isVirtual;
        }

        public Optional<String> getMetadataKey() {
            return Optional.ofNullable(metadataKey);
        }

        @Override
        public boolean isPhysical() {
            return false;
        }

        @Override
        public boolean isPersisted() {
            return !isVirtual;
        }

        @Override
        public Optional<String> explainExtras() {
            final StringBuilder sb = new StringBuilder();
            sb.append("METADATA");
            if (metadataKey != null) {
                sb.append(" FROM ");
                sb.append("'");
                sb.append(EncodingUtils.escapeSingleQuotes(metadataKey));
                sb.append("'");
            }
            if (isVirtual) {
                sb.append(" VIRTUAL");
            }
            return Optional.of(sb.toString());
        }

        @Override
        public Column copy(DataType newDataType) {
            return new MetadataColumn(name, newDataType, metadataKey, isVirtual);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            MetadataColumn that = (MetadataColumn) o;
            return isVirtual == that.isVirtual && Objects.equals(metadataKey, that.metadataKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), metadataKey, isVirtual);
        }
    }
}
