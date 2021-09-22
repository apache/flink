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

package org.apache.flink.connector.jdbc.dialect;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Base class for {@link JdbcDialect JdbcDialects} that implements basic data type validation and
 * the construction of basic {@code INSERT}, {@code UPDATE}, {@code DELETE}, and {@code SELECT}
 * statements.
 *
 * <p>Implementors should be careful to check the default SQL statements are performant for their
 * specific dialect and override them if necessary.
 */
@PublicEvolving
public abstract class AbstractDialect implements JdbcDialect {

    @Override
    public void validate(RowType rowType) throws ValidationException {
        for (RowType.RowField field : rowType.getFields()) {
            // TODO: We can't convert VARBINARY(n) data type to
            //  PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO in
            //  LegacyTypeInfoDataTypeConverter when n is smaller
            //  than Integer.MAX_VALUE
            if (!supportedTypes().contains(field.getType().getTypeRoot())
                    || (field.getType() instanceof VarBinaryType
                            && Integer.MAX_VALUE
                                    != ((VarBinaryType) field.getType()).getLength())) {
                throw new ValidationException(
                        String.format(
                                "The %s dialect doesn't support type: %s.",
                                dialectName(), field.getType()));
            }

            if (field.getType() instanceof DecimalType) {
                Range range =
                        decimalPrecisionRange()
                                .orElseThrow(
                                        () ->
                                                new IllegalStateException(
                                                        String.format(
                                                                "JdbcDialect %s supports DECIMAL type but no precision range has been set. "
                                                                        + "Ensure AbstractDialect#decimalPrecisionRange() is overriden to return a valid Range",
                                                                dialectName())));
                int precision = ((DecimalType) field.getType()).getPrecision();
                if (precision > range.max || precision < range.min) {
                    throw new ValidationException(
                            String.format(
                                    "The precision of field '%s' is out of the DECIMAL "
                                            + "precision range [%d, %d] supported by %s dialect.",
                                    field.getName(), range.min, range.max, dialectName()));
                }
            }

            if (field.getType() instanceof TimestampType) {
                Range range =
                        timestampPrecisionRange()
                                .orElseThrow(
                                        () ->
                                                new IllegalStateException(
                                                        String.format(
                                                                "JdbcDialect %s supports TIMESTAMP type but no precision range has been set."
                                                                        + "Ensure AbstractDialect#timestampPrecisionRange() is overriden to return a valid Range",
                                                                dialectName())));
                int precision = ((TimestampType) field.getType()).getPrecision();
                if (precision > range.max || precision < range.min) {
                    throw new ValidationException(
                            String.format(
                                    "The precision of field '%s' is out of the TIMESTAMP "
                                            + "precision range [%d, %d] supported by %s dialect.",
                                    field.getName(), range.min, range.max, dialectName()));
                }
            }
        }
    }

    /**
     * A simple {@code INSERT INTO} statement.
     *
     * <pre>{@code
     * INSERT INTO table_name (column_name [, ...])
     * VALUES (value [, ...])
     * }</pre>
     */
    @Override
    public String getInsertIntoStatement(String tableName, String[] fieldNames) {
        String columns =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String placeholders =
                Arrays.stream(fieldNames).map(f -> ":" + f).collect(Collectors.joining(", "));
        return "INSERT INTO "
                + quoteIdentifier(tableName)
                + "("
                + columns
                + ")"
                + " VALUES ("
                + placeholders
                + ")";
    }

    /**
     * A simple single row {@code UPDATE} statement.
     *
     * <pre>{@code
     * UPDATE table_name
     * SET col = val [, ...]
     * WHERE cond [AND ...]
     * }</pre>
     */
    @Override
    public String getUpdateStatement(
            String tableName, String[] fieldNames, String[] conditionFields) {
        String setClause =
                Arrays.stream(fieldNames)
                        .map(f -> format("%s = :%s", quoteIdentifier(f), f))
                        .collect(Collectors.joining(", "));
        String conditionClause =
                Arrays.stream(conditionFields)
                        .map(f -> format("%s = :%s", quoteIdentifier(f), f))
                        .collect(Collectors.joining(" AND "));
        return "UPDATE "
                + quoteIdentifier(tableName)
                + " SET "
                + setClause
                + " WHERE "
                + conditionClause;
    }

    /**
     * A simple single row {@code DELETE} statement.
     *
     * <pre>{@code
     * DELETE FROM table_name
     * WHERE cond [AND ...]
     * }</pre>
     */
    @Override
    public String getDeleteStatement(String tableName, String[] conditionFields) {
        String conditionClause =
                Arrays.stream(conditionFields)
                        .map(f -> format("%s = :%s", quoteIdentifier(f), f))
                        .collect(Collectors.joining(" AND "));
        return "DELETE FROM " + quoteIdentifier(tableName) + " WHERE " + conditionClause;
    }

    /**
     * A simple {@code SELECT} statement.
     *
     * <pre>{@code
     * SELECT expression [, ...]
     * FROM table_name
     * WHERE cond [AND ...]
     * }</pre>
     */
    @Override
    public String getSelectFromStatement(
            String tableName, String[] selectFields, String[] conditionFields) {
        String selectExpressions =
                Arrays.stream(selectFields)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String fieldExpressions =
                Arrays.stream(conditionFields)
                        .map(f -> format("%s = :%s", quoteIdentifier(f), f))
                        .collect(Collectors.joining(" AND "));
        return "SELECT "
                + selectExpressions
                + " FROM "
                + quoteIdentifier(tableName)
                + (conditionFields.length > 0 ? " WHERE " + fieldExpressions : "");
    }

    /**
     * A simple {@code SELECT} statement that checks for the existence of a single row.
     *
     * <pre>{@code
     * SELECT 1
     * FROM table_name
     * WHERE cond [AND ...]
     * }</pre>
     */
    @Override
    public String getRowExistsStatement(String tableName, String[] conditionFields) {
        String fieldExpressions =
                Arrays.stream(conditionFields)
                        .map(f -> format("%s = :%s", quoteIdentifier(f), f))
                        .collect(Collectors.joining(" AND "));
        return "SELECT 1 FROM " + quoteIdentifier(tableName) + " WHERE " + fieldExpressions;
    }

    /**
     * @return The inclusive range [min,max] of supported precisions for {@link TimestampType}
     *     columns. None if timestamp type is not supported.
     */
    public Optional<Range> timestampPrecisionRange() {
        return Optional.empty();
    }

    /**
     * @return The inclusive range [min,max] of supported precisions for {@link DecimalType}
     *     columns. None if decimal type is not supported.
     */
    public Optional<Range> decimalPrecisionRange() {
        return Optional.empty();
    }

    /**
     * Defines the set of supported types for the dialect. If the dialect supports {@code DECIMAL}
     * or {@code TIMESTAMP} types, be sure to override {@link #decimalPrecisionRange()} and {@link
     * #timestampPrecisionRange()} respectively.
     *
     * @return a set of logical type roots.
     */
    public abstract Set<LogicalTypeRoot> supportedTypes();

    /** A range from [min,max] where min <= max. */
    public static class Range {
        private final int min;

        private final int max;

        public static Range of(int min, int max) {
            Preconditions.checkArgument(
                    min <= max,
                    String.format(
                            "The range min value in range %d must be <= max value %d", min, max));
            return new Range(min, max);
        }

        private Range(int min, int max) {
            this.min = min;
            this.max = max;
        }
    }
}
