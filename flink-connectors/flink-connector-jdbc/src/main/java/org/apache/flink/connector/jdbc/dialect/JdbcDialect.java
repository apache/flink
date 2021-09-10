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
import org.apache.flink.connector.jdbc.converter.JdbcRowConverter;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Represents a dialect of SQL implemented by a particular JDBC system. Dialects should be immutable
 * and stateless.
 *
 * @see JdbcDialectFactory
 */
@PublicEvolving
public interface JdbcDialect extends Serializable {

    /**
     * Get the name of jdbc dialect.
     *
     * @return the dialect name.
     */
    String dialectName();

    /**
     * Get converter that convert jdbc object and Flink internal object each other.
     *
     * @param rowType the given row type
     * @return a row converter for the database
     */
    JdbcRowConverter getRowConverter(RowType rowType);

    /**
     * Get limit clause to limit the number of emitted row from the jdbc source.
     *
     * @param limit number of row to emit. The value of the parameter should be non-negative.
     * @return the limit clause.
     */
    String getLimitClause(long limit);

    /** @return True if two instances support the same dialect. */
    boolean equals(Object o);

    /** @inheritDoc */
    int hashCode();

    /**
     * Check if this dialect instance support a specific data type in table schema.
     *
     * @param dataType the physical table datatype.
     * @exception ValidationException in case of the table schema contains unsupported type.
     */
    default void validate(DataType dataType) throws ValidationException {}

    /**
     * @return the default driver class name, if user not configure the driver class name, then will
     *     use this one.
     */
    default Optional<String> defaultDriverName() {
        return Optional.empty();
    }

    /**
     * Quotes the identifier.
     *
     * <p>Used to put quotes around the identifier if the column name is a reserved keyword or
     * contains characters requiring quotes (e.g., space). Default using double quotes {@code "} to
     * quote.
     *
     * @return the quoted identifier.
     */
    default String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }

    /**
     * Constructs the dialects upsert statement if supported; such as MySQL's {@code DUPLICATE KEY
     * UPDATE}, or PostgreSQLs {@code ON CONFLICT... DO UPDATE SET..}.
     *
     * <p>If the dialect does not support native upsert statements, the writer will fallback to
     * {@code SELECT} + {@code Update}/{@code INSERT} which may have poor performance.
     *
     * @return The upsert statement if supported, otherwise None.
     */
    default Optional<String> getUpsertStatement(
            String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        return Optional.empty();
    }

    /**
     * Generates a query to determine if a row exists in the table.
     *
     * <p>By default, the dialect will fallback to a simple {@code SELECT} query.
     */
    default String getRowExistsStatement(String tableName, String[] conditionFields) {
        String fieldExpressions =
                Arrays.stream(conditionFields)
                        .map(f -> format("%s = :%s", quoteIdentifier(f), f))
                        .collect(Collectors.joining(" AND "));
        return "SELECT 1 FROM " + quoteIdentifier(tableName) + " WHERE " + fieldExpressions;
    }

    /** @return the dialects {@code INSERT INTO} statement. */
    default String getInsertIntoStatement(String tableName, String[] fieldNames) {
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
     * Constructs the dialects update statement for a single row with the given condition.
     *
     * <p>The default implementation does not use {@code LIMIT 1} as limit is dialect specific.
     *
     * @return A single row update statement.
     */
    default String getUpdateStatement(
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
     * Constructs the dialects delete statement for a single row with the given condition.
     *
     * <p>The default implementation does not use {@code LIMIT 1} as limit is dialect specific.
     *
     * @return A single row delete statement.
     */
    default String getDeleteStatement(String tableName, String[] conditionFields) {
        String conditionClause =
                Arrays.stream(conditionFields)
                        .map(f -> format("%s = :%s", quoteIdentifier(f), f))
                        .collect(Collectors.joining(" AND "));
        return "DELETE FROM " + quoteIdentifier(tableName) + " WHERE " + conditionClause;
    }

    /**
     * Constructs the dialects select statement for fields with given conditions.
     *
     * <p>The default implementation creates a simple {@code SELECT} statement.
     *
     * @return A select statement.
     */
    default String getSelectFromStatement(
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
}
