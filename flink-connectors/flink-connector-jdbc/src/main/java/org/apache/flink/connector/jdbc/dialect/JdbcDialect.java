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
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;
import java.util.Optional;

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

    /**
     * Check if this dialect instance support a specific data type in table schema.
     *
     * @param rowType the physical table datatype of a row in the database table.
     * @exception ValidationException in case of the table schema contains unsupported type.
     */
    void validate(RowType rowType) throws ValidationException;

    /**
     * @return the default driver class name, if user has not configured the driver class name, then
     *     this one will be used.
     */
    default Optional<String> defaultDriverName() {
        return Optional.empty();
    }

    /**
     * Quotes the identifier.
     *
     * <p>Used to put quotes around the identifier if the column name is a reserved keyword or
     * contains characters requiring quotes (e.g., space).
     *
     * @return the quoted identifier.
     */
    String quoteIdentifier(String identifier);

    /**
     * Constructs the dialects upsert statement if supported; such as MySQL's {@code DUPLICATE KEY
     * UPDATE}, or PostgreSQL's {@code ON CONFLICT... DO UPDATE SET..}. If supported, the returned
     * string will be used as a {@link java.sql.PreparedStatement}. Fields in the statement must be
     * in the same order as the {@code fieldNames} parameter.
     *
     * <p>If the dialect does not support native upsert statements, the writer will fallback to
     * {@code SELECT} + {@code UPDATE}/{@code INSERT} which may have poor performance.
     *
     * @return The upsert statement if supported, otherwise None.
     */
    Optional<String> getUpsertStatement(
            String tableName, String[] fieldNames, String[] uniqueKeyFields);

    /**
     * Generates a query to determine if a row exists in the table. The returned string will be used
     * as a {@link java.sql.PreparedStatement}.
     *
     * <p>By default, the dialect will fallback to a simple {@code SELECT} query.
     */
    String getRowExistsStatement(String tableName, String[] conditionFields);

    /**
     * Generates a string that will be used as a {@link java.sql.PreparedStatement} to insert a row
     * into a database table. Fields in the statement must be in the same order as the {@code
     * fieldNames} parameter.
     *
     * @return the dialects {@code INSERT INTO} statement.
     */
    String getInsertIntoStatement(String tableName, String[] fieldNames);

    /**
     * Constructs the dialects update statement for a single row with the given condition. The
     * returned string will be used as a {@link java.sql.PreparedStatement}. Fields in the statement
     * must be in the same order as the {@code fieldNames} parameter.
     *
     * @return A single row update statement.
     */
    String getUpdateStatement(String tableName, String[] fieldNames, String[] conditionFields);

    /**
     * Constructs the dialects delete statement for a single row with the given condition. The
     * returned string will be used as a {@link java.sql.PreparedStatement}. Fields in the statement
     * must be in the same order as the {@code fieldNames} parameter.
     *
     * @return A single row delete statement.
     */
    String getDeleteStatement(String tableName, String[] conditionFields);

    /**
     * Constructs the dialects select statement for fields with given conditions. The returned
     * string will be used as a {@link java.sql.PreparedStatement}. Fields in the statement must be
     * in the same order as the {@code fieldNames} parameter.
     *
     * @return A select statement.
     */
    String getSelectFromStatement(
            String tableName, String[] selectFields, String[] conditionFields);

    /**
     * Appends default JDBC properties to url for current dialect. Some database dialects will set
     * default JDBC properties for performance or optimization consideration, such as MySQL dialect
     * uses 'rewriteBatchedStatements=true' to enable execute multiple MySQL statements in batch
     * mode.
     *
     * @return A JDBC url that has appended the default properties.
     */
    default String appendDefaultUrlProperties(String url) {
        return url;
    }
}
