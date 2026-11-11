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

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.errors.ErrorAttribute;
import org.apache.flink.api.common.errors.ErrorCategory;
import org.apache.flink.api.common.errors.FlinkError;
import org.apache.flink.table.catalog.ObjectIdentifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.api.common.errors.ErrorCategory.USER;

/**
 * Errors of the Table API and SQL, each with a SQLSTATE.
 *
 * <p>Exceptions are created from a constant and its arguments, for example {@code new
 * ValidationException(TableError.TABLE_NOT_FOUND, objectIdentifier)}. The message ends with the
 * SQLSTATE, such as {@code "Table `c`.`db`.`t` does not exist. Check the name and the current
 * catalog and database. SQLSTATE: 42P01"}.
 */
@PublicEvolving
public enum TableError implements FlinkError {

    /** For a name that can be a table or a view, such as in a FROM clause or a catalog call. */
    TABLE_OR_VIEW_NOT_FOUND(
            USER,
            "42P01",
            "Table or view {objectName} does not exist. "
                    + "Check the name and the current catalog and database.",
            Parameter.sensitive("objectName", ObjectIdentifier.class)),

    /** For a name that must be a table, such as in INSERT INTO, ALTER TABLE or DROP TABLE. */
    TABLE_NOT_FOUND(
            USER,
            "42P01",
            "Table {objectName} does not exist. "
                    + "Check the name and the current catalog and database.",
            Parameter.sensitive("objectName", ObjectIdentifier.class)),

    /** For a name that must be a view, such as in DROP VIEW. */
    VIEW_NOT_FOUND(
            USER,
            "42P01",
            "View {objectName} does not exist. "
                    + "Check the name and the current catalog and database.",
            Parameter.sensitive("objectName", ObjectIdentifier.class)),

    /** For a name that must be a materialized table, such as in DROP MATERIALIZED TABLE. */
    MATERIALIZED_TABLE_NOT_FOUND(
            USER,
            "42P01",
            "Materialized table {objectName} does not exist. "
                    + "Check the name and the current catalog and database.",
            Parameter.sensitive("objectName", ObjectIdentifier.class)),

    /** For a name that is taken. Catalogs keep tables and views in one namespace. */
    TABLE_OR_VIEW_ALREADY_EXISTS(
            USER,
            "42P07",
            "Table or view {objectName} already exists. "
                    + "Choose a different name or drop the existing object.",
            Parameter.sensitive("objectName", ObjectIdentifier.class));

    private final ErrorCategory category;
    private final String sqlState;
    private final Map<ErrorAttribute, Object> attributes;
    private final String messageTemplate;
    private final List<Parameter> parameters;

    TableError(
            ErrorCategory category,
            String sqlState,
            String messageTemplate,
            Parameter... parameters) {
        this.category = category;
        this.sqlState = sqlState;
        this.attributes = Collections.singletonMap(ErrorAttribute.SQLSTATE, sqlState);
        this.messageTemplate = messageTemplate;
        this.parameters = Collections.unmodifiableList(Arrays.asList(parameters));
    }

    @Override
    public ErrorCategory getCategory() {
        return category;
    }

    @Override
    public Map<ErrorAttribute, Object> getAttributes() {
        return attributes;
    }

    @Override
    public String getMessageTemplate() {
        return messageTemplate;
    }

    @Override
    public List<Parameter> getParameters() {
        return parameters;
    }

    /** Returns the formatted template followed by the SQLSTATE. */
    @Override
    public String format(List<Object> arguments) {
        return withSqlState(FlinkError.super.format(arguments));
    }

    /** Returns the redacted message followed by the SQLSTATE. */
    @Override
    public String formatRedacted(List<Object> arguments, String replacement) {
        return withSqlState(FlinkError.super.formatRedacted(arguments, replacement));
    }

    private String withSqlState(String message) {
        return message + " SQLSTATE: " + sqlState;
    }
}
