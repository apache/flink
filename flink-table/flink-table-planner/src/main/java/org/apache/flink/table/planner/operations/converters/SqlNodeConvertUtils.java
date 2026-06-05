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

package org.apache.flink.table.planner.operations.converters;

import org.apache.flink.sql.parser.ddl.view.SqlAlterView;
import org.apache.flink.sql.parser.error.SqlValidateException;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogBaseTable.TableKind;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogView;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.utils.ValidationUtils;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.operations.converters.SqlNodeConverter.ConvertContext;

import org.apache.flink.shaded.guava33.com.google.common.base.Splitter;

import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSetOperator;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Collectors;

/** Utilities for SqlNode conversions. */
public class SqlNodeConvertUtils {

    /**
     * Returns the {@code AS}-query of a VIEW or MATERIALIZED TABLE DDL in verbatim shape: the
     * statement text from just after {@code AS} to its end, trimmed. Slicing to the statement end
     * (the {@code AS}-query is the last clause) keeps a comment after {@code AS} and queries whose
     * node position is narrower than their text, e.g. {@code WITH ... SELECT}.
     *
     * @param asQueryKeywordPos parser position of the {@code AS} keyword
     * @return the verbatim AS-query, or empty when no statement text is available
     */
    public static Optional<String> extractOriginalAsQueryText(
            ConvertContext context, SqlParserPos asQueryKeywordPos) {
        final String statementText = context.getStatementText();
        if (statementText == null) {
            return Optional.empty();
        }
        return offsetAfter(statementText, asQueryKeywordPos).stream()
                .mapToObj(start -> statementText.substring(start).strip())
                .findFirst();
    }

    /**
     * Returns the offset of the first character after the given parser position, or empty if the
     * position's end line falls outside {@code statementText}.
     */
    private static OptionalInt offsetAfter(String statementText, SqlParserPos pos) {
        final int endLine = pos.getEndLineNum();
        final int endCol = pos.getEndColumnNum();

        final Iterator<String> iterator = Splitter.on('\n').split(statementText).iterator();
        int currentLine = 0;
        int lineStartOffset = 0;
        while (iterator.hasNext()) {
            final String lineText = iterator.next();
            currentLine++;
            if (currentLine == endLine) {
                return OptionalInt.of(lineStartOffset + endCol);
            }
            lineStartOffset += lineText.length() + 1;
        }
        return OptionalInt.empty();
    }

    static PlannerQueryOperation toQueryOperation(SqlNode validated, ConvertContext context) {
        // transform to a relational tree
        RelRoot relational = context.toRelRoot(validated);
        return new PlannerQueryOperation(
                relational.project(), () -> context.toQuotedSqlString(validated));
    }

    /** convert the query part of a VIEW statement into a {@link CatalogView}. */
    static CatalogView toCatalogView(
            SqlNode query,
            SqlParserPos asQueryKeywordPos,
            List<SqlNode> viewFields,
            Map<String, String> viewOptions,
            String viewComment,
            ConvertContext context) {
        SqlNode validateQuery = context.getSqlValidator().validate(query);
        // FLINK-38950: SqlValidator.validate() mutates its input parameter. Always use the
        // returned validateQuery instead of the mutated query for all subsequent operations.

        // Check name is unique.
        // Don't rely on the calcite because if the field names are duplicate, calcite will add
        // index to identify the duplicate names.
        SqlValidatorNamespace validatedNamespace =
                context.getSqlValidator().getNamespace(validateQuery);
        validateDuplicatedColumnNames(validateQuery, viewFields, validatedNamespace);

        String expandedQuery = context.toQuotedSqlString(validateQuery);

        PlannerQueryOperation operation = toQueryOperation(validateQuery, context);
        ResolvedSchema schema = operation.getResolvedSchema();

        // the view column list in CREATE VIEW is optional, if it's not empty, we should update
        // the column name with the names in view column list.
        if (!viewFields.isEmpty()) {
            // alias column names:
            List<String> inputFieldNames = schema.getColumnNames();
            List<String> aliasFieldNames =
                    viewFields.stream().map(SqlNode::toString).collect(Collectors.toList());

            if (inputFieldNames.size() != aliasFieldNames.size()) {
                throw new ValidationException(
                        String.format(
                                "VIEW definition and input fields not match:\n\tDef fields: %s.\n\tInput fields: %s.",
                                aliasFieldNames, inputFieldNames));
            }

            schema = ResolvedSchema.physical(aliasFieldNames, schema.getColumnDataTypes());
        }

        final String originalQuery =
                extractOriginalAsQueryText(context, asQueryKeywordPos)
                        .orElse(context.toQuotedSqlString(query));

        return new ResolvedCatalogView(
                CatalogView.of(
                        Schema.newBuilder().fromResolvedSchema(schema).build(),
                        viewComment,
                        originalQuery,
                        expandedQuery,
                        viewOptions),
                schema);
    }

    /**
     * Validate the view to alter is valid and existed and return the {@link CatalogView} to alter.
     */
    static CatalogView validateAlterView(SqlAlterView alterView, ConvertContext context) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(alterView.getFullName());
        ObjectIdentifier viewIdentifier =
                context.getCatalogManager().qualifyIdentifier(unresolvedIdentifier);
        Optional<ContextResolvedTable> optionalCatalogTable =
                context.getCatalogManager().getTable(viewIdentifier);
        // check the view exist and is not a temporary view
        if (optionalCatalogTable.isEmpty() || optionalCatalogTable.get().isTemporary()) {
            throw new ValidationException(
                    String.format("View %s doesn't exist or is a temporary view.", viewIdentifier));
        }
        // check the view is exactly a view
        CatalogBaseTable baseTable = optionalCatalogTable.get().getResolvedTable();
        ValidationUtils.validateTableKind(baseTable, TableKind.VIEW, "alter view");

        return (CatalogView) baseTable;
    }

    private static void validateDuplicatedColumnNames(
            SqlNode query, List<SqlNode> viewFields, SqlValidatorNamespace namespace) {

        // If view fields is not empty, means the view column list is specified by user use syntax
        // 'CREATE VIEW viewName(x,x,x) AS SELECT x,x,x FROM table'. For this syntax, we need
        // validate whether the column name in the view column list is unique.
        List<String> columnNameList;
        if (!viewFields.isEmpty()) {
            columnNameList =
                    viewFields.stream().map(SqlNode::toString).collect(Collectors.toList());
        } else {
            Objects.requireNonNull(namespace);
            columnNameList = namespace.getType().getFieldNames();
        }

        Map<String, Integer> nameToPos = new HashMap<>();
        for (int i = 0; i < columnNameList.size(); i++) {
            String columnName = columnNameList.get(i);
            if (nameToPos.containsKey(columnName)) {
                SqlSelect select = extractSelect(query);
                // Can not get the origin schema.
                if (select == null) {
                    throw new ValidationException(
                            String.format(
                                    "SQL validation failed. Column `%s` has been specified.",
                                    columnName));
                }
                SqlParserPos errorPos = select.getSelectList().get(i).getParserPosition();
                String msg =
                        String.format(
                                "A column with the same name `%s` has been defined at %s.",
                                columnName,
                                select.getSelectList()
                                        .get(nameToPos.get(columnName))
                                        .getParserPosition());
                throw new ValidationException(
                        "SQL validation failed. " + msg, new SqlValidateException(errorPos, msg));
            }
            nameToPos.put(columnName, i);
        }
    }

    private static @Nullable SqlSelect extractSelect(SqlNode query) {
        if (query instanceof SqlSelect) {
            return (SqlSelect) query;
        } else if (query instanceof SqlBasicCall) {
            SqlBasicCall call = (SqlBasicCall) query;
            if (call.getOperator() instanceof SqlSetOperator) {
                // UNION/INTERSECT/EXCEPT/...
                return extractSelect(call.getOperandList().get(0));
            } else {
                return null;
            }
        } else if (query instanceof SqlWith) {
            SqlWith with = (SqlWith) query;
            return extractSelect(with.body);
        } else {
            return null;
        }
    }
}
