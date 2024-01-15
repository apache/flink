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

import org.apache.flink.sql.parser.ddl.SqlAlterView;
import org.apache.flink.sql.parser.error.SqlValidateException;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.operations.converters.SqlNodeConverter.ConvertContext;

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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/** Utilities for SqlNode conversions. */
class SqlNodeConvertUtils {

    static PlannerQueryOperation toQueryOperation(SqlNode validated, ConvertContext context) {
        // transform to a relational tree
        RelRoot relational = context.toRelRoot(validated);
        return new PlannerQueryOperation(
                relational.project(), () -> context.toQuotedSqlString(validated));
    }

    /** convert the query part of a VIEW statement into a {@link CatalogView}. */
    static CatalogView toCatalogView(
            SqlNode query,
            List<SqlNode> viewFields,
            Map<String, String> viewOptions,
            String viewComment,
            ConvertContext context) {
        // Put the sql string unparse (getQuotedSqlString()) in front of
        // the node conversion (toQueryOperation()),
        // because before Calcite 1.22.0, during sql-to-rel conversion, the SqlWindow
        // bounds state would be mutated as default when they are null (not specified).

        // This bug is fixed in CALCITE-3877 of Calcite 1.23.0.
        String originalQuery = context.toQuotedSqlString(query);
        SqlNode validateQuery = context.getSqlValidator().validate(query);

        // Check name is unique.
        // Don't rely on the calcite because if the field names are duplicate, calcite will add
        // index to identify the duplicate names.
        SqlValidatorNamespace validatedNamespace =
                context.getSqlValidator().getNamespace(validateQuery);
        validateDuplicatedColumnNames(query, viewFields, validatedNamespace);

        // The LATERAL operator was eliminated during sql validation, thus the unparsed SQL
        // does not contain LATERAL which is problematic,
        // the issue was resolved in CALCITE-4077
        // (always treat the table function as implicitly LATERAL).
        String expandedQuery = context.expandSqlIdentifiers(originalQuery);

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

        return CatalogView.of(
                Schema.newBuilder().fromResolvedSchema(schema).build(),
                viewComment,
                originalQuery,
                expandedQuery,
                viewOptions);
    }

    /**
     * Validate the view to alter is valid and existed and return the {@link CatalogView} to alter.
     */
    static CatalogView validateAlterView(SqlAlterView alterView, ConvertContext context) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(alterView.fullViewName());
        ObjectIdentifier viewIdentifier =
                context.getCatalogManager().qualifyIdentifier(unresolvedIdentifier);
        Optional<ContextResolvedTable> optionalCatalogTable =
                context.getCatalogManager().getTable(viewIdentifier);
        // check the view exist and is not a temporary view
        if (!optionalCatalogTable.isPresent() || optionalCatalogTable.get().isTemporary()) {
            throw new ValidationException(
                    String.format("View %s doesn't exist or is a temporary view.", viewIdentifier));
        }
        // check the view is exactly a view
        CatalogBaseTable baseTable = optionalCatalogTable.get().getResolvedTable();
        if (baseTable instanceof CatalogTable) {
            throw new ValidationException("ALTER VIEW for a table is not allowed");
        }
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
