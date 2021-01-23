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

package org.apache.flink.table.operations.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.expressions.utils.ApiExpressionDefaultVisitor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedRef;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.AS;
import static org.apache.flink.table.operations.utils.OperationExpressionsUtils.extractName;

/** Utility class for creating projection expressions from column operation. */
@Internal
final class ColumnOperationUtils {

    private static final DropColumnsExtractor dropColumnsExtractor = new DropColumnsExtractor();
    private static final RenameColumnExtractor renameColumnExtractor = new RenameColumnExtractor();

    /**
     * Creates a projection list that renames existing columns to new names.
     *
     * <p><b>NOTE:</b> Resulting expression are still unresolved.
     *
     * @param inputFields names of current columns
     * @param newAliases new aliases for current columns
     * @return projection expressions
     */
    static List<Expression> renameColumns(List<String> inputFields, List<Expression> newAliases) {
        LinkedHashMap<String, Expression> finalFields = new LinkedHashMap<>();

        inputFields.forEach(field -> finalFields.put(field, unresolvedRef(field)));
        newAliases.forEach(
                expr -> {
                    String name = expr.accept(renameColumnExtractor);
                    finalFields.put(name, expr);
                });

        return new ArrayList<>(finalFields.values());
    }

    /**
     * Creates a projection list that adds new or replaces existing (if a column with corresponding
     * name already exists) columns.
     *
     * <p><b>NOTE:</b> Resulting expression are still unresolved.
     *
     * @param inputFields names of current columns
     * @param newExpressions new columns to add
     * @return projection expressions
     */
    static List<Expression> addOrReplaceColumns(
            List<String> inputFields, List<Expression> newExpressions) {
        LinkedHashMap<String, Expression> finalFields = new LinkedHashMap<>();

        inputFields.forEach(field -> finalFields.put(field, unresolvedRef(field)));
        newExpressions.forEach(
                expr -> {
                    String name = extractName(expr).orElse(expr.toString());
                    finalFields.put(name, expr);
                });

        return new ArrayList<>(finalFields.values());
    }

    /**
     * Creates a projection list that removes given columns.
     *
     * <p><b>NOTE:</b> Resulting expression are still unresolved.
     *
     * @param inputFields names of current columns
     * @param dropExpressions columns to remove
     * @return projection expressions
     */
    static List<Expression> dropFields(List<String> inputFields, List<Expression> dropExpressions) {
        Set<String> columnsToDrop =
                dropExpressions.stream()
                        .map(expr -> expr.accept(dropColumnsExtractor))
                        .collect(Collectors.toSet());

        columnsToDrop.forEach(
                c -> {
                    if (!inputFields.contains(c)) {
                        throw new ValidationException(
                                format("Field %s does not exist in source table", c));
                    }
                });

        return inputFields.stream()
                .filter(oldName -> !columnsToDrop.contains(oldName))
                .map(ApiExpressionUtils::unresolvedRef)
                .collect(Collectors.toList());
    }

    private static class DropColumnsExtractor extends ApiExpressionDefaultVisitor<String> {

        @Override
        public String visit(UnresolvedReferenceExpression unresolvedReference) {
            return unresolvedReference.getName();
        }

        @Override
        protected String defaultMethod(Expression expression) {
            throw new ValidationException("Unexpected drop column expression: " + expression);
        }
    }

    private static class RenameColumnExtractor extends ApiExpressionDefaultVisitor<String> {

        @Override
        public String visit(UnresolvedCallExpression unresolvedCall) {
            if (unresolvedCall.getFunctionDefinition() == AS
                    && unresolvedCall.getChildren().get(0)
                            instanceof UnresolvedReferenceExpression) {
                UnresolvedReferenceExpression resolvedFieldReference =
                        (UnresolvedReferenceExpression) unresolvedCall.getChildren().get(0);
                return resolvedFieldReference.getName();
            } else {
                return defaultMethod(unresolvedCall);
            }
        }

        @Override
        protected String defaultMethod(Expression expression) {
            throw new ValidationException(
                    format(
                            "Invalid alias for a renaming column operation. Renaming must add an alias to an"
                                    + "existing field. E.g.: 'a as a1'. But was: %s",
                            expression));
        }
    }

    private ColumnOperationUtils() {}
}
