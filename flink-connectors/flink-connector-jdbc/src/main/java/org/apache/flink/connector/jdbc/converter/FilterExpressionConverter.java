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

package org.apache.flink.connector.jdbc.converter;

import org.apache.flink.connector.jdbc.dialect.FilterClause;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.utils.JdbcValueFormatter;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.TypeLiteralExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.FunctionDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;

/** Convert expressions to filter clauses. */
public class FilterExpressionConverter implements ExpressionVisitor<Optional<String>> {

    List<ResolvedExpression> acceptedFilters;
    List<ResolvedExpression> remainingFilters;
    JdbcDialect dialect;

    public FilterExpressionConverter(JdbcDialect dialect) {
        this.dialect = dialect;
        this.acceptedFilters = new ArrayList<>();
        this.remainingFilters = new ArrayList<>();
    }

    public String convert(List<ResolvedExpression> unconvertedExpressions) {
        return unconvertedExpressions.stream()
                .map(
                        resolvedExpression -> {
                            Optional<String> convertedFilter = resolvedExpression.accept(this);

                            if (convertedFilter.isPresent()) {
                                this.acceptedFilters.add(resolvedExpression);
                            } else {
                                this.remainingFilters.add(resolvedExpression);
                            }

                            return convertedFilter;
                        })
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(joining(" AND "));
    }

    public List<ResolvedExpression> getAcceptedFilters() {
        return acceptedFilters;
    }

    public List<ResolvedExpression> getRemainingFilters() {
        return remainingFilters;
    }

    @Override
    public Optional<String> visit(CallExpression call) {
        FunctionDefinition function = call.getFunctionDefinition();
        FilterClause filterClause = dialect.getFilterClause(function);
        if (filterClause == null) {
            return Optional.empty();
        }

        List<String> args =
                call.getResolvedChildren().stream()
                        .map(resolvedExpression -> resolvedExpression.accept(this))
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(Collectors.toList());

        if (args.size() == filterClause.getArgsNum()) {
            return Optional.of(String.join("", "(", filterClause.apply(args), ")"));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Optional<String> visit(ValueLiteralExpression valueLiteral) {
        return convertValueLiteral(valueLiteral);
    }

    @Override
    public Optional<String> visit(FieldReferenceExpression fieldReference) {
        return convertFieldReference(fieldReference);
    }

    @Override
    public Optional<String> visit(TypeLiteralExpression typeLiteral) {
        // Some Build-in Function like CAST need to call this method.
        // Need to convert Flink Table Api type to jdbc database type.
        // Different jdbc database has different type mapping, such as Oracle, Mysql and so on
        return convertTypeLiteral(typeLiteral);
    }

    @Override
    public Optional<String> visit(Expression other) {
        return Optional.empty();
    }

    private Optional<String> convertValueLiteral(ValueLiteralExpression expression) {
        return expression
                .getValueAs(expression.getOutputDataType().getLogicalType().getDefaultConversion())
                .map(JdbcValueFormatter::format);
    }

    private Optional<String> convertFieldReference(FieldReferenceExpression expression) {
        return Optional.of(quoteIdentifier(expression.getName()));
    }

    private Optional<String> convertTypeLiteral(TypeLiteralExpression expression) {
        return Optional.empty();
    }

    private String quoteIdentifier(String identifier) {
        return String.join("", "`", identifier, "`");
    }
}
