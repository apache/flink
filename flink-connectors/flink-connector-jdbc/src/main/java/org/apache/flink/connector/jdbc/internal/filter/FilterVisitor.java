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

package org.apache.flink.connector.jdbc.internal.filter;

import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.expressions.utils.ResolvedExpressionDefaultVisitor;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.logical.LogicalType;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Jdbc filter visitor. */
public class FilterVisitor extends ResolvedExpressionDefaultVisitor<Optional<String>> {

    private static final DateTimeFormatter DATE_TIME_FORMATTER =
            new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .append(DateTimeFormatter.ISO_LOCAL_DATE)
                    .appendLiteral(' ')
                    .append(DateTimeFormatter.ISO_LOCAL_TIME)
                    .toFormatter();

    private final Function<String, String> quoteIdentifier;

    public FilterVisitor(Function<String, String> quoteIdentifier) {
        this.quoteIdentifier = quoteIdentifier;
    }

    @Override
    protected Optional<String> defaultMethod(ResolvedExpression expression) {
        return Optional.empty();
    }

    @Override
    public Optional<String> visit(ResolvedExpression other) {
        if (other instanceof CallExpression) {
            return visit((CallExpression) other);
        }
        return Optional.empty();
    }

    @Override
    public Optional<String> visit(FieldReferenceExpression fieldReference) {
        return Optional.of(quoteIdentifier.apply(fieldReference.getName()));
    }

    @Override
    public Optional<String> visit(ValueLiteralExpression valueLiteral) {
        LogicalType logicalType = valueLiteral.getOutputDataType().getLogicalType();
        Optional<?> optional = valueLiteral.getValueAs(logicalType.getDefaultConversion());
        return optional.map(
                value -> {
                    if (value instanceof String) {
                        return formatStringValue((String) value);
                    } else if (value instanceof LocalDate) {
                        return formatLocalDateValue((LocalDate) value);
                    } else if (value instanceof LocalTime) {
                        return formatLocalTimeValue((LocalTime) value);
                    } else if (value instanceof LocalDateTime) {
                        return formatLocalDateTimeValue((LocalDateTime) value);
                    } else if (value instanceof Instant) {
                        return formatInstantValue((Instant) value);
                    }
                    return value.toString();
                });
    }

    @Override
    public Optional<String> visit(CallExpression call) {
        FunctionDefinition functionDefinition = call.getFunctionDefinition();
        final List<String> transformed =
                call.getChildren().stream()
                        .map(c -> c.accept(this))
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(Collectors.toList());
        if (transformed.size() != call.getChildren().size()) {
            return Optional.empty();
        }

        if (BuiltInFunctionDefinitions.LIKE.equals(functionDefinition)) {
            return Optional.of(buildLikeClause(transformed.get(0), transformed.get(1)));
        }
        if (BuiltInFunctionDefinitions.EQUALS.equals(functionDefinition)) {
            return Optional.of(buildEqualsClause(transformed.get(0), transformed.get(1)));
        }
        if (BuiltInFunctionDefinitions.NOT_EQUALS.equals(functionDefinition)) {
            return Optional.of(buildNotEqualsClause(transformed.get(0), transformed.get(1)));
        }
        if (BuiltInFunctionDefinitions.GREATER_THAN.equals(functionDefinition)) {
            return Optional.of(buildGreaterThanClause(transformed.get(0), transformed.get(1)));
        }
        if (BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL.equals(functionDefinition)) {
            return Optional.of(
                    buildGreaterThanOrEqualClause(transformed.get(0), transformed.get(1)));
        }
        if (BuiltInFunctionDefinitions.LESS_THAN.equals(functionDefinition)) {
            return Optional.of(buildLessThanClause(transformed.get(0), transformed.get(1)));
        }
        if (BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL.equals(functionDefinition)) {
            return Optional.of(buildLessThanOrEqualClause(transformed.get(0), transformed.get(1)));
        }
        if (BuiltInFunctionDefinitions.IS_NULL.equals(functionDefinition)) {
            return Optional.of(buildIsNullClause(transformed.get(0)));
        }
        if (BuiltInFunctionDefinitions.IS_NOT_NULL.equals(functionDefinition)) {
            return Optional.of(buildIsNotNullClause(transformed.get(0)));
        }
        if (BuiltInFunctionDefinitions.NOT.equals(functionDefinition)) {
            return Optional.of(buildNotClause(transformed.get(0)));
        }
        if (BuiltInFunctionDefinitions.AND.equals(functionDefinition)) {
            return Optional.of(buildAndClause(transformed.get(0), transformed.get(1)));
        }
        if (BuiltInFunctionDefinitions.OR.equals(functionDefinition)) {
            return Optional.of(buildOrClause(transformed.get(0), transformed.get(1)));
        }

        return Optional.empty();
    }

    protected String formatStringValue(String value) {
        return "'" + value + "'";
    }

    protected String formatLocalDateValue(LocalDate value) {
        return "'" + value.format(DateTimeFormatter.ISO_DATE) + "'";
    }

    protected String formatLocalTimeValue(LocalTime value) {
        return "'" + value.format(DateTimeFormatter.ISO_TIME) + "'";
    }

    protected String formatLocalDateTimeValue(LocalDateTime value) {
        return "'" + value.format(DATE_TIME_FORMATTER) + "'";
    }

    protected String formatInstantValue(Instant value) {
        return "'" + value.atZone(ZoneId.systemDefault()).format(DATE_TIME_FORMATTER) + "'";
    }

    protected String buildLikeClause(String field, String value) {
        return String.format("%s LIKE %s", field, value);
    }

    protected String buildEqualsClause(String field, String value) {
        return String.format("%s = %s", field, value);
    }

    protected String buildNotEqualsClause(String field, String value) {
        return String.format("%s <> %s", field, value);
    }

    protected String buildGreaterThanClause(String field, String value) {
        return String.format("%s > %s", field, value);
    }

    protected String buildGreaterThanOrEqualClause(String field, String value) {
        return String.format("%s >= %s", field, value);
    }

    protected String buildLessThanClause(String field, String value) {
        return String.format("%s < %s", field, value);
    }

    protected String buildLessThanOrEqualClause(String field, String value) {
        return String.format("%s <= %s", field, value);
    }

    protected String buildIsNullClause(String field) {
        return String.format("%s IS NULL", field);
    }

    protected String buildIsNotNullClause(String field) {
        return String.format("%s IS NOT NULL", field);
    }

    protected String buildNotClause(String expr) {
        return String.format("NOT %s", expr);
    }

    protected String buildAndClause(String left, String right) {
        return String.format("(%s AND %s)", left, right);
    }

    protected String buildOrClause(String left, String right) {
        return String.format("(%s OR %s)", left, right);
    }
}
