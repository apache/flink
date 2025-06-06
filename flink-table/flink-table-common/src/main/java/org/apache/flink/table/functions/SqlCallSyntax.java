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

package org.apache.flink.table.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.ExpressionUtils;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.SqlFactory;
import org.apache.flink.table.expressions.TimeIntervalUnit;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.utils.EncodingUtils;

import java.util.List;
import java.util.stream.Collectors;

/** Provides a format for unparsing {@link BuiltInFunctionDefinitions} into a SQL string. */
@Internal
public interface SqlCallSyntax {

    String unparse(String sqlName, List<ResolvedExpression> operands, SqlFactory sqlFactory);

    /**
     * Special case for aggregate functions, which can have a DISTINCT function applied. Called only
     * from the DISTINCT function.
     */
    default String unparseDistinct(
            String sqlName, List<ResolvedExpression> operands, SqlFactory sqlFactory) {
        throw new UnsupportedOperationException(
                "Only the FUNCTION syntax supports the DISTINCT clause.");
    }

    /** Function syntax, as in "Foo(x, y)". */
    SqlCallSyntax FUNCTION =
            new SqlCallSyntax() {
                @Override
                public String unparse(
                        String sqlName, List<ResolvedExpression> operands, SqlFactory sqlFactory) {
                    return doUnParse(sqlName, operands, false, sqlFactory);
                }

                @Override
                public String unparseDistinct(
                        String sqlName, List<ResolvedExpression> operands, SqlFactory sqlFactory) {
                    return doUnParse(sqlName, operands, true, sqlFactory);
                }

                private String doUnParse(
                        String sqlName,
                        List<ResolvedExpression> operands,
                        boolean isDistinct,
                        SqlFactory sqlFactory) {
                    return String.format(
                            "%s(%s%s)",
                            sqlName,
                            isDistinct ? "DISTINCT " : "",
                            operands.stream()
                                    .map(
                                            resolvedExpression ->
                                                    resolvedExpression.asSerializableString(
                                                            sqlFactory))
                                    .collect(Collectors.joining(", ")));
                }
            };

    /**
     * Function syntax for functions without parenthesis (e.g., CURRENT_DATE, LOCALTIMESTAMP,
     * LOCALTIME, CURRENT_TIMESTAMP, CURRENT_TIME).
     */
    SqlCallSyntax NO_PARENTHESIS = (sqlName, operands, sqlFactory) -> sqlName;

    /**
     * Function syntax for handling DISTINCT aggregates. Special case. It does not have a syntax
     * itself, but modifies the syntax of the nested call.
     */
    SqlCallSyntax DISTINCT =
            (sqlName, operands, sqlFactory) -> {
                final CallExpression callExpression = (CallExpression) operands.get(0);
                if (callExpression.getFunctionDefinition() instanceof BuiltInFunctionDefinition) {
                    final BuiltInFunctionDefinition builtinDefinition =
                            (BuiltInFunctionDefinition) callExpression.getFunctionDefinition();
                    return builtinDefinition
                            .getCallSyntax()
                            .unparseDistinct(
                                    builtinDefinition.getSqlName(),
                                    callExpression.getResolvedChildren(),
                                    sqlFactory);
                } else {
                    return SqlCallSyntax.FUNCTION.unparseDistinct(
                            callExpression.getFunctionName(),
                            callExpression.getResolvedChildren(),
                            sqlFactory);
                }
            };

    /** Function syntax for collection ctors, such as ARRAY[1, 2, 3] or MAP['a', 1, 'b', 2]. */
    SqlCallSyntax COLLECTION_CTOR =
            (sqlName, operands, sqlFactory) ->
                    String.format(
                            "%s[%s]",
                            sqlName,
                            operands.stream()
                                    .map(
                                            resolvedExpression ->
                                                    resolvedExpression.asSerializableString(
                                                            sqlFactory))
                                    .collect(Collectors.joining(", ")));

    /** Binary operator syntax, as in "x - y". */
    SqlCallSyntax BINARY_OP =
            (sqlName, operands, sqlFactory) ->
                    String.format(
                            "%s %s %s",
                            CallSyntaxUtils.asSerializableOperand(operands.get(0), sqlFactory),
                            sqlName,
                            CallSyntaxUtils.asSerializableOperand(operands.get(1), sqlFactory));

    /** Syntax for unparsing '+', Special handling for a plus on string arguments. */
    SqlCallSyntax PLUS_OP =
            (sqlName, operands, sqlFactory) -> {
                boolean isString =
                        operands.stream()
                                .anyMatch(
                                        op ->
                                                op.getOutputDataType()
                                                        .getLogicalType()
                                                        .is(LogicalTypeFamily.CHARACTER_STRING));
                if (isString) {
                    return FUNCTION.unparse(
                            BuiltInFunctionDefinitions.CONCAT.getSqlName(), operands, sqlFactory);
                } else {
                    return BINARY_OP.unparse(sqlName, operands, sqlFactory);
                }
            };

    /**
     * Binary operator syntax that in Table API can accept multiple operands, as in "x AND y AND t
     * AND w".
     */
    SqlCallSyntax MULTIPLE_BINARY_OP =
            (sqlName, operands, sqlFactory) ->
                    operands.stream()
                            .map(
                                    expression ->
                                            CallSyntaxUtils.asSerializableOperand(
                                                    expression, sqlFactory))
                            .collect(Collectors.joining(String.format(" %s ", sqlName)));

    /** Postfix unary operator syntax, as in "x ++". */
    SqlCallSyntax UNARY_SUFFIX_OP =
            (sqlName, operands, sqlFactory) ->
                    String.format(
                            "%s %s",
                            CallSyntaxUtils.asSerializableOperand(operands.get(0), sqlFactory),
                            sqlName);

    /** Prefix unary operator syntax, as in "- x". */
    SqlCallSyntax UNARY_PREFIX_OP =
            (sqlName, operands, sqlFactory) ->
                    String.format(
                            "%s %s",
                            sqlName,
                            CallSyntaxUtils.asSerializableOperand(operands.get(0), sqlFactory));

    /**
     * Special sql syntax for CAST operators (CAST, TRY_CAST, REINTERPRET_CAST).
     *
     * <p>Example: CAST(123 AS STRING)
     */
    SqlCallSyntax CAST =
            (sqlName, operands, sqlFactory) ->
                    String.format(
                            "%s(%s AS %s)",
                            sqlName,
                            operands.get(0).asSerializableString(sqlFactory),
                            operands.get(1).asSerializableString(sqlFactory));

    /**
     * Special sql syntax for SUBSTRING operators (SUBSTRING, SUBSTR).
     *
     * <p>Example: SUBSTR('abc' FROM 'abcdef' FOR 3)
     */
    SqlCallSyntax SUBSTRING =
            (sqlName, operands, sqlFactory) -> {
                final String s =
                        String.format(
                                "%s(%s FROM %s",
                                sqlName,
                                operands.get(0).asSerializableString(sqlFactory),
                                operands.get(1).asSerializableString(sqlFactory));
                if (operands.size() == 3) {
                    return s
                            + String.format(
                                    " FOR %s)", operands.get(2).asSerializableString(sqlFactory));
                }

                return s + ")";
            };

    /**
     * Special sql syntax for FLOOR and CEIL.
     *
     * <p>Examples:
     *
     * <ul>
     *   <li>FLOOR(TIME ‘12:44:31’ TO MINUTE)
     *   <li>FLOOR(123)
     * </ul>
     */
    SqlCallSyntax FLOOR_OR_CEIL =
            (sqlName, operands, sqlFactory) -> {
                if (operands.size() == 1) {
                    // case for numeric floor & ceil
                    return SqlCallSyntax.FUNCTION.unparse(sqlName, operands, sqlFactory);
                } else {
                    // case for flooring/ceiling to temporal units
                    return String.format(
                            "%s(%s TO %s)",
                            sqlName,
                            operands.get(0).asSerializableString(sqlFactory),
                            ((ValueLiteralExpression) operands.get(1))
                                    .getValueAs(TimeIntervalUnit.class)
                                    .get());
                }
            };

    /**
     * Special sql syntax for TRIM.
     *
     * <p>Example: TRIM(BOTH ' ' FROM ' 0 ');
     */
    SqlCallSyntax TRIM =
            (sqlName, operands, sqlFactory) -> {
                final boolean trimLeading =
                        ((ValueLiteralExpression) operands.get(0)).getValueAs(Boolean.class).get();
                final boolean trimTrailing =
                        ((ValueLiteralExpression) operands.get(1)).getValueAs(Boolean.class).get();
                final String format;

                // leading & trailing is translated to BOTH
                if (trimLeading && trimTrailing) {
                    format = "TRIM(BOTH %s FROM %s)";
                } else if (trimLeading) {
                    format = "TRIM(LEADING %s FROM %s)";
                } else if (trimTrailing) {
                    format = "TRIM(TRAILING %s FROM %s)";
                } else {
                    format = "TRIM(%s FROM %s)";
                }

                return String.format(
                        format,
                        operands.get(2).asSerializableString(sqlFactory),
                        operands.get(3).asSerializableString(sqlFactory));
            };

    /**
     * Special sql syntax for OVERLAY.
     *
     * <p>Example: OVERLAY('abcd' PLACING 'def' FROM 3 FOR 2)
     */
    SqlCallSyntax OVERLAY =
            (sqlName, operands, sqlFactory) -> {
                final String s =
                        String.format(
                                "OVERLAY(%s PLACING %s FROM %s",
                                operands.get(0).asSerializableString(sqlFactory),
                                operands.get(1).asSerializableString(sqlFactory),
                                operands.get(2).asSerializableString(sqlFactory));

                // optional length
                if (operands.size() == 4) {
                    return s
                            + String.format(
                                    " FOR %s)", operands.get(3).asSerializableString(sqlFactory));
                }

                return s + ")";
            };

    /** Special sql syntax for AS. The string literal is formatted as an identifier. */
    SqlCallSyntax AS =
            (sqlName, operands, sqlFactory) -> {
                if (operands.size() != 2) {
                    throw new TableException(
                            "The AS function with multiple aliases is not SQL"
                                    + " serializable. It should've been flattened during expression"
                                    + " resolution.");
                }
                final String identifier = ExpressionUtils.stringValue(operands.get(1));
                return String.format(
                        "%s %s %s",
                        CallSyntaxUtils.asSerializableOperand(operands.get(0), sqlFactory),
                        sqlName,
                        EncodingUtils.escapeIdentifier(identifier));
            };

    /** Call syntax for {@link BuiltInFunctionDefinitions#IN}. */
    SqlCallSyntax IN =
            (sqlName, operands, sqlFactory) ->
                    String.format(
                            "%s IN (%s)",
                            operands.get(0).asSerializableString(sqlFactory),
                            operands.subList(1, operands.size()).stream()
                                    .map(
                                            resolvedExpression ->
                                                    resolvedExpression.asSerializableString(
                                                            sqlFactory))
                                    .collect(Collectors.joining(", ")));

    SqlCallSyntax WINDOW_START_END =
            (sqlName, operands, sqlFactory) -> String.format("%s", sqlName);

    /**
     * Special sql syntax for LIKE.
     *
     * <p>Example: 'TE_ST' LIKE '%E&_S%' ESCAPE '&';
     */
    SqlCallSyntax LIKE =
            (sqlName, operands, sqlFactory) -> {
                if (operands.size() == 2) {
                    return String.format(
                            "%s %s %s",
                            CallSyntaxUtils.asSerializableOperand(operands.get(0), sqlFactory),
                            sqlName,
                            CallSyntaxUtils.asSerializableOperand(operands.get(1), sqlFactory));
                } else {
                    return String.format(
                            "%s %s %s ESCAPE %s",
                            CallSyntaxUtils.asSerializableOperand(operands.get(0), sqlFactory),
                            sqlName,
                            CallSyntaxUtils.asSerializableOperand(operands.get(1), sqlFactory),
                            CallSyntaxUtils.asSerializableOperand(operands.get(2), sqlFactory));
                }
            };

    SqlCallSyntax OVER =
            ((sqlName, operands, sqlFactory) -> {
                String projection = operands.get(0).asSerializableString(sqlFactory);
                String order = operands.get(1).asSerializableString(sqlFactory);
                String rangeBounds =
                        CallSyntaxUtils.overRangeToSerializableString(
                                operands.get(2), operands.get(3), sqlFactory);
                if (operands.size() == 4) {
                    return String.format("%s OVER(ORDER BY %s%s)", projection, order, rangeBounds);
                } else {
                    return String.format(
                            "%s OVER(PARTITION BY %s ORDER BY %s%s)",
                            projection,
                            CallSyntaxUtils.asSerializableOperand(operands.get(4), sqlFactory),
                            order,
                            rangeBounds);
                }
            });
}
