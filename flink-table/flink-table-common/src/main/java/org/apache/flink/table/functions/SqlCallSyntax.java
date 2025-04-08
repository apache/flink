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

    String unparse(String sqlName, List<ResolvedExpression> operands, SqlFactory context);

    /**
     * Special case for aggregate functions, which can have a DISTINCT function applied. Called only
     * from the DISTINCT function.
     */
    default String unparseDistinct(
            String sqlName, List<ResolvedExpression> operands, SqlFactory context) {
        throw new UnsupportedOperationException(
                "Only the FUNCTION syntax supports the DISTINCT clause.");
    }

    /** Function syntax, as in "Foo(x, y)". */
    SqlCallSyntax FUNCTION =
            new SqlCallSyntax() {
                @Override
                public String unparse(
                        String sqlName, List<ResolvedExpression> operands, SqlFactory context) {
                    return doUnParse(sqlName, operands, false, context);
                }

                @Override
                public String unparseDistinct(
                        String sqlName, List<ResolvedExpression> operands, SqlFactory context) {
                    return doUnParse(sqlName, operands, true, context);
                }

                private String doUnParse(
                        String sqlName,
                        List<ResolvedExpression> operands,
                        boolean isDistinct,
                        SqlFactory context) {
                    return String.format(
                            "%s(%s%s)",
                            sqlName,
                            isDistinct ? "DISTINCT " : "",
                            operands.stream()
                                    .map(
                                            resolvedExpression ->
                                                    resolvedExpression.asSerializableString(
                                                            context))
                                    .collect(Collectors.joining(", ")));
                }
            };

    /**
     * Function syntax for functions without parenthesis (e.g., CURRENT_DATE, LOCALTIMESTAMP,
     * LOCALTIME, CURRENT_TIMESTAMP, CURRENT_TIME).
     */
    SqlCallSyntax NO_PARENTHESIS = (sqlName, operands, context) -> sqlName;

    /**
     * Function syntax for handling DISTINCT aggregates. Special case. It does not have a syntax
     * itself, but modifies the syntax of the nested call.
     */
    SqlCallSyntax DISTINCT =
            (sqlName, operands, context) -> {
                final CallExpression callExpression = (CallExpression) operands.get(0);
                if (callExpression.getFunctionDefinition() instanceof BuiltInFunctionDefinition) {
                    final BuiltInFunctionDefinition builtinDefinition =
                            (BuiltInFunctionDefinition) callExpression.getFunctionDefinition();
                    return builtinDefinition
                            .getCallSyntax()
                            .unparseDistinct(
                                    builtinDefinition.getSqlName(),
                                    callExpression.getResolvedChildren(),
                                    context);
                } else {
                    return SqlCallSyntax.FUNCTION.unparseDistinct(
                            callExpression.getFunctionName(),
                            callExpression.getResolvedChildren(),
                            context);
                }
            };

    /** Function syntax for collection ctors, such as ARRAY[1, 2, 3] or MAP['a', 1, 'b', 2]. */
    SqlCallSyntax COLLECTION_CTOR =
            (sqlName, operands, context) ->
                    String.format(
                            "%s[%s]",
                            sqlName,
                            operands.stream()
                                    .map(
                                            resolvedExpression ->
                                                    resolvedExpression.asSerializableString(
                                                            context))
                                    .collect(Collectors.joining(", ")));

    /** Binary operator syntax, as in "x - y". */
    SqlCallSyntax BINARY_OP =
            (sqlName, operands, context) ->
                    String.format(
                            "%s %s %s",
                            CallSyntaxUtils.asSerializableOperand(operands.get(0), context),
                            sqlName,
                            CallSyntaxUtils.asSerializableOperand(operands.get(1), context));

    /** Syntax for unparsing '+', Special handling for a plus on string arguments. */
    SqlCallSyntax PLUS_OP =
            (sqlName, operands, context) -> {
                boolean isString =
                        operands.stream()
                                .anyMatch(
                                        op ->
                                                op.getOutputDataType()
                                                        .getLogicalType()
                                                        .is(LogicalTypeFamily.CHARACTER_STRING));
                if (isString) {
                    return FUNCTION.unparse(
                            BuiltInFunctionDefinitions.CONCAT.getSqlName(), operands, context);
                } else {
                    return BINARY_OP.unparse(sqlName, operands, context);
                }
            };

    /**
     * Binary operator syntax that in Table API can accept multiple operands, as in "x AND y AND t
     * AND w".
     */
    SqlCallSyntax MULTIPLE_BINARY_OP =
            (sqlName, operands, context) ->
                    operands.stream()
                            .map(
                                    expression ->
                                            CallSyntaxUtils.asSerializableOperand(
                                                    expression, context))
                            .collect(Collectors.joining(String.format(" %s ", sqlName)));

    /** Postfix unary operator syntax, as in "x ++". */
    SqlCallSyntax UNARY_SUFFIX_OP =
            (sqlName, operands, context) ->
                    String.format(
                            "%s %s",
                            CallSyntaxUtils.asSerializableOperand(operands.get(0), context),
                            sqlName);

    /** Prefix unary operator syntax, as in "- x". */
    SqlCallSyntax UNARY_PREFIX_OP =
            (sqlName, operands, context) ->
                    String.format(
                            "%s %s",
                            sqlName,
                            CallSyntaxUtils.asSerializableOperand(operands.get(0), context));

    /**
     * Special sql syntax for CAST operators (CAST, TRY_CAST, REINTERPRET_CAST).
     *
     * <p>Example: CAST(123 AS STRING)
     */
    SqlCallSyntax CAST =
            (sqlName, operands, context) ->
                    String.format(
                            "%s(%s AS %s)",
                            sqlName,
                            operands.get(0).asSerializableString(context),
                            operands.get(1).asSerializableString(context));

    /**
     * Special sql syntax for SUBSTRING operators (SUBSTRING, SUBSTR).
     *
     * <p>Example: SUBSTR('abc' FROM 'abcdef' FOR 3)
     */
    SqlCallSyntax SUBSTRING =
            (sqlName, operands, context) -> {
                final String s =
                        String.format(
                                "%s(%s FROM %s",
                                sqlName,
                                operands.get(0).asSerializableString(context),
                                operands.get(1).asSerializableString(context));
                if (operands.size() == 3) {
                    return s
                            + String.format(
                                    " FOR %s)", operands.get(2).asSerializableString(context));
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
            (sqlName, operands, context) -> {
                if (operands.size() == 1) {
                    // case for numeric floor & ceil
                    return SqlCallSyntax.FUNCTION.unparse(sqlName, operands, context);
                } else {
                    // case for flooring/ceiling to temporal units
                    return String.format(
                            "%s(%s TO %s)",
                            sqlName,
                            operands.get(0).asSerializableString(context),
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
            (sqlName, operands, context) -> {
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
                        operands.get(2).asSerializableString(context),
                        operands.get(3).asSerializableString(context));
            };

    /**
     * Special sql syntax for OVERLAY.
     *
     * <p>Example: OVERLAY('abcd' PLACING 'def' FROM 3 FOR 2)
     */
    SqlCallSyntax OVERLAY =
            (sqlName, operands, context) -> {
                final String s =
                        String.format(
                                "OVERLAY(%s PLACING %s FROM %s",
                                operands.get(0).asSerializableString(context),
                                operands.get(1).asSerializableString(context),
                                operands.get(2).asSerializableString(context));

                // optional length
                if (operands.size() == 4) {
                    return s
                            + String.format(
                                    " FOR %s)", operands.get(3).asSerializableString(context));
                }

                return s + ")";
            };

    /** Special sql syntax for AS. The string literal is formatted as an identifier. */
    SqlCallSyntax AS =
            (sqlName, operands, context) -> {
                if (operands.size() != 2) {
                    throw new TableException(
                            "The AS function with multiple aliases is not SQL"
                                    + " serializable. It should've been flattened during expression"
                                    + " resolution.");
                }
                final String identifier = ExpressionUtils.stringValue(operands.get(1));
                return String.format(
                        "%s %s %s",
                        CallSyntaxUtils.asSerializableOperand(operands.get(0), context),
                        sqlName,
                        EncodingUtils.escapeIdentifier(identifier));
            };

    /** Call syntax for {@link BuiltInFunctionDefinitions#IN}. */
    SqlCallSyntax IN =
            (sqlName, operands, context) ->
                    String.format(
                            "%s IN (%s)",
                            operands.get(0).asSerializableString(context),
                            operands.subList(1, operands.size()).stream()
                                    .map(
                                            resolvedExpression ->
                                                    resolvedExpression.asSerializableString(
                                                            context))
                                    .collect(Collectors.joining(", ")));

    SqlCallSyntax WINDOW_START_END = (sqlName, operands, context) -> String.format("%s", sqlName);

    /**
     * Special sql syntax for LIKE.
     *
     * <p>Example: 'TE_ST' LIKE '%E&_S%' ESCAPE '&';
     */
    SqlCallSyntax LIKE =
            (sqlName, operands, context) -> {
                if (operands.size() == 2) {
                    return String.format(
                            "%s %s %s",
                            CallSyntaxUtils.asSerializableOperand(operands.get(0), context),
                            sqlName,
                            CallSyntaxUtils.asSerializableOperand(operands.get(1), context));
                } else {
                    return String.format(
                            "%s %s %s ESCAPE %s",
                            CallSyntaxUtils.asSerializableOperand(operands.get(0), context),
                            sqlName,
                            CallSyntaxUtils.asSerializableOperand(operands.get(1), context),
                            CallSyntaxUtils.asSerializableOperand(operands.get(2), context));
                }
            };

    SqlCallSyntax OVER =
            ((sqlName, operands, context) -> {
                String projection = operands.get(0).asSerializableString(context);
                String order = operands.get(1).asSerializableString(context);
                String rangeBounds =
                        CallSyntaxUtils.overRangeToSerializableString(
                                operands.get(2), operands.get(3), context);
                if (operands.size() == 4) {
                    return String.format("%s OVER(ORDER BY %s%s)", projection, order, rangeBounds);
                } else {
                    return String.format(
                            "%s OVER(PARTITION BY %s ORDER BY %s%s)",
                            projection,
                            CallSyntaxUtils.asSerializableOperand(operands.get(4), context),
                            order,
                            rangeBounds);
                }
            });
}
