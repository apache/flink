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
import org.apache.flink.table.api.OverWindowRange;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.TableSymbol;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.Optional;

/** Utility functions that can be used for writing {@link SqlCallSyntax}. */
@Internal
class CallSyntaxUtils {

    /**
     * Converts the given {@link ResolvedExpression} into a SQL string. Wraps the string with
     * parenthesis if the expression is not a leaf expression such as e.g. {@link
     * ValueLiteralExpression} or {@link FieldReferenceExpression}.
     */
    static String asSerializableOperand(ResolvedExpression expression) {
        if (expression.getResolvedChildren().isEmpty()) {
            return expression.asSerializableString();
        }

        return String.format("(%s)", expression.asSerializableString());
    }

    static <T extends TableSymbol> T getSymbolLiteral(ResolvedExpression operands, Class<T> clazz) {
        return ((ValueLiteralExpression) operands).getValueAs(clazz).get();
    }

    static String overRangeToSerializableString(
            ResolvedExpression preceding, ResolvedExpression following) {
        if (((ValueLiteralExpression) preceding).isNull()
                || ((ValueLiteralExpression) following).isNull()) {
            return "";
        }
        return String.format(
                " %s BETWEEN %s AND %s",
                isRowsRange(preceding) ? "ROWS" : "RANGE",
                toStringPrecedingOrFollowing(preceding, true),
                toStringPrecedingOrFollowing(following, false));
    }

    private static String toStringPrecedingOrFollowing(
            ResolvedExpression precedingOrFollowing, boolean isPreceding) {
        final String suffix = isPreceding ? "PRECEDING" : "FOLLOWING";
        return Optional.of(precedingOrFollowing)
                .flatMap(
                        expr -> {
                            if (expr instanceof ValueLiteralExpression) {
                                return ((ValueLiteralExpression) expr)
                                        .getValueAs(OverWindowRange.class)
                                        .map(
                                                r -> {
                                                    switch (r) {
                                                        case CURRENT_ROW:
                                                        case CURRENT_RANGE:
                                                            return "CURRENT ROW";
                                                        case UNBOUNDED_ROW:
                                                        case UNBOUNDED_RANGE:
                                                            return "UNBOUNDED " + suffix;
                                                        default:
                                                            throw new IllegalStateException(
                                                                    "Unknown window range: " + r);
                                                    }
                                                });
                            } else {
                                return Optional.empty();
                            }
                        })
                .orElseGet(() -> precedingOrFollowing.asSerializableString() + " " + suffix);
    }

    private static boolean isRowsRange(ResolvedExpression expression) {
        LogicalType logicalType = expression.getOutputDataType().getLogicalType();
        boolean isSymbol = logicalType.is(LogicalTypeRoot.SYMBOL);
        if (isSymbol) {
            OverWindowRange windowRange = getSymbolLiteral(expression, OverWindowRange.class);
            return windowRange == OverWindowRange.CURRENT_ROW
                    || windowRange == OverWindowRange.UNBOUNDED_ROW;
        }

        return logicalType.is(LogicalTypeFamily.NUMERIC);
    }

    private CallSyntaxUtils() {}
}
