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

package org.apache.flink.table.expressions.resolver.rules;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.OverWindowRange;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.expressions.resolver.LocalOverWindow;
import org.apache.flink.table.expressions.utils.ApiExpressionDefaultVisitor;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedCall;
import static org.apache.flink.table.expressions.ApiExpressionUtils.valueLiteral;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.BIGINT;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.INTERVAL_DAY_TIME;

/**
 * Joins call to {@link BuiltInFunctionDefinitions#OVER} with corresponding over window and creates
 * a fully resolved over aggregation.
 */
@Internal
final class OverWindowResolverRule implements ResolverRule {

    private static final WindowKindExtractor OVER_WINDOW_KIND_EXTRACTOR = new WindowKindExtractor();

    @Override
    public List<Expression> apply(List<Expression> expression, ResolutionContext context) {
        return expression.stream()
                .map(expr -> expr.accept(new ExpressionResolverVisitor(context)))
                .collect(Collectors.toList());
    }

    private static class ExpressionResolverVisitor extends RuleExpressionVisitor<Expression> {

        ExpressionResolverVisitor(ResolutionContext context) {
            super(context);
        }

        @Override
        public Expression visit(UnresolvedCallExpression unresolvedCall) {

            if (unresolvedCall.getFunctionDefinition() == BuiltInFunctionDefinitions.OVER) {
                List<Expression> children = unresolvedCall.getChildren();
                Expression alias = children.get(1);

                LocalOverWindow referenceWindow =
                        resolutionContext
                                .getOverWindow(alias)
                                .orElseThrow(
                                        () ->
                                                new ValidationException(
                                                        "Could not resolve over call."));

                UnresolvedCallExpression agg = (UnresolvedCallExpression) children.get(0);

                final List<Expression> newArgs = new ArrayList<>();
                newArgs.add(agg);
                newArgs.add(referenceWindow.getOrderBy());
                if (agg.getFunctionDefinition() == BuiltInFunctionDefinitions.LAG
                        || agg.getFunctionDefinition() == BuiltInFunctionDefinitions.LEAD) {
                    if (referenceWindow.getPreceding().isPresent()
                            || referenceWindow.getFollowing().isPresent()) {
                        throw new ValidationException(
                                "LEAD/LAG functions do not support "
                                        + "providing RANGE/ROW bounds.");
                    }
                    newArgs.add(valueLiteral(null, DataTypes.NULL()));
                    newArgs.add(valueLiteral(null, DataTypes.NULL()));
                } else {
                    Expression preceding =
                            referenceWindow
                                    .getPreceding()
                                    .orElse(valueLiteral(OverWindowRange.UNBOUNDED_RANGE));
                    newArgs.add(preceding);
                    newArgs.add(calculateOverWindowFollowing(referenceWindow, preceding));
                }

                newArgs.addAll(referenceWindow.getPartitionBy());

                return unresolvedCall.replaceArgs(newArgs);
            } else {
                return unresolvedCall.replaceArgs(
                        unresolvedCall.getChildren().stream()
                                .map(expr -> expr.accept(this))
                                .collect(Collectors.toList()));
            }
        }

        private Expression calculateOverWindowFollowing(
                LocalOverWindow referenceWindow, Expression preceding) {
            return referenceWindow
                    .getFollowing()
                    .orElseGet(
                            () -> {
                                WindowKind kind = preceding.accept(OVER_WINDOW_KIND_EXTRACTOR);
                                if (kind == WindowKind.ROW) {
                                    return valueLiteral(OverWindowRange.CURRENT_ROW);
                                } else {
                                    return valueLiteral(OverWindowRange.CURRENT_RANGE);
                                }
                            });
        }

        @Override
        protected Expression defaultMethod(Expression expression) {
            return expression;
        }
    }

    private enum WindowKind {
        ROW,
        RANGE
    }

    private static class WindowKindExtractor extends ApiExpressionDefaultVisitor<WindowKind> {

        @Override
        public WindowKind visit(ValueLiteralExpression valueLiteral) {
            final LogicalType literalType = valueLiteral.getOutputDataType().getLogicalType();
            if (literalType.is(BIGINT)) {
                return WindowKind.ROW;
            } else if (literalType.is(INTERVAL_DAY_TIME)) {
                return WindowKind.RANGE;
            }

            return valueLiteral
                    .getValueAs(OverWindowRange.class)
                    .map(
                            v -> {
                                switch (v) {
                                    case CURRENT_ROW:
                                    case UNBOUNDED_ROW:
                                        return WindowKind.ROW;
                                    case CURRENT_RANGE:
                                    case UNBOUNDED_RANGE:
                                        return WindowKind.RANGE;
                                    default:
                                        throw new IllegalArgumentException(
                                                "Unexpected window range: " + v);
                                }
                            })
                    .orElseGet(() -> defaultMethod(valueLiteral));
        }

        @Override
        protected WindowKind defaultMethod(Expression expression) {
            throw new ValidationException(
                    "An over window expects literal or unbounded bounds for preceding.");
        }
    }
}
