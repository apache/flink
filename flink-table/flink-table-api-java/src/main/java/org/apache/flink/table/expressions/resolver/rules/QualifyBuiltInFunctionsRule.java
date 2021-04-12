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
import org.apache.flink.table.catalog.FunctionLookup;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Looks up built-in functions in a catalog for retrieving a fully qualified {@link
 * ObjectIdentifier}.
 */
@Internal
final class QualifyBuiltInFunctionsRule implements ResolverRule {

    @Override
    public List<Expression> apply(List<Expression> expression, ResolutionContext context) {
        return expression.stream()
                .map(expr -> expr.accept(new QualifyBuiltInFunctionVisitor(context)))
                .collect(Collectors.toList());
    }

    private static class QualifyBuiltInFunctionVisitor extends RuleExpressionVisitor<Expression> {

        QualifyBuiltInFunctionVisitor(ResolutionContext resolutionContext) {
            super(resolutionContext);
        }

        @Override
        public Expression visit(UnresolvedCallExpression unresolvedCall) {
            if (!unresolvedCall.getFunctionIdentifier().isPresent()
                    && unresolvedCall.getFunctionDefinition()
                            instanceof BuiltInFunctionDefinition) {
                final FunctionLookup.Result functionLookup =
                        resolutionContext
                                .functionLookup()
                                .lookupBuiltInFunction(
                                        ((BuiltInFunctionDefinition)
                                                unresolvedCall.getFunctionDefinition()));

                return ApiExpressionUtils.unresolvedCall(
                        functionLookup.getFunctionIdentifier(),
                        functionLookup.getFunctionDefinition(),
                        unresolvedCall.getChildren().stream()
                                .map(c -> c.accept(this))
                                .collect(Collectors.toList()));
            }

            return unresolvedCall;
        }

        @Override
        protected Expression defaultMethod(Expression expression) {
            return expression;
        }
    }
}
