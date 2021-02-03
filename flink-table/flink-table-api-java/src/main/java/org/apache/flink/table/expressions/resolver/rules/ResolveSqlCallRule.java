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
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.SqlCallExpression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.resolver.SqlExpressionResolver;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Resolves {@link SqlCallExpression}s to {@link ResolvedExpression} by delegating to the planner.
 */
@Internal
final class ResolveSqlCallRule implements ResolverRule {

    @Override
    public List<Expression> apply(List<Expression> expression, ResolutionContext context) {
        return expression.stream()
                .map(expr -> expr.accept(new TranslateSqlCallsVisitor(context)))
                .collect(Collectors.toList());
    }

    private static class TranslateSqlCallsVisitor extends RuleExpressionVisitor<Expression> {

        TranslateSqlCallsVisitor(ResolutionContext resolutionContext) {
            super(resolutionContext);
        }

        @Override
        public Expression visit(SqlCallExpression sqlCall) {
            final SqlExpressionResolver resolver = resolutionContext.sqlExpressionResolver();

            final TableSchema.Builder builder = TableSchema.builder();
            // input references
            resolutionContext
                    .referenceLookup()
                    .getAllInputFields()
                    .forEach(f -> builder.field(f.getName(), f.getOutputDataType()));
            // local references
            resolutionContext
                    .getLocalReferences()
                    .forEach(refs -> builder.field(refs.getName(), refs.getOutputDataType()));
            return resolver.resolveExpression(sqlCall.getSqlExpression(), builder.build());
        }

        @Override
        public Expression visit(UnresolvedCallExpression unresolvedCall) {
            return unresolvedCall.replaceArgs(resolveChildren(unresolvedCall.getChildren()));
        }

        @Override
        protected Expression defaultMethod(Expression expression) {
            return expression;
        }

        private List<Expression> resolveChildren(List<Expression> lookupChildren) {
            return lookupChildren.stream()
                    .map(child -> child.accept(this))
                    .collect(Collectors.toList());
        }
    }
}
