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
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.SqlCallExpression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.resolver.SqlExpressionResolver;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Resolves {@link SqlCallExpression}s to {@link ResolvedExpression} by delegating to the planner.
 */
@Internal
final class ResolveSqlCallRule implements ResolverRule {

    @Override
    public List<Expression> apply(List<Expression> expression, ResolutionContext context) {
        // only the top-level expressions may access the output data type
        final LogicalType outputType =
                context.getOutputDataType().map(DataType::getLogicalType).orElse(null);
        final TranslateSqlCallsVisitor visitor = new TranslateSqlCallsVisitor(context, outputType);
        return expression.stream().map(expr -> expr.accept(visitor)).collect(Collectors.toList());
    }

    private static class TranslateSqlCallsVisitor extends RuleExpressionVisitor<Expression> {

        private final @Nullable LogicalType outputType;

        TranslateSqlCallsVisitor(
                ResolutionContext resolutionContext, @Nullable LogicalType outputType) {
            super(resolutionContext);
            this.outputType = outputType;
        }

        @Override
        public Expression visit(SqlCallExpression sqlCall) {
            final SqlExpressionResolver resolver = resolutionContext.sqlExpressionResolver();

            final List<RowField> fields = new ArrayList<>();
            // input references
            resolutionContext
                    .referenceLookup()
                    .getAllInputFields()
                    .forEach(
                            f ->
                                    fields.add(
                                            new RowField(
                                                    f.getName(),
                                                    f.getOutputDataType().getLogicalType())));
            // local references
            resolutionContext
                    .getLocalReferences()
                    .forEach(
                            refs ->
                                    fields.add(
                                            new RowField(
                                                    refs.getName(),
                                                    refs.getOutputDataType().getLogicalType())));
            return resolver.resolveExpression(
                    sqlCall.getSqlExpression(), new RowType(false, fields), outputType);
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
            final TranslateSqlCallsVisitor visitor =
                    new TranslateSqlCallsVisitor(resolutionContext, null);
            return lookupChildren.stream()
                    .map(child -> child.accept(visitor))
                    .collect(Collectors.toList());
        }
    }
}
