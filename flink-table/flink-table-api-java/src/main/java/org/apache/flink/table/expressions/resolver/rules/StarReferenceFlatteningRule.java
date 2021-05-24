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
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;

/**
 * Replaces '*' with all available {@link
 * org.apache.flink.table.expressions.FieldReferenceExpression}s from underlying inputs.
 */
@Internal
final class StarReferenceFlatteningRule implements ResolverRule {

    @Override
    public List<Expression> apply(List<Expression> expression, ResolutionContext context) {
        return expression.stream()
                .flatMap(expr -> expr.accept(new FieldFlatteningVisitor(context)).stream())
                .collect(Collectors.toList());
    }

    private static class FieldFlatteningVisitor extends RuleExpressionVisitor<List<Expression>> {

        FieldFlatteningVisitor(ResolutionContext resolutionContext) {
            super(resolutionContext);
        }

        @Override
        public List<Expression> visit(UnresolvedReferenceExpression unresolvedReference) {
            if (unresolvedReference.getName().equals("*")) {
                return new ArrayList<>(resolutionContext.referenceLookup().getAllInputFields());
            } else {
                return singletonList(unresolvedReference);
            }
        }

        @Override
        public List<Expression> visit(UnresolvedCallExpression unresolvedCall) {
            final List<Expression> newArgs =
                    unresolvedCall.getChildren().stream()
                            .flatMap(e -> e.accept(this).stream())
                            .collect(Collectors.toList());
            return singletonList(unresolvedCall.replaceArgs(newArgs));
        }

        @Override
        protected List<Expression> defaultMethod(Expression expression) {
            return singletonList(expression);
        }
    }
}
