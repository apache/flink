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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.LocalReferenceExpression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;

import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedCall;

/**
 * Resolves {@link UnresolvedReferenceExpression} to either
 * {@link org.apache.flink.table.expressions.FieldReferenceExpression},
 * {@link org.apache.flink.table.expressions.TableReferenceExpression}, or
 * {@link LocalReferenceExpression} in this order.
 */
@Internal
final class ReferenceResolverRule implements ResolverRule {

	@Override
	public List<Expression> apply(List<Expression> expression, ResolutionContext context) {
		return expression.stream()
			.map(expr -> expr.accept(new ExpressionResolverVisitor(context)))
			.collect(Collectors.toList());
	}

	private static class ExpressionResolverVisitor extends RuleExpressionVisitor<Expression> {

		ExpressionResolverVisitor(ResolutionContext resolutionContext) {
			super(resolutionContext);
		}

		@Override
		public Expression visit(UnresolvedCallExpression unresolvedCall) {
			final List<Expression> resolvedArgs = unresolvedCall.getChildren()
				.stream()
				.map(expr -> expr.accept(this))
				.collect(Collectors.toList());

			return unresolvedCall.replaceArgs(resolvedArgs);
		}

		@Override
		public Expression visit(UnresolvedReferenceExpression unresolvedReference) {
			return resolutionContext.referenceLookup().lookupField(unresolvedReference.getName())
				.map(expr -> (Expression) expr)
				.orElseGet(() ->
					resolutionContext.tableLookup().lookupTable(unresolvedReference.getName())
						.map(expr -> (Expression) expr)
						.orElseGet(() -> resolutionContext.getLocalReference(unresolvedReference.getName())
							.orElseThrow(() -> failForField(unresolvedReference)
							)));
		}

		private ValidationException failForField(UnresolvedReferenceExpression fieldReference) {
			return new ValidationException(format("Cannot resolve field [%s], input field list:[%s].",
				fieldReference.getName(),
				resolutionContext.referenceLookup().getAllInputFields()
					.stream().map(FieldReferenceExpression::getName)
					.collect(Collectors.joining(", ")))
			);
		}

		@Override
		protected Expression defaultMethod(Expression expression) {
			return expression;
		}
	}
}
