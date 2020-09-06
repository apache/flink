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
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.expressions.resolver.LocalOverWindow;
import org.apache.flink.table.expressions.utils.ApiExpressionDefaultVisitor;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedCall;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.BIGINT;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.INTERVAL_DAY_TIME;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;

/**
 * Joins call to {@link BuiltInFunctionDefinitions#OVER} with corresponding over window
 * and creates a fully resolved over aggregation.
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

				LocalOverWindow referenceWindow = resolutionContext.getOverWindow(alias)
					.orElseThrow(() -> new ValidationException("Could not resolve over call."));

				Expression following = calculateOverWindowFollowing(referenceWindow);
				List<Expression> newArgs = new ArrayList<>(asList(
					children.get(0),
					referenceWindow.getOrderBy(),
					referenceWindow.getPreceding(),
					following));

				newArgs.addAll(referenceWindow.getPartitionBy());

				return unresolvedCall.replaceArgs(newArgs);
			} else {
				return unresolvedCall.replaceArgs(
					unresolvedCall.getChildren().stream()
						.map(expr -> expr.accept(this))
						.collect(Collectors.toList()));
			}
		}

		private Expression calculateOverWindowFollowing(LocalOverWindow referenceWindow) {
			return referenceWindow.getFollowing().orElseGet(() -> {
					WindowKind kind = referenceWindow.getPreceding().accept(OVER_WINDOW_KIND_EXTRACTOR);
					if (kind == WindowKind.ROW) {
						return unresolvedCall(BuiltInFunctionDefinitions.CURRENT_ROW);
					} else {
						return unresolvedCall(BuiltInFunctionDefinitions.CURRENT_RANGE);
					}
				}
			);
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
			if (hasRoot(literalType, BIGINT)) {
				return WindowKind.ROW;
			} else if (hasRoot(literalType, INTERVAL_DAY_TIME)) {
				return WindowKind.RANGE;
			}
			return defaultMethod(valueLiteral);
		}

		@Override
		public WindowKind visit(UnresolvedCallExpression unresolvedCall) {
			final FunctionDefinition definition = unresolvedCall.getFunctionDefinition();
			if (definition == BuiltInFunctionDefinitions.UNBOUNDED_ROW) {
				return WindowKind.ROW;
			} else if (definition == BuiltInFunctionDefinitions.UNBOUNDED_RANGE) {
				return WindowKind.RANGE;
			}
			return defaultMethod(unresolvedCall);
		}

		@Override
		protected WindowKind defaultMethod(Expression expression) {
			throw new ValidationException("An over window expects literal or unbounded bounds for preceding.");
		}
	}
}
