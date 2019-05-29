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

package org.apache.flink.table.expressions.rules;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.BuiltInFunctionDefinitions;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.PlannerExpression;
import org.apache.flink.table.plan.logical.LogicalOverWindow;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

/**
 * Joins call to {@link BuiltInFunctionDefinitions#OVER} with corresponding over window
 * and creates a fully resolved over aggregation.
 */
@Internal
final class OverWindowResolverRule implements ResolverRule {

	@Override
	public List<Expression> apply(List<Expression> expression, ResolutionContext context) {
		return expression.stream()
			.map(expr -> expr.accept(new ExpressionResolverVisitor(context)))
			.collect(Collectors.toList());
	}

	private class ExpressionResolverVisitor extends RuleExpressionVisitor<Expression> {

		ExpressionResolverVisitor(ResolutionContext context) {
			super(context);
		}

		@Override
		public Expression visitCall(CallExpression call) {

			if (call.getFunctionDefinition() == BuiltInFunctionDefinitions.OVER) {
				List<Expression> children = call.getChildren();
				Expression alias = children.get(1);

				LogicalOverWindow referenceWindow = resolutionContext.getOverWindow(alias)
					.orElseThrow(() -> new ValidationException("Could not resolve over call."));

				Expression following = calculateOverWindowFollowing(referenceWindow);
				List<Expression> newArgs = new ArrayList<>(asList(
					children.get(0),
					referenceWindow.orderBy(),
					referenceWindow.preceding(),
					following));

				newArgs.addAll(referenceWindow.partitionBy());
				return new CallExpression(call.getFunctionDefinition(), newArgs);
			} else {
				return new CallExpression(
					call.getFunctionDefinition(),
					call.getChildren().stream().map(expr -> expr.accept(this)).collect(toList()));
			}
		}

		private Expression calculateOverWindowFollowing(LogicalOverWindow referenceWindow) {
			return referenceWindow.following().orElseGet(() -> {
					PlannerExpression preceding = resolutionContext.bridge(referenceWindow.preceding());
					if (preceding.resultType() == BasicTypeInfo.LONG_TYPE_INFO) {
						return new CallExpression(BuiltInFunctionDefinitions.CURRENT_ROW, emptyList());
					} else {
						return new CallExpression(BuiltInFunctionDefinitions.CURRENT_RANGE, emptyList());
					}
				}
			);
		}

		@Override
		protected Expression defaultMethod(Expression expression) {
			return expression;
		}
	}
}
