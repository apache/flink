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

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.expressions.ApiExpressionDefaultVisitor;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FunctionDefinition;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.ORDER_ASC;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.ORDER_DESC;

/**
 * Utility class for sort specific expressions transformations.
 */
@Internal
public final class SortOperationUtils {

	private static final List<FunctionDefinition> ORDERING = Arrays.asList(ORDER_ASC, ORDER_DESC);

	/**
	 * Makes sure that all expressions are wrapped in ordering expression. If the expression is either
	 * {@link org.apache.flink.table.expressions.BuiltInFunctionDefinitions#ORDER_ASC} or
	 * {@link org.apache.flink.table.expressions.BuiltInFunctionDefinitions#ORDER_DESC} it does nothing, otherwise
	 * it inserts {@link org.apache.flink.table.expressions.BuiltInFunctionDefinitions#ORDER_ASC}.
	 */
	public static List<Expression> wrapInOrder(List<Expression> expression) {
		return expression.stream()
			.map(expr -> expr.accept(new OrderWrapper()))
			.collect(Collectors.toList());
	}

	private static class OrderWrapper extends ApiExpressionDefaultVisitor<Expression> {

		@Override
		public Expression visitCall(CallExpression call) {
			if (ORDERING.contains(call.getFunctionDefinition())) {
				return call;
			} else {
				return defaultMethod(call);
			}
		}

		@Override
		protected Expression defaultMethod(Expression expression) {
			return new CallExpression(ORDER_ASC, singletonList(expression));
		}
	}
}
