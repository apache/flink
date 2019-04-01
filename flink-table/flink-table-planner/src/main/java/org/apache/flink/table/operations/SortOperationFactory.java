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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.ApiExpressionDefaultVisitor;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionBridge;
import org.apache.flink.table.expressions.PlannerExpression;
import org.apache.flink.table.plan.logical.Limit;
import org.apache.flink.table.plan.logical.LogicalNode;
import org.apache.flink.table.plan.logical.Sort;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.ORDERING;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.ORDER_ASC;

/**
 * Utility class for creating a valid {@link Sort} operation.
 */
@Internal
public class SortOperationFactory {

	private final boolean isStreaming;
	private final ExpressionBridge<PlannerExpression> expressionBridge;
	private final OrderWrapper orderWrapper = new OrderWrapper();

	public SortOperationFactory(
			ExpressionBridge<PlannerExpression> expressionBridge,
			boolean isStreaming) {
		this.expressionBridge = expressionBridge;
		this.isStreaming = isStreaming;
	}

	/**
	 * Creates a valid {@link Sort} operation.
	 *
	 * <p><b>NOTE:</b> if the collation is not explicitly specified for any expression, it is wrapped in a
	 * default ascending order
	 *
	 * @param orders expressions describing order,
	 * @param child relational expression on top of which to apply the sort operation
	 * @return valid sort operation
	 */
	public Sort createSort(List<Expression> orders, TableOperation child) {
		failIfStreaming();

		List<PlannerExpression> convertedOrders = orders.stream()
			.map(f -> f.accept(orderWrapper))
			.map(expressionBridge::bridge)
			.collect(Collectors.toList());
		return new Sort(convertedOrders, (LogicalNode) child);
	}

	/**
	 * Creates a valid {@link Limit} operation.
	 *
	 * @param offset offset to start from
	 * @param fetch number of records to fetch
	 * @return valid limit operation
	 */
	public Limit createLimit(int offset, int fetch, TableOperation child) {
		failIfStreaming();

		if (!(child instanceof Sort)) {
			throw new ValidationException("A limit operation must be preceded by a sort operation.");
		}
		if (offset < 0) {
			throw new ValidationException("Offset should be greater than or equal to zero.");
		}

		return new Limit(offset, fetch, (LogicalNode) child);
	}

	private void failIfStreaming() {
		if (isStreaming) {
			throw new ValidationException("A limit operation on unbounded tables is currently not supported.");
		}
	}

	private class OrderWrapper extends ApiExpressionDefaultVisitor<Expression> {

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
