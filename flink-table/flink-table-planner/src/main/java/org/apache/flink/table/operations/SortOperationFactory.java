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

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.ORDERING;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.ORDER_ASC;

/**
 * Utility class for creating a valid {@link SortTableOperation} operation.
 */
@Internal
public class SortOperationFactory {

	private final boolean isStreaming;
	private final OrderWrapper orderWrapper = new OrderWrapper();

	public SortOperationFactory(boolean isStreaming) {
		this.isStreaming = isStreaming;
	}

	/**
	 * Creates a valid {@link SortTableOperation} operation.
	 *
	 * <p><b>NOTE:</b> if the collation is not explicitly specified for any expression, it is wrapped in a
	 * default ascending order
	 *
	 * @param orders expressions describing order,
	 * @param child relational expression on top of which to apply the sort operation
	 * @return valid sort operation
	 */
	public TableOperation createSort(List<Expression> orders, TableOperation child) {
		failIfStreaming();

		List<Expression> convertedOrders = orders.stream()
			.map(f -> f.accept(orderWrapper))
			.collect(Collectors.toList());
		return new SortTableOperation(convertedOrders, child);
	}

	/**
	 * Adds offset to the underlying {@link SortTableOperation} if it is a valid one.
	 *
	 * @param offset offset to add
	 * @param child should be {@link SortTableOperation}
	 * @return valid sort operation with applied offset
	 */
	public TableOperation createLimitWithOffset(int offset, TableOperation child) {
		SortTableOperation previousSort = validateAndGetChildSort(child);

		if (offset < 0) {
			throw new ValidationException("Offset should be greater or equal 0");
		}

		if (previousSort.getOffset() != -1) {
			throw new ValidationException("OFFSET already defined");
		}

		return new SortTableOperation(previousSort.getOrder(), previousSort.getChild(), offset, -1);
	}

	/**
	 * Adds fetch to the underlying {@link SortTableOperation} if it is a valid one.
	 *
	 * @param fetch fetch number to add
	 * @param child should be {@link SortTableOperation}
	 * @return valid sort operation with applied fetch
	 */
	public TableOperation createLimitWithFetch(int fetch, TableOperation child) {
		SortTableOperation previousSort = validateAndGetChildSort(child);

		if (fetch < 0) {
			throw new ValidationException("Fetch should be greater or equal 0");
		}

		int offset = Math.max(previousSort.getOffset(), 0);

		return new SortTableOperation(previousSort.getOrder(), previousSort.getChild(), offset, fetch);
	}

	private SortTableOperation validateAndGetChildSort(TableOperation child) {
		failIfStreaming();

		if (!(child instanceof SortTableOperation)) {
			throw new ValidationException("A limit operation must be preceded by a sort operation.");
		}

		SortTableOperation previousSort = (SortTableOperation) child;

		if ((previousSort).getFetch() != -1) {
			throw new ValidationException("FETCH is already defined.");
		}

		return previousSort;
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
