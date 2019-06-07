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

import static org.apache.flink.table.expressions.ApiExpressionUtils.call;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.ORDERING;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.ORDER_ASC;


/**
 * Utility class for creating a valid {@link SortQueryOperation} operation.
 */
@Internal
public class SortOperationFactory {

	private final boolean isStreaming;
	private final OrderWrapper orderWrapper = new OrderWrapper();

	public SortOperationFactory(boolean isStreaming) {
		this.isStreaming = isStreaming;
	}

	/**
	 * Creates a valid {@link SortQueryOperation} operation.
	 *
	 * <p><b>NOTE:</b> if the collation is not explicitly specified for any expression, it is wrapped in a
	 * default ascending order
	 *
	 * @param orders expressions describing order,
	 * @param child relational expression on top of which to apply the sort operation
	 * @return valid sort operation
	 */
	public QueryOperation createSort(List<Expression> orders, QueryOperation child) {
		failIfStreaming();

		List<Expression> convertedOrders = orders.stream()
			.map(f -> f.accept(orderWrapper))
			.collect(Collectors.toList());
		return new SortQueryOperation(convertedOrders, child);
	}

	/**
	 * Adds offset to the underlying {@link SortQueryOperation} if it is a valid one.
	 *
	 * @param offset offset to add
	 * @param child should be {@link SortQueryOperation}
	 * @return valid sort operation with applied offset
	 */
	public QueryOperation createLimitWithOffset(int offset, QueryOperation child) {
		SortQueryOperation previousSort = validateAndGetChildSort(child);

		if (offset < 0) {
			throw new ValidationException("Offset should be greater or equal 0");
		}

		if (previousSort.getOffset() != -1) {
			throw new ValidationException("OFFSET already defined");
		}

		return new SortQueryOperation(previousSort.getOrder(), previousSort.getChild(), offset, -1);
	}

	/**
	 * Adds fetch to the underlying {@link SortQueryOperation} if it is a valid one.
	 *
	 * @param fetch fetch number to add
	 * @param child should be {@link SortQueryOperation}
	 * @return valid sort operation with applied fetch
	 */
	public QueryOperation createLimitWithFetch(int fetch, QueryOperation child) {
		SortQueryOperation previousSort = validateAndGetChildSort(child);

		if (fetch < 0) {
			throw new ValidationException("Fetch should be greater or equal 0");
		}

		int offset = Math.max(previousSort.getOffset(), 0);

		return new SortQueryOperation(previousSort.getOrder(), previousSort.getChild(), offset, fetch);
	}

	private SortQueryOperation validateAndGetChildSort(QueryOperation child) {
		failIfStreaming();

		if (!(child instanceof SortQueryOperation)) {
			throw new ValidationException("A limit operation must be preceded by a sort operation.");
		}

		SortQueryOperation previousSort = (SortQueryOperation) child;

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
			return call(ORDER_ASC, expression);
		}
	}
}
