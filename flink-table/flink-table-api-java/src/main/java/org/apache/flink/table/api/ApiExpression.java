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

package org.apache.flink.table.api;

import org.apache.flink.table.api.internal.BaseExpressions;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionVisitor;

import java.util.List;

/**
 * Java API class that gives access to expression operations.
 *
 * @see BaseExpressions
 */
public final class ApiExpression extends BaseExpressions<Object, ApiExpression> implements Expression {
	private final Expression wrappedExpression;

	@Override
	public String asSummaryString() {
		return wrappedExpression.asSummaryString();
	}

	ApiExpression(Expression wrappedExpression) {
		if (wrappedExpression instanceof ApiExpression) {
			throw new UnsupportedOperationException("This is a bug. Please file an issue.");
		}
		this.wrappedExpression = wrappedExpression;
	}

	@Override
	public Expression toExpr() {
		return wrappedExpression;
	}

	@Override
	protected ApiExpression toApiSpecificExpression(Expression expression) {
		return new ApiExpression(expression);
	}

	@Override
	public List<Expression> getChildren() {
		return wrappedExpression.getChildren();
	}

	@Override
	public <R> R accept(ExpressionVisitor<R> visitor) {
		return wrappedExpression.accept(visitor);
	}
}
