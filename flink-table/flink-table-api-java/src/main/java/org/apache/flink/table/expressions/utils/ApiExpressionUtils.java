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

package org.apache.flink.table.expressions.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionUtils;
import org.apache.flink.table.expressions.LookupCallExpression;
import org.apache.flink.table.expressions.TableReferenceExpression;
import org.apache.flink.table.expressions.TypeLiteralExpression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.types.DataType;

import java.util.Arrays;

/**
 * Utilities for API-specific {@link Expression}s.
 */
@Internal
public final class ApiExpressionUtils {

	public static final long MILLIS_PER_SECOND = 1000L;

	public static final long MILLIS_PER_MINUTE = 60000L;

	public static final long MILLIS_PER_HOUR = 3600000L; // = 60 * 60 * 1000

	public static final long MILLIS_PER_DAY = 86400000L; // = 24 * 60 * 60 * 1000

	private ApiExpressionUtils() {
		// private
	}

	public static ValueLiteralExpression valueLiteral(Object value) {
		return new ValueLiteralExpression(value);
	}

	public static ValueLiteralExpression valueLiteral(Object value, DataType dataType) {
		return new ValueLiteralExpression(value, dataType);
	}

	public static TypeLiteralExpression typeLiteral(DataType dataType) {
		return new TypeLiteralExpression(dataType);
	}

	public static UnresolvedReferenceExpression unresolvedRef(String name) {
		return new UnresolvedReferenceExpression(name);
	}

	public static UnresolvedCallExpression unresolvedCall(FunctionDefinition functionDefinition, Expression... args) {
		return new UnresolvedCallExpression(functionDefinition, Arrays.asList(args));
	}

	public static TableReferenceExpression tableRef(String name, Table table) {
		return new TableReferenceExpression(name, table.getQueryOperation());
	}

	public static LookupCallExpression lookupCall(String name, Expression... args) {
		return new LookupCallExpression(name, Arrays.asList(args));
	}

	public static Expression toMonthInterval(Expression e, int multiplier) {
		return ExpressionUtils.extractValue(e, Integer.class)
			.map((v) -> intervalOfMonths(v * multiplier))
			.orElseThrow(() -> new ValidationException("Invalid constant for year-month interval: " + e));
	}

	public static ValueLiteralExpression intervalOfMillis(long millis) {
		return valueLiteral(
			millis,
			DataTypes.INTERVAL(DataTypes.SECOND(3)).notNull().bridgedTo(Long.class));
	}

	public static Expression toMilliInterval(Expression e, long multiplier) {
		return ExpressionUtils.extractValue(e, Long.class)
			.map((v) -> intervalOfMillis(v * multiplier))
			.orElseThrow(() -> new ValidationException("Invalid constant for day-time interval: " + e));
	}

	public static ValueLiteralExpression intervalOfMonths(int months) {
		return valueLiteral(
			months,
			DataTypes.INTERVAL(DataTypes.MONTH()).notNull().bridgedTo(Integer.class));
	}

	public static Expression toRowInterval(Expression e) {
		return ExpressionUtils.extractValue(e, Long.class)
			.map(ApiExpressionUtils::valueLiteral)
			.orElseThrow(() -> new ValidationException("Invalid constant for row interval: " + e));
	}

	/**
	 * Checks if the expression is a function call of given type.
	 *
	 * @param expression expression to check
	 * @param kind expected type of function
	 * @return true if the expression is function call of given type, false otherwise
	 */
	public static boolean isFunctionOfKind(Expression expression, FunctionKind kind) {
		if (expression instanceof UnresolvedCallExpression) {
			return ((UnresolvedCallExpression) expression).getFunctionDefinition().getKind() == kind;
		}
		if (expression instanceof CallExpression) {
			return ((CallExpression) expression).getFunctionDefinition().getKind() == kind;
		}
		return false;
	}

	/**
	 * Checks if the given expression is a given builtin function.
	 *
	 * @param expression expression to check
	 * @param functionDefinition expected function definition
	 * @return true if the given expression is a given function call
	 */
	public static boolean isFunction(Expression expression, BuiltInFunctionDefinition functionDefinition) {
		if (expression instanceof UnresolvedCallExpression) {
			return ((UnresolvedCallExpression) expression).getFunctionDefinition() == functionDefinition;
		}
		if (expression instanceof CallExpression) {
			return ((CallExpression) expression).getFunctionDefinition() == functionDefinition;
		}
		return false;
	}
}
