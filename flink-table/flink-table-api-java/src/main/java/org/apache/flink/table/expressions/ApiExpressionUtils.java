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

package org.apache.flink.table.expressions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.FunctionDefinition;
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

	public static final long MILLIS_PER_DAY = 86400000; // = 24 * 60 * 60 * 1000

	private ApiExpressionUtils() {
		// private
	}

	public static UnresolvedCallExpression call(FunctionDefinition functionDefinition, Expression... args) {
		return new UnresolvedCallExpression(functionDefinition, Arrays.asList(args));
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
}
