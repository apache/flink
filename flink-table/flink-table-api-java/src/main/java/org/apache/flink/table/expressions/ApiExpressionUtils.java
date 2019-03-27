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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.typeutils.RowIntervalTypeInfo;
import org.apache.flink.table.typeutils.TimeIntervalTypeInfo;

import java.util.Arrays;
import java.util.Optional;

import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.CAST;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.TIMES;

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

	public static CallExpression call(FunctionDefinition functionDefinition, Expression... args) {
		return new CallExpression(functionDefinition, Arrays.asList(args));
	}

	public static ValueLiteralExpression valueLiteral(Object value) {
		return new ValueLiteralExpression(value);
	}

	public static ValueLiteralExpression valueLiteral(Object value, TypeInformation<?> type) {
		return new ValueLiteralExpression(value, type);
	}

	public static TypeLiteralExpression typeLiteral(TypeInformation<?> type) {
		return new TypeLiteralExpression(type);
	}

	public static SymbolExpression symbol(TableSymbol symbol) {
		return new SymbolExpression(symbol);
	}

	public static UnresolvedFieldReferenceExpression unresolvedFieldRef(String name) {
		return new UnresolvedFieldReferenceExpression(name);
	}

	public static TableReferenceExpression tableRef(String name, Table table) {
		return new TableReferenceExpression(name, table);
	}

	public static LookupCallExpression lookupCall(String name, Expression... args) {
		return new LookupCallExpression(name, Arrays.asList(args));
	}

	public static Expression toMonthInterval(Expression e, int multiplier) {
		// check for constant
		return extractValue(e, BasicTypeInfo.INT_TYPE_INFO)
			.map((v) -> (Expression) valueLiteral(v * multiplier, TimeIntervalTypeInfo.INTERVAL_MONTHS))
			.orElse(
				call(
					CAST,
					call(
						TIMES,
						e,
						valueLiteral(multiplier)
					),
					typeLiteral(TimeIntervalTypeInfo.INTERVAL_MONTHS)
				)
			);
	}

	public static Expression toMilliInterval(Expression e, long multiplier) {
		final Optional<Expression> intInterval = extractValue(e, BasicTypeInfo.INT_TYPE_INFO)
			.map((v) -> valueLiteral(v * multiplier, TimeIntervalTypeInfo.INTERVAL_MILLIS));

		final Optional<Expression> longInterval = extractValue(e, BasicTypeInfo.LONG_TYPE_INFO)
			.map((v) -> valueLiteral(v * multiplier, TimeIntervalTypeInfo.INTERVAL_MILLIS));

		if (intInterval.isPresent()) {
			return intInterval.get();
		} else if (longInterval.isPresent()) {
			return longInterval.get();
		}
		return call(
			CAST,
			call(
				TIMES,
				e,
				valueLiteral(multiplier)
			),
			typeLiteral(TimeIntervalTypeInfo.INTERVAL_MONTHS)
		);
	}

	public static Expression toRowInterval(Expression e) {
		final Optional<Expression> intInterval = extractValue(e, BasicTypeInfo.INT_TYPE_INFO)
			.map((v) -> valueLiteral((long) v, RowIntervalTypeInfo.INTERVAL_ROWS));

		final Optional<Expression> longInterval = extractValue(e, BasicTypeInfo.LONG_TYPE_INFO)
			.map((v) -> valueLiteral(v, RowIntervalTypeInfo.INTERVAL_ROWS));

		if (intInterval.isPresent()) {
			return intInterval.get();
		} else if (longInterval.isPresent()) {
			return longInterval.get();
		}
		throw new ValidationException("Invalid value for row interval literal: " + e);
	}

	@SuppressWarnings("unchecked")
	public static <V> Optional<V> extractValue(Expression e, TypeInformation<V> type) {
		if (e instanceof ValueLiteralExpression) {
			final ValueLiteralExpression valueLiteral = (ValueLiteralExpression) e;
			if (valueLiteral.getType().equals(type)) {
				return Optional.of((V) valueLiteral.getValue());
			}
		}
		return Optional.empty();
	}
}
