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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.CAST;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.TIMES;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.WINDOW_PROPERTIES;
import static org.apache.flink.table.expressions.FunctionDefinition.Type.AGGREGATE_FUNCTION;

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

	public static UnresolvedReferenceExpression unresolvedRef(String name) {
		return new UnresolvedReferenceExpression(name);
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

	/**
	 * Container for extracted expressions of the same family.
	 */
	@Internal
	public static class CategorizedExpressions {
		private final Map<Expression, String> aggregations;
		private final Map<Expression, String> windowProperties;

		CategorizedExpressions(
			Map<Expression, String> aggregations,
			Map<Expression, String> windowProperties) {
			this.aggregations = aggregations;
			this.windowProperties = windowProperties;
		}

		public Map<Expression, String> getAggregations() {
			return aggregations;
		}

		public Map<Expression, String> getWindowProperties() {
			return windowProperties;
		}
	}

	/**
	 * Extracts and deduplicates all aggregation and window property expressions (zero, one, or more)
	 * from the given expressions.
	 *
	 * @param expressions a list of expressions to extract
	 * @param uniqueAttributeGenerator a supplier that every time returns a unique attribute
	 * @return a Tuple2, the first field contains the extracted and deduplicated aggregations,
	 * and the second field contains the extracted and deduplicated window properties.
	 */
	public static CategorizedExpressions extractAggregationsAndProperties(
			List<Expression> expressions,
			Supplier<String> uniqueAttributeGenerator) {
		AggregationAndPropertiesSplitter splitter = new AggregationAndPropertiesSplitter(uniqueAttributeGenerator);
		expressions.forEach(expr -> expr.accept(splitter));

		return new CategorizedExpressions(splitter.aggregates, splitter.properties);
	}

	private static class AggregationAndPropertiesSplitter extends ApiExpressionDefaultVisitor<Void> {

		private final Map<Expression, String> aggregates = new LinkedHashMap<>();
		private final Map<Expression, String> properties = new LinkedHashMap<>();
		private final Supplier<String> uniqueAttributeGenerator;

		private AggregationAndPropertiesSplitter(Supplier<String> uniqueAttributeGenerator) {
			this.uniqueAttributeGenerator = uniqueAttributeGenerator;
		}

		@Override
		public Void visitLookupCall(LookupCallExpression unresolvedCall) {
			throw new IllegalStateException("All calls should be resolved by now. Got: " + unresolvedCall);
		}

		@Override
		public Void visitCall(CallExpression call) {
			FunctionDefinition functionDefinition = call.getFunctionDefinition();
			if (functionDefinition.getType() == AGGREGATE_FUNCTION) {
				aggregates.computeIfAbsent(call, expr -> uniqueAttributeGenerator.get());
			} else if (WINDOW_PROPERTIES.contains(functionDefinition)) {
				properties.computeIfAbsent(call, expr -> uniqueAttributeGenerator.get());
			} else {
				call.getChildren().forEach(c -> c.accept(this));
			}
			return null;
		}

		@Override
		protected Void defaultMethod(Expression expression) {
			return null;
		}
	}

	/**
	 * Replaces expressions with deduplicated aggregations and properties.
	 *
	 * @param expressions     a list of expressions to replace
	 * @param aggNames  the deduplicated aggregations
	 * @param propNames the deduplicated properties
	 * @return a list of replaced expressions
	 */
	public static List<Expression> replaceAggregationsAndProperties(
			List<Expression> expressions,
			Map<Expression, String> aggNames,
			Map<Expression, String> propNames) {
		AggregationAndPropertiesReplacer replacer = new AggregationAndPropertiesReplacer(aggNames, propNames);
		return expressions.stream()
			.map(expr -> expr.accept(replacer))
			.collect(Collectors.toList());
	}

	private static class AggregationAndPropertiesReplacer extends ApiExpressionDefaultVisitor<Expression> {

		private final Map<Expression, String> aggregates;
		private final Map<Expression, String> properties;

		private AggregationAndPropertiesReplacer(
			Map<Expression, String> aggregates,
			Map<Expression, String> properties) {
			this.aggregates = aggregates;
			this.properties = properties;
		}

		@Override
		public Expression visitLookupCall(LookupCallExpression unresolvedCall) {
			throw new IllegalStateException("All calls should be resolved by now. Got: " + unresolvedCall);
		}

		@Override
		public Expression visitCall(CallExpression call) {
			if (aggregates.get(call) != null) {
				return unresolvedRef(aggregates.get(call));
			} else if (properties.get(call) != null) {
				return unresolvedRef(properties.get(call));
			}

			List<Expression> args = call.getChildren()
				.stream()
				.map(c -> c.accept(this))
				.collect(Collectors.toList());
			return new CallExpression(call.getFunctionDefinition(), args);
		}

		@Override
		protected Expression defaultMethod(Expression expression) {
			return expression;
		}
	}

	/**
	 * Extract all field references from the given expressions.
	 *
	 * @param expressions a list of expressions to extract
	 * @return a list of field references extracted from the given expressions
	 */
	public static List<Expression> extractFieldReferences(List<Expression> expressions) {
		FieldReferenceExtractor referenceExtractor = new FieldReferenceExtractor();
		return expressions.stream()
			.flatMap(expr -> expr.accept(referenceExtractor).stream())
			.distinct()
			.collect(Collectors.toList());
	}

	private static class FieldReferenceExtractor extends ApiExpressionDefaultVisitor<List<Expression>> {

		@Override
		public List<Expression> visitCall(CallExpression call) {
			FunctionDefinition functionDefinition = call.getFunctionDefinition();
			if (WINDOW_PROPERTIES.contains(functionDefinition)) {
				return Collections.emptyList();
			} else {
				return call.getChildren()
					.stream()
					.flatMap(c -> c.accept(this).stream())
					.distinct()
					.collect(Collectors.toList());
			}
		}

		@Override
		public List<Expression> visitLookupCall(LookupCallExpression unresolvedCall) {
			throw new IllegalStateException("All calls should be resolved by now. Got: " + unresolvedCall);
		}

		@Override
		public List<Expression> visitFieldReference(FieldReferenceExpression fieldReference) {
			return Collections.singletonList(fieldReference);
		}

		@Override
		public List<Expression> visitUnresolvedReference(UnresolvedReferenceExpression unresolvedReference) {
			return Collections.singletonList(unresolvedReference);
		}

		@Override
		protected List<Expression> defaultMethod(Expression expression) {
			return Collections.emptyList();
		}
	}
}
