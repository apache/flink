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
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.FunctionDefinition;
import org.apache.flink.table.expressions.LocalReferenceExpression;
import org.apache.flink.table.expressions.LookupCallExpression;
import org.apache.flink.table.expressions.TableReferenceExpression;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.expressions.ApiExpressionUtils.call;
import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedRef;
import static org.apache.flink.table.expressions.ApiExpressionUtils.valueLiteral;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.AS;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.WINDOW_PROPERTIES;
import static org.apache.flink.table.expressions.ExpressionUtils.extractValue;
import static org.apache.flink.table.expressions.ExpressionUtils.isFunctionOfType;
import static org.apache.flink.table.expressions.FunctionDefinition.Type.AGGREGATE_FUNCTION;

/**
 * Utility methods for transforming {@link Expression} to use them in {@link TableOperation}s.
 */
@Internal
public class OperationExpressionsUtils {

	private static final ExtractNameVisitor extractNameVisitor = new ExtractNameVisitor();

	/**
	 * Container for extracted expressions of the same family.
	 */
	public static class CategorizedExpressions {
		private final List<Expression> projections;
		private final List<Expression> aggregations;
		private final List<Expression> windowProperties;

		CategorizedExpressions(
				List<Expression> projections,
				List<Expression> aggregations,
				List<Expression> windowProperties) {
			this.projections = projections;
			this.aggregations = aggregations;
			this.windowProperties = windowProperties;
		}

		public List<Expression> getProjections() {
			return projections;
		}

		public List<Expression> getAggregations() {
			return aggregations;
		}

		public List<Expression> getWindowProperties() {
			return windowProperties;
		}
	}

	/**
	 * Extracts and deduplicates all aggregation and window property expressions (zero, one, or more)
	 * from the given expressions.
	 *
	 * @param expressions a list of expressions to extract
	 * @return a Tuple2, the first field contains the extracted and deduplicated aggregations,
	 * and the second field contains the extracted and deduplicated window properties.
	 */
	public static CategorizedExpressions extractAggregationsAndProperties(List<Expression> expressions) {
		AggregationAndPropertiesSplitter splitter = new AggregationAndPropertiesSplitter();
		expressions.forEach(expr -> expr.accept(splitter));

		List<Expression> projections = expressions.stream()
			.map(expr -> expr.accept(new AggregationAndPropertiesReplacer(splitter.aggregates,
				splitter.properties)))
			.collect(Collectors.toList());

		List<Expression> aggregates = nameExpressions(splitter.aggregates);
		List<Expression> properties = nameExpressions(splitter.properties);

		return new CategorizedExpressions(projections, aggregates, properties);
	}

	/**
	 * Extracts names from given expressions if they have one. Expressions that have names are:
	 * <ul>
	 * <li>{@link FieldReferenceExpression}</li>
	 * <li>{@link TableReferenceExpression}</li>
	 * <li>{@link LocalReferenceExpression}</li>
	 * <li>{@link org.apache.flink.table.expressions.BuiltInFunctionDefinitions#AS}</li>
	 * </ul>
	 *
	 * @param expressions list of expressions to extract names from
	 * @return corresponding list of optional names
	 */
	public static List<Optional<String>> extractNames(List<Expression> expressions) {
		return expressions.stream().map(OperationExpressionsUtils::extractName).collect(Collectors.toList());
	}

	/**
	 * Extracts name from given expression if it has one. Expressions that have names are:
	 * <ul>
	 * <li>{@link FieldReferenceExpression}</li>
	 * <li>{@link TableReferenceExpression}</li>
	 * <li>{@link LocalReferenceExpression}</li>
	 * <li>{@link org.apache.flink.table.expressions.BuiltInFunctionDefinitions#AS}</li>
	 * </ul>
	 *
	 * @param expression expression to extract name from
	 * @return optional name of given expression
	 */
	public static Optional<String> extractName(Expression expression) {
		return expression.accept(extractNameVisitor);
	}

	private static List<Expression> nameExpressions(Map<Expression, String> expressions) {
		return expressions.entrySet()
			.stream()
			.map(entry -> call(AS, entry.getKey(), valueLiteral(entry.getValue())))
			.collect(Collectors.toList());
	}

	private static class AggregationAndPropertiesSplitter extends ApiExpressionDefaultVisitor<Void> {

		private int uniqueId = 0;
		private final Map<Expression, String> aggregates = new LinkedHashMap<>();
		private final Map<Expression, String> properties = new LinkedHashMap<>();

		@Override
		public Void visitLookupCall(LookupCallExpression unresolvedCall) {
			throw new IllegalStateException("All calls should be resolved by now. Got: " + unresolvedCall);
		}

		@Override
		public Void visitCall(CallExpression call) {
			FunctionDefinition functionDefinition = call.getFunctionDefinition();
			if (isFunctionOfType(call, AGGREGATE_FUNCTION)) {
				aggregates.computeIfAbsent(call, expr -> "EXPR$" + uniqueId++);
			} else if (WINDOW_PROPERTIES.contains(functionDefinition)) {
				properties.computeIfAbsent(call, expr -> "EXPR$" + uniqueId++);
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

	private static class ExtractNameVisitor extends ApiExpressionDefaultVisitor<Optional<String>> {
		@Override
		public Optional<String> visitCall(CallExpression call) {
			if (call.getFunctionDefinition().equals(AS)) {
				return extractValue(call.getChildren().get(1), String.class);
			} else {
				return Optional.empty();
			}
		}

		@Override
		public Optional<String> visitLocalReference(LocalReferenceExpression localReference) {
			return Optional.of(localReference.getName());
		}

		@Override
		public Optional<String> visitTableReference(TableReferenceExpression tableReference) {
			return Optional.of(tableReference.getName());
		}

		@Override
		public Optional<String> visitFieldReference(FieldReferenceExpression fieldReference) {
			return Optional.of(fieldReference.getName());
		}

		@Override
		protected Optional<String> defaultMethod(Expression expression) {
			return Optional.empty();
		}
	}

	private OperationExpressionsUtils() {
	}
}
