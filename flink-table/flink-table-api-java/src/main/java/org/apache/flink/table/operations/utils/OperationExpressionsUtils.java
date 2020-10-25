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

package org.apache.flink.table.operations.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.LocalReferenceExpression;
import org.apache.flink.table.expressions.LookupCallExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.TableReferenceExpression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.utils.ApiExpressionDefaultVisitor;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.operations.QueryOperation;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.expressions.ApiExpressionUtils.isFunctionOfKind;
import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedCall;
import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedRef;
import static org.apache.flink.table.expressions.ApiExpressionUtils.valueLiteral;
import static org.apache.flink.table.expressions.ExpressionUtils.extractValue;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.AS;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.WINDOW_PROPERTIES;
import static org.apache.flink.table.functions.FunctionKind.AGGREGATE;

/**
 * Utility methods for transforming {@link Expression} to use them in {@link QueryOperation}s.
 *
 * <p>Note: Some of these utilities are intended to be used before expressions are fully resolved and
 * some afterwards.
 */
@Internal
public class OperationExpressionsUtils {

	// --------------------------------------------------------------------------------------------
	// Pre-expression resolution utils
	// --------------------------------------------------------------------------------------------

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

	private static List<Expression> nameExpressions(Map<Expression, String> expressions) {
		return expressions.entrySet()
			.stream()
			.map(entry -> unresolvedCall(AS, entry.getKey(), valueLiteral(entry.getValue())))
			.collect(Collectors.toList());
	}

	private static class AggregationAndPropertiesSplitter extends ApiExpressionDefaultVisitor<Void> {

		private int uniqueId = 0;
		private final Map<Expression, String> aggregates = new LinkedHashMap<>();
		private final Map<Expression, String> properties = new LinkedHashMap<>();

		@Override
		public Void visit(LookupCallExpression unresolvedCall) {
			throw new IllegalStateException("All lookup calls should be resolved by now. Got: " + unresolvedCall);
		}

		@Override
		public Void visit(UnresolvedCallExpression unresolvedCall) {
			FunctionDefinition functionDefinition = unresolvedCall.getFunctionDefinition();
			if (isFunctionOfKind(unresolvedCall, AGGREGATE)) {
				aggregates.computeIfAbsent(unresolvedCall, expr -> "EXPR$" + uniqueId++);
			} else if (WINDOW_PROPERTIES.contains(functionDefinition)) {
				properties.computeIfAbsent(unresolvedCall, expr -> "EXPR$" + uniqueId++);
			} else {
				unresolvedCall.getChildren().forEach(c -> c.accept(this));
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
		public Expression visit(LookupCallExpression unresolvedCall) {
			throw new IllegalStateException("All lookup calls should be resolved by now. Got: " + unresolvedCall);
		}

		@Override
		public Expression visit(CallExpression call) {
			throw new IllegalStateException("All calls should still be unresolved by now.");
		}

		@Override
		public Expression visit(UnresolvedCallExpression unresolvedCall) {
			if (aggregates.get(unresolvedCall) != null) {
				return unresolvedRef(aggregates.get(unresolvedCall));
			} else if (properties.get(unresolvedCall) != null) {
				return unresolvedRef(properties.get(unresolvedCall));
			}

			final List<Expression> args = unresolvedCall.getChildren()
				.stream()
				.map(c -> c.accept(this))
				.collect(Collectors.toList());
			return unresolvedCall.replaceArgs(args);
		}

		@Override
		protected Expression defaultMethod(Expression expression) {
			return expression;
		}
	}

	// --------------------------------------------------------------------------------------------
	// utils that can be used both before and after resolution
	// --------------------------------------------------------------------------------------------

	private static final ExtractNameVisitor extractNameVisitor = new ExtractNameVisitor();

	/**
	 * Extracts names from given expressions if they have one. Expressions that have names are:
	 * <ul>
	 * <li>{@link FieldReferenceExpression}</li>
	 * <li>{@link TableReferenceExpression}</li>
	 * <li>{@link LocalReferenceExpression}</li>
	 * <li>{@link BuiltInFunctionDefinitions#AS}</li>
	 * </ul>
	 *
	 * @param expressions list of expressions to extract names from
	 * @return corresponding list of optional names
	 */
	public static List<Optional<String>> extractNames(List<ResolvedExpression> expressions) {
		return expressions.stream().map(OperationExpressionsUtils::extractName).collect(Collectors.toList());
	}

	/**
	 * Extracts name from given expression if it has one. Expressions that have names are:
	 * <ul>
	 * <li>{@link FieldReferenceExpression}</li>
	 * <li>{@link TableReferenceExpression}</li>
	 * <li>{@link LocalReferenceExpression}</li>
	 * <li>{@link BuiltInFunctionDefinitions#AS}</li>
	 * </ul>
	 *
	 * @param expression expression to extract name from
	 * @return optional name of given expression
	 */
	public static Optional<String> extractName(Expression expression) {
		return expression.accept(extractNameVisitor);
	}

	private static class ExtractNameVisitor extends ApiExpressionDefaultVisitor<Optional<String>> {

		@Override
		public Optional<String> visit(LookupCallExpression lookupCall) {
			throw new IllegalStateException("All lookup calls should be resolved by now.");
		}

		@Override
		public Optional<String> visit(UnresolvedCallExpression unresolvedCall) {
			if (unresolvedCall.getFunctionDefinition() == AS) {
				return extractValue(unresolvedCall.getChildren().get(1), String.class);
			} else {
				return Optional.empty();
			}
		}

		@Override
		public Optional<String> visit(CallExpression call) {
			if (call.getFunctionDefinition() == AS) {
				return extractValue(call.getChildren().get(1), String.class);
			} else {
				return Optional.empty();
			}
		}

		@Override
		public Optional<String> visit(LocalReferenceExpression localReference) {
			return Optional.of(localReference.getName());
		}

		@Override
		public Optional<String> visit(TableReferenceExpression tableReference) {
			return Optional.of(tableReference.getName());
		}

		@Override
		public Optional<String> visit(FieldReferenceExpression fieldReference) {
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
