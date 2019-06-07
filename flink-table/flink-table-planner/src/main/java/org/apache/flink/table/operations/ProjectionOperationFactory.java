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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.ApiExpressionDefaultVisitor;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionBridge;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.LocalReferenceExpression;
import org.apache.flink.table.expressions.PlannerExpression;
import org.apache.flink.table.expressions.TableReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.expressions.ApiExpressionUtils.call;
import static org.apache.flink.table.expressions.ApiExpressionUtils.valueLiteral;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.AS;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.CAST;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.GET;
import static org.apache.flink.table.operations.OperationExpressionsUtils.extractName;
import static org.apache.flink.table.operations.OperationExpressionsUtils.extractNames;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.INTEGER;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;

/**
 * Utility class for creating valid {@link ProjectQueryOperation} operation.
 */
@Internal
public final class ProjectionOperationFactory {

	private final TransitiveExtractNameVisitor extractTransitiveNameVisitor = new TransitiveExtractNameVisitor();
	private final NamingVisitor namingVisitor = new NamingVisitor();
	private final StripAliases stripAliases = new StripAliases();
	private int currentFieldIndex = 0;

	private final ExpressionBridge<PlannerExpression> expressionBridge;

	public ProjectionOperationFactory(ExpressionBridge<PlannerExpression> expressionBridge) {
		this.expressionBridge = expressionBridge;
	}

	public QueryOperation create(
			List<Expression> projectList,
			QueryOperation child,
			boolean explicitAlias) {

		final List<Expression> namedExpressions = nameExpressions(projectList);
		String[] fieldNames = validateAndGetUniqueNames(namedExpressions);

		final List<Expression> finalExpression;
		if (explicitAlias) {
			finalExpression = namedExpressions;
		} else {
			finalExpression = namedExpressions.stream()
				.map(expr -> expr.accept(stripAliases))
				.collect(Collectors.toList());
		}

		TypeInformation[] fieldTypes = namedExpressions.stream()
			.map(expressionBridge::bridge)
			.map(PlannerExpression::resultType)
			.toArray(TypeInformation[]::new);

		TableSchema tableSchema = new TableSchema(fieldNames, fieldTypes);

		return new ProjectQueryOperation(finalExpression, child, tableSchema);
	}

	private String[] validateAndGetUniqueNames(List<Expression> namedExpressions) {
		// we need to maintain field names order to match with types
		final Set<String> names = new LinkedHashSet<>();

		extractNames(namedExpressions).stream()
			.map(name -> name.orElseThrow(() -> new TableException("Could not name a field in a projection.")))
			.forEach(name -> {
				if (!names.add(name)) {
					throw new ValidationException("Ambiguous column name: " + name);
				}
			});

		return names.toArray(new String[0]);
	}

	/**
	 * Ensures that all expressions have a derivable name. There is a few categories and naming
	 * rules that apply:
	 * <ul>
	 *     <li>{@link FieldReferenceExpression}, {@link TableReferenceExpression},
	 *     {@link LocalReferenceExpression} and {@link BuiltInFunctionDefinitions#AS}
	 *     are already named}</li>
	 *     <li>{@link BuiltInFunctionDefinitions#CAST} use the name of underlying expression
	 *     appended with the name of the type</li>
	 *     <li>{@link BuiltInFunctionDefinitions#GET} uses pattern
	 *     <i>[underlying name][$fieldName]{1, }</i></li>
	 *     <li>if none of the above apply a name <i>[_c(idx)]</i> is used where idx is
	 *     the index within given expressions</li>
	 * </ul>
	 */
	private List<Expression> nameExpressions(List<Expression> expression) {
		return IntStream.range(0, expression.size())
			.mapToObj(idx -> {
				currentFieldIndex = idx;
				return expression.get(idx).accept(namingVisitor);
			})
			.collect(Collectors.toList());
	}

	private class NamingVisitor extends ApiExpressionDefaultVisitor<Expression> {

		@Override
		public Expression visitCall(CallExpression call) {
			FunctionDefinition functionDefinition = call.getFunctionDefinition();
			final Optional<String> rename;
			if (functionDefinition.equals(CAST)) {
				rename = nameForCast(call);
			} else if (functionDefinition.equals(GET)) {
				rename = nameForGet(call);
			} else if (functionDefinition.equals(AS)) {
				rename = Optional.empty();
			} else {
				rename = Optional.of(getUniqueName());
			}

			return rename.map(name -> call(AS, call, valueLiteral(name))).orElse(call);
		}

		private Optional<String> nameForGet(CallExpression call) {
			return Optional.of(call.accept(extractTransitiveNameVisitor)
				.orElseGet(ProjectionOperationFactory.this::getUniqueName));
		}

		private Optional<String> nameForCast(CallExpression call) {
			Optional<String> innerName = call.getChildren().get(0).accept(extractTransitiveNameVisitor);
			Expression type = call.getChildren().get(1);
			return Optional.of(innerName.map(n -> String.format("%s-%s", n, type))
				.orElseGet(ProjectionOperationFactory.this::getUniqueName));
		}

		@Override
		public Expression visitValueLiteral(ValueLiteralExpression valueLiteralExpression) {
			return call(AS, valueLiteralExpression, valueLiteral(getUniqueName()));
		}

		@Override
		protected Expression defaultMethod(Expression expression) {
			return expression;
		}
	}

	private class StripAliases extends ApiExpressionDefaultVisitor<Expression> {

		@Override
		public Expression visitCall(CallExpression call) {
			if (call.getFunctionDefinition().equals(AS)) {
				return call.getChildren().get(0).accept(this);
			} else {
				return call;
			}
		}

		@Override
		protected Expression defaultMethod(Expression expression) {
			return expression;
		}
	}

	private class TransitiveExtractNameVisitor extends ApiExpressionDefaultVisitor<Optional<String>> {

		@Override
		public Optional<String> visitCall(CallExpression call) {
			if (call.getFunctionDefinition().equals(GET)) {
				return extractNameFromGet(call);
			} else {
				return defaultMethod(call);
			}
		}

		@Override
		protected Optional<String> defaultMethod(Expression expression) {
			return extractName(expression);
		}

		private Optional<String> extractNameFromGet(CallExpression call) {
			Expression child = call.getChildren().get(0);
			ValueLiteralExpression key = (ValueLiteralExpression) call.getChildren().get(1);

			final LogicalType keyType = key.getOutputDataType().getLogicalType();

			final String keySuffix;
			if (hasRoot(keyType, INTEGER)) {
				keySuffix = "$_" + key.getValueAs(Integer.class)
					.orElseThrow(() -> new TableException("Integer constant excepted."));
			} else {
				keySuffix = "$" + key.getValueAs(String.class)
					.orElseThrow(() -> new TableException("Integer constant excepted."));
			}
			return child.accept(this).map(p -> p + keySuffix);
		}
	}

	private String getUniqueName() {
		return "_c" + currentFieldIndex++;
	}
}
