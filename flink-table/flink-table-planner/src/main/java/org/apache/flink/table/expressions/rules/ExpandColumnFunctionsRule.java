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

package org.apache.flink.table.expressions.rules;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.ApiExpressionDefaultVisitor;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionUtils;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.AS;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.RANGE_TO;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.WITHOUT_COLUMNS;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.WITH_COLUMNS;

/**
 * Replaces column functions with all available {@link org.apache.flink.table.expressions.UnresolvedReferenceExpression}s
 * from underlying inputs.
 */
@Internal
final class ExpandColumnFunctionsRule implements ResolverRule {
	@Override
	public List<Expression> apply(List<Expression> expression, ResolutionContext context) {
		ColumnFunctionsExpander columnFunctionsExpander =
			new ColumnFunctionsExpander(
				context.referenceLookup().getAllInputFields().stream()
					.map(p -> new UnresolvedReferenceExpression(p.getName()))
					.collect(Collectors.toList())
			);
		return expression.stream()
			.flatMap(expr -> expr.accept(columnFunctionsExpander).stream())
			.collect(Collectors.toList());
	}

	/**
	 * Expands column functions to it's real parent's input references.
	 */
	class ColumnFunctionsExpander extends ApiExpressionDefaultVisitor<List<Expression>> {

		private final List<UnresolvedReferenceExpression> inputFieldReferences;
		private final ColumnsExpressionExpander columnsExpressionExpander;

		public ColumnFunctionsExpander(List<UnresolvedReferenceExpression> inputFieldReferences) {
			this.inputFieldReferences = inputFieldReferences;
			this.columnsExpressionExpander = new ColumnsExpressionExpander(inputFieldReferences);
		}

		@Override
		public List<Expression> visitCall(CallExpression call) {

			List<Expression> result;

			String definitionName = call.getFunctionDefinition().getName();
			if (definitionName.equals(WITH_COLUMNS.getName())) {
				result = resolveArgsOfColumns(call.getChildren(), false);
			} else if (definitionName.equals(WITHOUT_COLUMNS.getName())) {
				result = resolveArgsOfColumns(call.getChildren(), true);
			} else {
				List<Expression> args = call.getChildren()
					.stream()
					.flatMap(c -> c.accept(this).stream())
					.collect(Collectors.toList());
				result = Collections.singletonList(new CallExpression(call.getFunctionDefinition(), args));

				// validate as.
				if (definitionName.equals(AS.getName())) {
					for (int i = 1; i < args.size(); ++i) {
						if (!(args.get(i) instanceof ValueLiteralExpression)) {
							String errorMessage = String.join(
								", ",
								args.stream().map(e -> e.toString()).collect(Collectors.toList()));
							throw new ValidationException(String.format("Invalid AS, parameters are: [%s].", errorMessage));
						}
					}
				}
			}

			return result;
		}

		@Override
		protected List<Expression> defaultMethod(Expression expression) {
			return Collections.singletonList(expression);
		}

		/**
		 * Expand the columns expression in the input Expression List.
		 */
		private List<Expression> resolveArgsOfColumns(List<Expression> args, boolean isReverseProjection) {

			List<Expression> finalResult = new LinkedList<>();
			List<UnresolvedReferenceExpression> result = args.stream()
				.flatMap(e -> e.accept(this.columnsExpressionExpander).stream())
				.collect(Collectors.toList());

			if (isReverseProjection) {
				for (UnresolvedReferenceExpression field: inputFieldReferences) {
					if (indexOfName(result, field.getName()) == -1) {
						finalResult.add(field);
					}
				}
			} else {
				finalResult.addAll(result);
			}

			return finalResult;
		}
	}

	/**
	 * Expands a single column expression to it's real parent's input references.
	 */
	class ColumnsExpressionExpander extends ApiExpressionDefaultVisitor<List<UnresolvedReferenceExpression>> {

		private final List<UnresolvedReferenceExpression> inputFieldReferences;
		public ColumnsExpressionExpander(List<UnresolvedReferenceExpression> inputFieldReferences) {
			this.inputFieldReferences = inputFieldReferences;
		}

		@Override
		public List<UnresolvedReferenceExpression> visitValueLiteral(ValueLiteralExpression valueLiteralExpression) {
			return ExpressionUtils.extractValue(valueLiteralExpression, Integer.class)
				.map(i -> Collections.singletonList(inputFieldReferences.get(i - 1)))
				.orElseGet(() -> defaultMethod(valueLiteralExpression));
		}

		@Override
		public List<UnresolvedReferenceExpression> visitUnresolvedReference(
			UnresolvedReferenceExpression unresolvedReference) {
			if (unresolvedReference.getName().equals("*")) {
				return inputFieldReferences;
			} else {
				return Collections.singletonList(unresolvedReference);
			}
		}

		@Override
		public List<UnresolvedReferenceExpression> visitCall(CallExpression call) {
			if (isIndexRangeCall(call)) {
				int start = ExpressionUtils.extractValue(call.getChildren().get(0), Integer.class)
					.orElseThrow(() -> new ValidationException("Constant integer value expected."));
				int end = ExpressionUtils.extractValue(call.getChildren().get(1), Integer.class)
					.orElseThrow(() -> new ValidationException("Constant integer value expected."));
				Preconditions.checkArgument(
					start <= end,
					String.format("The start:%s of %s() or %s() should not bigger than end:%s.",
						start, WITH_COLUMNS.getName(), WITHOUT_COLUMNS.getName(), end));

				return inputFieldReferences.subList(start - 1, end);

			} else if (isNameRangeCall(call)) {
				String startName = ((UnresolvedReferenceExpression) call.getChildren().get(0)).getName();
				String endName = ((UnresolvedReferenceExpression) call.getChildren().get(1)).getName();

				int start = indexOfName(inputFieldReferences, startName);
				int end = indexOfName(inputFieldReferences, endName);
				Preconditions.checkArgument(
					start <= end,
					String.format("The start name:%s of %s() or %s() should not behind the end:%s.",
						startName, WITH_COLUMNS.getName(), WITHOUT_COLUMNS.getName(), endName));

				return inputFieldReferences.subList(start, end + 1);
			} else {
				return defaultMethod(call);
			}
		}

		@Override
		protected List<UnresolvedReferenceExpression> defaultMethod(Expression expression) {
			throw new TableException(
				String.format("The parameters of %s() or %s() only accept column name or column " +
					"index, but receive %s.",
					WITH_COLUMNS.getName(),
					WITHOUT_COLUMNS.getName(),
					expression.getClass().getSimpleName()));
		}

		/**
		 * Whether the expression is a column index range expression, e.g. withColumns(1 ~ 2).
		 */
		private boolean isIndexRangeCall(CallExpression expression) {
			return expression.getFunctionDefinition().getName().equals(RANGE_TO.getName()) &&
				expression.getChildren().get(0) instanceof ValueLiteralExpression &&
				expression.getChildren().get(1) instanceof ValueLiteralExpression;
		}

		/**
		 * Whether the expression is a column name range expression, e.g. withColumns(a ~ b).
		 */
		private boolean isNameRangeCall(CallExpression expression) {
			return expression.getFunctionDefinition().getName().equals(RANGE_TO.getName()) &&
				expression.getChildren().get(0) instanceof UnresolvedReferenceExpression &&
				expression.getChildren().get(1) instanceof UnresolvedReferenceExpression;
		}
	}

	/**
	 * Find the index of targetName in the list. Return -1 if not found.
	 */
	private static int indexOfName(List<UnresolvedReferenceExpression> inputFieldReferences, String targetName) {
		int i;
		for (i = 0; i < inputFieldReferences.size(); ++i) {
			if (inputFieldReferences.get(i).getName().equals(targetName)) {
				break;
			}
		}
		return i == inputFieldReferences.size() ? -1 : i;
	}
}
