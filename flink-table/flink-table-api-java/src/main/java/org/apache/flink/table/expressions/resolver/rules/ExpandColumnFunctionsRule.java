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

package org.apache.flink.table.expressions.resolver.rules;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionUtils;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.expressions.utils.ApiExpressionDefaultVisitor;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.expressions.utils.ApiExpressionUtils.unresolvedCall;
import static org.apache.flink.table.expressions.utils.ApiExpressionUtils.unresolvedRef;
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
					.map(p -> unresolvedRef(p.getName()))
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
		public List<Expression> visit(UnresolvedCallExpression unresolvedCall) {

			List<Expression> result;

			final FunctionDefinition definition = unresolvedCall.getFunctionDefinition();
			if (definition == WITH_COLUMNS) {
				result = resolveArgsOfColumns(unresolvedCall.getChildren(), false);
			} else if (definition == WITHOUT_COLUMNS) {
				result = resolveArgsOfColumns(unresolvedCall.getChildren(), true);
			} else {
				Expression[] args = unresolvedCall.getChildren()
					.stream()
					.flatMap(c -> c.accept(this).stream())
					.toArray(Expression[]::new);
				result = Collections.singletonList(unresolvedCall(unresolvedCall.getFunctionDefinition(), args));

				// validate alias
				if (definition == AS) {
					for (int i = 1; i < args.length; ++i) {
						if (!(args[i] instanceof ValueLiteralExpression)) {
							final String errorMessage = Stream.of(args)
								.map(Object::toString)
								.collect(Collectors.joining(", "));
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
		public List<UnresolvedReferenceExpression> visit(ValueLiteralExpression valueLiteral) {
			return ExpressionUtils.extractValue(valueLiteral, Integer.class)
				.map(i -> Collections.singletonList(inputFieldReferences.get(i - 1)))
				.orElseGet(() -> defaultMethod(valueLiteral));
		}

		@Override
		public List<UnresolvedReferenceExpression> visit(
			UnresolvedReferenceExpression unresolvedReference) {
			if (unresolvedReference.getName().equals("*")) {
				return inputFieldReferences;
			} else {
				return Collections.singletonList(unresolvedReference);
			}
		}

		@Override
		public List<UnresolvedReferenceExpression> visit(UnresolvedCallExpression unresolvedCall) {
			if (isIndexRangeCall(unresolvedCall)) {
				int start = ExpressionUtils.extractValue(unresolvedCall.getChildren().get(0), Integer.class)
					.orElseThrow(() -> new ValidationException("Constant integer value expected."));
				int end = ExpressionUtils.extractValue(unresolvedCall.getChildren().get(1), Integer.class)
					.orElseThrow(() -> new ValidationException("Constant integer value expected."));
				Preconditions.checkArgument(
					start <= end,
					String.format("The start:%s of %s() or %s() should not bigger than end:%s.",
						start, WITH_COLUMNS.getName(), WITHOUT_COLUMNS.getName(), end));

				return inputFieldReferences.subList(start - 1, end);

			} else if (isNameRangeCall(unresolvedCall)) {
				String startName = ((UnresolvedReferenceExpression) unresolvedCall.getChildren().get(0)).getName();
				String endName = ((UnresolvedReferenceExpression) unresolvedCall.getChildren().get(1)).getName();

				int start = indexOfName(inputFieldReferences, startName);
				int end = indexOfName(inputFieldReferences, endName);
				Preconditions.checkArgument(
					start <= end,
					String.format("The start name:%s of %s() or %s() should not behind the end:%s.",
						startName, WITH_COLUMNS.getName(), WITHOUT_COLUMNS.getName(), endName));

				return inputFieldReferences.subList(start, end + 1);
			} else {
				return defaultMethod(unresolvedCall);
			}
		}

		@Override
		protected List<UnresolvedReferenceExpression> defaultMethod(Expression expression) {
			throw new ValidationException(
				String.format(
					"The parameters of %s() or %s() only accept column names or column indices.",
					WITH_COLUMNS.getName(),
					WITHOUT_COLUMNS.getName()));
		}

		/**
		 * Whether the expression is a column index range expression, e.g. withColumns(1 ~ 2).
		 */
		private boolean isIndexRangeCall(UnresolvedCallExpression expression) {
			return expression.getFunctionDefinition() == RANGE_TO &&
				expression.getChildren().get(0) instanceof ValueLiteralExpression &&
				expression.getChildren().get(1) instanceof ValueLiteralExpression;
		}

		/**
		 * Whether the expression is a column name range expression, e.g. withColumns(a ~ b).
		 */
		private boolean isNameRangeCall(UnresolvedCallExpression expression) {
			return expression.getFunctionDefinition() == RANGE_TO &&
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
