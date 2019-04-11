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
import org.apache.flink.api.common.typeinfo.IntegerTypeInfo;
import org.apache.flink.table.api.TableException;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.COLUMNS;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.MINUS_PREFIX;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.RANGE_TO;

/**
 * Expands column expressions to it's real parent's input references.
 */
@Internal
public class ColumnsOperationExpander extends ApiExpressionDefaultVisitor<List<Expression>> {

	private final List<UnresolvedReferenceExpression> inputFieldReferences;

	public ColumnsOperationExpander(List<UnresolvedReferenceExpression> inputFieldReferences) {
		this.inputFieldReferences = inputFieldReferences;
	}

	@Override
	public List<Expression> visitCall(CallExpression call) {

		List<Expression> result;

		String definitionName = call.getFunctionDefinition().getName();
		if (definitionName.equals(COLUMNS.getName())) {
			result = resolveArgsOfColumns(call.getChildren(), false);
		} else if (definitionName.equals(MINUS_PREFIX.getName())
			&& call.getChildren().get(0) instanceof CallExpression
			&& ((CallExpression) call.getChildren().get(0)).getFunctionDefinition().getName()
			.equals(COLUMNS.getName())) {

			result = resolveArgsOfColumns(call.getChildren().get(0).getChildren(), true);
		} else {
			List<Expression> args = call.getChildren()
				.stream()
				.flatMap(c -> c.accept(this).stream())
				.collect(Collectors.toList());
			result = Collections.singletonList(new CallExpression(call.getFunctionDefinition(), args));
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
			.flatMap(e -> resolveSingleArg(e).stream())
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

	/**
	 * Expand the columns expression in the input Expression.
	 */
	private List<UnresolvedReferenceExpression> resolveSingleArg(Expression arg) {

		List<UnresolvedReferenceExpression> result;

		if (arg instanceof ValueLiteralExpression &&
			IntegerTypeInfo.class.isAssignableFrom(((ValueLiteralExpression) arg).getType().getClass())) {
			result = Collections.singletonList(inputFieldReferences.get((int) ((ValueLiteralExpression) arg).getValue() - 1));
		} else if (arg instanceof UnresolvedReferenceExpression
			&& ((UnresolvedReferenceExpression) arg).getName().equals("*")) {
			result = inputFieldReferences;
		} else if (arg instanceof UnresolvedReferenceExpression) {
			result = Collections.singletonList((UnresolvedReferenceExpression) arg);
		} else if (isIndexRange(arg)) {
			CallExpression range = (CallExpression) arg;
			int start = (int) ((ValueLiteralExpression) range.getChildren().get(0)).getValue();
			int end = (int) ((ValueLiteralExpression) range.getChildren().get(1)).getValue();
			Preconditions.checkArgument(
				start <= end,
				String.format("The start:%s of columns() should not bigger than end:%s.", start, end));

			result = inputFieldReferences.subList(start - 1, end);
		} else if (isNameRange(arg)) {
			CallExpression range = (CallExpression) arg;
			String startName = ((UnresolvedReferenceExpression) range.getChildren().get(0)).getName();
			String endName = ((UnresolvedReferenceExpression) range.getChildren().get(1)).getName();

			int start = indexOfName(inputFieldReferences, startName);
			int end = indexOfName(inputFieldReferences, endName);
			Preconditions.checkArgument(
				start <= end,
				String.format("The start name:%s of columns() should not behind the end:%s.", startName, endName));

			result = inputFieldReferences.subList(start, end + 1);
		} else {
			throw new TableException(
				String.format("The parameters of columns() only accept column name or column " +
					"index.", arg.getClass().getSimpleName()));
		}

		return result;
	}

	/**
	 * Find the index of targetName in the list. Return -1 if not found.
	 */
	private int indexOfName(List<UnresolvedReferenceExpression> inputFieldReferences, String targetName) {
		int i;
		for (i = 0; i < inputFieldReferences.size(); ++i) {
			if (inputFieldReferences.get(i).getName().equals(targetName)) {
				break;
			}
		}
		return i == inputFieldReferences.size() ? -1 : i;
	}

	/**
	 * Whether the expression is a column index range expression, e.g. columns(1 ~ 2).
	 */
	private boolean isIndexRange(Expression expression) {
		return expression instanceof CallExpression &&
			((CallExpression) expression).getFunctionDefinition().getName().equals(RANGE_TO.getName()) &&
			expression.getChildren().get(0) instanceof ValueLiteralExpression &&
			expression.getChildren().get(1) instanceof ValueLiteralExpression;
	}

	/**
	 * Whether the expression is a column name range expression, e.g. columns(a ~ b).
	 */
	private boolean isNameRange(Expression expression) {
		return expression instanceof CallExpression &&
			((CallExpression) expression).getFunctionDefinition().getName().equals(RANGE_TO.getName()) &&
			expression.getChildren().get(0) instanceof UnresolvedReferenceExpression &&
			expression.getChildren().get(1) instanceof UnresolvedReferenceExpression;
	}
}
