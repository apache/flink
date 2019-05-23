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
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.ApiExpressionDefaultVisitor;
import org.apache.flink.table.expressions.BuiltInFunctionDefinitions;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionBridge;
import org.apache.flink.table.expressions.ExpressionUtils;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.PlannerExpression;
import org.apache.flink.table.operations.JoinTableOperation.JoinType;

import java.util.HashSet;
import java.util.Set;

import static java.util.Arrays.asList;

/**
 * Utility class for creating a valid {@link JoinTableOperation} operation.
 */
@Internal
public class JoinOperationFactory {

	private final ExpressionBridge<PlannerExpression> expressionBridge;
	private final EquiJoinExistsChecker equiJoinExistsChecker = new EquiJoinExistsChecker();

	public JoinOperationFactory(ExpressionBridge<PlannerExpression> expressionBridge) {
		this.expressionBridge = expressionBridge;
	}

	/**
	 * Creates a valid {@link JoinTableOperation} operation.
	 *
	 * <p>It performs validations such as:
	 * <ul>
	 * <li>condition returns boolean</li>
	 * <li>the condition is either always true or contains equi join</li>
	 * <li>left and right side of the join do not contain ambiguous column names</li>
	 * <li>that correlated join is an INNER join</li>
	 * </ul>
	 *
	 * @param left left side of the relational operation
	 * @param right right side of the relational operation
	 * @param joinType what sort of join to create
	 * @param condition join condition to apply
	 * @param correlated if the join should be a correlated join
	 * @return valid join operation
	 */
	public TableOperation create(
			TableOperation left,
			TableOperation right,
			JoinType joinType,
			Expression condition,
			boolean correlated) {
		verifyConditionType(condition);
		validateNamesAmbiguity(left, right);
		validateCondition(right, joinType, condition, correlated);
		return new JoinTableOperation(left, right, joinType, condition, correlated);
	}

	private void validateCondition(TableOperation right, JoinType joinType, Expression condition, boolean correlated) {
		boolean alwaysTrue = ExpressionUtils.extractValue(condition, Types.BOOLEAN).orElse(false);

		if (alwaysTrue) {
			return;
		}

		Boolean equiJoinExists = condition.accept(equiJoinExistsChecker);
		if (correlated && right instanceof CalculatedTableOperation && joinType != JoinType.INNER) {
			throw new ValidationException(
				"Predicate for lateral left outer join with table function can only be empty or literal true.");
		} else if (!equiJoinExists) {
			throw new ValidationException(String.format(
				"Invalid join condition: %s. At least one equi-join predicate is required.",
				condition));
		}
	}

	private void verifyConditionType(Expression condition) {
		PlannerExpression plannerExpression = expressionBridge.bridge(condition);
		TypeInformation<?> conditionType = plannerExpression.resultType();
		if (conditionType != Types.BOOLEAN) {
			throw new ValidationException(String.format("Filter operator requires a boolean expression as input, " +
				"but %s is of type %s", condition, conditionType));
		}
	}

	private void validateNamesAmbiguity(TableOperation left, TableOperation right) {
		Set<String> leftNames = new HashSet<>(asList(left.getTableSchema().getFieldNames()));
		Set<String> rightNames = new HashSet<>(asList(right.getTableSchema().getFieldNames()));
		leftNames.retainAll(rightNames);
		if (!leftNames.isEmpty()) {
			throw new ValidationException(String.format("join relations with ambiguous names: %s", leftNames));
		}
	}

	private class EquiJoinExistsChecker extends ApiExpressionDefaultVisitor<Boolean> {

		@Override
		public Boolean visitCall(CallExpression call) {
			if (call.getFunctionDefinition() == BuiltInFunctionDefinitions.EQUALS) {
				return isJoinCondition(call.getChildren().get(0), call.getChildren().get(1));
			} else if (call.getFunctionDefinition() == BuiltInFunctionDefinitions.OR) {
				return false;
			} else if (call.getFunctionDefinition() == BuiltInFunctionDefinitions.AND) {
				return call.getChildren().get(0).accept(this) || call.getChildren().get(1).accept(this);
			}

			return false;
		}

		@Override
		protected Boolean defaultMethod(Expression expression) {
			return false;
		}
	}

	private boolean isJoinCondition(Expression left, Expression right) {
		if (left instanceof FieldReferenceExpression && right instanceof FieldReferenceExpression) {
			return ((FieldReferenceExpression) left).getInputIndex() !=
				((FieldReferenceExpression) right).getInputIndex();
		}

		return false;
	}
}
