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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.ExpressionUtils;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.utils.ResolvedExpressionDefaultVisitor;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.operations.CalculatedQueryOperation;
import org.apache.flink.table.operations.JoinQueryOperation;
import org.apache.flink.table.operations.JoinQueryOperation.JoinType;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import java.util.HashSet;
import java.util.Set;

import static java.util.Arrays.asList;

/** Utility class for creating a valid {@link JoinQueryOperation} operation. */
@Internal
final class JoinOperationFactory {

    private final EquiJoinExistsChecker equiJoinExistsChecker = new EquiJoinExistsChecker();

    /**
     * Creates a valid {@link JoinQueryOperation} operation.
     *
     * <p>It performs validations such as:
     *
     * <ul>
     *   <li>condition returns boolean
     *   <li>the condition is either always true or contains equi join
     *   <li>left and right side of the join do not contain ambiguous column names
     *   <li>that correlated join is an INNER join
     * </ul>
     *
     * @param left left side of the relational operation
     * @param right right side of the relational operation
     * @param joinType what sort of join to create
     * @param condition join condition to apply
     * @param correlated if the join should be a correlated join
     * @return valid join operation
     */
    QueryOperation create(
            QueryOperation left,
            QueryOperation right,
            JoinType joinType,
            ResolvedExpression condition,
            boolean correlated) {
        verifyConditionType(condition);
        validateNamesAmbiguity(left, right);
        validateCondition(right, joinType, condition, correlated);
        return new JoinQueryOperation(left, right, joinType, condition, correlated);
    }

    private void validateCondition(
            QueryOperation right,
            JoinType joinType,
            ResolvedExpression condition,
            boolean correlated) {
        boolean alwaysTrue = ExpressionUtils.extractValue(condition, Boolean.class).orElse(false);

        if (alwaysTrue) {
            return;
        }

        Boolean equiJoinExists = condition.accept(equiJoinExistsChecker);
        if (correlated && right instanceof CalculatedQueryOperation && joinType != JoinType.INNER) {
            throw new ValidationException(
                    "Predicate for lateral left outer join with table function can only be empty or literal true.");
        } else if (!equiJoinExists) {
            throw new ValidationException(
                    String.format(
                            "Invalid join condition: %s. At least one equi-join predicate is required.",
                            condition));
        }
    }

    private void verifyConditionType(ResolvedExpression condition) {
        DataType conditionType = condition.getOutputDataType();
        LogicalType logicalType = conditionType.getLogicalType();
        if (!LogicalTypeChecks.hasRoot(logicalType, LogicalTypeRoot.BOOLEAN)) {
            throw new ValidationException(
                    String.format(
                            "Filter operator requires a boolean expression as input, "
                                    + "but %s is of type %s",
                            condition, conditionType));
        }
    }

    private void validateNamesAmbiguity(QueryOperation left, QueryOperation right) {
        Set<String> leftNames = new HashSet<>(asList(left.getTableSchema().getFieldNames()));
        Set<String> rightNames = new HashSet<>(asList(right.getTableSchema().getFieldNames()));
        leftNames.retainAll(rightNames);
        if (!leftNames.isEmpty()) {
            throw new ValidationException(
                    String.format("join relations with ambiguous names: %s", leftNames));
        }
    }

    private class EquiJoinExistsChecker extends ResolvedExpressionDefaultVisitor<Boolean> {

        @Override
        public Boolean visit(CallExpression call) {
            if (call.getFunctionDefinition() == BuiltInFunctionDefinitions.EQUALS) {
                return isJoinCondition(
                        call.getResolvedChildren().get(0), call.getResolvedChildren().get(1));
            } else if (call.getFunctionDefinition() == BuiltInFunctionDefinitions.OR) {
                return false;
            } else if (call.getFunctionDefinition() == BuiltInFunctionDefinitions.AND) {
                return call.getChildren().get(0).accept(this)
                        || call.getChildren().get(1).accept(this);
            }

            return false;
        }

        @Override
        protected Boolean defaultMethod(ResolvedExpression expression) {
            return false;
        }
    }

    private boolean isJoinCondition(ResolvedExpression left, ResolvedExpression right) {
        if (left instanceof FieldReferenceExpression && right instanceof FieldReferenceExpression) {
            return ((FieldReferenceExpression) left).getInputIndex()
                    != ((FieldReferenceExpression) right).getInputIndex();
        }

        return false;
    }
}
