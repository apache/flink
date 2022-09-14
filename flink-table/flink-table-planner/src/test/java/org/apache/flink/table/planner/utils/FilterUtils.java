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

package org.apache.flink.table.planner.utils;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.LOWER;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.UPPER;

/** Utils for catalog and source to filter partition or row. */
public class FilterUtils {

    public static boolean shouldPushDown(ResolvedExpression expr, Set<String> filterableFields) {
        if (expr instanceof CallExpression && expr.getChildren().size() == 2) {
            return shouldPushDownUnaryExpression(
                            expr.getResolvedChildren().get(0), filterableFields)
                    && shouldPushDownUnaryExpression(
                            expr.getResolvedChildren().get(1), filterableFields);
        }
        return false;
    }

    public static boolean isRetainedAfterApplyingFilterPredicates(
            List<ResolvedExpression> predicates, Function<String, Comparable<?>> getter) {
        for (ResolvedExpression predicate : predicates) {
            if (predicate instanceof CallExpression) {
                FunctionDefinition definition =
                        ((CallExpression) predicate).getFunctionDefinition();
                boolean result = false;
                if (definition.equals(BuiltInFunctionDefinitions.OR)) {
                    // nested filter, such as (key1 > 2 or key2 > 3)
                    for (Expression expr : predicate.getChildren()) {
                        if (!(expr instanceof CallExpression && expr.getChildren().size() == 2)) {
                            throw new TableException(expr + " not supported!");
                        }
                        result = binaryFilterApplies((CallExpression) expr, getter);
                        if (result) {
                            break;
                        }
                    }
                } else if (predicate.getChildren().size() == 2) {
                    result = binaryFilterApplies((CallExpression) predicate, getter);
                } else {
                    throw new UnsupportedOperationException(
                            String.format("Unsupported expr: %s.", predicate));
                }
                if (!result) {
                    return false;
                }
            } else {
                throw new UnsupportedOperationException(
                        String.format("Unsupported expr: %s.", predicate));
            }
        }
        return true;
    }

    private static boolean shouldPushDownUnaryExpression(
            ResolvedExpression expr, Set<String> filterableFields) {
        // validate that type is comparable
        if (!isComparable(expr.getOutputDataType().getConversionClass())) {
            return false;
        }
        if (expr instanceof FieldReferenceExpression) {
            if (filterableFields.contains(((FieldReferenceExpression) expr).getName())) {
                return true;
            }
        }

        if (expr instanceof ValueLiteralExpression) {
            return true;
        }

        if (expr instanceof CallExpression && expr.getChildren().size() == 1) {
            if (((CallExpression) expr).getFunctionDefinition().equals(UPPER)
                    || ((CallExpression) expr).getFunctionDefinition().equals(LOWER)) {
                return shouldPushDownUnaryExpression(
                        expr.getResolvedChildren().get(0), filterableFields);
            }
        }
        // other resolved expressions return false
        return false;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static boolean binaryFilterApplies(
            CallExpression binExpr, Function<String, Comparable<?>> getter) {
        List<Expression> children = binExpr.getChildren();
        Preconditions.checkArgument(children.size() == 2);

        Comparable lhsValue = getValue(children.get(0), getter);
        Comparable rhsValue = getValue(children.get(1), getter);
        FunctionDefinition functionDefinition = binExpr.getFunctionDefinition();
        if (BuiltInFunctionDefinitions.GREATER_THAN.equals(functionDefinition)) {
            return lhsValue.compareTo(rhsValue) > 0;
        } else if (BuiltInFunctionDefinitions.LESS_THAN.equals(functionDefinition)) {
            return lhsValue.compareTo(rhsValue) < 0;
        } else if (BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL.equals(functionDefinition)) {
            return lhsValue.compareTo(rhsValue) >= 0;
        } else if (BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL.equals(functionDefinition)) {
            return lhsValue.compareTo(rhsValue) <= 0;
        } else if (BuiltInFunctionDefinitions.EQUALS.equals(functionDefinition)) {
            return lhsValue.compareTo(rhsValue) == 0;
        } else if (BuiltInFunctionDefinitions.NOT_EQUALS.equals(functionDefinition)) {
            return lhsValue.compareTo(rhsValue) != 0;
        } else {
            throw new UnsupportedOperationException("Unsupported operator: " + functionDefinition);
        }
    }

    private static boolean isComparable(Class<?> clazz) {
        return Comparable.class.isAssignableFrom(clazz);
    }

    private static Comparable<?> getValue(Expression expr, Function<String, Comparable<?>> getter) {
        if (expr instanceof ValueLiteralExpression) {
            Optional<?> value =
                    ((ValueLiteralExpression) expr)
                            .getValueAs(
                                    ((ValueLiteralExpression) expr)
                                            .getOutputDataType()
                                            .getConversionClass());
            return (Comparable<?>) value.orElse(null);
        }

        if (expr instanceof FieldReferenceExpression) {
            return getter.apply(((FieldReferenceExpression) expr).getName());
        }

        if (expr instanceof CallExpression && expr.getChildren().size() == 1) {
            Object child = getValue(expr.getChildren().get(0), getter);
            FunctionDefinition functionDefinition = ((CallExpression) expr).getFunctionDefinition();
            if (functionDefinition.equals(UPPER)) {
                return child.toString().toUpperCase();
            } else if (functionDefinition.equals(LOWER)) {
                return child.toString().toLowerCase();
            } else {
                throw new UnsupportedOperationException(
                        String.format("Unrecognized function definition: %s.", functionDefinition));
            }
        }
        throw new UnsupportedOperationException(expr + " not supported!");
    }
}
