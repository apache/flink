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

package org.apache.flink.table.planner.expressions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.TypeLiteralExpression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;

import java.util.List;

import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.AGG_DECIMAL_MINUS;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.AGG_DECIMAL_PLUS;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.AND;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.CAST;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.CONCAT;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.DIVIDE;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.EQUALS;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.GREATER_THAN;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.IF;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.IS_NULL;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.LESS_THAN;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.MINUS;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.MOD;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.NOT;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.OR;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.PLUS;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.REINTERPRET_CAST;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.TIMES;

/** Builder for {@link Expression}s. */
public class ExpressionBuilder {

    public static ValueLiteralExpression nullOf(DataType type) {
        return literal(null, type);
    }

    public static ValueLiteralExpression literal(Object value) {
        return ApiExpressionUtils.valueLiteral(value);
    }

    public static ValueLiteralExpression literal(Object value, DataType type) {
        if (value != null) {
            return ApiExpressionUtils.valueLiteral(value, type.notNull());
        } else {
            return ApiExpressionUtils.valueLiteral(null, type.nullable());
        }
    }

    public static UnresolvedCallExpression call(
            FunctionDefinition functionDefinition, Expression... args) {
        return ApiExpressionUtils.unresolvedCall(functionDefinition, args);
    }

    public static UnresolvedCallExpression call(
            FunctionDefinition functionDefinition, List<Expression> args) {
        return ApiExpressionUtils.unresolvedCall(
                functionDefinition, args.toArray(new Expression[0]));
    }

    public static UnresolvedCallExpression and(Expression arg1, Expression arg2) {
        return call(AND, arg1, arg2);
    }

    public static UnresolvedCallExpression or(Expression arg1, Expression arg2) {
        return call(OR, arg1, arg2);
    }

    public static UnresolvedCallExpression not(Expression arg) {
        return call(NOT, arg);
    }

    public static UnresolvedCallExpression isNull(Expression input) {
        return call(IS_NULL, input);
    }

    public static UnresolvedCallExpression ifThenElse(
            Expression condition, Expression ifTrue, Expression ifFalse) {
        return call(IF, condition, ifTrue, ifFalse);
    }

    public static UnresolvedCallExpression plus(Expression input1, Expression input2) {
        return call(PLUS, input1, input2);
    }

    /**
     * Used only for implementing SUM/AVG aggregations (with and without retractions) on a Decimal
     * type to avoid overriding decimal precision/scale calculation for sum/avg with the rules
     * applied for the normal plus.
     */
    @Internal
    public static UnresolvedCallExpression aggDecimalPlus(Expression input1, Expression input2) {
        return call(AGG_DECIMAL_PLUS, input1, input2);
    }

    public static UnresolvedCallExpression minus(Expression input1, Expression input2) {
        return call(MINUS, input1, input2);
    }

    /**
     * Used only for implementing SUM/AVG aggregations (with and without retractions) on a Decimal
     * type to avoid overriding decimal precision/scale calculation for sum/avg with the rules
     * applied for the normal minus.
     */
    @Internal
    public static UnresolvedCallExpression aggDecimalMinus(Expression input1, Expression input2) {
        return call(AGG_DECIMAL_MINUS, input1, input2);
    }

    public static UnresolvedCallExpression div(Expression input1, Expression input2) {
        return call(DIVIDE, input1, input2);
    }

    public static UnresolvedCallExpression times(Expression input1, Expression input2) {
        return call(TIMES, input1, input2);
    }

    public static UnresolvedCallExpression mod(Expression input1, Expression input2) {
        return call(MOD, input1, input2);
    }

    public static UnresolvedCallExpression equalTo(Expression input1, Expression input2) {
        return call(EQUALS, input1, input2);
    }

    public static UnresolvedCallExpression lessThanOrEqual(Expression input1, Expression input2) {
        return call(LESS_THAN_OR_EQUAL, input1, input2);
    }

    public static UnresolvedCallExpression lessThan(Expression input1, Expression input2) {
        return call(LESS_THAN, input1, input2);
    }

    public static UnresolvedCallExpression greaterThan(Expression input1, Expression input2) {
        return call(GREATER_THAN, input1, input2);
    }

    public static UnresolvedCallExpression cast(Expression child, Expression type) {
        return call(CAST, child, type);
    }

    public static UnresolvedCallExpression reinterpretCast(
            Expression child, Expression type, boolean checkOverflow) {
        return call(REINTERPRET_CAST, child, type, literal(checkOverflow));
    }

    public static TypeLiteralExpression typeLiteral(DataType type) {
        return ApiExpressionUtils.typeLiteral(type);
    }

    public static UnresolvedCallExpression concat(Expression input1, Expression input2) {
        return call(CONCAT, input1, input2);
    }
}
