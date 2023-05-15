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

package org.apache.flink.table.planner.delegation.hive.expressions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.TypeLiteralExpression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;

import java.util.List;

import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.AND;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.CAST;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.COALESCE;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.CONCAT;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.DIVIDE;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.EQUALS;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.GREATER_THAN;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.HIVE_AGG_DECIMAL_PLUS;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.IF;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.IS_NULL;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.IS_TRUE;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.LESS_THAN;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.MINUS;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.MOD;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.NOT;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.OR;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.PLUS;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.TIMES;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.TRY_CAST;

/**
 * Most is copied from Flink codebase.
 *
 * <p>Builder for {@link Expression}s.
 */
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

    public static UnresolvedCallExpression isTrue(Expression input) {
        return call(IS_TRUE, input);
    }

    public static UnresolvedCallExpression coalesce(Expression... args) {
        return call(COALESCE, args);
    }

    public static UnresolvedCallExpression ifThenElse(
            Expression condition, Expression ifTrue, Expression ifFalse) {
        return call(IF, condition, ifTrue, ifFalse);
    }

    public static UnresolvedCallExpression plus(Expression input1, Expression input2) {
        return call(PLUS, input1, input2);
    }

    /**
     * Used only for implementing native hive SUM/AVG aggregations on a Decimal type to avoid
     * overriding decimal precision/scale calculation for sum/avg with the rules applied for the
     * normal plus.
     */
    @Internal
    public static UnresolvedCallExpression hiveAggDecimalPlus(
            Expression input1, Expression input2) {
        return call(HIVE_AGG_DECIMAL_PLUS, input1, input2);
    }

    public static UnresolvedCallExpression minus(Expression input1, Expression input2) {
        return call(MINUS, input1, input2);
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

    public static UnresolvedCallExpression lessThan(Expression input1, Expression input2) {
        return call(LESS_THAN, input1, input2);
    }

    public static UnresolvedCallExpression greaterThan(Expression input1, Expression input2) {
        return call(GREATER_THAN, input1, input2);
    }

    public static UnresolvedCallExpression cast(Expression child, Expression type) {
        return call(CAST, child, type);
    }

    public static UnresolvedCallExpression tryCast(Expression child, Expression type) {
        return call(TRY_CAST, child, type);
    }

    public static TypeLiteralExpression typeLiteral(DataType type) {
        return ApiExpressionUtils.typeLiteral(type);
    }

    public static UnresolvedCallExpression concat(Expression input1, Expression input2) {
        return call(CONCAT, input1, input2);
    }
}
