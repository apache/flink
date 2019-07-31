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

import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.TypeLiteralExpression;
import org.apache.flink.table.expressions.utils.ApiExpressionUtils;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;

import java.util.List;

import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.AND;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.CAST;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.CONCAT;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.DIVIDE;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.EQUALS;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.GREATER_THAN;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.IF;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.IS_NULL;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.LESS_THAN;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.MINUS;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.MOD;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.NOT;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.OR;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.PLUS;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.REINTERPRET_CAST;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.TIMES;
import static org.apache.flink.table.planner.functions.InternalFunctionDefinitions.THROW_EXCEPTION;

/**
 * Builder for {@link Expression}s.
 */
public class ExpressionBuilder {

	public static Expression nullOf(DataType type) {
		return literal(null, type);
	}

	public static Expression literal(Object value) {
		return ApiExpressionUtils.valueLiteral(value);
	}

	public static Expression literal(Object value, DataType type) {
		return ApiExpressionUtils.valueLiteral(value, type);
	}

	public static Expression call(FunctionDefinition functionDefinition, Expression... args) {
		return ApiExpressionUtils.unresolvedCall(functionDefinition, args);
	}

	public static Expression call(FunctionDefinition functionDefinition, List<Expression> args) {
		return ApiExpressionUtils.unresolvedCall(functionDefinition, args.toArray(new Expression[0]));
	}

	public static Expression and(Expression arg1, Expression arg2) {
		return call(AND, arg1, arg2);
	}

	public static Expression or(Expression arg1, Expression arg2) {
		return call(OR, arg1, arg2);
	}

	public static Expression not(Expression arg) {
		return call(NOT, arg);
	}

	public static Expression isNull(Expression input) {
		return call(IS_NULL, input);
	}

	public static Expression ifThenElse(Expression condition, Expression ifTrue,
			Expression ifFalse) {
		return call(IF, condition, ifTrue, ifFalse);
	}

	public static Expression plus(Expression input1, Expression input2) {
		return call(PLUS, input1, input2);
	}

	public static Expression minus(Expression input1, Expression input2) {
		return call(MINUS, input1, input2);
	}

	public static Expression div(Expression input1, Expression input2) {
		return call(DIVIDE, input1, input2);
	}

	public static Expression times(Expression input1, Expression input2) {
		return call(TIMES, input1, input2);
	}

	public static Expression mod(Expression input1, Expression input2) {
		return call(MOD, input1, input2);
	}

	public static Expression equalTo(Expression input1, Expression input2) {
		return call(EQUALS, input1, input2);
	}

	public static Expression lessThan(Expression input1, Expression input2) {
		return call(LESS_THAN, input1, input2);
	}

	public static Expression greaterThan(Expression input1, Expression input2) {
		return call(GREATER_THAN, input1, input2);
	}

	public static Expression cast(Expression child, Expression type) {
		return call(CAST, child, type);
	}

	public static Expression reinterpretCast(Expression child, Expression type,
			boolean checkOverflow) {
		return call(REINTERPRET_CAST, child, type, literal(checkOverflow));
	}

	public static TypeLiteralExpression typeLiteral(DataType type) {
		return ApiExpressionUtils.typeLiteral(type);
	}

	public static Expression concat(Expression input1, Expression input2) {
		return call(CONCAT, input1, input2);
	}

	public static Expression throwException(String msg, DataType type) {
		return call(THROW_EXCEPTION, literal(msg), typeLiteral(type));
	}
}
