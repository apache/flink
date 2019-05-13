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

import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.AND;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.CAST;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.CONCAT;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.DIVIDE;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.EQUALS;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.GREATER_THAN;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.IF;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.IS_NULL;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.LESS_THAN;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.MINUS;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.NOT;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.OR;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.PLUS;
import static org.apache.flink.table.expressions.InternalFunctionDefinitions.THROW_EXCEPTION;

/**
 * Builder for {@link Expression}s.
 */
public class ExpressionBuilder {

	public static Expression nullOf(TypeInformation type) {
		return literal(null, type);
	}

	public static Expression literal(Object value) {
		return new ValueLiteralExpression(value);
	}

	public static Expression literal(Object value, TypeInformation<?> type) {
		return new ValueLiteralExpression(value, type);
	}

	public static Expression call(FunctionDefinition functionDefinition, Expression... args) {
		return new CallExpression(functionDefinition, Arrays.asList(args));
	}

	public static Expression call(FunctionDefinition functionDefinition, List<Expression> args) {
		return new CallExpression(functionDefinition, args);
	}

	public static Expression and(Expression arg1, Expression arg2) {
		return new CallExpression(AND, Arrays.asList(arg1, arg2));
	}

	public static Expression or(Expression arg1, Expression arg2) {
		return new CallExpression(OR, Arrays.asList(arg1, arg2));
	}

	public static Expression not(Expression arg) {
		return new CallExpression(NOT, Collections.singletonList(arg));
	}

	public static Expression isNull(Expression input) {
		return call(IS_NULL, input);
	}

	public static Expression ifThenElse(Expression condition, Expression ifTrue, Expression ifFalse) {
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

	public static Expression equalTo(Expression input1, Expression input2) {
		return call(EQUALS, input1, input2);
	}

	public static Expression lessThan(Expression input1, Expression input2) {
		return call(LESS_THAN, input1, input2);
	}

	public static Expression greaterThan(Expression input1, Expression input2) {
		return call(GREATER_THAN, input1, input2);
	}

	public static Expression cast(Expression input1, Expression input2) {
		return call(CAST, input1, input2);
	}

	public static TypeLiteralExpression typeLiteral(TypeInformation<?> type) {
		return new TypeLiteralExpression(type);
	}

	public static Expression concat(Expression input1, Expression input2) {
		return call(CONCAT, input1, input2);
	}

	public static Expression throwException(String msg, TypeInformation<?> type) {
		return call(THROW_EXCEPTION, typeLiteral(type));
	}
}
