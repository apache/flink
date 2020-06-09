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

package org.apache.flink.table.planner.expressions.converter;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionUtils;
import org.apache.flink.table.expressions.TableReferenceExpression;
import org.apache.flink.table.expressions.TypeLiteralExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.functions.InternalFunctionDefinitions;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.functions.sql.SqlThrowExceptionFunction;
import org.apache.flink.table.types.DataType;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.fun.SqlTrimFunction;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
import static org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType;
import static org.apache.flink.table.planner.expressions.converter.ExpressionConverter.extractValue;
import static org.apache.flink.table.planner.expressions.converter.ExpressionConverter.toRexNodes;
import static org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType;
import static org.apache.flink.table.runtime.typeutils.TypeCheckUtils.isCharacterString;
import static org.apache.flink.table.runtime.typeutils.TypeCheckUtils.isTemporal;
import static org.apache.flink.table.runtime.typeutils.TypeCheckUtils.isTimeInterval;

/**
 * Customized {@link CallExpressionConvertRule}, Functions conversion here all require special logic,
 * and there may be some special rules, such as needing get the literal values of inputs, such as
 * converting to combinations of functions, to convert to RexNode of calcite.
 */
public class CustomizedConvertRule implements CallExpressionConvertRule {

	private static final Map<FunctionDefinition, Conversion> DEFINITION_RULE_MAP = new HashMap<>();
	static {
		DEFINITION_RULE_MAP.put(BuiltInFunctionDefinitions.CAST, CustomizedConvertRule::convertCast);
		DEFINITION_RULE_MAP.put(BuiltInFunctionDefinitions.REINTERPRET_CAST, CustomizedConvertRule::convertReinterpretCast);
		DEFINITION_RULE_MAP.put(BuiltInFunctionDefinitions.IN, CustomizedConvertRule::convertIn);
		DEFINITION_RULE_MAP.put(BuiltInFunctionDefinitions.GET, CustomizedConvertRule::convertGet);
		DEFINITION_RULE_MAP.put(BuiltInFunctionDefinitions.TRIM, CustomizedConvertRule::convertTrim);
		DEFINITION_RULE_MAP.put(BuiltInFunctionDefinitions.AS, CustomizedConvertRule::convertAs);
		DEFINITION_RULE_MAP.put(BuiltInFunctionDefinitions.BETWEEN, CustomizedConvertRule::convertBetween);
		DEFINITION_RULE_MAP.put(BuiltInFunctionDefinitions.NOT_BETWEEN, CustomizedConvertRule::convertNotBetween);
		DEFINITION_RULE_MAP.put(BuiltInFunctionDefinitions.REPLACE, CustomizedConvertRule::convertReplace);
		DEFINITION_RULE_MAP.put(BuiltInFunctionDefinitions.PLUS, CustomizedConvertRule::convertPlus);
		DEFINITION_RULE_MAP.put(BuiltInFunctionDefinitions.CEIL, CustomizedConvertRule::convertCeil);
		DEFINITION_RULE_MAP.put(BuiltInFunctionDefinitions.FLOOR, CustomizedConvertRule::convertFloor);
		DEFINITION_RULE_MAP.put(BuiltInFunctionDefinitions.TEMPORAL_OVERLAPS, CustomizedConvertRule::convertTemporalOverlaps);
		DEFINITION_RULE_MAP.put(BuiltInFunctionDefinitions.TIMESTAMP_DIFF, CustomizedConvertRule::convertTimestampDiff);
		DEFINITION_RULE_MAP.put(BuiltInFunctionDefinitions.ARRAY, CustomizedConvertRule::convertArray);
		DEFINITION_RULE_MAP.put(BuiltInFunctionDefinitions.MAP, CustomizedConvertRule::convertMap);
		DEFINITION_RULE_MAP.put(BuiltInFunctionDefinitions.ROW, CustomizedConvertRule::convertRow);
		DEFINITION_RULE_MAP.put(BuiltInFunctionDefinitions.ORDER_ASC, CustomizedConvertRule::convertOrderAsc);
		DEFINITION_RULE_MAP.put(BuiltInFunctionDefinitions.SQRT, CustomizedConvertRule::convertSqrt);

		// blink expression
		DEFINITION_RULE_MAP.put(InternalFunctionDefinitions.THROW_EXCEPTION, CustomizedConvertRule::convertThrowException);
	}

	@Override
	public Optional<RexNode> convert(CallExpression call, ConvertContext context) {
		Conversion conversion = DEFINITION_RULE_MAP.get(call.getFunctionDefinition());
		return Optional.ofNullable(conversion).map(c -> c.convert(call, context));
	}

	private static RexNode convertCast(CallExpression call, ConvertContext context) {
		checkArgumentNumber(call, 2);
		RexNode child = context.toRexNode(call.getChildren().get(0));
		TypeLiteralExpression type = (TypeLiteralExpression) call.getChildren().get(1);
		return context.getRelBuilder().getRexBuilder().makeAbstractCast(
			context.getTypeFactory().createFieldTypeFromLogicalType(
				type.getOutputDataType().getLogicalType().copy(child.getType().isNullable())),
			child);
	}

	private static RexNode convertOrderAsc(CallExpression call, ConvertContext context) {
		checkArgumentNumber(call, 1);
		return context.toRexNode(call.getChildren().get(0));
	}

	private static RexNode convertTimestampDiff(CallExpression call, ConvertContext context) {
		checkArgumentNumber(call, 3);
		List<RexNode> childrenRexNode = toRexNodes(context, call.getChildren());
		// different orders between flink table api and calcite.
		return context.getRelBuilder().call(FlinkSqlOperatorTable.TIMESTAMP_DIFF, childrenRexNode.get(0), childrenRexNode.get(2),
			childrenRexNode.get(1));
	}

	private static RexNode convertNotBetween(CallExpression call, ConvertContext context) {
		checkArgumentNumber(call, 3);
		List<RexNode> childrenRexNode = toRexNodes(context, call.getChildren());
		RexNode expr = childrenRexNode.get(0);
		RexNode lowerBound = childrenRexNode.get(1);
		RexNode upperBound = childrenRexNode.get(2);
		return context.getRelBuilder().or(
			context.getRelBuilder().call(FlinkSqlOperatorTable.LESS_THAN, expr, lowerBound),
			context.getRelBuilder().call(FlinkSqlOperatorTable.GREATER_THAN, expr, upperBound));
	}

	private static RexNode convertBetween(CallExpression call, ConvertContext context) {
		checkArgumentNumber(call, 3);
		List<RexNode> childrenRexNode = toRexNodes(context, call.getChildren());
		RexNode expr = childrenRexNode.get(0);
		RexNode lowerBound = childrenRexNode.get(1);
		RexNode upperBound = childrenRexNode.get(2);
		return context.getRelBuilder().and(
			context.getRelBuilder().call(FlinkSqlOperatorTable.GREATER_THAN_OR_EQUAL, expr, lowerBound),
			context.getRelBuilder().call(FlinkSqlOperatorTable.LESS_THAN_OR_EQUAL, expr, upperBound));
	}

	private static RexNode convertCeil(CallExpression call, ConvertContext context) {
		checkArgumentNumber(call, 1, 2);
		List<Expression> children = call.getChildren();
		List<RexNode> childrenRexNode = toRexNodes(context, children);
		if (children.size() == 1) {
			return context.getRelBuilder().call(FlinkSqlOperatorTable.CEIL, childrenRexNode);
		} else {
			return context.getRelBuilder().call(FlinkSqlOperatorTable.CEIL, childrenRexNode.get(1), childrenRexNode.get(0));
		}
	}

	private static RexNode convertFloor(CallExpression call, ConvertContext context) {
		checkArgumentNumber(call, 1, 2);
		List<Expression> children = call.getChildren();
		List<RexNode> childrenRexNode = toRexNodes(context, children);
		if (children.size() == 1) {
			return context.getRelBuilder().call(FlinkSqlOperatorTable.FLOOR, childrenRexNode);
		} else {
			return context.getRelBuilder().call(FlinkSqlOperatorTable.FLOOR, childrenRexNode.get(1), childrenRexNode.get(0));
		}
	}

	private static RexNode convertArray(CallExpression call, ConvertContext context) {
		List<RexNode> childrenRexNode = toRexNodes(context, call.getChildren());
		RelDataType relDataType = context.getTypeFactory()
			.createFieldTypeFromLogicalType(call.getOutputDataType().getLogicalType());
		return context.getRelBuilder()
			.getRexBuilder()
			.makeCall(relDataType, FlinkSqlOperatorTable.ARRAY_VALUE_CONSTRUCTOR, childrenRexNode);
	}

	private static RexNode convertMap(CallExpression call, ConvertContext context) {
		List<Expression> children = call.getChildren();
		checkArgument(call, !children.isEmpty() && children.size() % 2 == 0);
		List<RexNode> childrenRexNode = toRexNodes(context, children);
		RelDataType mapType = context.getTypeFactory()
			.createFieldTypeFromLogicalType(call.getOutputDataType().getLogicalType());
		return context.getRelBuilder()
			.getRexBuilder()
			.makeCall(mapType, FlinkSqlOperatorTable.MAP_VALUE_CONSTRUCTOR, childrenRexNode);
	}

	private static RexNode convertRow(CallExpression call, ConvertContext context) {
		List<RexNode> childrenRexNode = toRexNodes(context, call.getChildren());
		RelDataType relDataType = context.getTypeFactory()
			.createFieldTypeFromLogicalType(call.getOutputDataType().getLogicalType());
		return context.getRelBuilder().getRexBuilder().makeCall(relDataType, FlinkSqlOperatorTable.ROW, childrenRexNode);
	}

	private static RexNode convertTemporalOverlaps(CallExpression call, ConvertContext context) {
		checkArgumentNumber(call, 4);
		List<RexNode> childrenRexNode = toRexNodes(context, call.getChildren());
		// Standard conversion of the OVERLAPS operator.
		// Source: [[org.apache.calcite.sql2rel.StandardConvertletTable#convertOverlaps()]]
		RexNode leftTimePoint = childrenRexNode.get(0);
		RexNode leftTemporal = childrenRexNode.get(1);
		RexNode rightTimePoint = childrenRexNode.get(2);
		RexNode rightTemporal = childrenRexNode.get(3);
		RexNode convLeftT;
		if (isTimeInterval(toLogicalType(leftTemporal.getType()))) {
			convLeftT = context.getRelBuilder().call(FlinkSqlOperatorTable.DATETIME_PLUS, leftTimePoint, leftTemporal);
		} else {
			convLeftT = leftTemporal;
		}
		// sort end points into start and end, such that (s0 <= e0) and (s1 <= e1).
		RexNode leftLe = context.getRelBuilder().call(FlinkSqlOperatorTable.LESS_THAN_OR_EQUAL, leftTimePoint, convLeftT);
		RexNode s0 = context.getRelBuilder().call(FlinkSqlOperatorTable.CASE, leftLe, leftTimePoint, convLeftT);
		RexNode e0 = context.getRelBuilder().call(FlinkSqlOperatorTable.CASE, leftLe, convLeftT, leftTimePoint);
		RexNode convRightT;
		if (isTimeInterval(toLogicalType(rightTemporal.getType()))) {
			convRightT = context.getRelBuilder().call(FlinkSqlOperatorTable.DATETIME_PLUS, rightTimePoint, rightTemporal);
		} else {
			convRightT = rightTemporal;
		}
		RexNode rightLe = context.getRelBuilder().call(FlinkSqlOperatorTable.LESS_THAN_OR_EQUAL, rightTimePoint, convRightT);
		RexNode s1 = context.getRelBuilder().call(FlinkSqlOperatorTable.CASE, rightLe, rightTimePoint, convRightT);
		RexNode e1 = context.getRelBuilder().call(FlinkSqlOperatorTable.CASE, rightLe, convRightT, rightTimePoint);

		// (e0 >= s1) AND (e1 >= s0)
		RexNode leftPred = context.getRelBuilder().call(FlinkSqlOperatorTable.GREATER_THAN_OR_EQUAL, e0, s1);
		RexNode rightPred = context.getRelBuilder().call(FlinkSqlOperatorTable.GREATER_THAN_OR_EQUAL, e1, s0);
		return context.getRelBuilder().call(FlinkSqlOperatorTable.AND, leftPred, rightPred);
	}

	private static RexNode convertPlus(CallExpression call, ConvertContext context) {
		checkArgumentNumber(call, 2);
		List<RexNode> childrenRexNode = toRexNodes(context, call.getChildren());
		if (isCharacterString(toLogicalType(childrenRexNode.get(0).getType()))) {
			return context.getRelBuilder().call(
				FlinkSqlOperatorTable.CONCAT,
				childrenRexNode.get(0),
				context.getRelBuilder().cast(childrenRexNode.get(1), VARCHAR));
		} else if (isCharacterString(toLogicalType(childrenRexNode.get(1).getType()))) {
			return context.getRelBuilder().call(
				FlinkSqlOperatorTable.CONCAT,
				context.getRelBuilder().cast(childrenRexNode.get(0), VARCHAR),
				childrenRexNode.get(1));
		} else if (isTimeInterval(toLogicalType(childrenRexNode.get(0).getType())) &&
			childrenRexNode.get(0).getType() == childrenRexNode.get(1).getType()) {
			return context.getRelBuilder().call(FlinkSqlOperatorTable.PLUS, childrenRexNode);
		} else if (isTimeInterval(toLogicalType(childrenRexNode.get(0).getType()))
			&& isTemporal(toLogicalType(childrenRexNode.get(1).getType()))) {
			// Calcite has a bug that can't apply INTERVAL + DATETIME (INTERVAL at left)
			// we manually switch them here
			return context.getRelBuilder().call(FlinkSqlOperatorTable.DATETIME_PLUS, childrenRexNode.get(1), childrenRexNode.get(0));
		} else if (isTemporal(toLogicalType(childrenRexNode.get(0).getType())) &&
			isTemporal(toLogicalType(childrenRexNode.get(1).getType()))) {
			return context.getRelBuilder().call(FlinkSqlOperatorTable.DATETIME_PLUS, childrenRexNode);
		} else {
			return context.getRelBuilder().call(FlinkSqlOperatorTable.PLUS, childrenRexNode);
		}
	}

	private static RexNode convertReplace(CallExpression call, ConvertContext context) {
		checkArgumentNumber(call, 2, 3);
		List<Expression> children = call.getChildren();
		List<RexNode> childrenRexNode = toRexNodes(context, children);
		if (children.size() == 2) {
			return context.getRelBuilder().call(
				FlinkSqlOperatorTable.REPLACE,
				childrenRexNode.get(0),
				childrenRexNode.get(1),
				context.getRelBuilder().call(FlinkSqlOperatorTable.CHAR_LENGTH, childrenRexNode.get(0)));
		} else {
			return context.getRelBuilder().call(FlinkSqlOperatorTable.REPLACE, childrenRexNode);
		}
	}

	private static RexNode convertAs(CallExpression call, ConvertContext context) {
		checkArgumentNumber(call, 2);
		String name = extractValue((ValueLiteralExpression) call.getChildren().get(1), String.class);
		RexNode child = context.toRexNode(call.getChildren().get(0));
		return context.getRelBuilder().alias(child, name);
	}

	private static RexNode convertTrim(CallExpression call, ConvertContext context) {
		checkArgumentNumber(call, 4);
		List<Expression> children = call.getChildren();
		ValueLiteralExpression removeLeadingExpr = (ValueLiteralExpression) children.get(0);
		Boolean removeLeading = extractValue(removeLeadingExpr, Boolean.class);
		ValueLiteralExpression removeTrailingExpr = (ValueLiteralExpression) children.get(1);
		Boolean removeTrailing = extractValue(removeTrailingExpr, Boolean.class);
		RexNode trimString = context.toRexNode(children.get(2));
		RexNode str = context.toRexNode(children.get(3));
		Enum trimMode;
		if (removeLeading && removeTrailing) {
			trimMode = SqlTrimFunction.Flag.BOTH;
		} else if (removeLeading) {
			trimMode = SqlTrimFunction.Flag.LEADING;
		} else if (removeTrailing) {
			trimMode = SqlTrimFunction.Flag.TRAILING;
		} else {
			throw new IllegalArgumentException("Unsupported trim mode.");
		}
		return context.getRelBuilder().call(
			FlinkSqlOperatorTable.TRIM,
			context.getRelBuilder().getRexBuilder().makeFlag(trimMode),
			trimString,
			str);
	}

	private static RexNode convertGet(CallExpression call, ConvertContext context) {
		checkArgumentNumber(call, 2);
		RexNode child = context.toRexNode(call.getChildren().get(0));
		ValueLiteralExpression keyLiteral = (ValueLiteralExpression) call.getChildren().get(1);
		Optional<Integer> indexOptional = ExpressionUtils
				.extractValue(keyLiteral, String.class)
				.map(child.getType().getFieldNames()::indexOf);
		int index = indexOptional.orElseGet(() -> extractValue(keyLiteral, Integer.class));
		return context.getRelBuilder().getRexBuilder().makeFieldAccess(child, index);
	}

	private static RexNode convertIn(CallExpression call, ConvertContext context) {
		checkArgument(call, call.getChildren().size() > 1);
		Expression headExpr = call.getChildren().get(1);
		if (headExpr instanceof TableReferenceExpression) {
			QueryOperation tableOperation = ((TableReferenceExpression) headExpr).getQueryOperation();
			RexNode child = context.toRexNode(call.getChildren().get(0));
			return RexSubQuery.in(((FlinkRelBuilder) context.getRelBuilder())
					.queryOperation(tableOperation).build(), ImmutableList.of(child));
		} else {
			List<RexNode> child = toRexNodes(context, call.getChildren());
			return context.getRelBuilder().call(FlinkSqlOperatorTable.IN, child);
		}
	}

	private static RexNode convertReinterpretCast(CallExpression call, ConvertContext context) {
		checkArgumentNumber(call, 3);
		RexNode child = context.toRexNode(call.getChildren().get(0));
		TypeLiteralExpression type = (TypeLiteralExpression) call.getChildren().get(1);
		RexNode checkOverflow = context.toRexNode(call.getChildren().get(2));
		return context.getRelBuilder().getRexBuilder().makeReinterpretCast(
			context.getTypeFactory().createFieldTypeFromLogicalType(
				type.getOutputDataType().getLogicalType().copy(child.getType().isNullable())),
			child,
			checkOverflow);
	}

	private static RexNode convertSqrt(CallExpression call, ConvertContext context) {
		checkArgumentNumber(call, 1);
		return context.getRelBuilder().call(FlinkSqlOperatorTable.POWER, toRexNodes(context,
			Arrays.asList(call.getChildren().get(0), ApiExpressionUtils.valueLiteral(0.5))));
	}

	private static RexNode convertThrowException(CallExpression call, ConvertContext context) {
		checkArgumentNumber(call, 2);
		DataType type = ((TypeLiteralExpression) call.getChildren().get(1)).getOutputDataType();
		SqlThrowExceptionFunction function = new SqlThrowExceptionFunction(
			context.getTypeFactory().createFieldTypeFromLogicalType(fromDataTypeToLogicalType(type)));
		return context.getRelBuilder().call(function, context.toRexNode(call.getChildren().get(0)));
	}

	private static void checkArgumentNumber(CallExpression call, int... numbers) {
		boolean find = false;
		for (int number : numbers) {
			if (call.getChildren().size() == number) {
				find = true;
				break;
			}
		}

		checkArgument(call, find);
	}

	private static void checkArgument(CallExpression call, boolean check) {
		if (!check) {
			throw new TableException("Invalid arguments for call: " + call);
		}
	}

	private interface Conversion {
		RexNode convert(CallExpression call, ConvertContext context);
	}
}
