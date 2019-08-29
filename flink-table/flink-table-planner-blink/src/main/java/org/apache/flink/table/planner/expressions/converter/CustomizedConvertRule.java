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

import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionUtils;
import org.apache.flink.table.expressions.TableReferenceExpression;
import org.apache.flink.table.expressions.TypeLiteralExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.expressions.utils.ApiExpressionUtils;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.functions.InternalFunctionDefinitions;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.functions.sql.SqlThrowExceptionFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

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
		DEFINITION_RULE_MAP.put(BuiltInFunctionDefinitions.IS_NULL, CustomizedConvertRule::convertIsNull);
		DEFINITION_RULE_MAP.put(BuiltInFunctionDefinitions.BETWEEN, CustomizedConvertRule::convertBetween);
		DEFINITION_RULE_MAP.put(BuiltInFunctionDefinitions.NOT_BETWEEN, CustomizedConvertRule::convertNotBetween);
		DEFINITION_RULE_MAP.put(BuiltInFunctionDefinitions.REPLACE, CustomizedConvertRule::convertReplace);
		DEFINITION_RULE_MAP.put(BuiltInFunctionDefinitions.PLUS, CustomizedConvertRule::convertPlus);
		DEFINITION_RULE_MAP.put(BuiltInFunctionDefinitions.CEIL, CustomizedConvertRule::convertCeil);
		DEFINITION_RULE_MAP.put(BuiltInFunctionDefinitions.FLOOR, CustomizedConvertRule::convertFloor);
		DEFINITION_RULE_MAP.put(BuiltInFunctionDefinitions.TEMPORAL_OVERLAPS, CustomizedConvertRule::convertTemporalOverlaps);
		DEFINITION_RULE_MAP.put(BuiltInFunctionDefinitions.TIMESTAMP_DIFF, CustomizedConvertRule::convertTimestampDiff);
		DEFINITION_RULE_MAP.put(BuiltInFunctionDefinitions.ARRAY, CustomizedConvertRule::convertArray);
		DEFINITION_RULE_MAP.put(BuiltInFunctionDefinitions.ARRAY_ELEMENT, CustomizedConvertRule::convertArrayElement);
		DEFINITION_RULE_MAP.put(BuiltInFunctionDefinitions.MAP, CustomizedConvertRule::convertMap);
		DEFINITION_RULE_MAP.put(BuiltInFunctionDefinitions.ROW, CustomizedConvertRule::convertRow);
		DEFINITION_RULE_MAP.put(BuiltInFunctionDefinitions.ORDER_ASC, CustomizedConvertRule::convertOrderAsc);

		DEFINITION_RULE_MAP.put(BuiltInFunctionDefinitions.SQRT, (children, context) ->
			context.getRelBuilder().call(FlinkSqlOperatorTable.POWER, context.toRexNodes(
				Arrays.asList(children.get(0), ApiExpressionUtils.valueLiteral(0.5)))));

		// blink expression
		DEFINITION_RULE_MAP.put(InternalFunctionDefinitions.THROW_EXCEPTION, (children, context) -> {
			DataType type = ((TypeLiteralExpression) children.get(1)).getOutputDataType();
			SqlThrowExceptionFunction function = new SqlThrowExceptionFunction(
					context.getTypeFactory().createFieldTypeFromLogicalType(fromDataTypeToLogicalType(type)));
			return context.getRelBuilder().call(function, context.toRexNode(children.get(0)));
		});
	}

	@Override
	public Optional<RexNode> convert(CallExpression call, ConvertContext context) {
		Conversion conversion = DEFINITION_RULE_MAP.get(call.getFunctionDefinition());
		if (conversion == null) {
			return Optional.empty();
		} else {
			return Optional.of(conversion.convert(call.getChildren(), context));
		}
	}

	private static RexNode convertCast(List<Expression> children, ConvertContext context) {
		RexNode child = context.toRexNode(children.get(0));
		TypeLiteralExpression type = (TypeLiteralExpression) children.get(1);
		return context.getRelBuilder().getRexBuilder().makeAbstractCast(
			context.getTypeFactory().createFieldTypeFromLogicalType(
				type.getOutputDataType().getLogicalType().copy(child.getType().isNullable())),
			child);
	}

	private static RexNode convertArrayElement(List<Expression> children, ConvertContext context) {
		List<RexNode> childrenRexNode = context.toRexNodes(children);
		return context.getRelBuilder().call(FlinkSqlOperatorTable.ELEMENT, childrenRexNode);
	}

	private static RexNode convertOrderAsc(List<Expression> children, ConvertContext context) {
		List<RexNode> childrenRexNode = context.toRexNodes(children);
		return childrenRexNode.get(0);
	}

	private static RexNode convertTimestampDiff(List<Expression> children, ConvertContext context) {
		List<RexNode> childrenRexNode = context.toRexNodes(children);
		return context.getRelBuilder().call(FlinkSqlOperatorTable.TIMESTAMP_DIFF, childrenRexNode.get(0), childrenRexNode.get(2),
			childrenRexNode.get(1));
	}

	private static RexNode convertIsNull(List<Expression> children, ConvertContext context) {
		List<RexNode> childrenRexNode = context.toRexNodes(children);
		return context.getRelBuilder().isNull(childrenRexNode.get(0));
	}

	private static RexNode convertNotBetween(List<Expression> children, ConvertContext context) {
		List<RexNode> childrenRexNode = context.toRexNodes(children);
		Preconditions.checkArgument(childrenRexNode.size() == 3);
		RexNode expr = childrenRexNode.get(0);
		RexNode lowerBound = childrenRexNode.get(1);
		RexNode upperBound = childrenRexNode.get(2);
		return context.getRelBuilder().or(
			context.getRelBuilder().call(FlinkSqlOperatorTable.LESS_THAN, expr, lowerBound),
			context.getRelBuilder().call(FlinkSqlOperatorTable.GREATER_THAN, expr, upperBound));
	}

	private static RexNode convertBetween(List<Expression> children, ConvertContext context) {
		List<RexNode> childrenRexNode = context.toRexNodes(children);
		Preconditions.checkArgument(childrenRexNode.size() == 3);
		RexNode expr = childrenRexNode.get(0);
		RexNode lowerBound = childrenRexNode.get(1);
		RexNode upperBound = childrenRexNode.get(2);
		return context.getRelBuilder().and(
			context.getRelBuilder().call(FlinkSqlOperatorTable.GREATER_THAN_OR_EQUAL, expr, lowerBound),
			context.getRelBuilder().call(FlinkSqlOperatorTable.LESS_THAN_OR_EQUAL, expr, upperBound));
	}

	private static RexNode convertCeil(List<Expression> children, ConvertContext context) {
		Preconditions.checkArgument(children.size() == 1 || children.size() == 2);
		List<RexNode> childrenRexNode = context.toRexNodes(children);
		if (children.size() == 1) {
			return context.getRelBuilder().call(FlinkSqlOperatorTable.CEIL, childrenRexNode);
		} else {
			return context.getRelBuilder().call(FlinkSqlOperatorTable.CEIL, childrenRexNode.get(1), childrenRexNode.get(0));
		}
	}

	private static RexNode convertFloor(List<Expression> children, ConvertContext context) {
		Preconditions.checkArgument(children.size() == 1 || children.size() == 2);
		List<RexNode> childrenRexNode = context.toRexNodes(children);
		if (children.size() == 1) {
			return context.getRelBuilder().call(FlinkSqlOperatorTable.FLOOR, childrenRexNode);
		} else {
			return context.getRelBuilder().call(FlinkSqlOperatorTable.FLOOR, childrenRexNode.get(1), childrenRexNode.get(0));
		}
	}

	private static RexNode convertArray(List<Expression> children, ConvertContext context) {
		List<RexNode> childrenRexNode = context.toRexNodes(children);
		ArrayType arrayType = new ArrayType(toLogicalType(childrenRexNode.get(0).getType()));
		// TODO get type from CallExpression directly
		RelDataType relDataType = context.getTypeFactory().createFieldTypeFromLogicalType(arrayType);
		return context.getRelBuilder().getRexBuilder().makeCall(relDataType, FlinkSqlOperatorTable.ARRAY_VALUE_CONSTRUCTOR, childrenRexNode);
	}

	private static RexNode convertMap(List<Expression> children, ConvertContext context) {
		Preconditions.checkArgument(!children.isEmpty() && children.size() % 2 == 0);
		// TODO get type from CallExpression directly
		List<RexNode> childrenRexNode = context.toRexNodes(children);
		RelDataType keyType = childrenRexNode.get(0).getType();
		RelDataType valueType = childrenRexNode.get(childrenRexNode.size() - 1).getType();
		RelDataType mapType = context.getTypeFactory().createMapType(keyType, valueType);
		return context.getRelBuilder().getRexBuilder().makeCall(mapType, FlinkSqlOperatorTable.MAP_VALUE_CONSTRUCTOR, childrenRexNode);
	}

	private static RexNode convertRow(List<Expression> children, ConvertContext context) {
		// TODO get type from CallExpression directly
		List<RexNode> childrenRexNode = context.toRexNodes(children);
		LogicalType[] childTypes = childrenRexNode.stream().map(rexNode -> toLogicalType(rexNode.getType()))
			.toArray(LogicalType[]::new);
		RowType rowType = RowType.of(childTypes);
		RelDataType relDataType = context.getTypeFactory().createFieldTypeFromLogicalType(rowType);
		return context.getRelBuilder().getRexBuilder().makeCall(relDataType, FlinkSqlOperatorTable.ROW, childrenRexNode);
	}

	private static RexNode convertTemporalOverlaps(List<Expression> children, ConvertContext context) {
		List<RexNode> childrenRexNode = context.toRexNodes(children);
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

	private static RexNode convertPlus(List<Expression> children, ConvertContext context) {
		List<RexNode> childrenRexNode = context.toRexNodes(children);
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

	private static RexNode convertReplace(List<Expression> children, ConvertContext context) {
		Preconditions.checkArgument(children.size() == 2 || children.size() == 3);
		List<RexNode> childrenRexNode = context.toRexNodes(children);
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

	private static RexNode convertAs(List<Expression> children, ConvertContext context) {
		String name = extractValue((ValueLiteralExpression) children.get(1), String.class);
		RexNode child = context.toRexNode(children.get(0));
		return context.getRelBuilder().alias(child, name);
	}

	private static RexNode convertTrim(List<Expression> children, ConvertContext context) {
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

	private static RexNode convertGet(List<Expression> children, ConvertContext context) {
		RexNode child = context.toRexNode(children.get(0));
		ValueLiteralExpression keyLiteral = (ValueLiteralExpression) children.get(1);
		Optional<Integer> indexOptional = ExpressionUtils.extractValue(keyLiteral, String.class).map(
			child.getType().getFieldNames()::indexOf);
		// Note: never replace the following code with :
		// int index = indexOptional.orElseGet(() -> extractValue(keyLiteral, Integer.class));
		// Because the logical in `orElseGet` always executed no matter whether indexOptional is present or not.
		int index;
		index = indexOptional.orElseGet(() -> extractValue(keyLiteral, Integer.class));
		return context.getRelBuilder().getRexBuilder().makeFieldAccess(child, index);
	}

	private static RexNode convertIn(List<Expression> children, ConvertContext context) {
		Expression headExpr = children.get(1);
		if (headExpr instanceof TableReferenceExpression) {
			QueryOperation tableOperation = ((TableReferenceExpression) headExpr).getQueryOperation();
			RexNode child = context.toRexNode(children.get(0));
			return RexSubQuery.in(((FlinkRelBuilder) context.getRelBuilder())
					.queryOperation(tableOperation).build(),
				ImmutableList.of(child));
		} else {
			List<RexNode> child = context.toRexNodes(children);
			return context.getRelBuilder().call(FlinkSqlOperatorTable.IN, child);
		}
	}

	private static RexNode convertReinterpretCast(List<Expression> children, ConvertContext context) {
		RexNode child = context.toRexNode(children.get(0));
		TypeLiteralExpression type = (TypeLiteralExpression) children.get(1);
		RexNode checkOverflow = context.toRexNode(children.get(2));
		return context.getRelBuilder().getRexBuilder().makeReinterpretCast(
			context.getTypeFactory().createFieldTypeFromLogicalType(
				type.getOutputDataType().getLogicalType().copy(child.getType().isNullable())),
			child,
			checkOverflow);
	}

	private interface Conversion {
		RexNode convert(List<Expression> children, ConvertContext context);
	}
}
