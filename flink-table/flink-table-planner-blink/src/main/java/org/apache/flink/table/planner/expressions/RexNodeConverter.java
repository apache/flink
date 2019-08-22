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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionUtils;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ResolvedExpressionVisitor;
import org.apache.flink.table.expressions.TableReferenceExpression;
import org.apache.flink.table.expressions.TimeIntervalUnit;
import org.apache.flink.table.expressions.TimePointUnit;
import org.apache.flink.table.expressions.TypeLiteralExpression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.expressions.utils.ApiExpressionUtils;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.AggregateFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.ScalarFunctionDefinition;
import org.apache.flink.table.functions.TableFunctionDefinition;
import org.apache.flink.table.functions.UserDefinedAggregateFunction;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.RexAggLocalVariable;
import org.apache.flink.table.planner.calcite.RexDistinctKeyVariable;
import org.apache.flink.table.planner.functions.InternalFunctionDefinitions;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.functions.sql.SqlThrowExceptionFunction;
import org.apache.flink.table.planner.functions.utils.UserDefinedFunctionUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.util.Preconditions;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OrdinalReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.TimestampWithTimeZoneString;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
import static org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType;
import static org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType;
import static org.apache.flink.table.runtime.typeutils.TypeCheckUtils.isCharacterString;
import static org.apache.flink.table.runtime.typeutils.TypeCheckUtils.isTemporal;
import static org.apache.flink.table.runtime.typeutils.TypeCheckUtils.isTimeInterval;
import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;

/**
 * Visit expression to generator {@link RexNode}.
 *
 * <p>TODO actually we should use {@link ResolvedExpressionVisitor} here as it is the output of the API.
 * we will update it after introduce Expression resolve in AggCodeGen.
 */
public class RexNodeConverter implements ExpressionVisitor<RexNode> {

	private final RelBuilder relBuilder;
	private final FlinkTypeFactory typeFactory;

	// store mapping from BuiltInFunctionDefinition to it's RexNodeConversion.
	private final Map<FunctionDefinition, RexNodeConversion> conversionsOfBuiltInFunc = new IdentityHashMap<>();

	public RexNodeConverter(RelBuilder relBuilder) {
		this.relBuilder = relBuilder;
		this.typeFactory = (FlinkTypeFactory) relBuilder.getRexBuilder().getTypeFactory();

		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.CAST, exprs -> convertCast(exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.REINTERPRET_CAST, exprs -> convertReinterpretCast(exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.IN, exprs -> convertIn(exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.GET, exprs -> convertGet(exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.TRIM, exprs -> convertTrim(exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.AS, exprs -> convertAs(exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.OVER, exprs -> convertOver(exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.IS_NULL, exprs -> convertIsNull(exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.BETWEEN, exprs -> convertBetween(exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.NOT_BETWEEN, exprs -> convertNotBetween(exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.REPLACE, exprs -> convertReplace(exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.PLUS, exprs -> convertPlus(exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.CEIL, exprs -> convertCeil(exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.FLOOR, exprs -> convertFloor(exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.TEMPORAL_OVERLAPS, exprs -> convertTemporalOverlaps(exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.TIMESTAMP_DIFF, exprs -> convertTimestampDiff(exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.ARRAY, exprs -> convertArray(exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.ARRAY_ELEMENT, exprs -> convertArrayElement(exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.MAP, exprs -> convertMap(exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.ROW, exprs -> convertRow(exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.ORDER_ASC, exprs -> convertOrderAsc(exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.SHA1, exprs -> convert(FlinkSqlOperatorTable.SHA1, exprs));
		// logic functions
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.AND, exprs -> convert(FlinkSqlOperatorTable.AND, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.OR, exprs -> convert(FlinkSqlOperatorTable.OR, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.NOT, exprs -> convert(FlinkSqlOperatorTable.NOT, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.IF, exprs -> convert(FlinkSqlOperatorTable.CASE, exprs));

		// comparison functions
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.EQUALS, exprs -> convert(FlinkSqlOperatorTable.EQUALS, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.GREATER_THAN, exprs -> convert(FlinkSqlOperatorTable.GREATER_THAN, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, exprs -> convert(FlinkSqlOperatorTable.GREATER_THAN_OR_EQUAL, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.LESS_THAN, exprs -> convert(FlinkSqlOperatorTable.LESS_THAN, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL, exprs -> convert(FlinkSqlOperatorTable.LESS_THAN_OR_EQUAL, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.NOT_EQUALS, exprs -> convert(FlinkSqlOperatorTable.NOT_EQUALS, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.IS_NULL, exprs -> convert(FlinkSqlOperatorTable.IS_NULL, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.IS_NOT_NULL, exprs -> convert(FlinkSqlOperatorTable.IS_NOT_NULL, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.IS_TRUE, exprs -> convert(FlinkSqlOperatorTable.IS_TRUE, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.IS_FALSE, exprs -> convert(FlinkSqlOperatorTable.IS_FALSE, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.IS_NOT_TRUE, exprs -> convert(FlinkSqlOperatorTable.IS_NOT_TRUE, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.IS_NOT_FALSE, exprs -> convert(FlinkSqlOperatorTable.IS_NOT_FALSE, exprs));

		// string functions
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.CHAR_LENGTH, exprs -> convert(FlinkSqlOperatorTable.CHAR_LENGTH, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.INIT_CAP, exprs -> convert(FlinkSqlOperatorTable.INITCAP, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.LIKE, exprs -> convert(FlinkSqlOperatorTable.LIKE, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.LOWER, exprs -> convert(FlinkSqlOperatorTable.LOWER, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.SIMILAR, exprs -> convert(FlinkSqlOperatorTable.SIMILAR_TO, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.SUBSTRING, exprs -> convert(FlinkSqlOperatorTable.SUBSTRING, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.UPPER, exprs -> convert(FlinkSqlOperatorTable.UPPER, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.POSITION, exprs -> convert(FlinkSqlOperatorTable.POSITION, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.OVERLAY, exprs -> convert(FlinkSqlOperatorTable.OVERLAY, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.CONCAT, exprs -> convert(FlinkSqlOperatorTable.CONCAT_FUNCTION, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.CONCAT_WS, exprs -> convert(FlinkSqlOperatorTable.CONCAT_WS, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.LPAD, exprs -> convert(FlinkSqlOperatorTable.LPAD, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.RPAD, exprs -> convert(FlinkSqlOperatorTable.RPAD, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.REGEXP_EXTRACT, exprs -> convert(FlinkSqlOperatorTable.REGEXP_EXTRACT, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.FROM_BASE64, exprs -> convert(FlinkSqlOperatorTable.FROM_BASE64, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.TO_BASE64, exprs -> convert(FlinkSqlOperatorTable.TO_BASE64, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.UUID, exprs -> convert(FlinkSqlOperatorTable.UUID, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.LTRIM, exprs -> convert(FlinkSqlOperatorTable.LTRIM, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.RTRIM, exprs -> convert(FlinkSqlOperatorTable.RTRIM, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.REPEAT, exprs -> convert(FlinkSqlOperatorTable.REPEAT, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.REGEXP_REPLACE, exprs -> convert(FlinkSqlOperatorTable.REGEXP_REPLACE, exprs));

		// math functions
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.MINUS, exprs -> convert(FlinkSqlOperatorTable.MINUS, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.DIVIDE, exprs -> convert(FlinkSqlOperatorTable.DIVIDE, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.TIMES, exprs -> convert(FlinkSqlOperatorTable.MULTIPLY, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.ABS, exprs -> convert(FlinkSqlOperatorTable.ABS, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.EXP, exprs -> convert(FlinkSqlOperatorTable.EXP, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.LOG10, exprs -> convert(FlinkSqlOperatorTable.LOG10, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.LOG2, exprs -> convert(FlinkSqlOperatorTable.LOG2, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.LN, exprs -> convert(FlinkSqlOperatorTable.LN, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.LOG, exprs -> convert(FlinkSqlOperatorTable.LOG, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.POWER, exprs -> convert(FlinkSqlOperatorTable.POWER, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.MOD, exprs -> convert(FlinkSqlOperatorTable.MOD, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.SQRT, exprs ->
				relBuilder.call(FlinkSqlOperatorTable.POWER,
						convertCallChildren(Arrays.asList(exprs.get(0), ApiExpressionUtils.valueLiteral(0.5)))));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.MINUS_PREFIX, exprs -> convert(FlinkSqlOperatorTable.UNARY_MINUS, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.SIN, exprs -> convert(FlinkSqlOperatorTable.SIN, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.COS, exprs -> convert(FlinkSqlOperatorTable.COS, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.SINH, exprs -> convert(FlinkSqlOperatorTable.SINH, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.TAN, exprs -> convert(FlinkSqlOperatorTable.TAN, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.TANH, exprs -> convert(FlinkSqlOperatorTable.TANH, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.COT, exprs -> convert(FlinkSqlOperatorTable.COT, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.ASIN, exprs -> convert(FlinkSqlOperatorTable.ASIN, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.ACOS, exprs -> convert(FlinkSqlOperatorTable.ACOS, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.ATAN, exprs -> convert(FlinkSqlOperatorTable.ATAN, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.ATAN2, exprs -> convert(FlinkSqlOperatorTable.ATAN2, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.COSH, exprs -> convert(FlinkSqlOperatorTable.COSH, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.DEGREES, exprs -> convert(FlinkSqlOperatorTable.DEGREES, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.RADIANS, exprs -> convert(FlinkSqlOperatorTable.RADIANS, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.SIGN, exprs -> convert(FlinkSqlOperatorTable.SIGN, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.ROUND, exprs -> convert(FlinkSqlOperatorTable.ROUND, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.PI, exprs -> convert(FlinkSqlOperatorTable.PI, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.E, exprs -> convert(FlinkSqlOperatorTable.E, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.RAND, exprs -> convert(FlinkSqlOperatorTable.RAND, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.RAND_INTEGER, exprs -> convert(FlinkSqlOperatorTable.RAND_INTEGER, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.BIN, exprs -> convert(FlinkSqlOperatorTable.BIN, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.HEX, exprs -> convert(FlinkSqlOperatorTable.HEX, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.TRUNCATE, exprs -> convert(FlinkSqlOperatorTable.TRUNCATE, exprs));

		// time functions
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.EXTRACT, exprs -> convert(FlinkSqlOperatorTable.EXTRACT, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.CURRENT_DATE, exprs -> convert(FlinkSqlOperatorTable.CURRENT_DATE, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.CURRENT_TIME, exprs -> convert(FlinkSqlOperatorTable.CURRENT_TIME, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.CURRENT_TIMESTAMP, exprs -> convert(FlinkSqlOperatorTable.CURRENT_TIMESTAMP, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.LOCAL_TIME, exprs -> convert(FlinkSqlOperatorTable.LOCALTIME, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.LOCAL_TIMESTAMP, exprs -> convert(FlinkSqlOperatorTable.LOCALTIMESTAMP, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.DATE_FORMAT, exprs -> convert(FlinkSqlOperatorTable.DATE_FORMAT, exprs));

		// collection
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.AT, exprs -> convert(FlinkSqlOperatorTable.ITEM, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.CARDINALITY, exprs -> convert(FlinkSqlOperatorTable.CARDINALITY, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.ORDER_DESC, exprs -> convert(FlinkSqlOperatorTable.DESC, exprs));

		// crypto hash
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.MD5, exprs -> convert(FlinkSqlOperatorTable.MD5, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.SHA2, exprs -> convert(FlinkSqlOperatorTable.SHA2, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.SHA224, exprs -> convert(FlinkSqlOperatorTable.SHA224, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.SHA256, exprs -> convert(FlinkSqlOperatorTable.SHA256, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.SHA384, exprs -> convert(FlinkSqlOperatorTable.SHA384, exprs));
		conversionsOfBuiltInFunc
				.put(BuiltInFunctionDefinitions.SHA512, exprs -> convert(FlinkSqlOperatorTable.SHA512, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.SHA1, exprs -> convert(FlinkSqlOperatorTable.SHA1, exprs));
		conversionsOfBuiltInFunc.put(BuiltInFunctionDefinitions.STREAM_RECORD_TIMESTAMP,
				exprs -> convert(FlinkSqlOperatorTable.STREAMRECORD_TIMESTAMP, exprs));

		// blink expression
		conversionsOfBuiltInFunc.put(InternalFunctionDefinitions.THROW_EXCEPTION, exprs -> {
			DataType type = ((TypeLiteralExpression) exprs.get(1)).getOutputDataType();
			return convert(new SqlThrowExceptionFunction(
					typeFactory.createFieldTypeFromLogicalType(fromDataTypeToLogicalType(type))),
					exprs.subList(0, 1));
		});
	}

	@Override
	public RexNode visit(CallExpression call) {
		FunctionDefinition func = call.getFunctionDefinition();
		if (func instanceof ScalarFunctionDefinition) {
			ScalarFunction scalaFunc = ((ScalarFunctionDefinition) func).getScalarFunction();
			List<RexNode> child = convertCallChildren(call.getChildren());
			SqlFunction sqlFunction = UserDefinedFunctionUtils.createScalarSqlFunction(
					scalaFunc.functionIdentifier(),
					scalaFunc.toString(),
					scalaFunc,
					typeFactory);
			return relBuilder.call(sqlFunction, child);
		} else if (func instanceof TableFunctionDefinition) {
			throw new RuntimeException("There is no possible reach here!");
		} else if (func instanceof AggregateFunctionDefinition) {
			UserDefinedAggregateFunction aggFunc = ((AggregateFunctionDefinition) func).getAggregateFunction();
			if (aggFunc instanceof AggregateFunction) {
				SqlFunction aggSqlFunction = UserDefinedFunctionUtils.createAggregateSqlFunction(
						aggFunc.functionIdentifier(),
						aggFunc.toString(),
						(AggregateFunction) aggFunc,
						fromLegacyInfoToDataType(aggFunc.getResultType()),
						fromLegacyInfoToDataType(aggFunc.getAccumulatorType()),
						typeFactory);
				List<RexNode> child = convertCallChildren(call.getChildren());
				return relBuilder.call(aggSqlFunction, child);
			} else {
				throw new UnsupportedOperationException("TableAggregateFunction is not supported yet!");
			}

		} else {
			FunctionDefinition def = call.getFunctionDefinition();
			if (conversionsOfBuiltInFunc.containsKey(def)) {
				RexNodeConversion conversion = conversionsOfBuiltInFunc.get(def);
				return conversion.convert(call);
			} else {
				throw new UnsupportedOperationException(def.toString());
			}
		}
	}

	private List<RexNode> convertCallChildren(List<Expression> children) {
		return children.stream()
				.map(expression -> expression.accept(RexNodeConverter.this))
				.collect(Collectors.toList());
	}

	private RexNode convert(SqlOperator sqlOperator, List<Expression> children) {
		List<RexNode> childrenRexNode = convertCallChildren(children);
		return relBuilder.call(sqlOperator, childrenRexNode);
	}

	private RexNode convertArrayElement(List<Expression> children) {
		List<RexNode> childrenRexNode = convertCallChildren(children);
		return relBuilder.call(FlinkSqlOperatorTable.ELEMENT, childrenRexNode);
	}

	private RexNode convertOrderAsc(List<Expression> children) {
		List<RexNode> childrenRexNode = convertCallChildren(children);
		return childrenRexNode.get(0);
	}

	private RexNode convertTimestampDiff(List<Expression> children) {
		List<RexNode> childrenRexNode = convertCallChildren(children);
		return relBuilder.call(FlinkSqlOperatorTable.TIMESTAMP_DIFF, childrenRexNode.get(0), childrenRexNode.get(2),
				childrenRexNode.get(1));
	}

	private RexNode convertIsNull(List<Expression> children) {
		List<RexNode> childrenRexNode = convertCallChildren(children);
		return relBuilder.isNull(childrenRexNode.get(0));
	}

	private RexNode convertNotBetween(List<Expression> children) {
		List<RexNode> childrenRexNode = convertCallChildren(children);
		Preconditions.checkArgument(childrenRexNode.size() == 3);
		RexNode expr = childrenRexNode.get(0);
		RexNode lowerBound = childrenRexNode.get(1);
		RexNode upperBound = childrenRexNode.get(2);
		return relBuilder.or(
				relBuilder.call(FlinkSqlOperatorTable.LESS_THAN, expr, lowerBound),
				relBuilder.call(FlinkSqlOperatorTable.GREATER_THAN, expr, upperBound));
	}

	private RexNode convertBetween(List<Expression> children) {
		List<RexNode> childrenRexNode = convertCallChildren(children);
		Preconditions.checkArgument(childrenRexNode.size() == 3);
		RexNode expr = childrenRexNode.get(0);
		RexNode lowerBound = childrenRexNode.get(1);
		RexNode upperBound = childrenRexNode.get(2);
		return relBuilder.and(
				relBuilder.call(FlinkSqlOperatorTable.GREATER_THAN_OR_EQUAL, expr, lowerBound),
				relBuilder.call(FlinkSqlOperatorTable.LESS_THAN_OR_EQUAL, expr, upperBound));
	}

	private RexNode convertCeil(List<Expression> children) {
		Preconditions.checkArgument(children.size() == 1 || children.size() == 2);
		List<RexNode> childrenRexNode = convertCallChildren(children);
		if (children.size() == 1) {
			return relBuilder.call(FlinkSqlOperatorTable.CEIL, childrenRexNode);
		} else {
			return relBuilder.call(FlinkSqlOperatorTable.CEIL, childrenRexNode.get(1), childrenRexNode.get(0));
		}
	}

	private RexNode convertFloor(List<Expression> children) {
		Preconditions.checkArgument(children.size() == 1 || children.size() == 2);
		List<RexNode> childrenRexNode = convertCallChildren(children);
		if (children.size() == 1) {
			return relBuilder.call(FlinkSqlOperatorTable.FLOOR, childrenRexNode);
		} else {
			return relBuilder.call(FlinkSqlOperatorTable.FLOOR, childrenRexNode.get(1), childrenRexNode.get(0));
		}
	}

	private RexNode convertArray(List<Expression> children) {
		List<RexNode> childrenRexNode = convertCallChildren(children);
		ArrayType arrayType = new ArrayType(toLogicalType(childrenRexNode.get(0).getType()));
		// TODO get type from CallExpression directly
		RelDataType relDataType = typeFactory.createFieldTypeFromLogicalType(arrayType);
		return relBuilder.getRexBuilder().makeCall(relDataType, FlinkSqlOperatorTable.ARRAY_VALUE_CONSTRUCTOR, childrenRexNode);
	}

	private RexNode convertMap(List<Expression> children) {
		Preconditions.checkArgument(!children.isEmpty() && children.size() % 2 == 0);
		// TODO get type from CallExpression directly
		List<RexNode> childrenRexNode = convertCallChildren(children);
		RelDataType keyType = childrenRexNode.get(0).getType();
		RelDataType valueType = childrenRexNode.get(childrenRexNode.size() - 1).getType();
		RelDataType mapType = typeFactory.createMapType(keyType, valueType);
		return relBuilder.getRexBuilder().makeCall(mapType, FlinkSqlOperatorTable.MAP_VALUE_CONSTRUCTOR, childrenRexNode);
	}

	private RexNode convertRow(List<Expression> children) {
		// TODO get type from CallExpression directly
		List<RexNode> childrenRexNode = convertCallChildren(children);
		LogicalType[] childTypes = childrenRexNode.stream().map(rexNode -> toLogicalType(rexNode.getType()))
				.toArray(LogicalType[]::new);
		RowType rowType = RowType.of(childTypes);
		RelDataType relDataType = typeFactory.createFieldTypeFromLogicalType(rowType);
		return relBuilder.getRexBuilder().makeCall(relDataType, FlinkSqlOperatorTable.ROW, childrenRexNode);
	}

	private RexNode convertTemporalOverlaps(List<Expression> children) {
		List<RexNode> childrenRexNode = convertCallChildren(children);
		// Standard conversion of the OVERLAPS operator.
		// Source: [[org.apache.calcite.sql2rel.StandardConvertletTable#convertOverlaps()]]
		RexNode leftTimePoint = childrenRexNode.get(0);
		RexNode leftTemporal = childrenRexNode.get(1);
		RexNode rightTimePoint = childrenRexNode.get(2);
		RexNode rightTemporal = childrenRexNode.get(3);
		RexNode convLeftT;
		if (isTimeInterval(toLogicalType(leftTemporal.getType()))) {
			convLeftT = relBuilder.call(FlinkSqlOperatorTable.DATETIME_PLUS, leftTimePoint, leftTemporal);
		} else {
			convLeftT = leftTemporal;
		}
		// sort end points into start and end, such that (s0 <= e0) and (s1 <= e1).
		RexNode leftLe = relBuilder.call(FlinkSqlOperatorTable.LESS_THAN_OR_EQUAL, leftTimePoint, convLeftT);
		RexNode s0 = relBuilder.call(FlinkSqlOperatorTable.CASE, leftLe, leftTimePoint, convLeftT);
		RexNode e0 = relBuilder.call(FlinkSqlOperatorTable.CASE, leftLe, convLeftT, leftTimePoint);
		RexNode convRightT;
		if (isTimeInterval(toLogicalType(rightTemporal.getType()))) {
			convRightT = relBuilder.call(FlinkSqlOperatorTable.DATETIME_PLUS, rightTimePoint, rightTemporal);
		} else {
			convRightT = rightTemporal;
		}
		RexNode rightLe = relBuilder.call(FlinkSqlOperatorTable.LESS_THAN_OR_EQUAL, rightTimePoint, convRightT);
		RexNode s1 = relBuilder.call(FlinkSqlOperatorTable.CASE, rightLe, rightTimePoint, convRightT);
		RexNode e1 = relBuilder.call(FlinkSqlOperatorTable.CASE, rightLe, convRightT, rightTimePoint);

		// (e0 >= s1) AND (e1 >= s0)
		RexNode leftPred = relBuilder.call(FlinkSqlOperatorTable.GREATER_THAN_OR_EQUAL, e0, s1);
		RexNode rightPred = relBuilder.call(FlinkSqlOperatorTable.GREATER_THAN_OR_EQUAL, e1, s0);
		return relBuilder.call(FlinkSqlOperatorTable.AND, leftPred, rightPred);
	}

	private RexNode convertPlus(List<Expression> children) {
		List<RexNode> childrenRexNode = convertCallChildren(children);
		if (isCharacterString(toLogicalType(childrenRexNode.get(0).getType()))) {
			return relBuilder.call(
					FlinkSqlOperatorTable.CONCAT,
					childrenRexNode.get(0),
					relBuilder.cast(childrenRexNode.get(1), VARCHAR));
		} else if (isCharacterString(toLogicalType(childrenRexNode.get(1).getType()))) {
			return relBuilder.call(
					FlinkSqlOperatorTable.CONCAT,
					relBuilder.cast(childrenRexNode.get(0), VARCHAR),
					childrenRexNode.get(1));
		} else if (isTimeInterval(toLogicalType(childrenRexNode.get(0).getType())) &&
				childrenRexNode.get(0).getType() == childrenRexNode.get(1).getType()) {
			return relBuilder.call(FlinkSqlOperatorTable.PLUS, childrenRexNode);
		} else if (isTimeInterval(toLogicalType(childrenRexNode.get(0).getType()))
				&& isTemporal(toLogicalType(childrenRexNode.get(1).getType()))) {
			// Calcite has a bug that can't apply INTERVAL + DATETIME (INTERVAL at left)
			// we manually switch them here
			return relBuilder.call(FlinkSqlOperatorTable.DATETIME_PLUS, childrenRexNode.get(1), childrenRexNode.get(0));
		} else if (isTemporal(toLogicalType(childrenRexNode.get(0).getType())) &&
				isTemporal(toLogicalType(childrenRexNode.get(1).getType()))) {
			return relBuilder.call(FlinkSqlOperatorTable.DATETIME_PLUS, childrenRexNode);
		} else {
			return relBuilder.call(FlinkSqlOperatorTable.PLUS, childrenRexNode);
		}
	}

	private RexNode convertReplace(List<Expression> children) {
		Preconditions.checkArgument(children.size() == 2 || children.size() == 3);
		List<RexNode> childrenRexNode = convertCallChildren(children);
		if (children.size() == 2) {
			return relBuilder.call(
					FlinkSqlOperatorTable.REPLACE,
					childrenRexNode.get(0),
					childrenRexNode.get(1),
					relBuilder.call(FlinkSqlOperatorTable.CHAR_LENGTH, childrenRexNode.get(0)));
		} else {
			return relBuilder.call(FlinkSqlOperatorTable.REPLACE, childrenRexNode);
		}
	}

	private RexNode convertOver(List<Expression> children) {
		List<Expression> args = children;
		Expression agg = args.get(0);
		SqlAggFunction aggFunc = agg.accept(new SqlAggFunctionVisitor(typeFactory));
		RelDataType aggResultType = typeFactory.createFieldTypeFromLogicalType(
				fromDataTypeToLogicalType(((ResolvedExpression) agg).getOutputDataType()));

		// assemble exprs by agg children
		List<RexNode> aggExprs = agg.getChildren().stream().map(expr -> expr.accept(this))
				.collect(Collectors.toList());

		// assemble order by key
		Expression orderKeyExpr = args.get(1);
		Set<SqlKind> kinds = new HashSet<>();
		RexNode collationRexNode = createCollation(orderKeyExpr.accept(this), RelFieldCollation.Direction.ASCENDING,
				null, kinds);
		ImmutableList<RexFieldCollation> orderKey = ImmutableList
				.of(new RexFieldCollation(collationRexNode, kinds));

		// assemble partition by keys
		List<RexNode> partitionKeys = args.subList(4, args.size()).stream().map(expr -> expr.accept(this))
				.collect(Collectors.toList());
		// assemble bounds
		Expression preceding = args.get(2);
		boolean isPhysical = LogicalTypeChecks.hasRoot(
				fromDataTypeToLogicalType(((ResolvedExpression) preceding).getOutputDataType()),
				LogicalTypeRoot.BIGINT);
		Expression following = args.get(3);
		RexWindowBound lowerBound = createBound(preceding, SqlKind.PRECEDING);
		RexWindowBound upperBound = createBound(following, SqlKind.FOLLOWING);

		// build RexOver
		return relBuilder.getRexBuilder().makeOver(
				aggResultType,
				aggFunc,
				aggExprs,
				partitionKeys,
				orderKey,
				lowerBound,
				upperBound,
				isPhysical,
				true,
				false,
				false);
	}

	private RexNode convertAs(List<Expression> children) {
		String name = extractValue((ValueLiteralExpression) children.get(1), String.class);
		RexNode child = children.get(0).accept(this);
		return relBuilder.alias(child, name);
	}

	private RexNode convertTrim(List<Expression> children) {
		ValueLiteralExpression removeLeadingExpr = (ValueLiteralExpression) children.get(0);
		Boolean removeLeading = extractValue(removeLeadingExpr, Boolean.class);
		ValueLiteralExpression removeTrailingExpr = (ValueLiteralExpression) children.get(1);
		Boolean removeTrailing = extractValue(removeTrailingExpr, Boolean.class);
		RexNode trimString = children.get(2).accept(this);
		RexNode str = children.get(3).accept(this);
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
		return relBuilder.call(
				FlinkSqlOperatorTable.TRIM,
				relBuilder.getRexBuilder().makeFlag(trimMode),
				trimString,
				str);
	}

	private RexNode convertGet(List<Expression> children) {
		RexNode child = children.get(0).accept(this);
		ValueLiteralExpression keyLiteral = (ValueLiteralExpression) children.get(1);
		Optional<Integer> indexOptional = ExpressionUtils.extractValue(keyLiteral, String.class).map(
				child.getType().getFieldNames()::indexOf);
		// Note: never replace the following code with :
		// int index = indexOptional.orElseGet(() -> extractValue(keyLiteral, Integer.class));
		// Because the logical in `orElseGet` always executed no matter whether indexOptional is present or not.
		int index;
		if (indexOptional.isPresent()) {
			index = indexOptional.get();
		} else {
			index = extractValue(keyLiteral, Integer.class);
		}
		return relBuilder.getRexBuilder().makeFieldAccess(child, index);
	}

	private RexNode convertIn(List<Expression> children) {
		Expression headExpr = children.get(1);
		if (headExpr instanceof TableReferenceExpression) {
			QueryOperation tableOperation = ((TableReferenceExpression) headExpr).getQueryOperation();
			RexNode child = children.get(0).accept(this);
			return RexSubQuery.in(
					((FlinkRelBuilder) relBuilder).queryOperation(tableOperation).build(),
					ImmutableList.of(child));
		} else {
			List<RexNode> child = convertCallChildren(children);
			return relBuilder.call(FlinkSqlOperatorTable.IN, child);
		}
	}

	private RexNode convertReinterpretCast(List<Expression> children) {
		RexNode child = children.get(0).accept(this);
		TypeLiteralExpression type = (TypeLiteralExpression) children.get(1);
		RexNode checkOverflow = children.get(2).accept(this);
		return relBuilder.getRexBuilder().makeReinterpretCast(
				typeFactory.createFieldTypeFromLogicalType(
						type.getOutputDataType().getLogicalType().copy(child.getType().isNullable())),
				child,
				checkOverflow);
	}

	private RexNode convertCast(List<Expression> children) {
		RexNode child = children.get(0).accept(this);
		TypeLiteralExpression type = (TypeLiteralExpression) children.get(1);
		return relBuilder.getRexBuilder().makeAbstractCast(
				typeFactory.createFieldTypeFromLogicalType(
						type.getOutputDataType().getLogicalType().copy(child.getType().isNullable())),
				child);
	}

	@Override
	public RexNode visit(ValueLiteralExpression valueLiteral) {
		LogicalType type = fromDataTypeToLogicalType(valueLiteral.getOutputDataType());
		RexBuilder rexBuilder = relBuilder.getRexBuilder();
		FlinkTypeFactory typeFactory = (FlinkTypeFactory) relBuilder.getTypeFactory();
		if (valueLiteral.isNull()) {
			return relBuilder.getRexBuilder()
					.makeCast(
							typeFactory.createFieldTypeFromLogicalType(type),
							relBuilder.getRexBuilder().constantNull());
		}

		switch (type.getTypeRoot()) {
			case DECIMAL:
				DecimalType dt = (DecimalType) type;
				BigDecimal bigDecimal = extractValue(valueLiteral, BigDecimal.class);
				RelDataType decType = relBuilder.getTypeFactory().createSqlType(SqlTypeName.DECIMAL,
						dt.getPrecision(), dt.getScale());
				return relBuilder.getRexBuilder().makeExactLiteral(bigDecimal, decType);
			case BIGINT:
				// create BIGINT literals for long type
				BigDecimal bigint = extractValue(valueLiteral, BigDecimal.class);
				return relBuilder.getRexBuilder().makeBigintLiteral(bigint);
			case FLOAT:
				//Float/Double type should be liked as java type here.
				return relBuilder.getRexBuilder().makeApproxLiteral(
						extractValue(valueLiteral, BigDecimal.class),
						relBuilder.getTypeFactory().createSqlType(SqlTypeName.FLOAT));
			case DOUBLE:
				//Float/Double type should be liked as java type here.
				return rexBuilder.makeApproxLiteral(
						extractValue(valueLiteral, BigDecimal.class),
						relBuilder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE));
			case DATE:
				return relBuilder.getRexBuilder().makeDateLiteral(DateString.fromCalendarFields(
						valueAsCalendar(extractValue(valueLiteral, java.sql.Date.class))));
			case TIME_WITHOUT_TIME_ZONE:
				return relBuilder.getRexBuilder().makeTimeLiteral(TimeString.fromCalendarFields(
						valueAsCalendar(extractValue(valueLiteral, java.sql.Time.class))), 0);
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				return relBuilder.getRexBuilder().makeTimestampLiteral(TimestampString.fromCalendarFields(
						valueAsCalendar(extractValue(valueLiteral, java.sql.Timestamp.class))), 3);
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				TimeZone timeZone = TimeZone.getTimeZone(((FlinkContext) ((FlinkRelBuilder) this.relBuilder)
						.getCluster().getPlanner().getContext()).getTableConfig().getLocalTimeZone());
				return this.relBuilder.getRexBuilder().makeTimestampWithLocalTimeZoneLiteral(
						new TimestampWithTimeZoneString(
								TimestampString.fromMillisSinceEpoch(
										extractValue(valueLiteral, java.time.Instant.class).toEpochMilli()),
								timeZone)
								.withTimeZone(DateTimeUtils.UTC_ZONE)
								.getLocalTimestampString(), 3);
			case INTERVAL_YEAR_MONTH:
				return this.relBuilder.getRexBuilder().makeIntervalLiteral(
						BigDecimal.valueOf(extractValue(valueLiteral, Integer.class)),
						new SqlIntervalQualifier(TimeUnit.YEAR, TimeUnit.MONTH, SqlParserPos.ZERO));
			case INTERVAL_DAY_TIME:
				return this.relBuilder.getRexBuilder().makeIntervalLiteral(
						BigDecimal.valueOf(extractValue(valueLiteral, Long.class)),
						new SqlIntervalQualifier(TimeUnit.DAY, TimeUnit.SECOND, SqlParserPos.ZERO));
			default:
				break;
		}
		Object object = extractValue(valueLiteral, Object.class);
		if (object instanceof TimePointUnit) {
			TimeUnit value;
			switch ((TimePointUnit) object) {
				case YEAR:
					value = TimeUnit.YEAR;
					break;
				case MONTH:
					value = TimeUnit.MONTH;
					break;
				case DAY:
					value = TimeUnit.DAY;
					break;
				case HOUR:
					value = TimeUnit.HOUR;
					break;
				case MINUTE:
					value = TimeUnit.MINUTE;
					break;
				case SECOND:
					value = TimeUnit.SECOND;
					break;
				case QUARTER:
					value = TimeUnit.QUARTER;
					break;
				case WEEK:
					value = TimeUnit.WEEK;
					break;
				case MILLISECOND:
					value = TimeUnit.MILLISECOND;
					break;
				case MICROSECOND:
					value = TimeUnit.MICROSECOND;
					break;
				default:
					throw new UnsupportedOperationException();
			}
			return relBuilder.getRexBuilder().makeFlag(value);
		} else if (object instanceof TimeIntervalUnit) {
			TimeUnitRange value;
			switch ((TimeIntervalUnit) object) {
				case YEAR:
					value = TimeUnitRange.YEAR;
					break;
				case YEAR_TO_MONTH:
					value = TimeUnitRange.YEAR_TO_MONTH;
					break;
				case QUARTER:
					value = TimeUnitRange.QUARTER;
					break;
				case MONTH:
					value = TimeUnitRange.MONTH;
					break;
				case WEEK:
					value = TimeUnitRange.WEEK;
					break;
				case DAY:
					value = TimeUnitRange.DAY;
					break;
				case DAY_TO_HOUR:
					value = TimeUnitRange.DAY_TO_HOUR;
					break;
				case DAY_TO_MINUTE:
					value = TimeUnitRange.DAY_TO_MINUTE;
					break;
				case DAY_TO_SECOND:
					value = TimeUnitRange.DAY_TO_SECOND;
					break;
				case HOUR:
					value = TimeUnitRange.HOUR;
					break;
				case SECOND:
					value = TimeUnitRange.SECOND;
					break;
				case HOUR_TO_MINUTE:
					value = TimeUnitRange.HOUR_TO_MINUTE;
					break;
				case HOUR_TO_SECOND:
					value = TimeUnitRange.HOUR_TO_SECOND;
					break;
				case MINUTE:
					value = TimeUnitRange.MINUTE;
					break;
				case MINUTE_TO_SECOND:
					value = TimeUnitRange.MINUTE_TO_SECOND;
					break;
				default:
					throw new UnsupportedOperationException();
			}
			return relBuilder.getRexBuilder().makeFlag(value);
		} else {
			return relBuilder.literal(extractValue(valueLiteral, Object.class));
		}
	}

	/**
	 * Extracts a value from a literal. Including planner-specific instances such as {@link Decimal}.
	 */
	@SuppressWarnings("unchecked")
	private static <T> T extractValue(ValueLiteralExpression literal, Class<T> clazz) {
		final Optional<Object> possibleObject = literal.getValueAs(Object.class);
		if (!possibleObject.isPresent()) {
			throw new TableException("Invalid literal.");
		}
		final Object object = possibleObject.get();

		if (clazz.equals(BigDecimal.class)) {
			final Optional<BigDecimal> possibleDecimal = literal.getValueAs(BigDecimal.class);
			if (possibleDecimal.isPresent()) {
				return (T) possibleDecimal.get();
			}
			if (object instanceof Decimal) {
				return (T) ((Decimal) object).toBigDecimal();
			}
		}

		return literal.getValueAs(clazz)
				.orElseThrow(() -> new TableException("Unsupported literal class: " + clazz));
	}

	/**
	 * Convert a Date value to a Calendar. Calcite's fromCalendarField functions use the
	 * Calendar.get methods, so the raw values of the individual fields are preserved when
	 * converted to the String formats.
	 *
	 * @return get the Calendar value
	 */
	private static Calendar valueAsCalendar(Object value) {
		Date date = (Date) value;
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		return cal;
	}

	@Override
	public RexNode visit(FieldReferenceExpression fieldReference) {
		return relBuilder.field(fieldReference.getName());
	}

	@Override
	public RexNode visit(TypeLiteralExpression typeLiteral) {
		throw new UnsupportedOperationException();
	}

	@Override
	public RexNode visit(Expression other) {
		if (other instanceof UnresolvedReferenceExpression) {
			return visitUnresolvedReferenceExpression((UnresolvedReferenceExpression) other);
		} else if (other instanceof ResolvedAggInputReference) {
			return visitResolvedAggInputReference((ResolvedAggInputReference) other);
		} else if (other instanceof ResolvedAggLocalReference) {
			return visitResolvedAggLocalReference((ResolvedAggLocalReference) other);
		} else if (other instanceof ResolvedDistinctKeyReference) {
			return visitResolvedDistinctKeyReference((ResolvedDistinctKeyReference) other);
		} else if (other instanceof UnresolvedCallExpression) {
			return visit((UnresolvedCallExpression) other);
		} else if (other instanceof RexNodeExpression) {
			return ((RexNodeExpression) other).getRexNode();
		} else {
			throw new UnsupportedOperationException(other.getClass().getSimpleName() + ":" + other.toString());
		}
	}

	private RexNode visitUnresolvedReferenceExpression(UnresolvedReferenceExpression field) {
		return relBuilder.field(field.getName());
	}

	private RexNode visitResolvedAggInputReference(ResolvedAggInputReference reference) {
		// using index to resolve field directly, name used in toString only
		return new RexInputRef(
				reference.getIndex(),
				typeFactory.createFieldTypeFromLogicalType(reference.getResultType()));
	}

	private RexNode visitResolvedAggLocalReference(ResolvedAggLocalReference reference) {
		LogicalType type = reference.getResultType();
		return new RexAggLocalVariable(
				reference.getFieldTerm(),
				reference.getNullTerm(),
				typeFactory.createFieldTypeFromLogicalType(type),
				type);
	}

	private RexNode visitResolvedDistinctKeyReference(ResolvedDistinctKeyReference reference) {
		LogicalType type = reference.getResultType();
		return new RexDistinctKeyVariable(
				reference.getName(),
				typeFactory.createFieldTypeFromLogicalType(type),
				type);
	}

	private RexNode visit(UnresolvedCallExpression call) {
		FunctionDefinition func = call.getFunctionDefinition();
		switch (func.getKind()) {
			case SCALAR:
				if (func instanceof ScalarFunctionDefinition) {
					ScalarFunction scalaFunc = ((ScalarFunctionDefinition) func).getScalarFunction();
					List<RexNode> child = convertCallChildren(call.getChildren());
					SqlFunction sqlFunction = UserDefinedFunctionUtils.createScalarSqlFunction(
							scalaFunc.functionIdentifier(),
							scalaFunc.toString(),
							scalaFunc,
							typeFactory);
					return relBuilder.call(sqlFunction, child);
				} else {
					FunctionDefinition def = call.getFunctionDefinition();
					if (conversionsOfBuiltInFunc.containsKey(def)) {
						RexNodeConversion conversion = conversionsOfBuiltInFunc.get(def);
						return conversion.convert(call);
					} else {
						throw new UnsupportedOperationException(def.toString());
					}
				}

			default:
				throw new UnsupportedOperationException();
		}
	}

	private RexNode createCollation(RexNode node, RelFieldCollation.Direction direction,
			RelFieldCollation.NullDirection nullDirection, Set<SqlKind> kinds) {
		switch (node.getKind()) {
			case DESCENDING:
				kinds.add(node.getKind());
				return createCollation(((RexCall) node).getOperands().get(0), RelFieldCollation.Direction.DESCENDING,
						nullDirection, kinds);
			case NULLS_FIRST:
				kinds.add(node.getKind());
				return createCollation(((RexCall) node).getOperands().get(0), direction,
						RelFieldCollation.NullDirection.FIRST, kinds);
			case NULLS_LAST:
				kinds.add(node.getKind());
				return createCollation(((RexCall) node).getOperands().get(0), direction,
						RelFieldCollation.NullDirection.LAST, kinds);
			default:
				if (nullDirection == null) {
					// Set the null direction if not specified.
					// Consistent with HIVE/SPARK/MYSQL/BLINK-RUNTIME.
					if (FlinkPlannerImpl.defaultNullCollation()
							.last(direction.equals(RelFieldCollation.Direction.DESCENDING))) {
						kinds.add(SqlKind.NULLS_LAST);
					} else {
						kinds.add(SqlKind.NULLS_FIRST);
					}
				}
				return node;
		}
	}

	private RexWindowBound createBound(Expression bound, SqlKind sqlKind) {
		if (bound instanceof CallExpression) {
			CallExpression callExpr = (CallExpression) bound;
			FunctionDefinition func = callExpr.getFunctionDefinition();
			if (BuiltInFunctionDefinitions.UNBOUNDED_ROW.equals(func) || BuiltInFunctionDefinitions.UNBOUNDED_RANGE
					.equals(func)) {
				SqlNode unbounded = sqlKind.equals(SqlKind.PRECEDING) ? SqlWindow
						.createUnboundedPreceding(SqlParserPos.ZERO) :
						SqlWindow.createUnboundedFollowing(SqlParserPos.ZERO);
				return RexWindowBound.create(unbounded, null);
			} else if (BuiltInFunctionDefinitions.CURRENT_ROW.equals(func) || BuiltInFunctionDefinitions.CURRENT_RANGE
					.equals(func)) {
				SqlNode currentRow = SqlWindow.createCurrentRow(SqlParserPos.ZERO);
				return RexWindowBound.create(currentRow, null);
			} else {
				throw new IllegalArgumentException("Unexpected expression: " + bound);
			}
		} else if (bound instanceof ValueLiteralExpression) {
			RelDataType returnType = typeFactory
					.createFieldTypeFromLogicalType(new DecimalType(true, 19, 0));
			SqlOperator sqlOperator = new SqlPostfixOperator(
					sqlKind.name(),
					sqlKind,
					2,
					new OrdinalReturnTypeInference(0),
					null,
					null);
			SqlNode[] operands = new SqlNode[] { SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO) };
			SqlNode node = new SqlBasicCall(sqlOperator, operands, SqlParserPos.ZERO);

			ValueLiteralExpression literalExpr = (ValueLiteralExpression) bound;
			RexNode literalRexNode = literalExpr.getValueAs(Double.class).map(
					v -> relBuilder.literal(BigDecimal.valueOf((Double) v))).orElse(
					relBuilder.literal(extractValue(literalExpr, Object.class)));

			List<RexNode> expressions = new ArrayList<>();
			expressions.add(literalRexNode);
			RexNode rexNode = relBuilder.getRexBuilder().makeCall(returnType, sqlOperator, expressions);
			return RexWindowBound.create(node, rexNode);
		} else {
			throw new TableException("Unexpected expression: " + bound);
		}
	}

	/**
	 * RexNodeConversion to define how to convert a {@link CallExpression} or a {@link UnresolvedCallExpression} which
	 * has built-in FunctionDefinition to RexNode.
	 */
	private interface RexNodeConversion {

		RexNode convert(List<Expression> children);

		default RexNode convert(CallExpression expression) {
			return convert(expression.getChildren());
		}

		default RexNode convert(UnresolvedCallExpression unresolvedCallExpression) {
			return convert(unresolvedCallExpression.getChildren());
		}
	}

}
