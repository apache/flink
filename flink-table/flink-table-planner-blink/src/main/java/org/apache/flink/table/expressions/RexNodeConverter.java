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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.calcite.FlinkPlannerImpl;
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.calcite.RexAggLocalVariable;
import org.apache.flink.table.calcite.RexDistinctKeyVariable;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.UserDefinedAggregateFunction;
import org.apache.flink.table.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.util.Preconditions;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
import static org.apache.flink.table.calcite.FlinkTypeFactory.toLogicalType;
import static org.apache.flink.table.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType;
import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;
import static org.apache.flink.table.typeutils.TypeCheckUtils.isTemporal;
import static org.apache.flink.table.typeutils.TypeCheckUtils.isTimeInterval;
import static org.apache.flink.table.typeutils.TypeCheckUtils.isVarchar;

/**
 * Visit expression to generator {@link RexNode}.
 */
public class RexNodeConverter implements ExpressionVisitor<RexNode> {

	private final RelBuilder relBuilder;
	private final FlinkTypeFactory typeFactory;

	private static final int DECIMAL_PRECISION_NEEDED_FOR_LONG = 19;

	/**
	 * The mapping only keeps part of FunctionDefinitions, which could be converted to SqlOperator in a very simple
	 * way.
	 */
	private static final Map<FunctionDefinition, SqlOperator> SIMPLE_DEF_SQL_OPERATOR_MAPPING = new HashMap<>();

	static {
		// logic functions
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.AND, FlinkSqlOperatorTable.AND);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.OR, FlinkSqlOperatorTable.OR);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.NOT, FlinkSqlOperatorTable.NOT);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.IF, FlinkSqlOperatorTable.CASE);

		// comparison functions
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.EQUALS, FlinkSqlOperatorTable.EQUALS);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING
				.put(BuiltInFunctionDefinitions.GREATER_THAN, FlinkSqlOperatorTable.GREATER_THAN);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING
				.put(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, FlinkSqlOperatorTable.GREATER_THAN_OR_EQUAL);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.LESS_THAN, FlinkSqlOperatorTable.LESS_THAN);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING
				.put(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL, FlinkSqlOperatorTable.LESS_THAN_OR_EQUAL);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.NOT_EQUALS, FlinkSqlOperatorTable.NOT_EQUALS);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.IS_NULL, FlinkSqlOperatorTable.IS_NULL);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.IS_NOT_NULL, FlinkSqlOperatorTable.IS_NOT_NULL);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.IS_TRUE, FlinkSqlOperatorTable.IS_TRUE);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.IS_FALSE, FlinkSqlOperatorTable.IS_FALSE);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.IS_NOT_TRUE, FlinkSqlOperatorTable.IS_NOT_TRUE);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING
				.put(BuiltInFunctionDefinitions.IS_NOT_FALSE, FlinkSqlOperatorTable.IS_NOT_FALSE);

		// string functions
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.CHAR_LENGTH, FlinkSqlOperatorTable.CHAR_LENGTH);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.INIT_CAP, FlinkSqlOperatorTable.INITCAP);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.LIKE, FlinkSqlOperatorTable.LIKE);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.LOWER, FlinkSqlOperatorTable.LOWER);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.SIMILAR, FlinkSqlOperatorTable.SIMILAR_TO);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.SUBSTRING, FlinkSqlOperatorTable.SUBSTRING);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.UPPER, FlinkSqlOperatorTable.UPPER);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.POSITION, FlinkSqlOperatorTable.POSITION);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.OVERLAY, FlinkSqlOperatorTable.OVERLAY);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.CONCAT, FlinkSqlOperatorTable.CONCAT);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.CONCAT_WS, FlinkSqlOperatorTable.CONCAT_WS);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.LPAD, FlinkSqlOperatorTable.LPAD);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.RPAD, FlinkSqlOperatorTable.RPAD);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING
				.put(BuiltInFunctionDefinitions.REGEXP_EXTRACT, FlinkSqlOperatorTable.REGEXP_EXTRACT);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.FROM_BASE64, FlinkSqlOperatorTable.FROM_BASE64);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.TO_BASE64, FlinkSqlOperatorTable.TO_BASE64);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.UUID, FlinkSqlOperatorTable.UUID);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.LTRIM, FlinkSqlOperatorTable.LTRIM);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.RTRIM, FlinkSqlOperatorTable.RTRIM);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.REPEAT, FlinkSqlOperatorTable.REPEAT);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.REGEXP_REPLACE, FlinkSqlOperatorTable.REGEXP_REPLACE);

		// math functions
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.MINUS, FlinkSqlOperatorTable.MINUS);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.DIVIDE, FlinkSqlOperatorTable.DIVIDE);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.TIMES, FlinkSqlOperatorTable.MULTIPLY);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.ABS, FlinkSqlOperatorTable.ABS);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.EXP, FlinkSqlOperatorTable.EXP);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.LOG10, FlinkSqlOperatorTable.LOG10);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.LOG2, FlinkSqlOperatorTable.LOG2);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.LN, FlinkSqlOperatorTable.LN);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.LOG, FlinkSqlOperatorTable.LOG);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.POWER, FlinkSqlOperatorTable.POWER);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.MOD, FlinkSqlOperatorTable.MOD);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.SQRT, FlinkSqlOperatorTable.SQRT);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.MINUS_PREFIX, FlinkSqlOperatorTable.UNARY_MINUS);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.SIN, FlinkSqlOperatorTable.SIN);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.COS, FlinkSqlOperatorTable.COS);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.SINH, FlinkSqlOperatorTable.SINH);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.TAN, FlinkSqlOperatorTable.TAN);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.TANH, FlinkSqlOperatorTable.TANH);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.COT, FlinkSqlOperatorTable.COT);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.ASIN, FlinkSqlOperatorTable.ASIN);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.ACOS, FlinkSqlOperatorTable.ACOS);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.ATAN, FlinkSqlOperatorTable.ATAN);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.ATAN2, FlinkSqlOperatorTable.ATAN2);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.COSH, FlinkSqlOperatorTable.COSH);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.DEGREES, FlinkSqlOperatorTable.DEGREES);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.RADIANS, FlinkSqlOperatorTable.RADIANS);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.SIGN, FlinkSqlOperatorTable.SIGN);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.ROUND, FlinkSqlOperatorTable.ROUND);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.PI, FlinkSqlOperatorTable.PI);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.E, FlinkSqlOperatorTable.E);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.RAND, FlinkSqlOperatorTable.RAND);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.RAND_INTEGER, FlinkSqlOperatorTable.RAND_INTEGER);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.BIN, FlinkSqlOperatorTable.BIN);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.HEX, FlinkSqlOperatorTable.HEX);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.TRUNCATE, FlinkSqlOperatorTable.TRUNCATE);

		// time functions
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.EXTRACT, FlinkSqlOperatorTable.EXTRACT);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING
				.put(BuiltInFunctionDefinitions.CURRENT_DATE, FlinkSqlOperatorTable.CURRENT_DATE);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING
				.put(BuiltInFunctionDefinitions.CURRENT_TIME, FlinkSqlOperatorTable.CURRENT_TIME);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING
				.put(BuiltInFunctionDefinitions.CURRENT_TIMESTAMP, FlinkSqlOperatorTable.CURRENT_TIMESTAMP);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.LOCAL_TIME, FlinkSqlOperatorTable.LOCALTIME);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING
				.put(BuiltInFunctionDefinitions.LOCAL_TIMESTAMP, FlinkSqlOperatorTable.LOCALTIMESTAMP);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.DATE_FORMAT, FlinkSqlOperatorTable.DATE_FORMAT);

		// collection
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.AT, FlinkSqlOperatorTable.ITEM);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.CARDINALITY, FlinkSqlOperatorTable.CARDINALITY);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.ORDER_DESC, FlinkSqlOperatorTable.DESC);

		// crypto hash
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.MD5, FlinkSqlOperatorTable.MD5);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.SHA2, FlinkSqlOperatorTable.SHA2);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.SHA224, FlinkSqlOperatorTable.SHA224);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.SHA256, FlinkSqlOperatorTable.SHA256);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.SHA384, FlinkSqlOperatorTable.SHA384);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.SHA512, FlinkSqlOperatorTable.SHA512);
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.SHA1, FlinkSqlOperatorTable.SHA1);

		// etc
		SIMPLE_DEF_SQL_OPERATOR_MAPPING.put(InternalFunctionDefinitions.THROW_EXCEPTION, FlinkSqlOperatorTable.THROW_EXCEPTION);

	}

	public RexNodeConverter(RelBuilder relBuilder) {
		this.relBuilder = relBuilder;
		this.typeFactory = (FlinkTypeFactory) relBuilder.getRexBuilder().getTypeFactory();
	}

	@Override
	public RexNode visitCall(CallExpression call) {
		FunctionDefinition func = call.getFunctionDefinition();
		if (func instanceof ScalarFunctionDefinition) {
			ScalarFunction scalaFunc = ((ScalarFunctionDefinition) func).getScalarFunction();
			List<RexNode> child = convertCallChildren(call);
			SqlFunction sqlFunction = UserDefinedFunctionUtils.createScalarSqlFunction(
					scalaFunc.functionIdentifier(),
					scalaFunc.toString(),
					scalaFunc,
					typeFactory);
			return relBuilder.call(sqlFunction, child);
		} else if (func instanceof TableFunctionDefinition) {
			throw new UnsupportedOperationException(func.getName());
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
				List<RexNode> child = convertCallChildren(call);
				return relBuilder.call(aggSqlFunction, child);
			} else {
				throw new UnsupportedOperationException(func.getName());
			}

		} else {
			return visitBuiltInFunc(call);
		}
	}

	private List<RexNode> convertCallChildren(CallExpression call) {
		return call.getChildren().stream()
				.map(expression -> expression.accept(RexNodeConverter.this))
				.collect(Collectors.toList());
	}

	private RexNode visitBuiltInFunc(CallExpression call) {
		FunctionDefinition def = call.getFunctionDefinition();

		if (BuiltInFunctionDefinitions.CAST.equals(def)) {
			RexNode child = call.getChildren().get(0).accept(this);
			TypeLiteralExpression type = (TypeLiteralExpression) call.getChildren().get(1);
			return relBuilder.getRexBuilder().makeAbstractCast(
					typeFactory.createFieldTypeFromLogicalType(
							fromDataTypeToLogicalType(type.getOutputDataType()).copy(child.getType().isNullable())),
					child);
		} else if (BuiltInFunctionDefinitions.REINTERPRET_CAST.equals(def)) {
			RexNode child = call.getChildren().get(0).accept(this);
			TypeLiteralExpression type = (TypeLiteralExpression) call.getChildren().get(1);
			RexNode checkOverflow = call.getChildren().get(2).accept(this);
			return relBuilder.getRexBuilder().makeReinterpretCast(
					typeFactory.createFieldTypeFromLogicalType(
							fromDataTypeToLogicalType(type.getOutputDataType()).copy(child.getType().isNullable())),
					child,
					checkOverflow);
		} else if (BuiltInFunctionDefinitions.IN.equals(def)) {
			Expression headExpr = call.getChildren().get(0);
			if (headExpr instanceof TableReferenceExpression) {
				// TODO convert TableReferenceExpression to RelNode
				throw new UnsupportedOperationException(def.getName());
			} else {
				List<RexNode> child = convertCallChildren(call);
				return relBuilder.call(FlinkSqlOperatorTable.IN, child);
			}
		} else if (BuiltInFunctionDefinitions.GET.equals(def)) {
			RexNode child = call.getChildren().get(0).accept(this);
			ValueLiteralExpression keyLiteral = (ValueLiteralExpression) call.getChildren().get(1);
			int index = ExpressionUtils.extractValue(keyLiteral, String.class)
					.map(child.getType().getFieldNames()::indexOf)
					.orElseGet(() -> extractValue(keyLiteral, Integer.class));
			return relBuilder.getRexBuilder().makeFieldAccess(child, index);
		} else if (BuiltInFunctionDefinitions.TRIM.equals(def)) {
			ValueLiteralExpression removeLeadingExpr = (ValueLiteralExpression) call.getChildren().get(0);
			Boolean removeLeading = extractValue(removeLeadingExpr, Boolean.class);
			ValueLiteralExpression removeTrailingExpr = (ValueLiteralExpression) call.getChildren().get(1);
			Boolean removeTrailing = extractValue(removeTrailingExpr, Boolean.class);
			RexNode trimString = call.getChildren().get(2).accept(this);
			RexNode str = call.getChildren().get(3).accept(this);
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
		} else if (BuiltInFunctionDefinitions.AS.equals(def)) {
			String name = extractValue((ValueLiteralExpression) call.getChildren().get(1), String.class);
			RexNode child = call.getChildren().get(0).accept(this);
			return relBuilder.alias(child, name);
		}
		else if (BuiltInFunctionDefinitions.OVER.equals(def)) {
			List<Expression> args = call.getChildren();
			Expression agg = args.get(0);
			SqlAggFunction aggFunc = agg.accept(new AggregateVisitors.AggFunctionVisitor(typeFactory));
			RelDataType aggResultType = agg.accept(new AggregateVisitors.AggFunctionReturnTypeVisitor(typeFactory, this));

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
			boolean isPhysical;
			RexWindowBound lowerBound;
			RexWindowBound upperBound;
			if (FlinkSqlOperatorTable.ROW_NUMBER.equals(aggFunc) || FlinkSqlOperatorTable.RANK.equals(aggFunc) ||
					FlinkSqlOperatorTable.DENSE_RANK.equals(aggFunc)) {
				isPhysical = true;
				lowerBound = RexWindowBound.create(SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO), null);
				upperBound = RexWindowBound.create(SqlWindow.createCurrentRow(SqlParserPos.ZERO), null);
			} else {
				Expression preceding = args.get(2);
				if (preceding instanceof CallExpression) {
					FunctionDefinition precedingDef =  ((CallExpression) preceding).getFunctionDefinition();
					isPhysical = BuiltInFunctionDefinitions.CURRENT_ROW.equals(precedingDef) ||
							BuiltInFunctionDefinitions.UNBOUNDED_ROW.equals(precedingDef);
				} else {
					isPhysical = ExpressionUtils.extractValue(preceding, Long.class).isPresent();
				}
				Expression following = args.get(3);
				lowerBound = createBound(preceding, SqlKind.PRECEDING);
				upperBound = createBound(following, SqlKind.FOLLOWING);
			}
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

		List<RexNode> child = convertCallChildren(call);
		if (BuiltInFunctionDefinitions.IF.equals(def)) {
			return relBuilder.call(FlinkSqlOperatorTable.CASE, child);
		} else if (BuiltInFunctionDefinitions.IS_NULL.equals(def)) {
			return relBuilder.isNull(child.get(0));
		} else if (SIMPLE_DEF_SQL_OPERATOR_MAPPING.containsKey(def)) {
			return relBuilder.call(SIMPLE_DEF_SQL_OPERATOR_MAPPING.get(def), child);
		}
		if (BuiltInFunctionDefinitions.BETWEEN.equals(def)) {
			return relBuilder.and(
					relBuilder.call(FlinkSqlOperatorTable.GREATER_THAN_OR_EQUAL, child),
					relBuilder.call(FlinkSqlOperatorTable.LESS_THAN_OR_EQUAL, child));
		} else if (BuiltInFunctionDefinitions.NOT_BETWEEN.equals(def)) {
			return relBuilder.or(
					relBuilder.call(FlinkSqlOperatorTable.LESS_THAN, child),
					relBuilder.call(FlinkSqlOperatorTable.GREATER_THAN, child));
		} else if (BuiltInFunctionDefinitions.REPLACE.equals(def)) {
			Preconditions.checkArgument(child.size() == 2 || child.size() == 3);
			if (child.size() == 2) {
				return relBuilder.call(
						FlinkSqlOperatorTable.REPLACE,
						child.get(0),
						child.get(1),
						relBuilder.call(FlinkSqlOperatorTable.CHAR_LENGTH, child.get(0)));
			} else {
				return relBuilder.call(FlinkSqlOperatorTable.REPLACE, child);
			}
		} else if (BuiltInFunctionDefinitions.PLUS.equals(def)) {
			if (isVarchar(toLogicalType(child.get(0).getType()))) {
				return relBuilder.call(
						FlinkSqlOperatorTable.CONCAT,
						child.get(0),
						relBuilder.cast(child.get(1), VARCHAR));
			} else if (isVarchar(toLogicalType(child.get(1).getType()))) {
				return relBuilder.call(
						FlinkSqlOperatorTable.CONCAT,
						relBuilder.cast(child.get(0), VARCHAR),
						child.get(1));
			} else if (isTimeInterval(toLogicalType(child.get(0).getType())) &&
					child.get(0).getType() == child.get(1).getType()) {
				return relBuilder.call(FlinkSqlOperatorTable.PLUS, child);
			} else if (isTimeInterval(toLogicalType(child.get(0).getType()))
					&& isTemporal(toLogicalType(child.get(1).getType()))) {
				// Calcite has a bug that can't apply INTERVAL + DATETIME (INTERVAL at left)
				// we manually switch them here
				return relBuilder.call(FlinkSqlOperatorTable.DATETIME_PLUS, child);
			} else if (isTemporal(toLogicalType(child.get(0).getType())) &&
					isTemporal(toLogicalType(child.get(1).getType()))) {
				return relBuilder.call(FlinkSqlOperatorTable.DATETIME_PLUS, child);
			} else {
				return relBuilder.call(FlinkSqlOperatorTable.PLUS, child);
			}
		} else if (BuiltInFunctionDefinitions.CEIL.equals(def)) {
			Preconditions.checkArgument(child.size() == 1 || child.size() == 2);
			if (child.size() == 1) {
				return relBuilder.call(FlinkSqlOperatorTable.CEIL, child);
			} else {
				return relBuilder.call(FlinkSqlOperatorTable.CEIL, child.get(1), child.get(0));
			}
		} else if (BuiltInFunctionDefinitions.FLOOR.equals(def)) {
			Preconditions.checkArgument(child.size() == 1 || child.size() == 2);
			if (child.size() == 1) {
				return relBuilder.call(FlinkSqlOperatorTable.FLOOR, child);
			} else {
				return relBuilder.call(FlinkSqlOperatorTable.FLOOR, child.get(1), child.get(0));
			}
		} else if (BuiltInFunctionDefinitions.TEMPORAL_OVERLAPS.equals(def)) {
			// Standard conversion of the OVERLAPS operator.
			// Source: [[org.apache.calcite.sql2rel.StandardConvertletTable#convertOverlaps()]]
			RexNode leftTimePoint = child.get(0);
			RexNode leftTemporal = child.get(1);
			RexNode rightTimePoint = child.get(2);
			RexNode rightTemporal = child.get(3);
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
		} else if (BuiltInFunctionDefinitions.TIMESTAMP_DIFF.equals(def)) {
			return relBuilder.call(FlinkSqlOperatorTable.TIMESTAMP_DIFF, child.get(0), child.get(2), child.get(1));
		} else if (BuiltInFunctionDefinitions.ARRAY.equals(def)) {
			ArrayType arrayType = new ArrayType(toLogicalType(child.get(0).getType()));
			RelDataType relDataType = typeFactory.createFieldTypeFromLogicalType(arrayType);
			return relBuilder.getRexBuilder()
					.makeCall(relDataType, FlinkSqlOperatorTable.ARRAY_VALUE_CONSTRUCTOR, child);
		} else if (BuiltInFunctionDefinitions.ARRAY_ELEMENT.equals(def)) {
			return relBuilder.call(FlinkSqlOperatorTable.ELEMENT, child);
		} else if (BuiltInFunctionDefinitions.MAP.equals(def)) {
			Preconditions.checkArgument(!child.isEmpty() && child.size() % 2 == 0);
			RelDataType keyType = child.get(0).getType();
			RelDataType valueType = child.get(child.size() - 1).getType();
			RelDataType mapType = typeFactory.createMapType(keyType, valueType);
			return relBuilder.getRexBuilder().makeCall(mapType, FlinkSqlOperatorTable.MAP_VALUE_CONSTRUCTOR, child);
		} else if (BuiltInFunctionDefinitions.ROW.equals(def)) {
			LogicalType[] childTypes = child.stream().map(rexNode -> toLogicalType(rexNode.getType()))
					.toArray(LogicalType[]::new);
			RowType rowType = RowType.of(childTypes);
			RelDataType relDataType = typeFactory.createFieldTypeFromLogicalType(rowType);
			return relBuilder.getRexBuilder().makeCall(relDataType, FlinkSqlOperatorTable.ROW, child);
		} else if (BuiltInFunctionDefinitions.ORDER_ASC.equals(def)) {
			return child.get(0);
		} else {
			throw new UnsupportedOperationException(def.getName());
		}
	}

	@Override
	public RexNode visitValueLiteral(ValueLiteralExpression expr) {
		LogicalType type = fromDataTypeToLogicalType(expr.getOutputDataType());
		RexBuilder rexBuilder = relBuilder.getRexBuilder();
		FlinkTypeFactory typeFactory = (FlinkTypeFactory) relBuilder.getTypeFactory();
		if (expr.isNull()) {
			return relBuilder.getRexBuilder()
					.makeCast(
							typeFactory.createFieldTypeFromLogicalType(type),
							relBuilder.getRexBuilder().constantNull());
		}

		if (type instanceof DecimalType) {
			DecimalType dt = (DecimalType) type;
			BigDecimal bigDecimal = extractValue(expr, BigDecimal.class);
			RelDataType decType = relBuilder.getTypeFactory().createSqlType(SqlTypeName.DECIMAL,
					dt.getPrecision(), dt.getScale());
			return relBuilder.getRexBuilder().makeExactLiteral(bigDecimal, decType);
		} else if (type instanceof BigIntType) {
			// create BIGINT literals for long type
			BigDecimal bigint = extractValue(expr, BigDecimal.class);
			return relBuilder.getRexBuilder().makeBigintLiteral(bigint);
		} else if (type instanceof FloatType) {
			//Float/Double type should be liked as java type here.
			return relBuilder.getRexBuilder().makeApproxLiteral(
					extractValue(expr, BigDecimal.class),
					relBuilder.getTypeFactory().createSqlType(SqlTypeName.FLOAT));
		} else if (type instanceof DoubleType) {
			//Float/Double type should be liked as java type here.
			return rexBuilder.makeApproxLiteral(
					extractValue(expr, BigDecimal.class),
					relBuilder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE));
		} else if (type instanceof DateType) {
			return relBuilder.getRexBuilder().makeDateLiteral(
					DateString.fromCalendarFields(valueAsCalendar(extractValue(expr, java.sql.Date.class))));
		} else if (type instanceof TimeType) {
			return relBuilder.getRexBuilder().makeTimeLiteral(
					TimeString.fromCalendarFields(valueAsCalendar(extractValue(expr, java.sql.Time.class))), 0);
		} else if (type instanceof TimestampType) {
			return relBuilder.getRexBuilder().makeTimestampLiteral(
					TimestampString.fromCalendarFields(valueAsCalendar(extractValue(expr, java.sql.Timestamp.class))), 3);
		} else if (type instanceof YearMonthIntervalType) {
			BigDecimal interval = BigDecimal.valueOf(extractValue(expr, Integer.class));
			SqlIntervalQualifier intervalQualifier = new SqlIntervalQualifier(
					TimeUnit.YEAR, TimeUnit.MONTH, SqlParserPos.ZERO);
			return relBuilder.getRexBuilder().makeIntervalLiteral(interval, intervalQualifier);
		} else if (type instanceof DayTimeIntervalType) {
			BigDecimal interval = BigDecimal.valueOf(extractValue(expr, Long.class));
			SqlIntervalQualifier intervalQualifier = new SqlIntervalQualifier(
					TimeUnit.DAY, TimeUnit.SECOND, SqlParserPos.ZERO);
			return relBuilder.getRexBuilder().makeIntervalLiteral(interval, intervalQualifier);
		} else {
			Object object = extractValue(expr, Object.class);
			if (object instanceof TimePointUnit) {
				throw new UnsupportedOperationException();
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
				return relBuilder.literal(extractValue(expr, Object.class));
			}
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
	public RexNode visitFieldReference(FieldReferenceExpression fieldReference) {
		return relBuilder.field(fieldReference.getName());
	}

	@Override
	public RexNode visitTypeLiteral(TypeLiteralExpression typeLiteral) {
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
		} else if (other instanceof RexPlannerExpression) {
			return ((RexPlannerExpression) other).getRexNode();
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
			RelDataType returnType = typeFactory.createFieldTypeFromLogicalType(new DecimalType(true, DECIMAL_PRECISION_NEEDED_FOR_LONG, 0));
			SqlOperator sqlOperator = new SqlPostfixOperator(
					sqlKind.name(),
					sqlKind,
					2,
					new OrdinalReturnTypeInference(0),
					null,
					null);
			SqlNode[] operands = new SqlNode[]{ SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO)};
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

}
