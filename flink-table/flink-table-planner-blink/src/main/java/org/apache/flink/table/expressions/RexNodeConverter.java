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
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.calcite.RexAggLocalVariable;
import org.apache.flink.table.calcite.RexDistinctKeyVariable;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.InternalFunctionDefinitions;
import org.apache.flink.table.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
import static org.apache.flink.table.calcite.FlinkTypeFactory.toLogicalType;
import static org.apache.flink.table.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType;
import static org.apache.flink.table.typeutils.TypeCheckUtils.isTemporal;
import static org.apache.flink.table.typeutils.TypeCheckUtils.isTimeInterval;
import static org.apache.flink.table.typeutils.TypeCheckUtils.isVarchar;

/**
 * Visit expression to generator {@link RexNode}.
 *
 * <p>TODO actually we should use {@link ResolvedExpressionVisitor} here as it is the output of the API
 */
public class RexNodeConverter implements ExpressionVisitor<RexNode> {

	private final RelBuilder relBuilder;
	private final FlinkTypeFactory typeFactory;

	public RexNodeConverter(RelBuilder relBuilder) {
		this.relBuilder = relBuilder;
		this.typeFactory = (FlinkTypeFactory) relBuilder.getRexBuilder().getTypeFactory();
	}

	public RexNode visit(UnresolvedCallExpression call) {
		switch (call.getFunctionDefinition().getKind()) {
			case SCALAR:
				return translateScalarCall(call.getFunctionDefinition(), call.getChildren());
			default: throw new UnsupportedOperationException();
		}
	}

	@Override
	public RexNode visit(CallExpression call) {
		switch (call.getFunctionDefinition().getKind()) {
			case SCALAR:
				return translateScalarCall(call.getFunctionDefinition(), call.getChildren());
			default: throw new UnsupportedOperationException();
		}
	}

	private List<RexNode> convertCallChildren(List<Expression> children) {
		return children.stream()
				.map(expression -> expression.accept(RexNodeConverter.this))
				.collect(Collectors.toList());
	}

	private RexNode translateScalarCall(FunctionDefinition def, List<Expression> children) {

		if (def.equals(BuiltInFunctionDefinitions.CAST)) {
			RexNode child = children.get(0).accept(this);
			TypeLiteralExpression type = (TypeLiteralExpression) children.get(1);
			return relBuilder.getRexBuilder().makeAbstractCast(
					typeFactory.createFieldTypeFromLogicalType(
							type.getOutputDataType().getLogicalType().copy(child.getType().isNullable())),
					child);
		} else if (def.equals(BuiltInFunctionDefinitions.REINTERPRET_CAST)) {
			RexNode child = children.get(0).accept(this);
			TypeLiteralExpression type = (TypeLiteralExpression) children.get(1);
			RexNode checkOverflow = children.get(2).accept(this);
			return relBuilder.getRexBuilder().makeReinterpretCast(
					typeFactory.createFieldTypeFromLogicalType(
							type.getOutputDataType().getLogicalType().copy(child.getType().isNullable())),
					child,
					checkOverflow);
		}

		List<RexNode> child = convertCallChildren(children);
		if (BuiltInFunctionDefinitions.IF.equals(def)) {
			return relBuilder.call(FlinkSqlOperatorTable.CASE, child);
		} else if (BuiltInFunctionDefinitions.IS_NULL.equals(def)) {
			return relBuilder.isNull(child.get(0));
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
		} else if (BuiltInFunctionDefinitions.MINUS.equals(def)) {
			return relBuilder.call(FlinkSqlOperatorTable.MINUS, child);
		} else if (BuiltInFunctionDefinitions.EQUALS.equals(def)) {
			return relBuilder.call(FlinkSqlOperatorTable.EQUALS, child);
		} else if (BuiltInFunctionDefinitions.DIVIDE.equals(def)) {
			return relBuilder.call(FlinkSqlOperatorTable.DIVIDE, child);
		} else if (BuiltInFunctionDefinitions.LESS_THAN.equals(def)) {
			return relBuilder.call(FlinkSqlOperatorTable.LESS_THAN, child);
		} else if (BuiltInFunctionDefinitions.GREATER_THAN.equals(def)) {
			return relBuilder.call(FlinkSqlOperatorTable.GREATER_THAN, child);
		} else if (BuiltInFunctionDefinitions.OR.equals(def)) {
			return relBuilder.call(FlinkSqlOperatorTable.OR, child);
		} else if (BuiltInFunctionDefinitions.CONCAT.equals(def)) {
			return relBuilder.call(FlinkSqlOperatorTable.CONCAT, child);
		} else if (InternalFunctionDefinitions.THROW_EXCEPTION.equals(def)) {
			return relBuilder.call(FlinkSqlOperatorTable.THROW_EXCEPTION, child);
		} else if (BuiltInFunctionDefinitions.AND.equals(def)) {
			return relBuilder.call(FlinkSqlOperatorTable.AND, child);
		} else if (BuiltInFunctionDefinitions.NOT.equals(def)) {
			return relBuilder.call(FlinkSqlOperatorTable.NOT, child);
		} else if (BuiltInFunctionDefinitions.TIMES.equals(def)) {
			return relBuilder.call(FlinkSqlOperatorTable.MULTIPLY, child);
		} else if (BuiltInFunctionDefinitions.MOD.equals(def)) {
			return relBuilder.call(FlinkSqlOperatorTable.MOD, child);
		} else {
			throw new UnsupportedOperationException(def.toString());
		}
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

		if (type instanceof DecimalType) {
			DecimalType dt = (DecimalType) type;
			BigDecimal bigDecimal = extractValue(valueLiteral, BigDecimal.class);
			RelDataType decType = relBuilder.getTypeFactory().createSqlType(SqlTypeName.DECIMAL,
					dt.getPrecision(), dt.getScale());
			return relBuilder.getRexBuilder().makeExactLiteral(bigDecimal, decType);
		} else if (type instanceof BigIntType) {
			// create BIGINT literals for long type
			BigDecimal bigint = extractValue(valueLiteral, BigDecimal.class);
			return relBuilder.getRexBuilder().makeBigintLiteral(bigint);
		} else if (type instanceof FloatType) {
			//Float/Double type should be liked as java type here.
			return relBuilder.getRexBuilder().makeApproxLiteral(
					extractValue(valueLiteral, BigDecimal.class),
					relBuilder.getTypeFactory().createSqlType(SqlTypeName.FLOAT));
		} else if (type instanceof DoubleType) {
			//Float/Double type should be liked as java type here.
			return rexBuilder.makeApproxLiteral(
					extractValue(valueLiteral, BigDecimal.class),
					relBuilder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE));
		} else if (type instanceof DateType) {
			return relBuilder.getRexBuilder().makeDateLiteral(
					DateString.fromCalendarFields(valueAsCalendar(extractValue(valueLiteral, java.sql.Date.class))));
		} else if (type instanceof TimeType) {
			return relBuilder.getRexBuilder().makeTimeLiteral(
					TimeString.fromCalendarFields(valueAsCalendar(extractValue(valueLiteral, java.sql.Time.class))), 0);
		} else if (type instanceof TimestampType) {
			return relBuilder.getRexBuilder().makeTimestampLiteral(
					TimestampString.fromCalendarFields(valueAsCalendar(extractValue(valueLiteral, java.sql.Timestamp.class))), 3);
		} else if (type instanceof YearMonthIntervalType) {
			BigDecimal interval = BigDecimal.valueOf(extractValue(valueLiteral, Integer.class));
			SqlIntervalQualifier intervalQualifier = new SqlIntervalQualifier(
					TimeUnit.YEAR, TimeUnit.MONTH, SqlParserPos.ZERO);
			return relBuilder.getRexBuilder().makeIntervalLiteral(interval, intervalQualifier);
		} else if (type instanceof DayTimeIntervalType) {
			BigDecimal interval = BigDecimal.valueOf(extractValue(valueLiteral, Long.class));
			SqlIntervalQualifier intervalQualifier = new SqlIntervalQualifier(
					TimeUnit.DAY, TimeUnit.SECOND, SqlParserPos.ZERO);
			return relBuilder.getRexBuilder().makeIntervalLiteral(interval, intervalQualifier);
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
}
