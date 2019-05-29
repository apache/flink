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

import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.calcite.RexAggLocalVariable;
import org.apache.flink.table.calcite.RexDistinctKeyVariable;
import org.apache.flink.table.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.type.DecimalType;
import org.apache.flink.table.type.InternalType;
import org.apache.flink.table.type.InternalTypes;

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
import java.util.stream.Collectors;

import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
import static org.apache.flink.table.calcite.FlinkTypeFactory.toInternalType;
import static org.apache.flink.table.type.TypeConverters.createInternalTypeFromTypeInfo;
import static org.apache.flink.table.types.utils.TypeConversions.fromDataTypeToLegacyInfo;
import static org.apache.flink.table.typeutils.TypeCheckUtils.isString;
import static org.apache.flink.table.typeutils.TypeCheckUtils.isTemporal;
import static org.apache.flink.table.typeutils.TypeCheckUtils.isTimeInterval;

/**
 * Visit expression to generator {@link RexNode}.
 */
public class RexNodeConverter implements ExpressionVisitor<RexNode> {

	private final RelBuilder relBuilder;
	private final FlinkTypeFactory typeFactory;

	public RexNodeConverter(RelBuilder relBuilder) {
		this.relBuilder = relBuilder;
		this.typeFactory = (FlinkTypeFactory) relBuilder.getRexBuilder().getTypeFactory();
	}

	@Override
	public RexNode visitCall(CallExpression call) {
		switch (call.getFunctionDefinition().getType()) {
			case SCALAR_FUNCTION:
				return visitScalarFunc(call);
			default: throw new UnsupportedOperationException();
		}
	}

	private List<RexNode> convertCallChildren(CallExpression call) {
		return call.getChildren().stream()
				.map(expression -> expression.accept(RexNodeConverter.this))
				.collect(Collectors.toList());
	}

	private RexNode visitScalarFunc(CallExpression call) {
		FunctionDefinition def = call.getFunctionDefinition();

		if (call.getFunctionDefinition().equals(BuiltInFunctionDefinitions.CAST)) {
			RexNode child = call.getChildren().get(0).accept(this);
			TypeLiteralExpression type = (TypeLiteralExpression) call.getChildren().get(1);
			return relBuilder.getRexBuilder().makeAbstractCast(
					typeFactory.createTypeFromInternalType(
							createInternalTypeFromTypeInfo(
								fromDataTypeToLegacyInfo(type.getDataType())),
							child.getType().isNullable()),
					child);
		} else if (call.getFunctionDefinition().equals(BuiltInFunctionDefinitions.REINTERPRET_CAST)) {
			RexNode child = call.getChildren().get(0).accept(this);
			TypeLiteralExpression type = (TypeLiteralExpression) call.getChildren().get(1);
			RexNode checkOverflow = call.getChildren().get(2).accept(this);
			return relBuilder.getRexBuilder().makeReinterpretCast(
					typeFactory.createTypeFromInternalType(
							createInternalTypeFromTypeInfo(
								fromDataTypeToLegacyInfo(type.getDataType())),
							child.getType().isNullable()),
					child,
					checkOverflow);
		}

		List<RexNode> child = convertCallChildren(call);
		if (BuiltInFunctionDefinitions.IF.equals(def)) {
			return relBuilder.call(FlinkSqlOperatorTable.CASE, child);
		} else if (BuiltInFunctionDefinitions.IS_NULL.equals(def)) {
			return relBuilder.isNull(child.get(0));
		} else if (BuiltInFunctionDefinitions.PLUS.equals(def)) {
			if (isString(toInternalType(child.get(0).getType()))) {
				return relBuilder.call(
						FlinkSqlOperatorTable.CONCAT,
						child.get(0),
						relBuilder.cast(child.get(1), VARCHAR));
			} else if (isString(toInternalType(child.get(1).getType()))) {
				return relBuilder.call(
						FlinkSqlOperatorTable.CONCAT,
						relBuilder.cast(child.get(0), VARCHAR),
						child.get(1));
			} else if (isTimeInterval(toInternalType(child.get(0).getType())) &&
					child.get(0).getType() == child.get(1).getType()) {
				return relBuilder.call(FlinkSqlOperatorTable.PLUS, child);
			} else if (isTimeInterval(toInternalType(child.get(0).getType()))
					&& isTemporal(toInternalType(child.get(1).getType()))) {
				// Calcite has a bug that can't apply INTERVAL + DATETIME (INTERVAL at left)
				// we manually switch them here
				return relBuilder.call(FlinkSqlOperatorTable.DATETIME_PLUS, child);
			} else if (isTemporal(toInternalType(child.get(0).getType())) &&
					isTemporal(toInternalType(child.get(1).getType()))) {
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
			throw new UnsupportedOperationException(def.getName());
		}
	}

	@Override
	public RexNode visitSymbol(SymbolExpression symbolExpression) {
		throw new UnsupportedOperationException();
	}

	@Override
	public RexNode visitValueLiteral(ValueLiteralExpression expr) {
		InternalType type = createInternalTypeFromTypeInfo(expr.getType());
		Object value = expr.getValue();
		RexBuilder rexBuilder = relBuilder.getRexBuilder();
		FlinkTypeFactory typeFactory = (FlinkTypeFactory) relBuilder.getTypeFactory();
		if (value == null) {
			return relBuilder.getRexBuilder()
					.makeCast(
							typeFactory.createTypeFromInternalType(type, true),
							relBuilder.getRexBuilder().constantNull());
		}

		if (type instanceof DecimalType) {
			DecimalType dt = (DecimalType) type;
			BigDecimal bigDecValue = (BigDecimal) value;
			RelDataType decType = relBuilder.getTypeFactory().createSqlType(SqlTypeName.DECIMAL,
					dt.precision(), dt.scale());
			return relBuilder.getRexBuilder().makeExactLiteral(bigDecValue, decType);
		} else if (InternalTypes.LONG.equals(type)) {
			// create BIGINT literals for long type
			BigDecimal bigint = value instanceof BigDecimal ? (BigDecimal) value : BigDecimal.valueOf((long) value);
			return relBuilder.getRexBuilder().makeBigintLiteral(bigint);
		} else if (InternalTypes.FLOAT.equals(type)) {
			//Float/Double type should be liked as java type here.
			return relBuilder.getRexBuilder().makeApproxLiteral(
					BigDecimal.valueOf(((Number) value).floatValue()),
					relBuilder.getTypeFactory().createSqlType(SqlTypeName.FLOAT));
		} else if (InternalTypes.DOUBLE.equals(type)) {
			//Float/Double type should be liked as java type here.
			return rexBuilder.makeApproxLiteral(
					BigDecimal.valueOf(((Number) value).doubleValue()),
					relBuilder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE));
		} else if (InternalTypes.DATE.equals(type)) {
			return relBuilder.getRexBuilder().makeDateLiteral(
					DateString.fromCalendarFields(valueAsCalendar(value)));
		} else if (InternalTypes.TIME.equals(type)) {
			return relBuilder.getRexBuilder().makeTimeLiteral(
					TimeString.fromCalendarFields(valueAsCalendar(value)), 0);
		} else if (InternalTypes.TIMESTAMP.equals(type)) {
			return relBuilder.getRexBuilder().makeTimestampLiteral(
					TimestampString.fromCalendarFields(valueAsCalendar(value)), 3);
		} else if (InternalTypes.INTERVAL_MONTHS.equals(type)) {
			BigDecimal interval = BigDecimal.valueOf((int) value);
			SqlIntervalQualifier intervalQualifier = new SqlIntervalQualifier(
					TimeUnit.YEAR, TimeUnit.MONTH, SqlParserPos.ZERO);
			return relBuilder.getRexBuilder().makeIntervalLiteral(interval, intervalQualifier);
		} else if (InternalTypes.TIMESTAMP.equals(type)) {
			BigDecimal interval = BigDecimal.valueOf((long) value);
			SqlIntervalQualifier intervalQualifier = new SqlIntervalQualifier(
					TimeUnit.DAY, TimeUnit.SECOND, SqlParserPos.ZERO);
			return relBuilder.getRexBuilder().makeIntervalLiteral(interval, intervalQualifier);
		} else {
			return relBuilder.literal(value);
		}
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
				typeFactory.createTypeFromInternalType(reference.getResultType(), true));
	}

	private RexNode visitResolvedAggLocalReference(ResolvedAggLocalReference reference) {
		InternalType type = reference.getResultType();
		return new RexAggLocalVariable(
				reference.getFieldTerm(),
				reference.getNullTerm(),
				typeFactory.createTypeFromInternalType(type, true),
				type);
	}

	private RexNode visitResolvedDistinctKeyReference(ResolvedDistinctKeyReference reference) {
		InternalType type = reference.getResultType();
		return new RexDistinctKeyVariable(
				reference.getName(),
				typeFactory.createTypeFromInternalType(type, true),
				type);
	}
}
