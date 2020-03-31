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
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.LocalReferenceExpression;
import org.apache.flink.table.expressions.TimeIntervalUnit;
import org.apache.flink.table.expressions.TimePointUnit;
import org.apache.flink.table.expressions.TypeLiteralExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.RexFieldVariable;
import org.apache.flink.table.planner.expressions.RexNodeExpression;
import org.apache.flink.table.planner.expressions.converter.CallExpressionConvertRule.ConvertContext;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType;
import static org.apache.flink.table.util.TimestampStringUtils.fromLocalDateTime;

/**
 * Visit expression to generator {@link RexNode}.
 */
public class ExpressionConverter implements ExpressionVisitor<RexNode> {

	private static final List<CallExpressionConvertRule> FUNCTION_CONVERT_CHAIN = Arrays.asList(
		new LegacyScalarFunctionConvertRule(),
		new FunctionDefinitionConvertRule(),
		new OverConvertRule(),
		new DirectConvertRule(),
		new CustomizedConvertRule()
	);

	private final RelBuilder relBuilder;
	private final FlinkTypeFactory typeFactory;
	private final DataTypeFactory dataTypeFactory;

	public ExpressionConverter(RelBuilder relBuilder) {
		this.relBuilder = relBuilder;
		this.typeFactory = (FlinkTypeFactory) relBuilder.getRexBuilder().getTypeFactory();
		this.dataTypeFactory = ShortcutUtils.unwrapContext(relBuilder.getCluster())
			.getCatalogManager()
			.getDataTypeFactory();
	}

	@Override
	public RexNode visit(CallExpression call) {
		for (CallExpressionConvertRule rule : FUNCTION_CONVERT_CHAIN) {
			Optional<RexNode> converted = rule.convert(call, newFunctionContext());
			if (converted.isPresent()) {
				return converted.get();
			}
		}
		throw new RuntimeException("Unknown call expression: " + call);
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
			case TINYINT:
				return relBuilder.getRexBuilder().makeLiteral(
						extractValue(valueLiteral, Object.class),
						typeFactory.createSqlType(SqlTypeName.TINYINT),
						true);
			case SMALLINT:
				return relBuilder.getRexBuilder().makeLiteral(
						extractValue(valueLiteral, Object.class),
						typeFactory.createSqlType(SqlTypeName.SMALLINT),
						true);
			case INTEGER:
				return relBuilder.getRexBuilder().makeLiteral(
						extractValue(valueLiteral, Object.class),
						typeFactory.createSqlType(SqlTypeName.INTEGER),
						true);
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
				TimestampType timestampType = (TimestampType) type;
				LocalDateTime datetime = extractValue(valueLiteral, LocalDateTime.class);
				return relBuilder.getRexBuilder().makeTimestampLiteral(
					fromLocalDateTime(datetime), timestampType.getPrecision());
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				LocalZonedTimestampType lzTs = (LocalZonedTimestampType) type;
				TimeZone timeZone = TimeZone.getTimeZone(this.relBuilder.getCluster()
					.getPlanner()
					.getContext()
					.unwrap(FlinkContext.class)
					.getTableConfig()
					.getLocalTimeZone());
				Instant instant = extractValue(valueLiteral, Instant.class);
				return this.relBuilder.getRexBuilder().makeTimestampWithLocalTimeZoneLiteral(
					fromLocalDateTime(LocalDateTime.ofInstant(instant, timeZone.toZoneId())),
					lzTs.getPrecision());
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
			TimeUnit value = timePointUnitToTimeUnit((TimePointUnit) object);
			return relBuilder.getRexBuilder().makeFlag(value);
		} else if (object instanceof TimeIntervalUnit) {
			TimeUnitRange value = intervalUnitToUnitRange((TimeIntervalUnit) object);
			return relBuilder.getRexBuilder().makeFlag(value);
		} else {
			return relBuilder.literal(extractValue(valueLiteral, Object.class));
		}
	}

	@Override
	public RexNode visit(FieldReferenceExpression fieldReference) {
		// We can not use inputCount+inputIndex+FieldIndex to construct field of calcite.
		// See QueryOperationConverter.SingleRelVisitor.visit(AggregateQueryOperation).
		// Calcite will shuffle the output order of groupings.
		// So the output fields order will be changed too.
		// See RelBuilder.aggregate, it use ImmutableBitSet to store groupings,
		return relBuilder.field(fieldReference.getName());
	}

	@Override
	public RexNode visit(TypeLiteralExpression typeLiteral) {
		throw new UnsupportedOperationException();
	}

	@Override
	public RexNode visit(Expression other) {
		if (other instanceof RexNodeExpression) {
			return ((RexNodeExpression) other).getRexNode();
		} else if (other instanceof LocalReferenceExpression) {
			LocalReferenceExpression local = (LocalReferenceExpression) other;
			return new RexFieldVariable(
				local.getName(),
				typeFactory.createFieldTypeFromLogicalType(
					fromDataTypeToLogicalType(local.getOutputDataType())));
		} else {
			throw new UnsupportedOperationException(other.getClass().getSimpleName() + ":" + other.toString());
		}
	}

	public static List<RexNode> toRexNodes(ConvertContext context, List<Expression> expr) {
		return expr.stream().map(context::toRexNode).collect(Collectors.toList());
	}

	private ConvertContext newFunctionContext() {
		return new ConvertContext() {
			@Override
			public RexNode toRexNode(Expression expr) {
				return expr.accept(ExpressionConverter.this);
			}

			@Override
			public RelBuilder getRelBuilder() {
				return relBuilder;
			}

			@Override
			public FlinkTypeFactory getTypeFactory() {
				return typeFactory;
			}

			@Override
			public DataTypeFactory getDataTypeFactory() {
				return dataTypeFactory;
			}
		};
	}

	private static TimeUnit timePointUnitToTimeUnit(TimePointUnit unit) {
		switch (unit) {
			case YEAR:
				return TimeUnit.YEAR;
			case MONTH:
				return TimeUnit.MONTH;
			case DAY:
				return TimeUnit.DAY;
			case HOUR:
				return TimeUnit.HOUR;
			case MINUTE:
				return TimeUnit.MINUTE;
			case SECOND:
				return TimeUnit.SECOND;
			case QUARTER:
				return TimeUnit.QUARTER;
			case WEEK:
				return TimeUnit.WEEK;
			case MILLISECOND:
				return TimeUnit.MILLISECOND;
			case MICROSECOND:
				return TimeUnit.MICROSECOND;
			default:
				throw new UnsupportedOperationException("TimePointUnit is: " + unit);
		}
	}

	private static TimeUnitRange intervalUnitToUnitRange(TimeIntervalUnit intervalUnit) {
		switch (intervalUnit) {
			case YEAR:
				return TimeUnitRange.YEAR;
			case YEAR_TO_MONTH:
				return TimeUnitRange.YEAR_TO_MONTH;
			case QUARTER:
				return TimeUnitRange.QUARTER;
			case MONTH:
				return TimeUnitRange.MONTH;
			case WEEK:
				return TimeUnitRange.WEEK;
			case DAY:
				return TimeUnitRange.DAY;
			case DAY_TO_HOUR:
				return TimeUnitRange.DAY_TO_HOUR;
			case DAY_TO_MINUTE:
				return TimeUnitRange.DAY_TO_MINUTE;
			case DAY_TO_SECOND:
				return TimeUnitRange.DAY_TO_SECOND;
			case HOUR:
				return TimeUnitRange.HOUR;
			case SECOND:
				return TimeUnitRange.SECOND;
			case HOUR_TO_MINUTE:
				return TimeUnitRange.HOUR_TO_MINUTE;
			case HOUR_TO_SECOND:
				return TimeUnitRange.HOUR_TO_SECOND;
			case MINUTE:
				return TimeUnitRange.MINUTE;
			case MINUTE_TO_SECOND:
				return TimeUnitRange.MINUTE_TO_SECOND;
			default:
				throw new UnsupportedOperationException("TimeIntervalUnit is: " + intervalUnit);
		}
	}

	/**
	 * Extracts a value from a literal. Including planner-specific instances such as {@link Decimal}.
	 */
	@SuppressWarnings("unchecked")
	public static <T> T extractValue(ValueLiteralExpression literal, Class<T> clazz) {
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
}
