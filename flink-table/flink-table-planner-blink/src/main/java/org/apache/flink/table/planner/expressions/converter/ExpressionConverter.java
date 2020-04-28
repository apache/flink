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
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.RexFieldVariable;
import org.apache.flink.table.planner.expressions.RexNodeExpression;
import org.apache.flink.table.planner.expressions.converter.CallExpressionConvertRule.ConvertContext;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimeType;

import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
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

		RelDataType relDataType = typeFactory.createFieldTypeFromLogicalType(type);

		if (valueLiteral.isNull()) {
			return rexBuilder.makeNullLiteral(relDataType);
		}

		Object value = null;
		switch (type.getTypeRoot()) {
			case DECIMAL:
			case TINYINT:
			case SMALLINT:
			case INTEGER:
			case BIGINT:
			case FLOAT:
			case DOUBLE:
				value = extractValue(valueLiteral, BigDecimal.class);
				break;
			case VARCHAR:
			case CHAR:
				value = extractValue(valueLiteral, String.class);
				break;
			case BINARY:
			case VARBINARY:
				value = new ByteString(extractValue(valueLiteral, byte[].class));
				break;
			case INTERVAL_YEAR_MONTH:
				// convert to total months
				value = BigDecimal.valueOf(extractValue(valueLiteral, Period.class).toTotalMonths());
				break;
			case INTERVAL_DAY_TIME:
				// TODO planner supports only milliseconds precision
				// convert to total millis
				value = BigDecimal.valueOf(extractValue(valueLiteral, Duration.class).toMillis());
				break;
			case DATE:
				value = DateString.fromDaysSinceEpoch((int) extractValue(valueLiteral, LocalDate.class).toEpochDay());
				break;
			case TIME_WITHOUT_TIME_ZONE:
				// TODO type factory strips the precision, for literals we can be more lenient already
				// Moreover conversion from long supports precision up to TIME(3) planner does not support higher
				// precisions
				TimeType timeType = (TimeType) type;
				int precision = timeType.getPrecision();
				relDataType = typeFactory.createSqlType(SqlTypeName.TIME, Math.min(precision, 3));
				value = TimeString.fromMillisOfDay(
					extractValue(valueLiteral, LocalTime.class).get(ChronoField.MILLI_OF_DAY)
				);
				break;
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				LocalDateTime datetime = extractValue(valueLiteral, LocalDateTime.class);
				value = fromLocalDateTime(datetime);
				break;
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				// normalize to UTC
				Instant instant = extractValue(valueLiteral, Instant.class);
				value = fromLocalDateTime(instant.atOffset(ZoneOffset.UTC).toLocalDateTime());
				break;
			default:
				value = extractValue(valueLiteral, Object.class);
				if (value instanceof TimePointUnit) {
					value = timePointUnitToTimeUnit((TimePointUnit) value);
				} else if (value instanceof TimeIntervalUnit) {
					value = intervalUnitToUnitRange((TimeIntervalUnit) value);
				}
				break;
		}

		return rexBuilder.makeLiteral(
			value,
			relDataType,
			// This flag ensures that the resulting RexNode will match the type of the literal.
			// It might be wrapped with a CAST though, if the value requires adjusting. In majority of
			// cases the type will be simply pushed down into the RexLiteral, see RexBuilder#makeCast.
			true
		);
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
}
