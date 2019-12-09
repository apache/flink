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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.ValueDataTypeConverter;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Period;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Expression for constant literal values.
 *
 * <p>By design, this class can take any value described by a {@link DataType}. However, it is
 * recommended to use instances with default conversion (see {@link DataType#getConversionClass()}.
 *
 * <p>Equals/hashCode support of this expression depends on the equals/hashCode support of the value.
 *
 * <p>The data type can be extracted automatically from non-null values using value-based extraction
 * (see {@link ValueDataTypeConverter}).
 *
 * <p>Symbols (enums extending from {@link TableSymbol}) are considered as literal values.
 */
@PublicEvolving
public final class ValueLiteralExpression implements ResolvedExpression {

	private final @Nullable Object value;

	private final DataType dataType;

	public ValueLiteralExpression(Object value) {
		this(value, deriveDataTypeFromValue(value));
	}

	public ValueLiteralExpression(Object value, DataType dataType) {
		validateValueDataType(value, Preconditions.checkNotNull(dataType, "Data type must not be null."));
		this.value = value; // can be null
		this.dataType = dataType;
	}

	public boolean isNull() {
		return value == null;
	}

	/**
	 * Returns the value (excluding null) as an instance of the given class.
	 */
	@SuppressWarnings("unchecked")
	public <T> Optional<T> getValueAs(Class<T> clazz) {
		if (value == null) {
			return Optional.empty();
		}

		final Class<?> valueClass = value.getClass();

		Object convertedValue = null;

		if (clazz.isInstance(value)) {
			convertedValue = clazz.cast(value);
		}

		else if (valueClass == Integer.class && clazz == Long.class) {
			final Integer integer = (Integer) value;
			convertedValue = integer.longValue();
		}

		else if (valueClass == Duration.class && clazz == Long.class) {
			final Duration duration = (Duration) value;
			convertedValue = duration.toMillis();
		}

		else if (valueClass == Long.class && clazz == Duration.class) {
			final Long longVal = (Long) value;
			convertedValue = Duration.ofMillis(longVal);
		}

		else if (valueClass == Period.class && clazz == Integer.class) {
			final Period period = (Period) value;
			convertedValue = (int) period.toTotalMonths();
		}

		else if (valueClass == Integer.class && clazz == Period.class) {
			final Integer integer = (Integer) value;
			convertedValue = Period.ofMonths(integer);
		}

		else if (valueClass == java.sql.Date.class && clazz == java.time.LocalDate.class) {
			final java.sql.Date date = (java.sql.Date) value;
			convertedValue = date.toLocalDate();
		}

		else if (valueClass == java.sql.Time.class && clazz == java.time.LocalTime.class) {
			final java.sql.Time time = (java.sql.Time) value;
			convertedValue = time.toLocalTime();
		}

		else if (valueClass == java.sql.Timestamp.class && clazz == java.time.LocalDateTime.class) {
			final java.sql.Timestamp timestamp = (java.sql.Timestamp) value;
			convertedValue = timestamp.toLocalDateTime();
		}

		else if (valueClass == java.time.LocalDate.class && clazz == java.sql.Date.class) {
			final java.time.LocalDate date = (java.time.LocalDate) value;
			convertedValue = java.sql.Date.valueOf(date);
		}

		else if (valueClass == java.time.LocalTime.class && clazz == java.sql.Time.class) {
			final java.time.LocalTime time = (java.time.LocalTime) value;
			convertedValue = java.sql.Time.valueOf(time);
		}

		else if (valueClass == java.time.LocalDateTime.class && clazz == java.sql.Timestamp.class) {
			final java.time.LocalDateTime dateTime = (java.time.LocalDateTime) value;
			convertedValue = java.sql.Timestamp.valueOf(dateTime);
		}

		else if (Number.class.isAssignableFrom(valueClass) && clazz == BigDecimal.class) {
			convertedValue = new BigDecimal(String.valueOf(value));
		}

		// we can offer more conversions in the future, these conversions must not necessarily
		// comply with the logical type conversions
		return Optional.ofNullable((T) convertedValue);
	}

	@Override
	public DataType getOutputDataType() {
		return dataType;
	}

	@Override
	public List<ResolvedExpression> getResolvedChildren() {
		return Collections.emptyList();
	}

	@Override
	public String asSummaryString() {
		return stringifyValue(value);
	}

	@Override
	public List<Expression> getChildren() {
		return Collections.emptyList();
	}

	@Override
	public <R> R accept(ExpressionVisitor<R> visitor) {
		return visitor.visit(this);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ValueLiteralExpression that = (ValueLiteralExpression) o;
		return Objects.deepEquals(value, that.value) && dataType.equals(that.dataType);
	}

	@Override
	public int hashCode() {
		return Objects.hash(value, dataType);
	}

	@Override
	public String toString() {
		return asSummaryString();
	}

	// --------------------------------------------------------------------------------------------

	private static DataType deriveDataTypeFromValue(Object value) {
		return ValueDataTypeConverter.extractDataType(value)
			.orElseThrow(() ->
				new ValidationException("Cannot derive a data type for value '" + value + "'. " +
					"The data type must be specified explicitly."));
	}

	private static void validateValueDataType(Object value, DataType dataType) {
		final LogicalType logicalType = dataType.getLogicalType();
		if (value == null) {
			if (!logicalType.isNullable()) {
				throw new ValidationException(
					String.format(
						"Data type '%s' does not support null values.",
						dataType));
			}
			return;
		}
		final Class<?> candidate = value.getClass();
		// ensure value and data type match
		if (!dataType.getConversionClass().isAssignableFrom(candidate)) {
			throw new ValidationException(
				String.format(
					"Data type '%s' with conversion class '%s' does not support a value literal of class '%s'.",
					dataType,
					dataType.getConversionClass().getName(),
					value.getClass().getName()));
		}
		// check for proper input as this cannot be checked in data type
		if (!logicalType.supportsInputConversion(candidate)) {
			throw new ValidationException(
				String.format(
					"Data type '%s' does not support a conversion from class '%s'.",
					dataType,
					candidate.getName()));
		}
	}

	/**
	 * Supports (nested) arrays and makes string values more explicit.
	 */
	private static String stringifyValue(Object value) {
		if (value instanceof String[]) {
			final String[] array = (String[]) value;
			return Stream.of(array)
				.map(ValueLiteralExpression::stringifyValue)
				.collect(Collectors.joining(", ", "[", "]"));
		} else if (value instanceof Object[]) {
			final Object[] array = (Object[]) value;
			return Stream.of(array)
				.map(ValueLiteralExpression::stringifyValue)
				.collect(Collectors.joining(", ", "[", "]"));
		} else if (value instanceof String) {
			return "'" + ((String) value).replace("'", "''") + "'";
		}
		return EncodingUtils.objectToString(value);
	}
}
