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
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.utils.ValueDataTypeConverter;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.ZonedDateTime;
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

	public ValueLiteralExpression(@Nonnull Object value) {
		this(value, deriveDataTypeFromValue(value));
	}

	public ValueLiteralExpression(@Nullable Object value, DataType dataType) {
		validateValueDataType(value, Preconditions.checkNotNull(dataType, "Data type must not be null."));
		this.value = value; // can be null
		this.dataType = dataType;
	}

	public boolean isNull() {
		return value == null;
	}

	/**
	 * Returns the value (excluding null) as an instance of the given class.
	 *
	 * <p>It supports conversions to default conversion classes of {@link LogicalType LogicalTypes} and
	 * additionally to {@link BigDecimal} for all types of {@link LogicalTypeFamily#NUMERIC}. This method
	 * should not be called with other classes.
	 *
	 * <p>Note to implementers: Whenever we add a new class here, make sure to also update the planner
	 * for supporting the class via {@link CallContext#getArgumentValue(int, Class)}.
	 */
	@SuppressWarnings("unchecked")
	public <T> Optional<T> getValueAs(Class<T> clazz) {
		Preconditions.checkArgument(!clazz.isPrimitive());

		if (value == null) {
			return Optional.empty();
		}

		Object convertedValue = null;

		if (clazz.isInstance(value)) {
			convertedValue = clazz.cast(value);
		} else {
			Class<?> valueClass = value.getClass();
			if (clazz == Period.class) {
				convertedValue = convertToPeriod(value, valueClass);
			} else if (clazz == Duration.class) {
				convertedValue = convertToDuration(value, valueClass);
			} else if (clazz == LocalDate.class) {
				convertedValue = convertToLocalDate(value, valueClass);
			} else if (clazz == LocalTime.class) {
				convertedValue = convertToLocalTime(value, valueClass);
			} else if (clazz == LocalDateTime.class) {
				convertedValue = convertToLocalDateTime(value, valueClass);
			} else if (clazz == OffsetDateTime.class) {
				convertedValue = convertToOffsetDateTime(value, valueClass);
			} else if (clazz == Instant.class) {
				convertedValue = convertToInstant(value, valueClass);
			} else if (clazz == BigDecimal.class) {
				convertedValue = convertToBigDecimal(value);
			}
		}

		return Optional.ofNullable((T) convertedValue);
	}

	private @Nullable LocalDate convertToLocalDate(Object value, Class<?> valueClass) {
		if (valueClass == java.sql.Date.class) {
			return ((Date) value).toLocalDate();
		} else if (valueClass == Integer.class) {
			return LocalDate.ofEpochDay((int) value);
		}
		return null;
	}

	private @Nullable LocalTime convertToLocalTime(Object value, Class<?> valueClass) {
		if (valueClass == java.sql.Time.class) {
			return ((Time) value).toLocalTime();
		} else if (valueClass == Integer.class) {
			return LocalTime.ofNanoOfDay((int) value * 1_000_000L);
		} else if (valueClass == Long.class) {
			return LocalTime.ofNanoOfDay((long) value);
		}
		return null;
	}

	private @Nullable LocalDateTime convertToLocalDateTime(Object value, Class<?> valueClass) {
		if (valueClass == java.sql.Timestamp.class) {
			return ((Timestamp) value).toLocalDateTime();
		}

		return null;
	}

	private @Nullable OffsetDateTime convertToOffsetDateTime(Object value, Class<?> valueClass) {
		if (valueClass == ZonedDateTime.class) {
			return ((ZonedDateTime) value).toOffsetDateTime();
		}

		return null;
	}

	private @Nullable Instant convertToInstant(Object value, Class<?> valueClass) {
		if (valueClass == Integer.class) {
			return Instant.ofEpochSecond((int) value);
		} else if (valueClass == Long.class) {
			return Instant.ofEpochMilli((long) value);
		}

		return null;
	}

	private @Nullable Duration convertToDuration(Object value, Class<?> valueClass) {
		if (valueClass == Long.class) {
			final Long longValue = (Long) value;
			return Duration.ofMillis(longValue);
		}

		return null;
	}

	private @Nullable Period convertToPeriod(Object value, Class<?> valueClass) {
		if (valueClass == Integer.class) {
			final Integer integer = (Integer) value;
			return Period.ofMonths(integer);
		}

		return null;
	}

	private @Nullable BigDecimal convertToBigDecimal(Object value) {
		if (Number.class.isAssignableFrom(value.getClass())) {
			return new BigDecimal(String.valueOf(value));
		}

		return null;
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

		if (logicalType.isNullable()) {
			throw new ValidationException("Literals that have a non-null value must not have a nullable data type.");
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
		return StringUtils.arrayAwareToString(value);
	}
}
