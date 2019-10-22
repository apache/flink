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

package org.apache.flink.table.types.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;

import java.math.BigDecimal;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Value-based data type extractor that supports extraction of clearly identifiable data types for
 * input conversion.
 *
 * <p>This converter is more precise than {@link ClassDataTypeConverter} because it also considers
 * nullability, length, precision, and scale of values.
 */
@Internal
public final class ValueDataTypeConverter {

	/**
	 * Returns the clearly identifiable data type if possible. For example, {@code 12L} can be
	 * expressed as {@code DataTypes.BIGINT().notNull()}. However, for example, {@code null} could
	 * be any type and is not supported.
	 *
	 * <p>All types of the {@link LogicalTypeFamily#PREDEFINED} family, symbols, and arrays are supported.
	 */
	public static Optional<DataType> extractDataType(Object value) {
		if (value == null) {
			return Optional.empty();
		}

		DataType convertedDataType = null;

		if (value instanceof String) {
			convertedDataType = convertToCharType((String) value);
		}

		// byte arrays have higher priority than regular arrays
		else if (value instanceof byte[]) {
			convertedDataType = convertToBinaryType((byte[]) value);
		}

		else if (value instanceof BigDecimal) {
			convertedDataType = convertToDecimalType((BigDecimal) value);
		}

		else if (value instanceof java.time.LocalTime) {
			convertedDataType = convertToTimeType((java.time.LocalTime) value);
		}

		else if (value instanceof java.time.LocalDateTime) {
			convertedDataType = convertToTimestampType(((java.time.LocalDateTime) value).getNano());
		}

		else if (value instanceof java.sql.Timestamp) {
			convertedDataType = convertToTimestampType(((java.sql.Timestamp) value).getNanos());
		}

		else if (value instanceof java.time.ZonedDateTime) {
			convertedDataType = convertToZonedTimestampType(((java.time.ZonedDateTime) value).getNano());
		}

		else if (value instanceof java.time.OffsetDateTime) {
			convertedDataType = convertToZonedTimestampType(((java.time.OffsetDateTime) value).getNano());
		}

		else if (value instanceof java.time.Instant) {
			convertedDataType = convertToLocalZonedTimestampType(((java.time.Instant) value).getNano());
		}

		else if (value instanceof java.time.Period) {
			convertedDataType = convertToYearMonthIntervalType(((java.time.Period) value).getYears());
		}

		else if (value instanceof java.time.Duration) {
			final java.time.Duration duration = (java.time.Duration) value;
			convertedDataType = convertToDayTimeIntervalType(duration.toDays(), duration.getNano());
		}

		else if (value instanceof Object[]) {
			// don't let the class-based extraction kick in if array elements differ
			return convertToArrayType((Object[]) value)
				.map(dt -> dt.notNull().bridgedTo(value.getClass()));
		}

		final Optional<DataType> resultType;
		if (convertedDataType != null) {
			resultType = Optional.of(convertedDataType);
		} else {
			// class-based extraction is possible for BOOLEAN, TINYINT, SMALLINT, INT, FLOAT, DOUBLE,
			// DATE, TIME with java.sql.Time, and arrays of primitive types
			resultType = ClassDataTypeConverter.extractDataType(value.getClass());
		}
		return resultType.map(dt -> dt.notNull().bridgedTo(value.getClass()));
	}

	private static DataType convertToCharType(String string) {
		if (string.isEmpty()) {
			return new AtomicDataType(CharType.ofEmptyLiteral());
		}
		return DataTypes.CHAR(string.length());
	}

	private static DataType convertToBinaryType(byte[] bytes) {
		if (bytes.length == 0) {
			return new AtomicDataType(BinaryType.ofEmptyLiteral());
		}
		return DataTypes.BINARY(bytes.length);
	}

	private static DataType convertToDecimalType(BigDecimal decimal) {
		// let underlying layers check if precision and scale are supported
		return DataTypes.DECIMAL(decimal.precision(), decimal.scale());
	}

	private static DataType convertToTimeType(java.time.LocalTime time) {
		return DataTypes.TIME(fractionalSecondPrecision(time.getNano()));
	}

	private static DataType convertToTimestampType(int nanos) {
		return DataTypes.TIMESTAMP(fractionalSecondPrecision(nanos));
	}

	private static DataType convertToZonedTimestampType(int nanos) {
		return DataTypes.TIMESTAMP_WITH_TIME_ZONE(fractionalSecondPrecision(nanos));
	}

	private static DataType convertToLocalZonedTimestampType(int nanos) {
		return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(fractionalSecondPrecision(nanos));
	}

	private static DataType convertToYearMonthIntervalType(int years) {
		return DataTypes.INTERVAL(DataTypes.YEAR(yearPrecision(years)), DataTypes.MONTH());
	}

	private static DataType convertToDayTimeIntervalType(long days, int nanos) {
		return DataTypes.INTERVAL(
			DataTypes.DAY(dayPrecision(days)),
			DataTypes.SECOND(fractionalSecondPrecision(nanos)));
	}

	private static Optional<DataType> convertToArrayType(Object[] array) {
		// fallback to class based-extraction if no values exist
		if (array.length == 0 || Stream.of(array).allMatch(Objects::isNull)) {
			return extractElementTypeFromClass(array);
		}

		return extractElementTypeFromValues(array);
	}

	private static Optional<DataType> extractElementTypeFromValues(Object[] array) {
		DataType elementType = null;
		for (Object element : array) {
			// null values are wildcard array elements
			if (element == null) {
				continue;
			}

			final Optional<DataType> possibleElementType = extractDataType(element);
			if (!possibleElementType.isPresent()) {
				return Optional.empty();
			}

			// for simplification, we assume that array elements can always be nullable
			// otherwise mismatches could occur when dealing with nested arrays
			final DataType extractedElementType = possibleElementType.get().nullable();

			// ensure that all elements have the same type;
			// in theory the logic could be improved by converting an array with elements
			// [CHAR(1), CHAR(2)] into an array of CHAR(2) but this can lead to value
			// modification (i.e. adding spaces) which is not intended.
			if (elementType != null && !extractedElementType.equals(elementType)) {
				return Optional.empty();
			}
			elementType = extractedElementType;
		}

		return Optional.ofNullable(elementType)
			.map(DataTypes::ARRAY);
	}

	private static Optional<DataType> extractElementTypeFromClass(Object[] array) {
		final Optional<DataType> possibleElementType =
			ClassDataTypeConverter.extractDataType(array.getClass().getComponentType());

		// for simplification, we assume that array elements can always be nullable
		return possibleElementType
			.map(DataType::nullable)
			.map(DataTypes::ARRAY);
	}

	private static int fractionalSecondPrecision(int nanos) {
		return String.format("%09d", nanos).replaceAll("0+$", "").length();
	}

	private static int yearPrecision(int years) {
		return String.valueOf(years).length();
	}

	private static int dayPrecision(long days) {
		return String.valueOf(days).length();
	}

	private ValueDataTypeConverter() {
		// no instantiation
	}
}
