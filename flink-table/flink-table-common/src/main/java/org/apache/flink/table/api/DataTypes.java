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

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DayTimeIntervalType.DayTimeResolution;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.TypeInformationRawType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.YearMonthIntervalType.YearMonthResolution;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A {@link DataType} can be used to declare input and/or output types of operations. This class
 * enumerates all pre-defined data types of the Table & SQL API.
 *
 * <p>NOTE: Planners might not support every data type with the desired precision or parameter. Please
 * see the planner compatibility and limitations section in the website documentation before using a
 * data type.
 */
@PublicEvolving
public final class DataTypes {

	// we use SQL-like naming for data types and avoid Java keyword clashes
	// CHECKSTYLE.OFF: MethodName

	/**
	 * Data type of a fixed-length character string {@code CHAR(n)} where {@code n} is the number
	 * of code points. {@code n} must have a value between 1 and {@link Integer#MAX_VALUE} (both inclusive).
	 *
	 * @see CharType
	 */
	public static DataType CHAR(int n) {
		return new AtomicDataType(new CharType(n));
	}

	/**
	 * Data type of a variable-length character string {@code VARCHAR(n)} where {@code n} is the
	 * maximum number of code points. {@code n} must have a value between 1 and {@link Integer#MAX_VALUE}
	 * (both inclusive).
	 *
	 * @see VarCharType
	 */
	public static DataType VARCHAR(int n) {
		return new AtomicDataType(new VarCharType(n));
	}

	/**
	 * Data type of a variable-length character string with defined maximum length. This is a shortcut
	 * for {@code VARCHAR(2147483647)} for representing JVM strings.
	 *
	 * @see VarCharType
	 */
	public static DataType STRING() {
		return VARCHAR(Integer.MAX_VALUE);
	}

	/**
	 * Data type of a boolean with a (possibly) three-valued logic of {@code TRUE, FALSE, UNKNOWN}.
	 *
	 * @see BooleanType
	 */
	public static DataType BOOLEAN() {
		return new AtomicDataType(new BooleanType());
	}

	/**
	 * Data type of a fixed-length binary string (=a sequence of bytes) {@code BINARY(n)} where
	 * {@code n} is the number of bytes. {@code n} must have a value between 1 and {@link Integer#MAX_VALUE}
	 * (both inclusive).
	 *
	 * @see BinaryType
	 */
	public static DataType BINARY(int n) {
		return new AtomicDataType(new BinaryType(n));
	}

	/**
	 * Data type of a variable-length binary string (=a sequence of bytes) {@code VARBINARY(n)} where
	 * {@code n} is the maximum number of bytes. {@code n} must have a value between 1 and {@link Integer#MAX_VALUE}
	 * (both inclusive).
	 *
	 * @see VarBinaryType
	 */
	public static DataType VARBINARY(int n) {
		return new AtomicDataType(new VarBinaryType(n));
	}

	/**
	 * Data type of a variable-length binary string (=a sequence of bytes) with defined maximum length.
	 * This is a shortcut for {@code VARBINARY(2147483647)} for representing JVM byte arrays.
	 *
	 * @see VarBinaryType
	 */
	public static DataType BYTES() {
		return VARBINARY(Integer.MAX_VALUE);
	}

	/**
	 * Data type of a decimal number with fixed precision and scale {@code DECIMAL(p, s)} where {@code p}
	 * is the number of digits in a number (=precision) and {@code s} is the number of digits to the
	 * right of the decimal point in a number (=scale). {@code p} must have a value between 1 and 38
	 * (both inclusive). {@code s} must have a value between 0 and {@code p} (both inclusive).
	 *
	 * @see DecimalType
	 */
	public static DataType DECIMAL(int precision, int scale) {
		return new AtomicDataType(new DecimalType(precision, scale));
	}

	/**
	 * Data type of a 1-byte signed integer with values from -128 to 127.
	 *
	 * @see TinyIntType
	 */
	public static DataType TINYINT() {
		return new AtomicDataType(new TinyIntType());
	}

	/**
	 * Data type of a 2-byte signed integer with values from -32,768 to 32,767.
	 *
	 * @see SmallIntType
	 */
	public static DataType SMALLINT() {
		return new AtomicDataType(new SmallIntType());
	}

	/**
	 * Data type of a 4-byte signed integer with values from -2,147,483,648 to 2,147,483,647.
	 *
	 * @see IntType
	 */
	public static DataType INT() {
		return new AtomicDataType(new IntType());
	}

	/**
	 * Data type of an 8-byte signed integer with values from -9,223,372,036,854,775,808 to
	 * 9,223,372,036,854,775,807.
	 *
	 * @see BigIntType
	 */
	public static DataType BIGINT() {
		return new AtomicDataType(new BigIntType());
	}

	/**
	 * Data type of a 4-byte single precision floating point number.
	 *
	 * @see FloatType
	 */
	public static DataType FLOAT() {
		return new AtomicDataType(new FloatType());
	}

	/**
	 * Data type of an 8-byte double precision floating point number.
	 *
	 * @see DoubleType
	 */
	public static DataType DOUBLE() {
		return new AtomicDataType(new DoubleType());
	}

	/**
	 * Data type of a date consisting of {@code year-month-day} with values ranging from {@code 0000-01-01}
	 * to {@code 9999-12-31}.
	 *
	 * <p>Compared to the SQL standard, the range starts at year {@code 0000}.
	 *
	 * @see DataType
	 */
	public static DataType DATE() {
		return new AtomicDataType(new DateType());
	}

	/**
	 * Data type of a time WITHOUT time zone {@code TIME(p)} where {@code p} is the number of digits
	 * of fractional seconds (=precision). {@code p} must have a value between 0 and 9 (both inclusive).
	 *
	 * <p>An instance consists of {@code hour:minute:second[.fractional]} with up to nanosecond precision
	 * and values ranging from {@code 00:00:00.000000000} to {@code 23:59:59.999999999}.
	 *
	 * <p>Compared to the SQL standard, leap seconds (23:59:60 and 23:59:61) are not supported as the
	 * semantics are closer to {@link java.time.LocalTime}. A time WITH time zone is not provided.
	 *
	 * @see #TIME()
	 * @see TimeType
	 */
	public static DataType TIME(int precision) {
		return new AtomicDataType(new TimeType(precision));
	}

	/**
	 * Data type of a time WITHOUT time zone {@code TIME} with no fractional seconds by default.
	 *
	 * <p>An instance consists of {@code hour:minute:second} with up to second precision
	 * and values ranging from {@code 00:00:00} to {@code 23:59:59}.
	 *
	 * <p>Compared to the SQL standard, leap seconds (23:59:60 and 23:59:61) are not supported as the
	 * semantics are closer to {@link java.time.LocalTime}. A time WITH time zone is not provided.
	 *
	 * @see #TIME(int)
	 * @see TimeType
	 */
	public static DataType TIME() {
		return new AtomicDataType(new TimeType());
	}

	/**
	 * Data type of a timestamp WITHOUT time zone {@code TIMESTAMP(p)} where {@code p} is the number
	 * of digits of fractional seconds (=precision). {@code p} must have a value between 0 and 9 (both
	 * inclusive).
	 *
	 * <p>An instance consists of {@code year-month-day hour:minute:second[.fractional]} with up to
	 * nanosecond precision and values ranging from {@code 0000-01-01 00:00:00.000000000} to
	 * {@code 9999-12-31 23:59:59.999999999}.
	 *
	 * <p>Compared to the SQL standard, leap seconds (23:59:60 and 23:59:61) are not supported as the
	 * semantics are closer to {@link java.time.LocalDateTime}.
	 *
	 * @see #TIMESTAMP_WITH_TIME_ZONE(int)
	 * @see #TIMESTAMP_WITH_LOCAL_TIME_ZONE(int)
	 * @see TimestampType
	 */
	public static DataType TIMESTAMP(int precision) {
		return new AtomicDataType(new TimestampType(precision));
	}

	/**
	 * Data type of a timestamp WITHOUT time zone {@code TIMESTAMP} with 6 digits of fractional seconds
	 * by default.
	 *
	 * <p>An instance consists of {@code year-month-day hour:minute:second[.fractional]} with up to
	 * microsecond precision and values ranging from {@code 0000-01-01 00:00:00.000000} to
	 * {@code 9999-12-31 23:59:59.999999}.
	 *
	 * <p>Compared to the SQL standard, leap seconds (23:59:60 and 23:59:61) are not supported as the
	 * semantics are closer to {@link java.time.LocalDateTime}.
	 *
	 * @see #TIMESTAMP(int)
	 * @see #TIMESTAMP_WITH_TIME_ZONE(int)
	 * @see #TIMESTAMP_WITH_LOCAL_TIME_ZONE(int)
	 * @see TimestampType
	 */
	public static DataType TIMESTAMP() {
		return new AtomicDataType(new TimestampType());
	}

	/**
	 * Data type of a timestamp WITH time zone {@code TIMESTAMP(p) WITH TIME ZONE} where {@code p} is
	 * the number of digits of fractional seconds (=precision). {@code p} must have a value between 0
	 * and 9 (both inclusive).
	 *
	 * <p>An instance consists of {@code year-month-day hour:minute:second[.fractional] zone} with up
	 * to nanosecond precision and values ranging from {@code 0000-01-01 00:00:00.000000000 +14:59} to
	 * {@code 9999-12-31 23:59:59.999999999 -14:59}.
	 *
	 * <p>Compared to the SQL standard, leap seconds (23:59:60 and 23:59:61) are not supported as the
	 * semantics are closer to {@link java.time.OffsetDateTime}.
	 *
	 * @see #TIMESTAMP(int)
	 * @see #TIMESTAMP_WITH_LOCAL_TIME_ZONE(int)
	 * @see ZonedTimestampType
	 */
	public static DataType TIMESTAMP_WITH_TIME_ZONE(int precision) {
		return new AtomicDataType(new ZonedTimestampType(precision));
	}

	/**
	 * Data type of a timestamp WITH time zone {@code TIMESTAMP WITH TIME ZONE} with 6 digits of fractional
	 * seconds by default.
	 *
	 * <p>An instance consists of {@code year-month-day hour:minute:second[.fractional] zone} with up
	 * to microsecond precision and values ranging from {@code 0000-01-01 00:00:00.000000 +14:59} to
	 * {@code 9999-12-31 23:59:59.999999 -14:59}.
	 *
	 * <p>Compared to the SQL standard, leap seconds (23:59:60 and 23:59:61) are not supported as the
	 * semantics are closer to {@link java.time.OffsetDateTime}.
	 *
	 * @see #TIMESTAMP_WITH_TIME_ZONE(int)
	 * @see #TIMESTAMP(int)
	 * @see #TIMESTAMP_WITH_LOCAL_TIME_ZONE(int)
	 * @see ZonedTimestampType
	 */
	public static DataType TIMESTAMP_WITH_TIME_ZONE() {
		return new AtomicDataType(new ZonedTimestampType());
	}

	/**
	 * Data type of a timestamp WITH LOCAL time zone {@code TIMESTAMP(p) WITH LOCAL TIME ZONE} where
	 * {@code p} is the number of digits of fractional seconds (=precision). {@code p} must have a value
	 * between 0 and 9 (both inclusive).
	 *
	 * <p>An instance consists of {@code year-month-day hour:minute:second[.fractional] zone} with up
	 * to nanosecond precision and values ranging from {@code 0000-01-01 00:00:00.000000000 +14:59} to
	 * {@code 9999-12-31 23:59:59.999999999 -14:59}. Leap seconds (23:59:60 and 23:59:61) are not supported
	 * as the semantics are closer to {@link java.time.OffsetDateTime}.
	 *
	 * <p>Compared to {@link ZonedTimestampType}, the time zone offset information is not stored physically
	 * in every datum. Instead, the type assumes {@link java.time.Instant} semantics in UTC time zone
	 * at the edges of the table ecosystem. Every datum is interpreted in the local time zone configured
	 * in the current session for computation and visualization.
	 *
	 * <p>This type fills the gap between time zone free and time zone mandatory timestamp types by
	 * allowing the interpretation of UTC timestamps according to the configured session timezone.
	 *
	 * @see #TIMESTAMP(int)
	 * @see #TIMESTAMP_WITH_TIME_ZONE(int)
	 * @see LocalZonedTimestampType
	 */
	public static DataType TIMESTAMP_WITH_LOCAL_TIME_ZONE(int precision) {
		return new AtomicDataType(new LocalZonedTimestampType(precision));
	}

	/**
	 * Data type of a timestamp WITH LOCAL time zone {@code TIMESTAMP WITH LOCAL TIME ZONE} with 6 digits
	 * of fractional seconds by default.
	 *
	 * <p>An instance consists of {@code year-month-day hour:minute:second[.fractional] zone} with up
	 * to microsecond precision and values ranging from {@code 0000-01-01 00:00:00.000000 +14:59} to
	 * {@code 9999-12-31 23:59:59.999999 -14:59}. Leap seconds (23:59:60 and 23:59:61) are not supported
	 * as the semantics are closer to {@link java.time.OffsetDateTime}.
	 *
	 * <p>Compared to {@link ZonedTimestampType}, the time zone offset information is not stored physically
	 * in every datum. Instead, the type assumes {@link java.time.Instant} semantics in UTC time zone
	 * at the edges of the table ecosystem. Every datum is interpreted in the local time zone configured
	 * in the current session for computation and visualization.
	 *
	 * <p>This type fills the gap between time zone free and time zone mandatory timestamp types by
	 * allowing the interpretation of UTC timestamps according to the configured session timezone.
	 *
	 * @see #TIMESTAMP_WITH_LOCAL_TIME_ZONE(int)
	 * @see #TIMESTAMP(int)
	 * @see #TIMESTAMP_WITH_TIME_ZONE(int)
	 * @see LocalZonedTimestampType
	 */
	public static DataType TIMESTAMP_WITH_LOCAL_TIME_ZONE() {
		return new AtomicDataType(new LocalZonedTimestampType());
	}

	/**
	 * Data type of a temporal interval. There are two types of temporal intervals: day-time intervals
	 * with up to nanosecond granularity or year-month intervals with up to month granularity.
	 *
	 * <p>An interval of day-time consists of {@code +days hours:months:seconds.fractional} with values
	 * ranging from {@code -999999 23:59:59.999999999} to {@code +999999 23:59:59.999999999}. The type
	 * must be parameterized to one of the following resolutions: interval of days, interval of days to
	 * hours, interval of days to minutes, interval of days to seconds, interval of hours, interval of
	 * hours to minutes, interval of hours to seconds, interval of minutes, interval of minutes to seconds,
	 * or interval of seconds. The value representation is the same for all types of resolutions. For
	 * example, an interval of seconds of 70 is always represented in an interval-of-days-to-seconds
	 * format (with default precisions): {@code +00 00:01:10.000000}).
	 *
	 * <p>An interval of year-month consists of {@code +years-months} with values ranging from {@code -9999-11}
	 * to {@code +9999-11}. The type must be parameterized to one of the following resolutions: interval
	 * of years, interval of years to months, or interval of months. The value representation is the
	 * same for all types of resolutions. For example, an interval of months of 50 is always represented
	 * in an interval-of-years-to-months format (with default year precision): {@code +04-02}.
	 *
	 * <p>Examples: {@code INTERVAL(DAY(2))} for a day-time interval or {@code INTERVAL(YEAR(4))} for
	 * a year-month interval.
	 *
	 * @see DayTimeIntervalType
	 * @see YearMonthIntervalType
	 */
	public static DataType INTERVAL(Resolution resolution) {
		Preconditions.checkNotNull(resolution, "Interval resolution must not be null.");
		return new AtomicDataType(Resolution.resolveInterval(resolution, null));
	}

	/**
	 * Data type of a temporal interval. There are two types of temporal intervals: day-time intervals
	 * with up to nanosecond granularity or year-month intervals with up to month granularity.
	 *
	 * <p>An interval of day-time consists of {@code +days hours:months:seconds.fractional} with values
	 * ranging from {@code -999999 23:59:59.999999999} to {@code +999999 23:59:59.999999999}. The type
	 * must be parameterized to one of the following resolutions: interval of days, interval of days to
	 * hours, interval of days to minutes, interval of days to seconds, interval of hours, interval of
	 * hours to minutes, interval of hours to seconds, interval of minutes, interval of minutes to seconds,
	 * or interval of seconds. The value representation is the same for all types of resolutions. For
	 * example, an interval of seconds of 70 is always represented in an interval-of-days-to-seconds
	 * format (with default precisions): {@code +00 00:01:10.000000}.
	 *
	 * <p>An interval of year-month consists of {@code +years-months} with values ranging from {@code -9999-11}
	 * to {@code +9999-11}. The type must be parameterized to one of the following resolutions: interval
	 * of years, interval of years to months, or interval of months. The value representation is the
	 * same for all types of resolutions. For example, an interval of months of 50 is always represented
	 * in an interval-of-years-to-months format (with default year precision): {@code +04-02}.
	 *
	 * <p>Examples: {@code INTERVAL(DAY(2), SECOND(9))} for a day-time interval or {@code INTERVAL(YEAR(4), MONTH())}
	 * for a year-month interval.
	 *
	 * @see DayTimeIntervalType
	 * @see YearMonthIntervalType
	 */
	public static DataType INTERVAL(Resolution upperResolution, Resolution lowerResolution) {
		Preconditions.checkNotNull(upperResolution, "Upper interval resolution must not be null.");
		Preconditions.checkNotNull(lowerResolution, "Lower interval resolution must not be null.");
		return new AtomicDataType(Resolution.resolveInterval(upperResolution, lowerResolution));
	}

	/**
	 * Data type of an array of elements with same subtype.
	 *
	 * <p>Compared to the SQL standard, the maximum cardinality of an array cannot be specified but
	 * is fixed at {@link Integer#MAX_VALUE}. Also, any valid type is supported as a subtype.
	 *
	 * @see ArrayType
	 */
	public static DataType ARRAY(DataType elementDataType) {
		Preconditions.checkNotNull(elementDataType, "Element data type must not be null.");
		return new CollectionDataType(new ArrayType(elementDataType.getLogicalType()), elementDataType);
	}

	/**
	 * Data type of a multiset (=bag). Unlike a set, it allows for multiple instances for each of its
	 * elements with a common subtype. Each unique value (including {@code NULL}) is mapped to some
	 * multiplicity.
	 *
	 * <p>There is no restriction of element types; it is the responsibility of the user to ensure
	 * uniqueness.
	 *
	 * @see MultisetType
	 */
	public static DataType MULTISET(DataType elementDataType) {
		Preconditions.checkNotNull(elementDataType, "Element data type must not be null.");
		return new CollectionDataType(new MultisetType(elementDataType.getLogicalType()), elementDataType);
	}

	/**
	 * Data type of an associative array that maps keys (including {@code NULL}) to values (including
	 * {@code NULL}). A map cannot contain duplicate keys; each key can map to at most one value.
	 *
	 * <p>There is no restriction of key types; it is the responsibility of the user to ensure uniqueness.
	 * The map type is an extension to the SQL standard.
	 *
	 * @see MapType
	 */
	public static DataType MAP(DataType keyDataType, DataType valueDataType) {
		Preconditions.checkNotNull(keyDataType, "Key data type must not be null.");
		Preconditions.checkNotNull(valueDataType, "Value data type must not be null.");
		return new KeyValueDataType(
			new MapType(keyDataType.getLogicalType(), valueDataType.getLogicalType()),
			keyDataType,
			valueDataType);
	}

	/**
	 * Data type of a sequence of fields. A field consists of a field name, field type, and an optional
	 * description. The most specific type of a row of a table is a row type. In this case, each column
	 * of the row corresponds to the field of the row type that has the same ordinal position as the
	 * column.
	 *
	 * <p>Compared to the SQL standard, an optional field description simplifies the handling with
	 * complex structures.
	 *
	 * @see RowType
	 */
	public static DataType ROW(Field... fields) {
		final List<RowType.RowField> logicalFields = Stream.of(fields)
			.map(f -> Preconditions.checkNotNull(f, "Field definition must not be null."))
			.map(f -> new RowType.RowField(f.name, f.dataType.getLogicalType(), f.description))
			.collect(Collectors.toList());
		final Map<String, DataType> fieldDataTypes = Stream.of(fields)
			.collect(Collectors.toMap(f -> f.name, f -> f.dataType));
		return new FieldsDataType(new RowType(logicalFields), fieldDataTypes);
	}

	/**
	 * Data type for representing untyped {@code NULL} values. A null type has no other value except
	 * {@code NULL}, thus, it can be cast to any nullable type similar to JVM semantics.
	 *
	 * <p>This type helps in representing unknown types in API calls that use a {@code NULL} literal
	 * as well as bridging to formats such as JSON or Avro that define such a type as well.
	 *
	 * <p>The null type is an extension to the SQL standard.
	 *
	 * @see NullType
	 */
	public static DataType NULL() {
		return new AtomicDataType(new NullType());
	}

	/**
	 * Data type of an arbitrary serialized type. This type is a black box within the table ecosystem
	 * and is only deserialized at the edges.
	 *
	 * <p>The raw type is an extension to the SQL standard.
	 *
	 * <p>This method assumes that a {@link TypeSerializer} instance is present. Use {@link #RAW(TypeInformation)}
	 * for generating a serializer from Flink's core type system automatically in subsequent layers.
	 *
	 * @param clazz originating value class
	 * @param serializer type serializer
	 *
	 * @see RawType
	 */
	public static <T> DataType RAW(Class<T> clazz, TypeSerializer<T> serializer) {
		return new AtomicDataType(new RawType<>(clazz, serializer));
	}

	/**
	 * @deprecated Use {@link #RAW(Class, TypeSerializer)} instead.
	 */
	@Deprecated
	public static <T> DataType ANY(Class<T> clazz, TypeSerializer<T> serializer) {
		return RAW(clazz, serializer);
	}

	/**
	 * Data type of an arbitrary serialized type backed by {@link TypeInformation}. This type is
	 * a black box within the table ecosystem and is only deserialized at the edges.
	 *
	 * <p>The raw type is an extension to the SQL standard.
	 *
	 * <p>Compared to an {@link #RAW(Class, TypeSerializer)}, this type does not contain a {@link TypeSerializer}
	 * yet. The serializer will be generated from the enclosed {@link TypeInformation} but needs access
	 * to the {@link ExecutionConfig} of the current execution environment. Thus, this type is just a
	 * placeholder.
	 *
	 * @see TypeInformationRawType
	 */
	public static <T> DataType RAW(TypeInformation<T> typeInformation) {
		return new AtomicDataType(new TypeInformationRawType<>(typeInformation));
	}

	/**
	 * @deprecated Use {@link #RAW(TypeInformation)} instead.
	 */
	@Deprecated
	public static <T> DataType ANY(TypeInformation<T> typeInformation) {
		return RAW(typeInformation);
	}

	// --------------------------------------------------------------------------------------------
	// Helper functions
	// --------------------------------------------------------------------------------------------

	/**
	 * Resolution in seconds with 6 digits for fractional seconds by default.
	 *
	 * @see #SECOND(int)
	 */
	public static Resolution SECOND() {
		return new Resolution(Resolution.IntervalUnit.SECOND, DayTimeIntervalType.DEFAULT_FRACTIONAL_PRECISION);
	}

	/**
	 * Resolution in seconds and (possibly) fractional seconds. The precision is the number of
	 * digits of fractional seconds. It must have a value between 0 and 9 (both inclusive). If
	 * no fractional is specified, it is equal to 6 by default.
	 *
	 * @see #SECOND()
	 */
	public static Resolution SECOND(int precision) {
		return new Resolution(Resolution.IntervalUnit.SECOND, precision);
	}

	/**
	 * Resolution in minutes.
	 */
	public static Resolution MINUTE() {
		return new Resolution(Resolution.IntervalUnit.MINUTE);
	}

	/**
	 * Resolution in hours.
	 */
	public static Resolution HOUR() {
		return new Resolution(Resolution.IntervalUnit.HOUR);
	}

	/**
	 * Resolution in days. The precision is the number of digits of days. It must have a value
	 * between 1 and 6 (both inclusive). If no precision is specified, it is equal to 2 by default.
	 *
	 * @see #DAY()
	 */
	public static Resolution DAY(int precision) {
		return new Resolution(Resolution.IntervalUnit.DAY, precision);
	}

	/**
	 * Resolution in days with 2 digits for the number of days by default.
	 *
	 * @see #DAY(int)
	 */
	public static Resolution DAY() {
		return new Resolution(Resolution.IntervalUnit.DAY, DayTimeIntervalType.DEFAULT_DAY_PRECISION);
	}

	/**
	 * Resolution in months.
	 */
	public static Resolution MONTH() {
		return new Resolution(Resolution.IntervalUnit.MONTH);
	}

	/**
	 * Resolution in years. The precision is the number of digits of years. It must have a value
	 * between 1 and 4 (both inclusive). If no precision is specified, it is equal to 2.
	 *
	 * @see #YEAR()
	 */
	public static Resolution YEAR(int precision) {
		return new Resolution(Resolution.IntervalUnit.YEAR, precision);
	}

	/**
	 * Resolution in years with 2 digits for the number of years by default.
	 *
	 * @see #YEAR(int)
	 */
	public static Resolution YEAR() {
		return new Resolution(Resolution.IntervalUnit.YEAR, YearMonthIntervalType.DEFAULT_PRECISION);
	}

	/**
	 * Field definition with field name and data type.
	 */
	public static Field FIELD(String name, DataType dataType) {
		return new Field(
			Preconditions.checkNotNull(name, "Field name must not be null."),
			Preconditions.checkNotNull(dataType, "Field data type must not be null."),
			null);
	}

	/**
	 * Field definition with field name, data type, and a description.
	 */
	public static Field FIELD(String name, DataType dataType, String description) {
		return new Field(
			Preconditions.checkNotNull(name, "Field name must not be null."),
			Preconditions.checkNotNull(dataType, "Field data type must not be null."),
			Preconditions.checkNotNull(description, "Field description must not be null."));
	}

	// --------------------------------------------------------------------------------------------
	// Helper classes
	// --------------------------------------------------------------------------------------------

	/**
	 * Helper class for defining the resolution of an interval.
	 */
	public static final class Resolution {

		private static final int EMPTY_PRECISION = -1;

		private enum IntervalUnit {
			SECOND,
			MINUTE,
			HOUR,
			DAY,
			MONTH,
			YEAR
		}

		private static final Map<List<IntervalUnit>, BiFunction<Integer, Integer, LogicalType>> resolutionMapping = new HashMap<>();
		static {
			addResolutionMapping(
				IntervalUnit.YEAR, null,
				(p1, p2) -> new YearMonthIntervalType(YearMonthResolution.YEAR, p1));
			addResolutionMapping(
				IntervalUnit.MONTH, null,
				(p1, p2) -> new YearMonthIntervalType(YearMonthResolution.MONTH));
			addResolutionMapping(
				IntervalUnit.YEAR, IntervalUnit.MONTH,
				(p1, p2) -> new YearMonthIntervalType(YearMonthResolution.YEAR_TO_MONTH, p1));
			addResolutionMapping(
				IntervalUnit.DAY, null,
				(p1, p2) -> new DayTimeIntervalType(DayTimeResolution.DAY, p1, DayTimeIntervalType.DEFAULT_FRACTIONAL_PRECISION));
			addResolutionMapping(
				IntervalUnit.DAY, IntervalUnit.HOUR,
				(p1, p2) -> new DayTimeIntervalType(DayTimeResolution.DAY_TO_HOUR, p1, DayTimeIntervalType.DEFAULT_FRACTIONAL_PRECISION));
			addResolutionMapping(
				IntervalUnit.DAY, IntervalUnit.MINUTE,
				(p1, p2) -> new DayTimeIntervalType(DayTimeResolution.DAY_TO_MINUTE, p1, DayTimeIntervalType.DEFAULT_FRACTIONAL_PRECISION));
			addResolutionMapping(
				IntervalUnit.DAY, IntervalUnit.SECOND,
				(p1, p2) -> new DayTimeIntervalType(DayTimeResolution.DAY_TO_SECOND, p1, p2));
			addResolutionMapping(
				IntervalUnit.HOUR, null,
				(p1, p2) -> new DayTimeIntervalType(DayTimeResolution.HOUR));
			addResolutionMapping(
				IntervalUnit.HOUR, IntervalUnit.MINUTE,
				(p1, p2) -> new DayTimeIntervalType(DayTimeResolution.HOUR_TO_MINUTE));
			addResolutionMapping(
				IntervalUnit.HOUR, IntervalUnit.SECOND,
				(p1, p2) -> new DayTimeIntervalType(DayTimeResolution.HOUR_TO_SECOND, DayTimeIntervalType.DEFAULT_DAY_PRECISION, p2));
			addResolutionMapping(
				IntervalUnit.MINUTE, null,
				(p1, p2) -> new DayTimeIntervalType(DayTimeResolution.MINUTE));
			addResolutionMapping(
				IntervalUnit.MINUTE, IntervalUnit.SECOND,
				(p1, p2) -> new DayTimeIntervalType(DayTimeResolution.MINUTE_TO_SECOND, DayTimeIntervalType.DEFAULT_DAY_PRECISION, p2));
			addResolutionMapping(
				IntervalUnit.SECOND, null,
				(p1, p2) -> new DayTimeIntervalType(DayTimeResolution.SECOND, DayTimeIntervalType.DEFAULT_DAY_PRECISION, p1));
		}

		private static void addResolutionMapping(
				IntervalUnit leftUnit,
				IntervalUnit rightUnit,
				BiFunction<Integer, Integer, LogicalType> typeProvider) {
			resolutionMapping.put(Arrays.asList(leftUnit, rightUnit), typeProvider);
		}

		private static LogicalType resolveInterval(Resolution fromResolution, @Nullable Resolution toResolution) {
			final IntervalUnit toBoundary = toResolution == null ? null : toResolution.unit;
			final int toPrecision = toResolution == null ? EMPTY_PRECISION : toResolution.precision;

			final BiFunction<Integer, Integer, LogicalType> typeProvider = resolutionMapping.get(
				Arrays.asList(fromResolution.unit, toBoundary));
			if (typeProvider == null) {
				throw new ValidationException(String.format("Unsupported interval definition '%s TO %s'. " +
					"Please check the documentation for supported combinations for year-month and day-time intervals.",
					fromResolution.unit,
					toBoundary));
			}
			return typeProvider.apply(
				fromResolution.precision,
				toPrecision);
		}

		private final int precision;

		private final IntervalUnit unit;

		private Resolution(IntervalUnit unit, int precision) {
			this.unit = unit;
			this.precision = precision;
		}

		private Resolution(IntervalUnit unit) {
			this(unit, EMPTY_PRECISION);
		}

		@Override
		public String toString() {
			if (precision != EMPTY_PRECISION) {
				return String.format("%s(%d)", unit, precision);
			}
			return unit.toString();
		}
	}

	/**
	 * Helper class for defining the field of a row or structured type.
	 */
	public static final class Field {

		private final String name;

		private final DataType dataType;

		private final @Nullable String description;

		private Field(String name, DataType dataType, String description) {
			this.name = name;
			this.dataType = dataType;
			this.description = description;
		}

		public String getName() {
			return name;
		}

		public DataType getDataType() {
			return dataType;
		}

		public Optional<String> getDescription() {
			return Optional.ofNullable(description);
		}
	}

	private DataTypes() {
		// no instances
	}

	// CHECKSTYLE.ON: MethodName
}
