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

package org.apache.flink.table.types.logical;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.TimestampData;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Logical type of a timestamp WITH LOCAL time zone consisting of {@code year-month-day hour:minute:second[.fractional] zone}
 * with up to nanosecond precision and values ranging from {@code 0000-01-01 00:00:00.000000000 +14:59} to
 * {@code 9999-12-31 23:59:59.999999999 -14:59}. Leap seconds (23:59:60 and 23:59:61) are not
 * supported as the semantics are closer to {@link java.time.OffsetDateTime}.
 *
 * <p>The serialized string representation is {@code TIMESTAMP(p) WITH LOCAL TIME ZONE} where {@code p} is
 * the number of digits of fractional seconds (=precision). {@code p} must have a value between 0 and
 * 9 (both inclusive). If no precision is specified, {@code p} is equal to 6.
 *
 * <p>Compared to {@link ZonedTimestampType}, the time zone offset information is not stored physically
 * in every datum. Instead, the type assumes {@link java.time.Instant} semantics in UTC time zone at
 * the edges of the table ecosystem. Every datum is interpreted in the local time zone configured in
 * the current session for computation and visualization.
 *
 * <p>This type fills the gap between time zone free and time zone mandatory timestamp types by allowing
 * the interpretation of UTC timestamps according to the configured session time zone. A conversion
 * from and to {@code int} describes the number of seconds since epoch. A conversion from and to {@code long}
 * describes the number of milliseconds since epoch.
 *
 * @see TimestampType
 * @see ZonedTimestampType
 */
@PublicEvolving
public final class LocalZonedTimestampType extends LogicalType {

	public static final int MIN_PRECISION = TimestampType.MIN_PRECISION;

	public static final int MAX_PRECISION = TimestampType.MAX_PRECISION;

	public static final int DEFAULT_PRECISION = TimestampType.DEFAULT_PRECISION;

	private static final String FORMAT = "TIMESTAMP(%d) WITH LOCAL TIME ZONE";

	private static final Set<String> NULL_OUTPUT_CONVERSION = conversionSet(
		java.time.Instant.class.getName(),
		Integer.class.getName(),
		Long.class.getName(),
		TimestampData.class.getName());

	private static final Set<String> NOT_NULL_INPUT_OUTPUT_CONVERSION = conversionSet(
		java.time.Instant.class.getName(),
		Integer.class.getName(),
		int.class.getName(),
		Long.class.getName(),
		long.class.getName());

	private static final Class<?> DEFAULT_CONVERSION = java.time.Instant.class;

	private final TimestampKind kind;

	private final int precision;

	/**
	 * Internal constructor that allows attaching additional metadata about time attribute
	 * properties. The additional metadata does not affect equality or serializability.
	 *
	 * <p>Use {@link #getKind()} for comparing this metadata.
	 */
	@Internal
	public LocalZonedTimestampType(boolean isNullable, TimestampKind kind, int precision) {
		super(isNullable, LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
		if (precision < MIN_PRECISION || precision > MAX_PRECISION) {
			throw new ValidationException(
				String.format(
					"Timestamp with local time zone precision must be between %d and %d (both inclusive).",
					MIN_PRECISION,
					MAX_PRECISION));
		}
		this.kind = kind;
		this.precision = precision;
	}

	public LocalZonedTimestampType(boolean isNullable, int precision) {
		this(isNullable, TimestampKind.REGULAR, precision);
	}

	public LocalZonedTimestampType(int precision) {
		this(true, precision);
	}

	public LocalZonedTimestampType() {
		this(DEFAULT_PRECISION);
	}

	@Internal
	public TimestampKind getKind() {
		return kind;
	}

	public int getPrecision() {
		return precision;
	}

	@Override
	public LogicalType copy(boolean isNullable) {
		return new LocalZonedTimestampType(isNullable, kind, precision);
	}

	@Override
	public String asSerializableString() {
		return withNullability(FORMAT, precision);
	}

	@Override
	public String asSummaryString() {
		if (kind != TimestampKind.REGULAR) {
			return String.format("%s *%s*", asSerializableString(), kind);
		}
		return asSerializableString();
	}

	@Override
	public boolean supportsInputConversion(Class<?> clazz) {
		return NOT_NULL_INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
	}

	@Override
	public boolean supportsOutputConversion(Class<?> clazz) {
		if (isNullable()) {
			return NULL_OUTPUT_CONVERSION.contains(clazz.getName());
		}
		return NOT_NULL_INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
	}

	@Override
	public Class<?> getDefaultConversion() {
		return DEFAULT_CONVERSION;
	}

	@Override
	public List<LogicalType> getChildren() {
		return Collections.emptyList();
	}

	@Override
	public <R> R accept(LogicalTypeVisitor<R> visitor) {
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
		if (!super.equals(o)) {
			return false;
		}
		LocalZonedTimestampType that = (LocalZonedTimestampType) o;
		return precision == that.precision;
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), precision);
	}
}
