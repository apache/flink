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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.ValidationException;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Logical type of a time WITHOUT time zone consisting of {@code hour:minute:second[.fractional]} with
 * up to nanosecond precision and values ranging from {@code 00:00:00.000000000} to
 * {@code 23:59:59.999999999}. Compared to the SQL standard, leap seconds (23:59:60 and 23:59:61) are
 * not supported as the semantics are closer to {@link java.time.LocalTime}. A time WITH time zone
 * is not provided.
 *
 * <p>The serialized string representation is {@code TIME(p)} where {@code p} is the number of digits
 * of fractional seconds (=precision). {@code p} must have a value between 0 and 9 (both inclusive).
 * If no precision is specified, {@code p} is equal to 0. {@code TIME(p) WITHOUT TIME ZONE} is a synonym
 * for this type.
 *
 * <p>A conversion from and to {@code int} describes the number of milliseconds of the day. A
 * conversion from and to {@code long} describes the number of nanoseconds of the day.
 */
@PublicEvolving
public final class TimeType extends LogicalType {

	public static final int MIN_PRECISION = 0;

	public static final int MAX_PRECISION = 9;

	public static final int DEFAULT_PRECISION = 0;

	private static final String FORMAT = "TIME(%d)";

	private static final Set<String> NULL_OUTPUT_CONVERSION = conversionSet(
		java.sql.Time.class.getName(),
		java.time.LocalTime.class.getName(),
		Integer.class.getName(),
		Long.class.getName());

	private static final Set<String> NOT_NULL_INPUT_OUTPUT_CONVERSION = conversionSet(
		java.sql.Time.class.getName(),
		java.time.LocalTime.class.getName(),
		Integer.class.getName(),
		int.class.getName(),
		Long.class.getName(),
		long.class.getName());

	private static final Class<?> DEFAULT_CONVERSION = java.time.LocalTime.class;

	private final int precision;

	public TimeType(boolean isNullable, int precision) {
		super(isNullable, LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE);
		if (precision < MIN_PRECISION || precision > MAX_PRECISION) {
			throw new ValidationException(
				String.format(
					"Time precision must be between %d and %d (both inclusive).",
					MIN_PRECISION,
					MAX_PRECISION));
		}
		this.precision = precision;
	}

	public TimeType(int precision) {
		this(true, precision);
	}

	public TimeType() {
		this(DEFAULT_PRECISION);
	}

	public int getPrecision() {
		return precision;
	}

	@Override
	public LogicalType copy(boolean isNullable) {
		return new TimeType(isNullable, precision);
	}

	@Override
	public String asSerializableString() {
		return withNullability(FORMAT, precision);
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
		TimeType timeType = (TimeType) o;
		return precision == timeType.precision;
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), precision);
	}
}
