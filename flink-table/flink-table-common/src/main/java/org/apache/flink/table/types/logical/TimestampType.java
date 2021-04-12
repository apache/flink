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
 * Logical type of a timestamp WITHOUT time zone consisting of {@code year-month-day
 * hour:minute:second[.fractional]} with up to nanosecond precision and values ranging from {@code
 * 0000-01-01 00:00:00.000000000} to {@code 9999-12-31 23:59:59.999999999}. Compared to the SQL
 * standard, leap seconds (23:59:60 and 23:59:61) are not supported as the semantics are closer to
 * {@link java.time.LocalDateTime}.
 *
 * <p>The serialized string representation is {@code TIMESTAMP(p)} where {@code p} is the number of
 * digits of fractional seconds (=precision). {@code p} must have a value between 0 and 9 (both
 * inclusive). If no precision is specified, {@code p} is equal to 6. {@code TIMESTAMP(p) WITHOUT
 * TIME ZONE} is a synonym for this type.
 *
 * <p>A conversion from and to {@code long} is not supported as this would imply a time zone.
 * However, this type is time zone free. For more {@link java.time.Instant}-like semantics use
 * {@link LocalZonedTimestampType}.
 *
 * @see ZonedTimestampType
 * @see LocalZonedTimestampType
 */
@PublicEvolving
public final class TimestampType extends LogicalType {
    private static final long serialVersionUID = 1L;

    public static final int MIN_PRECISION = 0;

    public static final int MAX_PRECISION = 9;

    public static final int DEFAULT_PRECISION = 6;

    private static final String FORMAT = "TIMESTAMP(%d)";

    private static final Set<String> INPUT_OUTPUT_CONVERSION =
            conversionSet(
                    java.sql.Timestamp.class.getName(),
                    java.time.LocalDateTime.class.getName(),
                    TimestampData.class.getName());

    private static final Class<?> DEFAULT_CONVERSION = java.time.LocalDateTime.class;

    private final TimestampKind kind;

    private final int precision;

    /**
     * Internal constructor that allows attaching additional metadata about time attribute
     * properties. The additional metadata does not affect equality or serializability.
     *
     * <p>Use {@link #getKind()} for comparing this metadata.
     */
    @Internal
    public TimestampType(boolean isNullable, TimestampKind kind, int precision) {
        super(isNullable, LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE);
        if (precision < MIN_PRECISION || precision > MAX_PRECISION) {
            throw new ValidationException(
                    String.format(
                            "Timestamp precision must be between %d and %d (both inclusive).",
                            MIN_PRECISION, MAX_PRECISION));
        }
        if (kind == TimestampKind.PROCTIME) {
            throw new ValidationException("TimestampType can not be used as PROCTIME type.");
        }
        this.kind = kind;
        this.precision = precision;
    }

    public TimestampType(boolean isNullable, int precision) {
        this(isNullable, TimestampKind.REGULAR, precision);
    }

    public TimestampType(int precision) {
        this(true, precision);
    }

    public TimestampType() {
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
        return new TimestampType(isNullable, kind, precision);
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
        return INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
    }

    @Override
    public boolean supportsOutputConversion(Class<?> clazz) {
        return INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
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
        TimestampType that = (TimestampType) o;
        return precision == that.precision;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), precision);
    }
}
