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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Logical type of a timestamp WITH time zone consisting of {@code year-month-day
 * hour:minute:second[.fractional] zone} with up to nanosecond precision and values ranging from
 * {@code 0000-01-01 00:00:00.000000000 +14:59} to {@code 9999-12-31 23:59:59.999999999 -14:59}.
 * Compared to the SQL standard, leap seconds (23:59:60 and 23:59:61) are not supported as the
 * semantics are closer to {@link java.time.OffsetDateTime}.
 *
 * <p>The serialized string representation is {@code TIMESTAMP(p) WITH TIME ZONE} where {@code p} is
 * the number of digits of fractional seconds (=precision). {@code p} must have a value between 0
 * and 9 (both inclusive). If no precision is specified, {@code p} is equal to 6.
 *
 * <p>Compared to {@link LocalZonedTimestampType}, the time zone offset information is physically
 * stored in every datum. It is used individually for every computation, visualization, or
 * communication to external systems.
 *
 * <p>A conversion from {@link java.time.ZonedDateTime} ignores the zone ID.
 *
 * @see TimestampType
 * @see LocalZonedTimestampType
 */
@PublicEvolving
public final class ZonedTimestampType extends LogicalType {

    public static final int MIN_PRECISION = TimestampType.MIN_PRECISION;

    public static final int MAX_PRECISION = TimestampType.MAX_PRECISION;

    public static final int DEFAULT_PRECISION = TimestampType.DEFAULT_PRECISION;

    private static final String FORMAT = "TIMESTAMP(%d) WITH TIME ZONE";

    private static final Set<String> INPUT_CONVERSION =
            conversionSet(
                    java.time.ZonedDateTime.class.getName(),
                    java.time.OffsetDateTime.class.getName());

    private static final Set<String> OUTPUT_CONVERSION =
            conversionSet(java.time.OffsetDateTime.class.getName());

    private static final Class<?> DEFAULT_CONVERSION = java.time.OffsetDateTime.class;

    private final TimestampKind kind;

    private final int precision;

    /**
     * Internal constructor that allows attaching additional metadata about time attribute
     * properties. The additional metadata does not affect equality or serializability.
     *
     * <p>Use {@link #getKind()} for comparing this metadata.
     */
    @Internal
    public ZonedTimestampType(boolean isNullable, TimestampKind kind, int precision) {
        super(isNullable, LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE);
        if (precision < MIN_PRECISION || precision > MAX_PRECISION) {
            throw new ValidationException(
                    String.format(
                            "Timestamp with time zone precision must be between %d and %d (both inclusive).",
                            MIN_PRECISION, MAX_PRECISION));
        }
        this.kind = kind;
        this.precision = precision;
    }

    public ZonedTimestampType(boolean isNullable, int precision) {
        this(isNullable, TimestampKind.REGULAR, precision);
    }

    public ZonedTimestampType(int precision) {
        this(true, precision);
    }

    public ZonedTimestampType() {
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
        return new ZonedTimestampType(isNullable, kind, precision);
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
        return INPUT_CONVERSION.contains(clazz.getName());
    }

    @Override
    public boolean supportsOutputConversion(Class<?> clazz) {
        return OUTPUT_CONVERSION.contains(clazz.getName());
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
        ZonedTimestampType that = (ZonedTimestampType) o;
        return precision == that.precision;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), precision);
    }
}
