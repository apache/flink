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
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Logical type for a group of day-time interval types. The type must be parameterized to one of the
 * following resolutions with up to nanosecond precision: interval of days, interval of days to
 * hours, interval of days to minutes, interval of days to seconds, interval of hours, interval of
 * hours to minutes, interval of hours to seconds, interval of minutes, interval of minutes to
 * seconds, or interval of seconds.
 *
 * <p>An interval of day-time consists of {@code +days hours:months:seconds.fractional} with values
 * ranging from {@code -999999 23:59:59.999999999} to {@code +999999 23:59:59.999999999}. The value
 * representation is the same for all types of resolutions. For example, an interval of seconds of
 * 70 is always represented in an interval-of-days-to-seconds format (with default precisions):
 * {@code +00 00:01:10.000000}.
 *
 * <p>The serialized string representation is {@code INTERVAL DAY(p1)}, {@code INTERVAL DAY(p1) TO
 * HOUR}, {@code INTERVAL DAY(p1) TO MINUTE}, {@code INTERVAL DAY(p1) TO SECOND(p2)}, {@code
 * INTERVAL HOUR}, {@code INTERVAL HOUR TO MINUTE}, {@code INTERVAL HOUR TO SECOND(p2)}, {@code
 * INTERVAL MINUTE}, {@code INTERVAL MINUTE TO SECOND(p2)}, or {@code INTERVAL SECOND(p2)} where
 * {@code p1} is the number of digits of days (=day precision) and {@code p2} is the number of
 * digits of fractional seconds (=fractional precision). {@code p1} must have a value between 1 and
 * 6 (both inclusive). {@code p2} must have a value between 0 and 9 (both inclusive). If no {@code
 * p1} is specified, it is equal to 2 by default. If no {@code p2} is specified, it is equal to 6 by
 * default.
 *
 * <p>A conversion from and to {@code long} describes the number of milliseconds.
 *
 * @see YearMonthIntervalType
 */
@PublicEvolving
public final class DayTimeIntervalType extends LogicalType {
    private static final long serialVersionUID = 1L;

    public static final int MIN_DAY_PRECISION = 1;

    public static final int MAX_DAY_PRECISION = 6;

    public static final int DEFAULT_DAY_PRECISION = 2;

    public static final int MIN_FRACTIONAL_PRECISION = 0;

    public static final int MAX_FRACTIONAL_PRECISION = 9;

    public static final int DEFAULT_FRACTIONAL_PRECISION = 6;

    private static final String DAY_FORMAT = "INTERVAL DAY(%1$d)";

    private static final String DAY_TO_HOUR_FORMAT = "INTERVAL DAY(%1$d) TO HOUR";

    private static final String DAY_TO_MINUTE_FORMAT = "INTERVAL DAY(%1$d) TO MINUTE";

    private static final String DAY_TO_SECOND_FORMAT = "INTERVAL DAY(%1$d) TO SECOND(%2$d)";

    private static final String HOUR_FORMAT = "INTERVAL HOUR";

    private static final String HOUR_TO_MINUTE_FORMAT = "INTERVAL HOUR TO MINUTE";

    private static final String HOUR_TO_SECOND_FORMAT = "INTERVAL HOUR TO SECOND(%2$d)";

    private static final String MINUTE_FORMAT = "INTERVAL MINUTE";

    private static final String MINUTE_TO_SECOND_FORMAT = "INTERVAL MINUTE TO SECOND(%2$d)";

    private static final String SECOND_FORMAT = "INTERVAL SECOND(%2$d)";

    private static final Set<String> NULL_OUTPUT_CONVERSION =
            conversionSet(java.time.Duration.class.getName(), Long.class.getName());

    private static final Set<String> NOT_NULL_INPUT_OUTPUT_CONVERSION =
            conversionSet(
                    java.time.Duration.class.getName(), Long.class.getName(), long.class.getName());

    private static final Class<?> DEFAULT_CONVERSION = java.time.Duration.class;

    /**
     * Supported resolutions of this type.
     *
     * <p>Note: The order of this enum reflects the granularity from coarse to fine.
     */
    public enum DayTimeResolution {
        DAY,
        DAY_TO_HOUR,
        DAY_TO_MINUTE,
        DAY_TO_SECOND,
        HOUR,
        HOUR_TO_MINUTE,
        HOUR_TO_SECOND,
        MINUTE,
        MINUTE_TO_SECOND,
        SECOND
    }

    private final DayTimeResolution resolution;

    private final int dayPrecision;

    private final int fractionalPrecision;

    public DayTimeIntervalType(
            boolean isNullable,
            DayTimeResolution resolution,
            int dayPrecision,
            int fractionalPrecision) {
        super(isNullable, LogicalTypeRoot.INTERVAL_DAY_TIME);
        Preconditions.checkNotNull(resolution);
        if (needsDefaultDayPrecision(resolution) && dayPrecision != DEFAULT_DAY_PRECISION) {
            throw new ValidationException(
                    String.format(
                            "Day precision of sub-day intervals must be equal to the default precision %d.",
                            DEFAULT_DAY_PRECISION));
        }
        if (needsDefaultFractionalPrecision(resolution)
                && fractionalPrecision != DEFAULT_FRACTIONAL_PRECISION) {
            throw new ValidationException(
                    String.format(
                            "Fractional precision of super-second intervals must be equal to the default precision %d.",
                            DEFAULT_FRACTIONAL_PRECISION));
        }
        if (dayPrecision < MIN_DAY_PRECISION || dayPrecision > MAX_DAY_PRECISION) {
            throw new ValidationException(
                    String.format(
                            "Day precision of day-time intervals must be between %d and %d (both inclusive).",
                            MIN_DAY_PRECISION, MAX_DAY_PRECISION));
        }
        if (fractionalPrecision < MIN_FRACTIONAL_PRECISION
                || fractionalPrecision > MAX_FRACTIONAL_PRECISION) {
            throw new ValidationException(
                    String.format(
                            "Fractional precision of day-time intervals must be between %d and %d (both inclusive).",
                            MIN_FRACTIONAL_PRECISION, MAX_FRACTIONAL_PRECISION));
        }
        this.resolution = resolution;
        this.dayPrecision = dayPrecision;
        this.fractionalPrecision = fractionalPrecision;
    }

    public DayTimeIntervalType(
            DayTimeResolution resolution, int dayPrecision, int fractionalPrecision) {
        this(true, resolution, dayPrecision, fractionalPrecision);
    }

    public DayTimeIntervalType(DayTimeResolution resolution) {
        this(resolution, DEFAULT_DAY_PRECISION, DEFAULT_FRACTIONAL_PRECISION);
    }

    public DayTimeResolution getResolution() {
        return resolution;
    }

    public int getDayPrecision() {
        return dayPrecision;
    }

    public int getFractionalPrecision() {
        return fractionalPrecision;
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new DayTimeIntervalType(isNullable, resolution, dayPrecision, fractionalPrecision);
    }

    @Override
    public String asSerializableString() {
        return withNullability(getResolutionFormat(), dayPrecision, fractionalPrecision);
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
        DayTimeIntervalType that = (DayTimeIntervalType) o;
        return dayPrecision == that.dayPrecision
                && fractionalPrecision == that.fractionalPrecision
                && resolution == that.resolution;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), resolution, dayPrecision, fractionalPrecision);
    }

    // --------------------------------------------------------------------------------------------

    private boolean needsDefaultDayPrecision(DayTimeResolution resolution) {
        switch (resolution) {
            case HOUR:
            case HOUR_TO_MINUTE:
            case HOUR_TO_SECOND:
            case MINUTE:
            case MINUTE_TO_SECOND:
            case SECOND:
                return true;
            default:
                return false;
        }
    }

    private boolean needsDefaultFractionalPrecision(DayTimeResolution resolution) {
        switch (resolution) {
            case DAY:
            case DAY_TO_HOUR:
            case DAY_TO_MINUTE:
            case HOUR:
            case HOUR_TO_MINUTE:
            case MINUTE:
                return true;
            default:
                return false;
        }
    }

    private String getResolutionFormat() {
        switch (resolution) {
            case DAY:
                return DAY_FORMAT;
            case DAY_TO_HOUR:
                return DAY_TO_HOUR_FORMAT;
            case DAY_TO_MINUTE:
                return DAY_TO_MINUTE_FORMAT;
            case DAY_TO_SECOND:
                return DAY_TO_SECOND_FORMAT;
            case HOUR:
                return HOUR_FORMAT;
            case HOUR_TO_MINUTE:
                return HOUR_TO_MINUTE_FORMAT;
            case HOUR_TO_SECOND:
                return HOUR_TO_SECOND_FORMAT;
            case MINUTE:
                return MINUTE_FORMAT;
            case MINUTE_TO_SECOND:
                return MINUTE_TO_SECOND_FORMAT;
            case SECOND:
                return SECOND_FORMAT;
            default:
                throw new UnsupportedOperationException();
        }
    }
}
