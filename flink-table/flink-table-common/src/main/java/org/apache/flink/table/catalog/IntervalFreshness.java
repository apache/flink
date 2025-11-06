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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.ValidationException;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

/**
 * The {@link IntervalFreshness} represents freshness definition of {@link
 * CatalogMaterializedTable}. It encapsulates the string interval value along with time unit,
 * allowing for flexible representation of different freshness type. Moreover, it can provide
 * detailed raw information for some specific operations.
 */
@PublicEvolving
public class IntervalFreshness {

    private static final String SECOND_CRON_EXPRESSION_TEMPLATE = "0/%s * * * * ? *";
    private static final String MINUTE_CRON_EXPRESSION_TEMPLATE = "0 0/%s * * * ? *";
    private static final String HOUR_CRON_EXPRESSION_TEMPLATE = "0 0 0/%s * * ? *";
    private static final String ONE_DAY_CRON_EXPRESSION_TEMPLATE = "0 0 0 * * ? *";
    private static final long SECOND_CRON_UPPER_BOUND = 60;
    private static final long MINUTE_CRON_UPPER_BOUND = 60;
    private static final long HOUR_CRON_UPPER_BOUND = 24;

    private final int interval;
    private final TimeUnit timeUnit;

    private IntervalFreshness(int interval, TimeUnit timeUnit) {
        this.interval = interval;
        this.timeUnit = timeUnit;
    }

    public static IntervalFreshness of(String interval, TimeUnit timeUnit) {
        final int validateIntervalInput = validateIntervalInput(interval);
        return new IntervalFreshness(validateIntervalInput, timeUnit);
    }

    private static int validateIntervalInput(final String interval) {
        final int parsedInt;
        try {
            parsedInt = Integer.parseInt(interval);
        } catch (Exception e) {
            final String errorMessage =
                    String.format(
                            "The freshness interval currently only supports positive integer type values. But was: %s",
                            interval);
            throw new ValidationException(errorMessage, e);
        }

        if (parsedInt <= 0) {
            final String errorMessage =
                    String.format(
                            "The freshness interval currently only supports positive integer type values. But was: %s",
                            interval);
            throw new ValidationException(errorMessage);
        }
        return parsedInt;
    }

    public static IntervalFreshness ofSecond(String interval) {
        return IntervalFreshness.of(interval, TimeUnit.SECOND);
    }

    public static IntervalFreshness ofMinute(String interval) {
        return IntervalFreshness.of(interval, TimeUnit.MINUTE);
    }

    public static IntervalFreshness ofHour(String interval) {
        return IntervalFreshness.of(interval, TimeUnit.HOUR);
    }

    public static IntervalFreshness ofDay(String interval) {
        return IntervalFreshness.of(interval, TimeUnit.DAY);
    }

    /**
     * Validates that the given freshness can be converted to a cron expression in full refresh
     * mode. Since freshness and cron expression cannot be converted equivalently, there are
     * currently only a limited patterns of freshness that are supported.
     *
     * @param intervalFreshness the freshness to validate
     * @throws ValidationException if the freshness cannot be converted to a valid cron expression
     */
    public static void validateFreshnessForCron(IntervalFreshness intervalFreshness) {
        switch (intervalFreshness.getTimeUnit()) {
            case SECOND:
                validateCronConstraints(intervalFreshness, SECOND_CRON_UPPER_BOUND);
                break;
            case MINUTE:
                validateCronConstraints(intervalFreshness, MINUTE_CRON_UPPER_BOUND);
                break;
            case HOUR:
                validateCronConstraints(intervalFreshness, HOUR_CRON_UPPER_BOUND);
                break;
            case DAY:
                validateDayConstraints(intervalFreshness);
                break;
            default:
                throw new ValidationException(
                        String.format(
                                "Unknown freshness time unit: %s.",
                                intervalFreshness.getTimeUnit()));
        }
    }

    /**
     * Converts the freshness of materialized table to cron expression in full refresh mode. The
     * freshness must first pass validation via {@link #validateFreshnessForCron}.
     *
     * @param intervalFreshness the freshness to convert
     * @return the corresponding cron expression
     * @throws ValidationException if the freshness cannot be converted to a valid cron expression
     */
    public static String convertFreshnessToCron(IntervalFreshness intervalFreshness) {
        // First validate that conversion is possible
        validateFreshnessForCron(intervalFreshness);

        // Then perform the conversion
        switch (intervalFreshness.getTimeUnit()) {
            case SECOND:
                return String.format(
                        SECOND_CRON_EXPRESSION_TEMPLATE, intervalFreshness.getIntervalInt());
            case MINUTE:
                return String.format(
                        MINUTE_CRON_EXPRESSION_TEMPLATE, intervalFreshness.getIntervalInt());
            case HOUR:
                return String.format(
                        HOUR_CRON_EXPRESSION_TEMPLATE, intervalFreshness.getIntervalInt());
            case DAY:
                return ONE_DAY_CRON_EXPRESSION_TEMPLATE;
            default:
                throw new ValidationException(
                        String.format(
                                "Unknown freshness time unit: %s.",
                                intervalFreshness.getTimeUnit()));
        }
    }

    private static void validateCronConstraints(
            IntervalFreshness intervalFreshness, long cronUpperBound) {
        int interval = intervalFreshness.getIntervalInt();
        TimeUnit timeUnit = intervalFreshness.getTimeUnit();
        // Freshness must be less than cronUpperBound for corresponding time unit when convert it
        // to cron expression
        if (interval >= cronUpperBound) {
            throw new ValidationException(
                    String.format(
                            "In full refresh mode, freshness must be less than %s when the time unit is %s.",
                            cronUpperBound, timeUnit));
        }
        // Freshness must be factors of cronUpperBound for corresponding time unit
        if (cronUpperBound % interval != 0) {
            throw new ValidationException(
                    String.format(
                            "In full refresh mode, only freshness that are factors of %s are currently supported when the time unit is %s.",
                            cronUpperBound, timeUnit));
        }
    }

    private static void validateDayConstraints(IntervalFreshness intervalFreshness) {
        // Since the number of days in each month is different, only one day of freshness is
        // currently supported when the time unit is DAY
        int interval = intervalFreshness.getIntervalInt();
        if (interval > 1) {
            throw new ValidationException(
                    "In full refresh mode, freshness must be 1 when the time unit is DAY.");
        }
    }

    /**
     * Creates an IntervalFreshness from a Duration, choosing the most appropriate time unit.
     * Prefers larger units when possible (e.g., 60 seconds → 1 minute).
     */
    public static IntervalFreshness fromDuration(Duration duration) {
        if (duration.equals(duration.truncatedTo(ChronoUnit.DAYS))) {
            return new IntervalFreshness((int) duration.toDays(), TimeUnit.DAY);
        }
        if (duration.equals(duration.truncatedTo(ChronoUnit.HOURS))) {
            return new IntervalFreshness((int) duration.toHours(), TimeUnit.HOUR);
        }
        if (duration.equals(duration.truncatedTo(ChronoUnit.MINUTES))) {
            return new IntervalFreshness((int) duration.toMinutes(), TimeUnit.MINUTE);
        }

        return new IntervalFreshness((int) duration.getSeconds(), TimeUnit.SECOND);
    }

    /**
     * @deprecated Use {@link #getIntervalInt()} instead.
     */
    @Deprecated
    public String getInterval() {
        return String.valueOf(interval);
    }

    public int getIntervalInt() {
        return interval;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    public Duration toDuration() {
        switch (timeUnit) {
            case SECOND:
                return Duration.ofSeconds(interval);
            case MINUTE:
                return Duration.ofMinutes(interval);
            case HOUR:
                return Duration.ofHours(interval);
            case DAY:
                return Duration.ofDays(interval);
            default:
                throw new IllegalStateException("Unexpected value: " + timeUnit);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IntervalFreshness that = (IntervalFreshness) o;
        return Objects.equals(interval, that.interval) && timeUnit == that.timeUnit;
    }

    @Override
    public String toString() {
        return "INTERVAL '" + interval + "' " + timeUnit;
    }

    @Override
    public int hashCode() {
        return Objects.hash(interval, timeUnit);
    }

    // --------------------------------------------------------------------------------------------
    // TimeUnit enums
    // --------------------------------------------------------------------------------------------

    /** An enumeration of time unit representing the unit of interval freshness. */
    @PublicEvolving
    public enum TimeUnit {
        SECOND,
        MINUTE,
        HOUR,
        DAY
    }
}
