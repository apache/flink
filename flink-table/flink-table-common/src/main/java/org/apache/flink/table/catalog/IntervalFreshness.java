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
import java.util.Objects;

/**
 * The {@link IntervalFreshness} represents freshness definition of {@link
 * CatalogMaterializedTable}. It encapsulates the string interval value along with time unit,
 * allowing for flexible representation of different freshness type. Moreover, it can provide
 * detailed raw information for some specific operations.
 */
@PublicEvolving
public class IntervalFreshness {

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
        final String errorMessage =
                String.format(
                        "The freshness interval currently only supports positive integer type values. But was: %s",
                        interval);
        final int parsedInt;
        try {
            parsedInt = Integer.parseInt(interval);
        } catch (Exception e) {
            throw new ValidationException(errorMessage, e);
        }

        if (parsedInt <= 0) {
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

    /**
     * Creates an IntervalFreshness from a Duration, choosing the most appropriate time unit.
     * Prefers larger units when possible (e.g., 60 seconds → 1 minute).
     */
    public static IntervalFreshness fromDuration(Duration duration) {
        long totalSeconds = duration.getSeconds();

        long days = duration.toDays();
        if (days * 24 * 60 * 60 == totalSeconds) {
            return IntervalFreshness.ofDay(String.valueOf(days));
        }

        long hours = duration.toHours();
        if (hours * 60 * 60 == totalSeconds) {
            return IntervalFreshness.ofHour(String.valueOf(hours));
        }

        long minutes = duration.toMinutes();
        if (minutes * 60 == totalSeconds) {
            return IntervalFreshness.ofMinute(String.valueOf(minutes));
        }

        return IntervalFreshness.ofSecond(String.valueOf(totalSeconds));
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
