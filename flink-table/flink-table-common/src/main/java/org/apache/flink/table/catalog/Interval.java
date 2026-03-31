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
import org.apache.flink.table.utils.DateTimeUtils;

import java.time.Duration;
import java.time.Period;
import java.util.Objects;

/**
 * A class representing day-time and year-month intervals.
 *
 * <p>Depending on the time unit, the interval is backed day-time or year-month. See {@link
 * TimeUnit} for more details.
 */
@PublicEvolving
public class Interval {
    private final Duration duration;
    private final Period period;
    private final TimeUnit timeUnit;

    private Interval(TimeUnit timeUnit, Duration duration, Period period) {
        this.timeUnit = timeUnit;
        this.duration = duration;
        this.period = period;
    }

    public static Interval of(Duration duration, TimeUnit timeUnit) {
        return new Interval(timeUnit, duration, null);
    }

    public static Interval of(Period period, TimeUnit timeUnit) {
        return new Interval(timeUnit, null, period);
    }

    public static Interval of(int duration, TimeUnit timeUnit) {
        if (timeUnit.isDayTime) {
            return of(toDuration(duration, timeUnit), timeUnit);
        } else {
            return of(toPeriod(duration, timeUnit), timeUnit);
        }
    }

    public Duration getDuration() {
        return duration;
    }

    public Period getPeriod() {
        return period;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    public int getInterval() {
        switch (timeUnit) {
            case SECOND:
                return (int) duration.toSeconds();
            case MINUTE:
                return (int) duration.toMinutes();
            case HOUR:
                return (int) duration.toHours();
            case DAY:
                return (int) duration.toDays();
            case WEEK:
                return (int) (duration.toDays() / DateTimeUtils.DAYS_PER_WEEK);
            case MONTH:
                return period.getMonths();
            case QUARTER:
                return period.getMonths() / DateTimeUtils.MONTHS_PER_QUARTER;
            case YEAR:
                return period.getYears();
            default:
                // Could happen if new TimeUnit is introduced and not supported here
                throw new ValidationException(
                        String.format("TimeUnit %s is not supported", timeUnit));
        }
    }

    private static Duration toDuration(int interval, TimeUnit timeUnit) {
        switch (timeUnit) {
            case SECOND:
                return Duration.ofSeconds(interval);
            case MINUTE:
                return Duration.ofMinutes(interval);
            case HOUR:
                return Duration.ofHours(interval);
            case DAY:
                return Duration.ofDays(interval);
            case WEEK:
                return Duration.ofDays((long) interval * DateTimeUtils.DAYS_PER_WEEK);
            default:
                throw new IllegalArgumentException("Unsupported time unit: " + timeUnit);
        }
    }

    private static Period toPeriod(int interval, TimeUnit timeUnit) {
        switch (timeUnit) {
            case MONTH:
                return Period.ofMonths(interval);
            case QUARTER:
                return Period.ofMonths(interval * DateTimeUtils.MONTHS_PER_QUARTER);
            case YEAR:
                return Period.ofYears(interval);
            default:
                throw new IllegalArgumentException("Unsupported time unit: " + timeUnit);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Interval interval = (Interval) o;
        return Objects.equals(duration, interval.duration)
                && Objects.equals(period, interval.period)
                && timeUnit == interval.timeUnit;
    }

    @Override
    public int hashCode() {
        return Objects.hash(duration, period, timeUnit);
    }

    @Override
    public String toString() {
        return "INTERVAL '" + getInterval() + "' " + timeUnit;
    }

    // --------------------------------------------------------------------------------------------
    // TimeUnit enums
    // --------------------------------------------------------------------------------------------

    /** An enumeration of time unit representing the unit of interval. */
    @PublicEvolving
    public enum TimeUnit {
        SECOND(true),
        MINUTE(true),
        HOUR(true),
        DAY(true),
        WEEK(true),
        MONTH(false),
        QUARTER(false),
        YEAR(false);

        private final boolean isDayTime;

        TimeUnit(boolean isDayTime) {
            this.isDayTime = isDayTime;
        }
    }
}
