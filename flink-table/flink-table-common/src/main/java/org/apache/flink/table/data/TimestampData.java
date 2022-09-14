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

package org.apache.flink.table.data;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.util.Preconditions;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * An internal data structure representing data of {@link TimestampType} and {@link
 * LocalZonedTimestampType}.
 *
 * <p>This data structure is immutable and consists of a milliseconds and nanos-of-millisecond since
 * {@code 1970-01-01 00:00:00}. It might be stored in a compact representation (as a long value) if
 * values are small enough.
 */
@PublicEvolving
public final class TimestampData implements Comparable<TimestampData> {

    // the number of milliseconds in a day
    private static final long MILLIS_PER_DAY = 86400000; // = 24 * 60 * 60 * 1000

    // this field holds the integral second and the milli-of-second
    private final long millisecond;

    // this field holds the nano-of-millisecond
    private final int nanoOfMillisecond;

    private TimestampData(long millisecond, int nanoOfMillisecond) {
        Preconditions.checkArgument(nanoOfMillisecond >= 0 && nanoOfMillisecond <= 999_999);
        this.millisecond = millisecond;
        this.nanoOfMillisecond = nanoOfMillisecond;
    }

    /** Returns the number of milliseconds since {@code 1970-01-01 00:00:00}. */
    public long getMillisecond() {
        return millisecond;
    }

    /**
     * Returns the number of nanoseconds (the nanoseconds within the milliseconds).
     *
     * <p>The value range is from 0 to 999,999.
     */
    public int getNanoOfMillisecond() {
        return nanoOfMillisecond;
    }

    /** Converts this {@link TimestampData} object to a {@link Timestamp}. */
    public Timestamp toTimestamp() {
        return Timestamp.valueOf(toLocalDateTime());
    }

    /** Converts this {@link TimestampData} object to a {@link LocalDateTime}. */
    public LocalDateTime toLocalDateTime() {
        int date = (int) (millisecond / MILLIS_PER_DAY);
        int time = (int) (millisecond % MILLIS_PER_DAY);
        if (time < 0) {
            --date;
            time += MILLIS_PER_DAY;
        }
        long nanoOfDay = time * 1_000_000L + nanoOfMillisecond;
        LocalDate localDate = LocalDate.ofEpochDay(date);
        LocalTime localTime = LocalTime.ofNanoOfDay(nanoOfDay);
        return LocalDateTime.of(localDate, localTime);
    }

    /** Converts this {@link TimestampData} object to a {@link Instant}. */
    public Instant toInstant() {
        long epochSecond = millisecond / 1000;
        int milliOfSecond = (int) (millisecond % 1000);
        if (milliOfSecond < 0) {
            --epochSecond;
            milliOfSecond += 1000;
        }
        long nanoAdjustment = milliOfSecond * 1_000_000 + nanoOfMillisecond;
        return Instant.ofEpochSecond(epochSecond, nanoAdjustment);
    }

    @Override
    public int compareTo(TimestampData that) {
        int cmp = Long.compare(this.millisecond, that.millisecond);
        if (cmp == 0) {
            cmp = this.nanoOfMillisecond - that.nanoOfMillisecond;
        }
        return cmp;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TimestampData)) {
            return false;
        }
        TimestampData that = (TimestampData) obj;
        return this.millisecond == that.millisecond
                && this.nanoOfMillisecond == that.nanoOfMillisecond;
    }

    @Override
    public String toString() {
        return toLocalDateTime().toString();
    }

    @Override
    public int hashCode() {
        int ret = (int) millisecond ^ (int) (millisecond >> 32);
        return 31 * ret + nanoOfMillisecond;
    }

    // ------------------------------------------------------------------------------------------
    // Constructor Utilities
    // ------------------------------------------------------------------------------------------

    /**
     * Creates an instance of {@link TimestampData} from milliseconds.
     *
     * <p>The nanos-of-millisecond field will be set to zero.
     *
     * @param milliseconds the number of milliseconds since {@code 1970-01-01 00:00:00}; a negative
     *     number is the number of milliseconds before {@code 1970-01-01 00:00:00}
     */
    public static TimestampData fromEpochMillis(long milliseconds) {
        return new TimestampData(milliseconds, 0);
    }

    /**
     * Creates an instance of {@link TimestampData} from milliseconds and a nanos-of-millisecond.
     *
     * @param milliseconds the number of milliseconds since {@code 1970-01-01 00:00:00}; a negative
     *     number is the number of milliseconds before {@code 1970-01-01 00:00:00}
     * @param nanosOfMillisecond the nanoseconds within the millisecond, from 0 to 999,999
     */
    public static TimestampData fromEpochMillis(long milliseconds, int nanosOfMillisecond) {
        return new TimestampData(milliseconds, nanosOfMillisecond);
    }

    /**
     * Creates an instance of {@link TimestampData} from an instance of {@link LocalDateTime}.
     *
     * @param dateTime an instance of {@link LocalDateTime}
     */
    public static TimestampData fromLocalDateTime(LocalDateTime dateTime) {
        long epochDay = dateTime.toLocalDate().toEpochDay();
        long nanoOfDay = dateTime.toLocalTime().toNanoOfDay();

        long millisecond = epochDay * MILLIS_PER_DAY + nanoOfDay / 1_000_000;
        int nanoOfMillisecond = (int) (nanoOfDay % 1_000_000);

        return new TimestampData(millisecond, nanoOfMillisecond);
    }

    /**
     * Creates an instance of {@link TimestampData} from an instance of {@link Timestamp}.
     *
     * @param timestamp an instance of {@link Timestamp}
     */
    public static TimestampData fromTimestamp(Timestamp timestamp) {
        return fromLocalDateTime(timestamp.toLocalDateTime());
    }

    /**
     * Creates an instance of {@link TimestampData} from an instance of {@link Instant}.
     *
     * @param instant an instance of {@link Instant}
     */
    public static TimestampData fromInstant(Instant instant) {
        long epochSecond = instant.getEpochSecond();
        int nanoSecond = instant.getNano();

        long millisecond = epochSecond * 1_000 + nanoSecond / 1_000_000;
        int nanoOfMillisecond = nanoSecond % 1_000_000;

        return new TimestampData(millisecond, nanoOfMillisecond);
    }

    /**
     * Returns whether the timestamp data is small enough to be stored in a long of milliseconds.
     */
    public static boolean isCompact(int precision) {
        return precision <= 3;
    }
}
