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

package org.apache.flink.table.dataformat;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.runtime.util.SegmentsUtil;
import org.apache.flink.util.Preconditions;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * Immutable SQL TIMESTAMP and TIMESTAMP_WITH_LOCAL_TIME_ZONE with nanosecond precision.
 *
 * <p>This class is composite of a millisecond and nanoOfMillisecond. The millisecond part
 * holds the integral second and the milli-of-second. The nanoOfMillisecond holds the
 * nano-of-millisecond, which should between 0 - 999_999.
 */
public class SqlTimestamp implements Comparable<SqlTimestamp> {

	// the number of milliseconds in a day
	private static final long MILLIS_PER_DAY = 86400000; // = 24 * 60 * 60 * 1000

	// this field holds the integral second and the milli-of-second
	private final long millisecond;

	// this field holds the nano-of-millisecond
	private final int nanoOfMillisecond;

	private SqlTimestamp(long millisecond, int nanoOfMillisecond) {
		Preconditions.checkArgument(nanoOfMillisecond >= 0 && nanoOfMillisecond <= 999_999);
		this.millisecond = millisecond;
		this.nanoOfMillisecond = nanoOfMillisecond;
	}

	@Override
	public int compareTo(SqlTimestamp that) {
		int cmp = Long.compare(this.millisecond, that.millisecond);
		if (cmp == 0) {
			cmp = this.nanoOfMillisecond - that.nanoOfMillisecond;
		}
		return cmp;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof SqlTimestamp)) {
			return false;
		}
		SqlTimestamp that = (SqlTimestamp) obj;
		return this.millisecond == that.millisecond &&
				this.nanoOfMillisecond == that.nanoOfMillisecond;
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

	public long getMillisecond() {
		return millisecond;
	}

	public int getNanoOfMillisecond() {
		return nanoOfMillisecond;
	}

	/**
	 * Obtains an instance of {@code SqlTimestamp} from a millisecond.
	 *
	 * <p>This returns a {@code SqlTimestamp} with the specified millisecond.
	 * The nanoOfMillisecond field will be set to zero.
	 *
	 * @param millisecond the number of milliseconds since January 1, 1970, 00:00:00 GMT
	 *                    A negative number is the number of milliseconds before
	 *                    January 1, 1970, 00:00:00 GMT
	 * @return an instance of {@code SqlTimestamp}
	 */
	public static SqlTimestamp fromEpochMillis(long millisecond) {
		return new SqlTimestamp(millisecond, 0);
	}

	/**
	 * Obtains an instance of {@code SqlTimestamp} from a millisecond and a nanoOfMillisecond.
	 *
	 * <p>This returns a {@code SqlTimestamp} with the specified millisecond and nanoOfMillisecond.
	 *
	 * @param millisecond the number of milliseconds since January 1, 1970, 00:00:00 GMT
	 *                    A negative number is the number of milliseconds before
	 *                    January 1, 1970, 00:00:00 GMT
	 * @param nanoOfMillisecond the nanosecond within the millisecond, from 0 to 999,999
	 * @return an instance of {@code SqlTimestamp}
	 */
	public static SqlTimestamp fromEpochMillis(long millisecond, int nanoOfMillisecond) {
		return new SqlTimestamp(millisecond, nanoOfMillisecond);
	}

	/**
	 * Convert this {@code SqlTimestmap} object to a {@link Timestamp}.
	 *
	 * @return An instance of {@link Timestamp}
	 */
	public Timestamp toTimestamp() {
		return Timestamp.valueOf(toLocalDateTime());
	}

	/**
	 * Obtains an instance of {@code SqlTimestamp} from an instance of {@link Timestamp}.
	 *
	 * <p>This returns a {@code SqlTimestamp} with the specified {@link Timestamp}.
	 *
	 * @param ts an instance of {@link Timestamp}
	 * @return an instance of {@code SqlTimestamp}
	 */
	public static SqlTimestamp fromTimestamp(Timestamp ts) {
		return fromLocalDateTime(ts.toLocalDateTime());
	}

	/**
	 * Convert this {@code SqlTimestamp} object to a {@link LocalDateTime}.
	 *
	 * @return An instance of {@link LocalDateTime}
	 */
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

	/**
	 * Obtains an instance of {@code SqlTimestamp} from an instance of {@link LocalDateTime}.
	 *
	 * <p>This returns a {@code SqlTimestamp} with the specified {@link LocalDateTime}.
	 *
	 * @param dateTime an instance of {@link LocalDateTime}
	 * @return an instance of {@code SqlTimestamp}
	 */
	public static SqlTimestamp fromLocalDateTime(LocalDateTime dateTime) {
		long epochDay = dateTime.toLocalDate().toEpochDay();
		long nanoOfDay = dateTime.toLocalTime().toNanoOfDay();

		long millisecond = epochDay * MILLIS_PER_DAY + nanoOfDay / 1_000_000;
		int nanoOfMillisecond = (int) (nanoOfDay % 1_000_000);

		return new SqlTimestamp(millisecond, nanoOfMillisecond);
	}

	/**
	 * Convert this {@code SqlTimestamp} object to a {@link Instant}.
	 *
	 * @return an instance of {@link Instant}
	 */
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

	/**
	 * Obtains an instance of {@code SqlTimestamp} from an instance of {@link Instant}.
	 *
	 * <p>This returns a {@code SqlTimestmap} with the specified {@link Instant}.
	 *
	 * @param instant an instance of {@link Instant}
	 * @return an instance of {@code SqlTimestamp}
	 */
	public static SqlTimestamp fromInstant(Instant instant) {
		long epochSecond = instant.getEpochSecond();
		int nanoSecond = instant.getNano();

		long millisecond = epochSecond * 1_000 + nanoSecond / 1_000_000;
		int nanoOfMillisecond = nanoSecond % 1_000_000;

		return new SqlTimestamp(millisecond, nanoOfMillisecond);
	}

	/**
	 * Apache Calcite and Flink's planner use the number of milliseconds since epoch to represent a
	 * Timestamp type compactly if the number of digits of fractional seconds is between 0 - 3.
	 *
	 * @param precision the number of digits of fractional seconds
	 * @return true if precision is less than or equal to 3, false otherwise
	 */
	public static boolean isCompact(int precision) {
		return precision <= 3;
	}

	/**
	 * Obtains an instance of {@code SqlTimestamp} from underlying {@link MemorySegment}.
	 *
	 * @param segments the underlying MemorySegments
	 * @param baseOffset the base offset of current instance of {@code SqlTimestamp}
	 * @param offsetAndNanos the offset of millseconds part and nanoseconds
	 * @return an instance of {@code SqlTimestamp}
	 */
	public static SqlTimestamp readTimestampFieldFromSegments(
			MemorySegment[] segments, int baseOffset, long offsetAndNanos) {
		final int nanoOfMillisecond = (int) offsetAndNanos;
		final int subOffset = (int) (offsetAndNanos >> 32);
		final long millisecond = SegmentsUtil.getLong(segments, baseOffset + subOffset);
		return SqlTimestamp.fromEpochMillis(millisecond, nanoOfMillisecond);
	}
}
