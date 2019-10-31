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

import org.apache.flink.util.Preconditions;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * Immutable SQL TIMESTAMP type with nanosecond precision.
 *
 * <p>This class is composite of a millisecond and nanoOfMillisecond. The millisecond part
 * holds the integral second and the milli-of-second. The nanoOfMillisecond holds the
 * nano-of-millisecond, which should between 0 - 999_999.
 */
public class DateTime implements Comparable<DateTime> {

	// the number of milliseconds in a day
	private static final long MILLIS_PER_DAY = 86400000; // = 24 * 60 * 60 * 1000

	// this field holds the integral second and the milli-of-second
	private final long millisecond;

	// this field holds the nano-of-millisecond
	private final int nanoOfMillisecond;

	private DateTime(long millisecond, int nanoOfMillisecond) {
		Preconditions.checkArgument(nanoOfMillisecond >= 0 && nanoOfMillisecond <= 999_999);
		this.millisecond = millisecond;
		this.nanoOfMillisecond = nanoOfMillisecond;
	}

	@Override
	public int compareTo(DateTime that) {
		int cmp = Long.compare(this.millisecond, that.millisecond);
		if (cmp == 0) {
			cmp = this.nanoOfMillisecond - that.nanoOfMillisecond;
		}
		return cmp;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof DateTime)) {
			return false;
		}
		DateTime that = (DateTime) obj;
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
	 * Obtains an instance of {@code DateTime} from a millisecond.
	 *
	 * <p>This returns a {@code DateTime} with the specified millisecond.
	 * The nanoOfMillisecond field will be set to zero.
	 *
	 * @param millisecond the number of milliseconds since January 1, 1970, 00:00:00 GMT
	 *                    A negative number is the number of milliseconds before
	 *                    January 1, 1970, 00:00:00 GMT
	 * @return an instance of {@code DateTime}
	 */
	public static DateTime fromEpochMillis(long millisecond) {
		return new DateTime(millisecond, 0);
	}

	/**
	 * Obtains an instance of {@code DateTime} from a millisecond and a nanoOfMillisecond.
	 *
	 * <p>This returns a {@code DateTime} with the specified millisecond and nanoOfMillisecond.
	 *
	 * @param millisecond the number of milliseconds since January 1, 1970, 00:00:00 GMT
	 *                    A negative number is the number of milliseconds before
	 *                    January 1, 1970, 00:00:00 GMT
	 * @param nanoOfMillisecond the nanosecond within the millisecond, from 0 to 999,999
	 * @return an instance of {@code DateTime}
	 */
	public static DateTime fromEpochMillis(long millisecond, int nanoOfMillisecond) {
		return new DateTime(millisecond, nanoOfMillisecond);
	}

	/**
	 * Convert this {@code Timestmap} object to a {@link java.sql.Timestamp}.
	 *
	 * @return An instance of {@link java.sql.Timestamp}
	 */
	public java.sql.Timestamp toTimestamp() {
		return java.sql.Timestamp.valueOf(toLocalDateTime());
	}

	/**
	 * Obtains an instance of {@code DateTime} from an instance of {@link java.sql.Timestamp}.
	 *
	 * <p>This returns a {@code DateTime} with the specified {@link java.sql.Timestamp}.
	 *
	 * @param ts an instance of {@link java.sql.Timestamp}
	 * @return an instance of {@code DateTime}
	 */
	public static DateTime fromTimestamp(java.sql.Timestamp ts) {
		return fromLocalDateTime(ts.toLocalDateTime());
	}

	/**
	 * Convert this {@code DateTime} object to a {@link LocalDateTime}.
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
	 * Obtains an instance of {@code DateTime} from an instance of {@link LocalDateTime}.
	 *
	 * <p>This returns a {@code DateTime} with the specified {@link LocalDateTime}.
	 *
	 * @param dateTime an instance of {@link LocalDateTime}
	 * @return an instance of {@code DateTime}
	 */
	public static DateTime fromLocalDateTime(LocalDateTime dateTime) {
		long epochDay = dateTime.toLocalDate().toEpochDay();
		long nanoOfDay = dateTime.toLocalTime().toNanoOfDay();

		long millisecond = epochDay * MILLIS_PER_DAY + nanoOfDay / 1_000_000;
		int nanoOfMillisecond = (int) (nanoOfDay % 1_000_000);

		return new DateTime(millisecond, nanoOfMillisecond);
	}

	public static boolean isCompact(int precision) {
		return precision <= 3;
	}
}
