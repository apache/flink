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

import static org.apache.calcite.avatica.util.DateTimeUtils.MILLIS_PER_DAY;
import static org.apache.calcite.avatica.util.DateTimeUtils.unixTimestamp;

/**
 * Immutable SQL TIMESTAMP type with nanosecond precision.
 *
 * <p>This class is composite of a millisecond and nanoOfMillisecond. The millisecond part
 * holds the integral second and the milli-of-second. The nanoOfMillisecond holds the
 * nano-of-millisecond, which should between 0 - 999_999.
 */
public class Timestamp implements Comparable<Timestamp> {

	// this field holds the integral second and the milli-of-second
	private final long millisecond;

	// this field holds the nano-of-millisecond
	private final int nanoOfMillisecond;

	public Timestamp(long millisecond, int nanoOfMillisecond) {
		Preconditions.checkArgument(nanoOfMillisecond >= 0 && nanoOfMillisecond <= 999_999);
		this.millisecond = millisecond;
		this.nanoOfMillisecond = nanoOfMillisecond;
	}

	@Override
	public int compareTo(Timestamp that) {
		int cmp = Long.compare(this.millisecond, that.millisecond);
		if (cmp == 0) {
			cmp = this.nanoOfMillisecond - that.nanoOfMillisecond;
		}
		return cmp;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Timestamp)) {
			return false;
		}
		Timestamp that = (Timestamp) obj;
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
	 * Obtains an instance of {@code Timestamp} from a millisecond.
	 *
	 * <p>This returns a {@code Timestamp} with the specified millisecond.
	 * The nanoOfMillisecond field will be set to zero.
	 *
	 * @param millisecond the number of milliseconds since January 1, 1970, 00:00:00 GMT
	 *                    A negative number is the number of milliseconds before
	 *                    January 1, 1970, 00:00:00 GMT
	 * @return an instance of {@code Timestamp}
	 */
	public static Timestamp fromLong(long millisecond) {
		return new Timestamp(millisecond, 0);
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
	 * Obtains an instance of {@code Timestamp} from an instance of {@link java.sql.Timestamp}.
	 *
	 * <p>This returns a {@code Timestamp} with the specified {@link java.sql.Timestamp}.
	 *
	 * @param ts an instance of {@link java.sql.Timestamp}
	 * @return an instance of {@code Timestamp}
	 */
	public static Timestamp fromTimestamp(java.sql.Timestamp ts) {
		return fromLocalDateTime(ts.toLocalDateTime());
	}

	/**
	 * Convert this {@code Timestamp} object to a {@link LocalDateTime}.
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
	 * Obtains an instance of {@code Timestamp} from an instance of {@link LocalDateTime}.
	 *
	 * <p>This returns a {@code Timestamp} with the specified {@link LocalDateTime}.
	 *
	 * @param dateTime an instance of {@link LocalDateTime}
	 * @return an instance of {@code Timestamp}
	 */
	public static Timestamp fromLocalDateTime(LocalDateTime dateTime) {
		int nano = dateTime.getNano();
		long millis = unixTimestamp(
			dateTime.getYear(),
			dateTime.getMonthValue(),
			dateTime.getDayOfMonth(),
			dateTime.getHour(),
			dateTime.getMinute(),
			dateTime.getSecond()) + (long) (nano / 1_000_000);
		int nanosOfMilli = nano % 1_000_000;
		return new Timestamp(millis, nanosOfMilli);
	}

	public static boolean isCompact(int precision) {
		return precision <= 3;
	}
}
