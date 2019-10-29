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

import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.util.Preconditions;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;

import static org.apache.calcite.avatica.util.DateTimeUtils.timestampStringToUnixDate;
import static org.apache.calcite.avatica.util.DateTimeUtils.unixTimestamp;
import static org.apache.calcite.avatica.util.DateTimeUtils.unixTimestampToString;

/**
 * Immutable SQL TIMESTAMP type with nanosecond precision.
 */
public class PreciseTimestamp implements Comparable<PreciseTimestamp> {

	public static final int MAX_PRECISION = TimestampType.MAX_PRECISION;

	public static final int MIN_PRECISION = TimestampType.MIN_PRECISION;

	private final long milliseconds;

	private final int nanoseconds;

	private final int precision;

	private PreciseTimestamp(long milliseconds, int nanoseconds, int precision) {
		Preconditions.checkArgument(precision >= MIN_PRECISION && precision <= MAX_PRECISION);
		Preconditions.checkArgument(nanoseconds >= 0 && nanoseconds <= 999_999_999);
		this.milliseconds = milliseconds;
		this.nanoseconds = nanoseconds;
		this.precision = precision;
	}

	@Override
	public int compareTo(PreciseTimestamp that) {
		return this.milliseconds == that.milliseconds ?
			Integer.compare(this.nanoseconds, that.nanoseconds) :
			Long.compare(this.milliseconds, that.milliseconds);
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof PreciseTimestamp)) {
			return false;
		}
		PreciseTimestamp that = (PreciseTimestamp) obj;
		return this.compareTo(that) == 0;
	}

	@Override
	public String toString() {
		// yyyy-MM-dd HH:mm:ss
		StringBuilder timestampString = new StringBuilder(unixTimestampToString(milliseconds));
		if (precision > 0 && nanoseconds > 0) {
			// append fraction part
			timestampString.append('.').append(nonosSecondsToString(nanoseconds, precision));
		}
		return timestampString.toString();
	}

	public long toLong() {
		return milliseconds;
	}

	public static PreciseTimestamp fromLong(long milliseconds, int precision) {
		Timestamp t = new Timestamp(milliseconds);
		long millis = isCompact(precision) ?
			zeroLastNDigits(t.getTime(), 3 - precision) : t.getTime();
		int nanos = (int) zeroLastNDigits((long) t.getNanos(), 9 - precision);
		return new PreciseTimestamp(millis, nanos, precision);
	}

	public Timestamp toTimestamp() {
		return Timestamp.valueOf(this.toString());
	}

	public static PreciseTimestamp fromTimestamp(Timestamp t, int precision) {
		String timestampString = t.toString();
		long millis = timestampStringToUnixDate(timestampString);
		int nanos = timestampStringToNanoseconds(timestampString);
		return new PreciseTimestamp(millis, nanos, precision);
	}

	public LocalDateTime toLocalDateTime() {
		long epochSeconds = (milliseconds >= 0 || nanoseconds == 0) ?
			(milliseconds / 1000) : (milliseconds / 1000 - 1);
		return LocalDateTime.ofEpochSecond(epochSeconds, nanoseconds, ZoneOffset.UTC);
	}

	public static PreciseTimestamp fromLocalDateTime(LocalDateTime ldt, int precision) {
		int nanos = (int) zeroLastNDigits((long) ldt.getNano(), 9 - precision);
		long millis = unixTimestamp(
			ldt.getYear(),
			ldt.getMonthValue(),
			ldt.getDayOfMonth(),
			ldt.getHour(),
			ldt.getMinute(),
			ldt.getSecond()) + (long) (nanos / 1_000_000);
		return new PreciseTimestamp(millis, nanos, precision);
	}

	public byte[] toUnscaledBytes() {
		byte[] bytes8 = new byte[8];
		long l = milliseconds;
		for (int i = 0; i < 8; i++) {
			bytes8[7 - i] = (byte) l;
			l >>>= 8;
		}

		if (isCompact()) {
			return bytes8;
		}

		byte[] bytes12 = new byte[12];
		System.arraycopy(bytes8, 0, bytes12, 0, 8);
		int n = nanoseconds;
		for (int i = 0; i < 4; i++) {
			bytes12[11 - i] = (byte) n;
			n >>>= 8;
		}

		return bytes12;
	}

	public static PreciseTimestamp fromUnscaledBytes(byte[] bytes, int precision) {
		assert bytes.length == 8 || bytes.length == 12;

		long l = 0;
		for (int i = 0; i < 8; i++) {
			l <<= 8;
			l |= (bytes[i] & (0xff));
		}

		if (isCompact(precision)) {
			assert bytes.length == 8;
			return new PreciseTimestamp(l, 0, precision);
		}

		assert bytes.length == 12;
		int n = 0;
		for (int i = 8; i < 12; i++) {
			n <<= 8;
			n |= (bytes[i] & (0xff));
		}
		return new PreciseTimestamp(l, n, precision);
	}

	public PreciseTimestamp copy() {
		return new PreciseTimestamp(milliseconds, nanoseconds, precision);
	}

	public static PreciseTimestamp zero(int precision) {
		return fromLong(0, precision);
	}

	public boolean isCompact() {
		return this.precision <= 3;
	}

	public static boolean isCompact(int precision) {
		return precision <= 3;
	}

	private static long zeroLastNDigits(long number, int n) {
		long tenToTheN = (long) Math.pow(10, n);
		return (number / tenToTheN) * tenToTheN;
	}

	private static int timestampStringToNanoseconds(String s) {
		final int dot = s.indexOf('.');
		if (dot < 0) {
			return 0;
		} else {
			return parseFraction(s.substring(dot + 1).trim(), 100_000_000);
		}
	}

	/** Parses a fraction, multiplying the first character by {@code multiplier},
	 * the second character by {@code multiplier / 10},
	 * the third character by {@code multiplier / 100}, and so forth.
	 *
	 * <p>Copied from Apache Calcite Avatica (DateTimeUtils::parseFraction L687-L719)
	 *
	 * <p>For example, {@code parseFraction("1234", 100)} yields {@code 123}. */
	private static int parseFraction(String v, int multiplier) {
		int r = 0;
		for (int i = 0; i < v.length(); i++) {
			char c = v.charAt(i);
			int x = c < '0' || c > '9' ? 0 : (c - '0');
			r += multiplier * x;
			if (multiplier < 10) {
				// We're at the last digit. Check for rounding.
				if (i + 1 < v.length()
					&& v.charAt(i + 1) >= '5') {
					++r;
				}
				break;
			}
			multiplier /= 10;
		}
		return r;
	}

	private static String nonosSecondsToString(int nanoseconds, int precision) {
		final char[] buf = new char[9];
		Arrays.fill(buf, '0');
		char[] digits = Integer.toString(nanoseconds).toCharArray();
		int size = digits.length;
		System.arraycopy(digits, 0, buf, 9 - size , size);
		return new String(buf, 0, precision);
	}
}
