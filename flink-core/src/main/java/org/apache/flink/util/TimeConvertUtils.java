/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util;

import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.Date;
import java.util.TimeZone;

/**
 * A {@link TimeConvertUtils} is used to convert Time Type.
 */
public class TimeConvertUtils {

	public static final Charset UTF_8 = Charset.forName("UTF-8");

	private static final TimeZone UTC_TZ = TimeZone.getTimeZone("UTC");

	/** The julian date of the epoch, 1970-01-01. */
	public static final int EPOCH_JULIAN = 2440588;

	/**
	 * The number of milliseconds in a second.
	 */
	public static final long MILLIS_PER_SECOND = 1000L;

	/**
	 * The number of milliseconds in a minute.
	 */
	public static final long MILLIS_PER_MINUTE = 60000L;

	/**
	 * The number of milliseconds in an hour.
	 */
	public static final long MILLIS_PER_HOUR = 3600000L; // = 60 * 60 * 1000

	/**
	 * The number of milliseconds in a day.
	 *
	 * <p>This is the modulo 'mask' used when converting
	 * TIMESTAMP values to DATE and TIME values.
	 */
	public static final long MILLIS_PER_DAY = 86400000; // = 24 * 60 * 60 * 1000

	public static int toInt(Date v) {
		return toInt(v, UTC_TZ);
	}

	public static int toInt(Date v, TimeZone timeZone) {
		return (int) (toLong(v, timeZone) / MILLIS_PER_DAY);
	}

	public static int toInt(java.sql.Time v) {
		return (int) (toLong(v) % MILLIS_PER_DAY);
	}

	public static long toLong(Date v) {
		return toLong(v, UTC_TZ);
	}

	// mainly intended for java.sql.Timestamp but works for other dates also
	public static long toLong(Date v, TimeZone timeZone) {
		final long time = v.getTime();
		return time + timeZone.getOffset(time);
	}

	public static long toLong(Timestamp v) {
		return toLong(v, UTC_TZ);
	}

	public static long toLong(Object o) {
		return o instanceof Long ? (Long) o
				: o instanceof Number ? toLong(o)
				: o instanceof String ? toLong(o)
				: (Long) cannotConvert(o, long.class);
	}

	private static Object cannotConvert(Object o, Class toType) {
		throw new RuntimeException("Cannot convert " + o + " to " + toType);
	}

	public static Timestamp internalToTimestamp(long v) {
		return new Timestamp(v - UTC_TZ.getOffset(v));
	}

	public static Timestamp internalToTimestamp(Long v) {
		return v == null ? null : internalToTimestamp(v.longValue());
	}

	public static java.sql.Date internalToDate(int v) {
		final long t = v * MILLIS_PER_DAY;
		return new java.sql.Date(t - UTC_TZ.getOffset(t));
	}

	public static java.sql.Date internalToDate(Integer v) {
		return v == null ? null : internalToDate(v.intValue());
	}

	public static java.sql.Time internalToTime(int v) {
		return new java.sql.Time(v - UTC_TZ.getOffset(v));
	}

	public static java.sql.Time internalToTime(Integer v) {
		return v == null ? null : internalToTime(v.intValue());
	}

	public static int dateStringToUnixDate(String s) {
		int hyphen1 = s.indexOf('-');
		int y;
		int m;
		int d;
		if (hyphen1 < 0) {
			y = Integer.parseInt(s.trim());
			m = 1;
			d = 1;
		} else {
			y = Integer.parseInt(s.substring(0, hyphen1).trim());
			final int hyphen2 = s.indexOf('-', hyphen1 + 1);
			if (hyphen2 < 0) {
				m = Integer.parseInt(s.substring(hyphen1 + 1).trim());
				d = 1;
			} else {
				m = Integer.parseInt(s.substring(hyphen1 + 1, hyphen2).trim());
				d = Integer.parseInt(s.substring(hyphen2 + 1).trim());
			}
		}
		return ymdToUnixDate(y, m, d);
	}

	public static int timeStringToUnixDate(String v) {
		return timeStringToUnixDate(v, 0);
	}

	public static int timeStringToUnixDate(String v, int start) {
		final int colon1 = v.indexOf(':', start);
		int hour;
		int minute;
		int second;
		int milli;
		if (colon1 < 0) {
			hour = Integer.parseInt(v.trim());
			minute = 1;
			second = 1;
			milli = 0;
		} else {
			hour = Integer.parseInt(v.substring(start, colon1).trim());
			final int colon2 = v.indexOf(':', colon1 + 1);
			if (colon2 < 0) {
				minute = Integer.parseInt(v.substring(colon1 + 1).trim());
				second = 1;
				milli = 0;
			} else {
				minute = Integer.parseInt(v.substring(colon1 + 1, colon2).trim());
				int dot = v.indexOf('.', colon2);
				if (dot < 0) {
					second = Integer.parseInt(v.substring(colon2 + 1).trim());
					milli = 0;
				} else {
					second = Integer.parseInt(v.substring(colon2 + 1, dot).trim());
					milli = parseFraction(v.substring(dot + 1).trim(), 100);
				}
			}
		}
		return hour * (int) MILLIS_PER_HOUR
				+ minute * (int) MILLIS_PER_MINUTE
				+ second * (int) MILLIS_PER_SECOND
				+ milli;
	}

	/** Parses a fraction, multiplying the first character by {@code multiplier},
	 * the second character by {@code multiplier / 10},
	 * the third character by {@code multiplier / 100}, and so forth.
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

	public static long timestampStringToUnixDate(String s) {
		final long d;
		final long t;
		s = s.trim();
		int space = s.indexOf(' ');
		if (space >= 0) {
			d = dateStringToUnixDate(s.substring(0, space));
			t = timeStringToUnixDate(s, space + 1);
		} else {
			int hyphen = s.indexOf('-');
			if (hyphen >= 0) {
				d = dateStringToUnixDate(s);
				t = 0;
			} else {
				// Numeric To Timestamp : Precision is second
				d = 0;
				t = Integer.parseInt(s) * 1000;
			}
		}
		return d * MILLIS_PER_DAY + t;
	}

	public static String unixTimestampToString(long timestamp) {
		return unixTimestampToString(timestamp, 0);
	}

	public static String unixTimestampToString(long timestamp, int precision) {
		final StringBuilder buf = new StringBuilder(17);
		int date = (int) (timestamp / MILLIS_PER_DAY);
		int time = (int) (timestamp % MILLIS_PER_DAY);
		if (time < 0) {
			--date;
			time += MILLIS_PER_DAY;
		}
		unixDateToString(buf, date);
		buf.append(' ');
		unixTimeToString(buf, time, precision);
		return buf.toString();
	}

	/** Helper for CAST({timestamp} AS VARCHAR(n)). */
	public static String unixTimeToString(int time) {
		return unixTimeToString(time, 0);
	}

	public static String unixTimeToString(int time, int precision) {
		final StringBuilder buf = new StringBuilder(8);
		unixTimeToString(buf, time, precision);
		return buf.toString();
	}

	private static void unixTimeToString(StringBuilder buf, int time, int precision) {
		while (time < 0) {
			time += MILLIS_PER_DAY;
		}
		int h = time / 3600000;
		int time2 = time % 3600000;
		int m = time2 / 60000;
		int time3 = time2 % 60000;
		int s = time3 / 1000;
		int ms = time3 % 1000;
		int2(buf, h);
		buf.append(':');
		int2(buf, m);
		buf.append(':');
		int2(buf, s);
		if (precision > 0) {
			buf.append('.');
			while (precision > 0) {
				buf.append((char) ('0' + (ms / 100)));
				ms = ms % 100;
				ms = ms * 10;

				// keep consistent with Timestamp.toString()
				if (ms == 0) {
					break;
				}

				--precision;
			}
		}
	}

	private static void int2(StringBuilder buf, int i) {
		buf.append((char) ('0' + (i / 10) % 10));
		buf.append((char) ('0' + i % 10));
	}

	private static void int4(StringBuilder buf, int i) {
		buf.append((char) ('0' + (i / 1000) % 10));
		buf.append((char) ('0' + (i / 100) % 10));
		buf.append((char) ('0' + (i / 10) % 10));
		buf.append((char) ('0' + i % 10));
	}

	public static String unixDateToString(int date) {
		final StringBuilder buf = new StringBuilder(10);
		unixDateToString(buf, date);
		return buf.toString();
	}

	private static void unixDateToString(StringBuilder buf, int date) {
		julianToString(buf, date + EPOCH_JULIAN);
	}

	private static void julianToString(StringBuilder buf, int julian) {
		// Algorithm the book "Astronomical Algorithms" by Jean Meeus, 1998
		int b, c;
		if (julian > 2299160) {
			int a = julian + 32044;
			b = (4 * a + 3) / 146097;
			c = a - b * 146097 / 4;
		} else {
			b = 0;
			c = julian + 32082;
		}
		int d = (4 * c + 3) / 1461;
		int e = c - (1461 * d) / 4;
		int m = (5 * e + 2) / 153;
		int day = e - (153 * m + 2) / 5 + 1;
		int month = m + 3 - 12 * (m / 10);
		int year = b * 100 + d - 4800 + (m / 10);

		int4(buf, year);
		buf.append('-');
		int2(buf, month);
		buf.append('-');
		int2(buf, day);
	}

	public static int ymdToUnixDate(int year, int month, int day) {
		final int julian = ymdToJulian(year, month, day);
		return julian - EPOCH_JULIAN;
	}

	public static int ymdToJulian(int year, int month, int day) {
		int a = (14 - month) / 12;
		int y = year + 4800 - a;
		int m = month + 12 * a - 3;
		int j = day + (153 * m + 2) / 5
				+ 365 * y
				+ y / 4
				- y / 100
				+ y / 400
				- 32045;
		if (j < 2299161) {
			j = day + (153 * m + 2) / 5 + 365 * y + y / 4 - 32083;
		}
		return j;
	}

	public static String unixDateTimeToString(Object o) {
		if (o instanceof java.sql.Date) {
			return unixDateToString(toInt((java.sql.Date) o));
		} else if (o instanceof java.sql.Time) {
			return unixTimeToString(toInt((java.sql.Time) o));
		} else if (o instanceof Timestamp) {
			return unixTimestampToString(toLong((Timestamp) o), 3);
		} else {
			return o.toString();
		}
	}
}

