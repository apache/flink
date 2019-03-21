/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.functions;

import org.apache.flink.api.java.tuple.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Utility functions for datetime types: date, time, timestamp.
 * Currently, it is a bit messy putting date time functions in various classes because
 * the runtime module does not depend on calcite..
 */
public class DateTimeUtils {

	private static final Logger LOG = LoggerFactory.getLogger(DateTimeUtils.class);

	private static final TimeZone LOCAL_TZ = TimeZone.getDefault();

	private static final String[] DEFAULT_DATETIME_FORMATS = new String[]{
		"yyyy-MM-dd HH:mm:ss",
		"yyyy-MM-dd HH:mm:ss.S",
		"yyyy-MM-dd HH:mm:ss.SS",
		"yyyy-MM-dd HH:mm:ss.SSS"
	};

	/**
	 * The number of milliseconds in a day.
	 *
	 * <p>This is the modulo 'mask' used when converting
	 * TIMESTAMP values to DATE and TIME values.
	 */
	private static final long MILLIS_PER_DAY = 86400000L; // = 24 * 60 * 60 * 1000

	/**
	 * A ThreadLocal cache map for SimpleDateFormat, because SimpleDateFormat is not thread-safe.
	 * (format, timezone) => formatter
	 */
	private static final ThreadLocalCache<Tuple2<String, TimeZone>, SimpleDateFormat> FORMATTER_CACHE =
		new ThreadLocalCache<Tuple2<String, TimeZone>, SimpleDateFormat>(64) {
			@Override
			public SimpleDateFormat getNewInstance(Tuple2<String, TimeZone> key) {
				SimpleDateFormat sdf = new SimpleDateFormat(key.f0);
				sdf.setTimeZone(key.f1);
				return sdf;
			}
		};

	/** Converts the Java type used for UDF parameters of SQL TIME type
	 * ({@link java.sql.Time}) to internal representation (int).
	 *
	 * <p>Converse of {@link #internalToTime(int)}. */
	public static int toInt(java.sql.Time v) {
		return (int) (toLong(v) % MILLIS_PER_DAY);
	}

	/** Converts the Java type used for UDF parameters of SQL DATE type
	 * ({@link java.sql.Date}) to internal representation (int).
	 *
	 * <p>Converse of {@link #internalToDate(int)}. */
	public static int toInt(java.util.Date v) {
		return toInt(v, LOCAL_TZ);
	}

	public static int toInt(java.util.Date v, TimeZone timeZone) {
		return (int) (toLong(v, timeZone) / MILLIS_PER_DAY);
	}

	public static long toLong(java.util.Date v) {
		return toLong(v, LOCAL_TZ);
	}

	/** Converts the Java type used for UDF parameters of SQL TIMESTAMP type
	 * ({@link java.sql.Timestamp}) to internal representation (long).
	 *
	 * <p>Converse of {@link #internalToTimestamp(long)}. */
	public static long toLong(Timestamp v) {
		return toLong(v, LOCAL_TZ);
	}

	public static long toLong(java.util.Date v, TimeZone timeZone) {
		long time = v.getTime();
		return time + (long) timeZone.getOffset(time);
	}

	/** Converts the internal representation of a SQL DATE (int) to the Java
	 * type used for UDF parameters ({@link java.sql.Date}). */
	public static java.sql.Date internalToDate(int v) {
		final long t = v * MILLIS_PER_DAY;
		return new java.sql.Date(t - LOCAL_TZ.getOffset(t));
	}

	/** Converts the internal representation of a SQL TIME (int) to the Java
	 * type used for UDF parameters ({@link java.sql.Time}). */
	public static java.sql.Time internalToTime(int v) {
		return new java.sql.Time(v - LOCAL_TZ.getOffset(v));
	}

	/** Converts the internal representation of a SQL TIMESTAMP (long) to the Java
	 * type used for UDF parameters ({@link java.sql.Timestamp}). */
	public static java.sql.Timestamp internalToTimestamp(long v) {
		return new java.sql.Timestamp(v - LOCAL_TZ.getOffset(v));
	}

	/**
	 * Parse date time string to timestamp based on the given time zone and
	 * "yyyy-MM-dd HH:mm:ss" format. Returns null if parsing failed.
	 *
	 * @param dateText the date time string
	 * @param tz the time zone
	 */
	public static Long strToTimestamp(String dateText, TimeZone tz) {
		return strToTimestamp(dateText, DEFAULT_DATETIME_FORMATS[0], tz);
	}

	/**
	 * Parse date time string to timestamp based on the given time zone and format.
	 * Returns null if parsing failed.
	 *
	 * @param dateText the date time string
	 * @param format date time string format
	 * @param tz the time zone
	 */
	public static Long strToTimestamp(String dateText, String format, TimeZone tz) {
		SimpleDateFormat formatter = FORMATTER_CACHE.get(Tuple2.of(format, tz));
		try {
			return formatter.parse(dateText).getTime();
		} catch (ParseException e) {
			return null;
		}
	}

	/**
	 * Format a timestamp as specific.
	 * @param ts the timestamp to format.
	 * @param format the string formatter.
	 * @param tz the time zone
	 */
	public static String dateFormat(long ts, String format, TimeZone tz) {
		SimpleDateFormat formatter = FORMATTER_CACHE.get(Tuple2.of(format, tz));
		Date dateTime = new Date(ts);
		return formatter.format(dateTime);
	}

	/**
	 * Convert a timestamp to string.
	 * @param ts the timestamp to convert.
	 * @param precision	the milli second precision to preserve
	 * @param tz the time zone
	 */
	public static String timestampToString(long ts, int precision, TimeZone tz) {
		int p = (precision <= 3 && precision >= 0) ? precision : 3;
		String format = DEFAULT_DATETIME_FORMATS[p];
		return dateFormat(ts, format, tz);
	}

	/** Helper for CAST({time} AS VARCHAR(n)). */
	public static String timeToString(int time) {
		final StringBuilder buf = new StringBuilder(8);
		timeToString(buf, time, 0); // set milli second precision to 0
		return buf.toString();
	}

	private static void timeToString(StringBuilder buf, int time, int precision) {
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
}
