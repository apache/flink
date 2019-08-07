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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.utils.ThreadLocalCache;

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.TimeZone;

/**
 * Utility functions for datetime types: date, time, timestamp.
 * Currently, it is a bit messy putting date time functions in various classes because
 * the runtime module does not depend on calcite..
 */
public class SqlDateTimeUtils {

	private static final Logger LOG = LoggerFactory.getLogger(SqlDateTimeUtils.class);

	/** The julian date of the epoch, 1970-01-01. */
	private static final int EPOCH_JULIAN = 2440588;

	/**
	 * The number of milliseconds in a second.
	 */
	private static final long MILLIS_PER_SECOND = 1000L;

	/**
	 * The number of milliseconds in a minute.
	 */
	private static final long MILLIS_PER_MINUTE = 60000L;

	/**
	 * The number of milliseconds in an hour.
	 */
	private static final long MILLIS_PER_HOUR = 3600000L; // = 60 * 60 * 1000

	/**
	 * The number of milliseconds in a day.
	 *
	 * <p>This is the modulo 'mask' used when converting
	 * TIMESTAMP values to DATE and TIME values.
	 */
	private static final long MILLIS_PER_DAY = 86400000; // = 24 * 60 * 60 * 1000

	/** The SimpleDateFormat string for ISO dates, "yyyy-MM-dd". */
	private static final String DATE_FORMAT_STRING = "yyyy-MM-dd";

	/** The SimpleDateFormat string for ISO times, "HH:mm:ss". */
	private static final String TIME_FORMAT_STRING = "HH:mm:ss";

	/** The SimpleDateFormat string for ISO timestamps, "yyyy-MM-dd HH:mm:ss". */
	private static final String TIMESTAMP_FORMAT_STRING =
		DATE_FORMAT_STRING + " " + TIME_FORMAT_STRING;

	/** The UTC time zone. */
	public static final TimeZone UTC_ZONE = TimeZone.getTimeZone("UTC");

	/** The local time zone. */
	private static final TimeZone LOCAL_TZ = TimeZone.getDefault();

	private static final String[] DEFAULT_DATETIME_FORMATS = new String[]{
		"yyyy-MM-dd HH:mm:ss",
		"yyyy-MM-dd HH:mm:ss.S",
		"yyyy-MM-dd HH:mm:ss.SS",
		"yyyy-MM-dd HH:mm:ss.SSS"
	};

	/**
	 * A ThreadLocal cache map for SimpleDateFormat, because SimpleDateFormat is not thread-safe.
	 * (string_format) => formatter
	 */
	private static final ThreadLocalCache<String, SimpleDateFormat> FORMATTER_CACHE =
		new ThreadLocalCache<String, SimpleDateFormat>() {
			@Override
			public SimpleDateFormat getNewInstance(String key) {
				return new SimpleDateFormat(key);
			}
		};

	/**
	 * A ThreadLocal cache map for TimeZone.
	 * (string_zone_id) => TimeZone
	 */
	private static final ThreadLocalCache<String, TimeZone> TIMEZONE_CACHE =
		new ThreadLocalCache<String, TimeZone>() {
			@Override
			public TimeZone getNewInstance(String tz) {
				return TimeZone.getTimeZone(tz);
			}
		};

	// --------------------------------------------------------------------------------------------
	// Date/Time/Timestamp --> internal int/int/long conversion
	// --------------------------------------------------------------------------------------------

	/** Converts the internal representation of a SQL DATE (int) to the Java
	 * type used for UDF parameters ({@link java.sql.Date}). */
	public static java.sql.Date internalToDate(int v) {
		// note that, in this case, can't handle Daylight Saving Time
		final long t = v * MILLIS_PER_DAY;
		return new java.sql.Date(t - LOCAL_TZ.getOffset(t));
	}

	/** Converts the internal representation of a SQL TIME (int) to the Java
	 * type used for UDF parameters ({@link java.sql.Time}). */
	public static java.sql.Time internalToTime(int v) {
		// note that, in this case, can't handle Daylight Saving Time
		return new java.sql.Time(v - LOCAL_TZ.getOffset(v));
	}

	/** Converts the internal representation of a SQL TIMESTAMP (long) to the Java
	 * type used for UDF parameters ({@link java.sql.Timestamp}). */
	public static java.sql.Timestamp internalToTimestamp(long v) {
		return new java.sql.Timestamp(v - LOCAL_TZ.getOffset(v));
	}

	/** Converts the Java type used for UDF parameters of SQL DATE type
	 * ({@link java.sql.Date}) to internal representation (int).
	 *
	 * <p>Converse of {@link #internalToDate(int)}. */
	public static int dateToInternal(java.sql.Date date) {
		long ts = date.getTime() + LOCAL_TZ.getOffset(date.getTime());
		return (int) (ts / MILLIS_PER_DAY);
	}

	/** Converts the Java type used for UDF parameters of SQL TIME type
	 * ({@link java.sql.Time}) to internal representation (int).
	 *
	 * <p>Converse of {@link #internalToTime(int)}. */
	public static int timeToInternal(java.sql.Time time) {
		long ts = time.getTime() + LOCAL_TZ.getOffset(time.getTime());
		return (int) (ts % MILLIS_PER_DAY);
	}

	/** Converts the Java type used for UDF parameters of SQL TIMESTAMP type
	 * ({@link java.sql.Timestamp}) to internal representation (long).
	 *
	 * <p>Converse of {@link #internalToTimestamp(long)}. */
	public static long timestampToInternal(java.sql.Timestamp ts) {
		long time = ts.getTime();
		return time + LOCAL_TZ.getOffset(time);
	}

	// --------------------------------------------------------------------------------------------
	// int/long/double/Decimal --> Date/Timestamp internal representation
	// --------------------------------------------------------------------------------------------

	public static int toDate(int v) {
		return v;
	}

	public static long toTimestamp(long v) {
		return v;
	}

	public static long toTimestamp(double v) {
		return (long) v;
	}

	public static long toTimestamp(Decimal v) {
		return Decimal.castToLong(v);
	}

	// --------------------------------------------------------------------------------------------
	// String --> String/timestamp conversion
	// --------------------------------------------------------------------------------------------

	// --------------------------------------------------------------------------------------------
	// String --> Timestamp conversion
	// --------------------------------------------------------------------------------------------

	public static Long toTimestamp(String dateStr) {
		return toTimestamp(dateStr, UTC_ZONE);
	}

	/**
	 * Parse date time string to timestamp based on the given time zone and
	 * "yyyy-MM-dd HH:mm:ss" format. Returns null if parsing failed.
	 *
	 * @param dateStr the date time string
	 * @param tz the time zone
	 */
	public static Long toTimestamp(String dateStr, TimeZone tz) {
		int length = dateStr.length();
		String format;
		if (length == 10) {
			format = DATE_FORMAT_STRING;
		} else if (length == 21) {
			format = DEFAULT_DATETIME_FORMATS[1];
		} else if (length == 22) {
			format = DEFAULT_DATETIME_FORMATS[2];
		} else if (length == 23) {
			format = DEFAULT_DATETIME_FORMATS[3];
		} else {
			// otherwise fall back to the default
			format = DEFAULT_DATETIME_FORMATS[0];
		}
		return toTimestamp(dateStr, format, tz);
	}

	/**
	 * Parse date time string to timestamp based on the given time zone and format.
	 * Returns null if parsing failed.
	 *
	 * @param dateStr the date time string
	 * @param format date time string format
	 * @param tz the time zone
	 */
	public static Long toTimestamp(String dateStr, String format, TimeZone tz) {
		SimpleDateFormat formatter = FORMATTER_CACHE.get(format);
		formatter.setTimeZone(tz);
		try {
			return formatter.parse(dateStr).getTime();
		} catch (ParseException e) {
			return null;
		}
	}

	public static Long toTimestamp(String dateStr, String format) {
		return toTimestamp(dateStr, format, UTC_ZONE);
	}

	/**
	 * Parse date time string to timestamp based on the given time zone string and format.
	 * Returns null if parsing failed.
	 *
	 * @param dateStr the date time string
	 * @param format the date time string format
	 * @param tzStr the time zone id string
	 */
	public static Long toTimestampTz(String dateStr, String format, String tzStr) {
		TimeZone tz = TIMEZONE_CACHE.get(tzStr);
		return toTimestamp(dateStr, format, tz);
	}

	public static Long toTimestampTz(String dateStr, String tzStr) {
		// use "yyyy-MM-dd HH:mm:ss" as default format
		return toTimestampTz(dateStr, TIMESTAMP_FORMAT_STRING, tzStr);
	}

	// --------------------------------------------------------------------------------------------
	// String --> Date conversion
	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the epoch days since 1970-01-01.
	 */
	public static int strToDate(String dateStr, String fromFormat) {
		// It is OK to use UTC, we just want get the epoch days
		// TODO  use offset, better performance
		long ts = parseToTimeMillis(dateStr, fromFormat, TimeZone.getTimeZone("UTC"));
		ZoneId zoneId = ZoneId.of("UTC");
		Instant instant = Instant.ofEpochMilli(ts);
		ZonedDateTime zdt = ZonedDateTime.ofInstant(instant, zoneId);
		return DateTimeUtils.ymdToUnixDate(zdt.getYear(), zdt.getMonthValue(), zdt.getDayOfMonth());
	}

	// --------------------------------------------------------------------------------------------
	// DATE_FORMAT
	// --------------------------------------------------------------------------------------------

	/**
	 * Format a timestamp as specific.
	 * @param ts the timestamp to format.
	 * @param format the string formatter.
	 * @param tz the time zone
	 */
	public static String dateFormat(long ts, String format, TimeZone tz) {
		SimpleDateFormat formatter = FORMATTER_CACHE.get(format);
		formatter.setTimeZone(tz);
		Date dateTime = new Date(ts);
		return formatter.format(dateTime);
	}

	/**
	 * Format a string datetime as specific.
	 * @param dateStr the string datetime.
	 * @param fromFormat the original date format.
	 * @param toFormat the target date format.
	 * @param tz the time zone.
	 */
	public static String dateFormat(String dateStr, String fromFormat, String toFormat, TimeZone tz) {
		SimpleDateFormat fromFormatter = FORMATTER_CACHE.get(fromFormat);
		fromFormatter.setTimeZone(tz);
		SimpleDateFormat toFormatter = FORMATTER_CACHE.get(toFormat);
		toFormatter.setTimeZone(tz);
		try {
			return toFormatter.format(fromFormatter.parse(dateStr));
		} catch (ParseException e) {
			LOG.error("Exception when formatting: '" + dateStr +
				"' from: '" + fromFormat + "' to: '" + toFormat + "'", e);
			return null;
		}
	}

	public static String dateFormat(String dateStr, String toFormat, TimeZone tz) {
		// use yyyy-MM-dd HH:mm:ss as default
		return dateFormat(dateStr, TIMESTAMP_FORMAT_STRING, toFormat, tz);
	}

	public static String dateFormat(long ts, String format) {
		return dateFormat(ts, format, UTC_ZONE);
	}

	public static String dateFormat(String dateStr, String fromFormat, String toFormat) {
		return dateFormat(dateStr, fromFormat, toFormat, UTC_ZONE);
	}

	public static String dateFormat(String dateStr, String toFormat) {
		return dateFormat(dateStr, toFormat, UTC_ZONE);
	}

	public static String dateFormatTz(long ts, String format, String tzStr) {
		TimeZone tz = TIMEZONE_CACHE.get(tzStr);
		return dateFormat(ts, format, tz);
	}

	public static String dateFormatTz(long ts, String tzStr) {
		// use yyyy-MM-dd HH:mm:ss as default
		return dateFormatTz(ts, TIMESTAMP_FORMAT_STRING, tzStr);
	}

	/**
	 * Convert datetime string from a time zone to another time zone.
	 * @param dateStr the date time string
	 * @param format the date time format
	 * @param tzFrom the original time zone
	 * @param tzTo the target time zone
	 */
	public static String convertTz(String dateStr, String format, String tzFrom, String tzTo) {
		Long ts = toTimestampTz(dateStr, format, tzFrom);
		if (null != ts) { // avoid NPE
			return dateFormatTz(ts, tzTo);
		}
		return null;
	}

	public static String convertTz(String dateStr, String tzFrom, String tzTo) {
		// use yyyy-MM-dd HH:mm:ss as default
		return convertTz(dateStr, TIMESTAMP_FORMAT_STRING, tzFrom, tzTo);
	}

	public static String timestampToString(long ts, int precision) {
		int p = (precision <= 3 && precision >= 0) ? precision : 3;
		String format = DEFAULT_DATETIME_FORMATS[p];
		return dateFormat(ts, format, UTC_ZONE);
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
	public static String unixTimeToString(int time) {
		final StringBuilder buf = new StringBuilder(8);
		unixTimeToString(buf, time, 0); // set milli second precision to 0
		return buf.toString();
	}

	private static void unixTimeToString(StringBuilder buf, int time, int precision) {
		// we copy this method from Calcite DateTimeUtils but add the following changes
		// time may be negative which means time milli seconds before 00:00:00
		// this maybe a bug in calcite avatica
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


	/**
	 * Parses a given datetime string to milli seconds since 1970-01-01 00:00:00 UTC
	 * using the default format "yyyy-MM-dd" or "yyyy-MM-dd HH:mm:ss" depends on the string length.
	 */
	private static long parseToTimeMillis(String dateStr, TimeZone tz) {
		String format;
		if (dateStr.length() <= 10) {
			format = DATE_FORMAT_STRING;
		} else {
			format = TIMESTAMP_FORMAT_STRING;
		}
		return parseToTimeMillis(dateStr, format, tz) + getMillis(dateStr);
	}

	private static long parseToTimeMillis(String dateStr, String format, TimeZone tz) {
		SimpleDateFormat formatter = FORMATTER_CACHE.get(format);
		formatter.setTimeZone(tz);
		try {
			Date date = formatter.parse(dateStr);
			return date.getTime();
		} catch (ParseException e) {
			LOG.error(
				String.format("Exception when parsing datetime string '%s' in format '%s'", dateStr, format),
				e);
			return Long.MIN_VALUE;
		}
	}

	/**
	 * Returns the milli second part of the datetime.
	 */
	private static int getMillis(String dateStr) {
		int length = dateStr.length();
		if (length == 19) {
			// "1999-12-31 12:34:56", no milli second left
			return 0;
		} else if (length == 21) {
			// "1999-12-31 12:34:56.7", return 7
			return Integer.parseInt(dateStr.substring(20)) * 100;
		} else if (length == 22) {
			// "1999-12-31 12:34:56.78", return 78
			return Integer.parseInt(dateStr.substring(20)) * 10;
		} else if (length >= 23 && length <= 26) {
			// "1999-12-31 12:34:56.123" ~ "1999-12-31 12:34:56.123456"
			return Integer.parseInt(dateStr.substring(20, 23));
		} else {
			return 0;
		}
	}

	// --------------------------------------------------------------------------------------------
	// EXTRACT YEAR/MONTH/QUARTER
	// --------------------------------------------------------------------------------------------

	public static int extractYearMonth(TimeUnitRange range, int v) {
		switch (range) {
			case YEAR: return v / 12;
			case MONTH: return v % 12;
			case QUARTER: return (v % 12 + 2) / 3;
			default: throw new UnsupportedOperationException("Unsupported TimeUnitRange: " + range);
		}
	}

	private static final DateType REUSE_DATE_TYPE = new DateType();
	private static final TimestampType REUSE_TIMESTAMP_TYPE = new TimestampType(3);

	public static long extractFromDate(TimeUnitRange range, long ts) {
		return convertExtract(range, ts, REUSE_DATE_TYPE, TimeZone.getTimeZone("UTC"));
	}

	public static long unixTimeExtract(TimeUnitRange range, int ts) {
		return DateTimeUtils.unixTimeExtract(range, ts);
	}

	public static long extractFromTimestamp(TimeUnitRange range, long ts, TimeZone tz) {
		return convertExtract(range, ts, REUSE_TIMESTAMP_TYPE, tz);
	}

	private static long convertExtract(TimeUnitRange range, long ts, LogicalType type, TimeZone tz) {
		TimeUnit startUnit = range.startUnit;
		long offset = tz.getOffset(ts);
		long utcTs = ts + offset;

		switch (startUnit) {
			case MILLENNIUM:
			case CENTURY:
			case YEAR:
			case QUARTER:
			case MONTH:
			case DAY:
			case DOW:
			case DOY:
			case WEEK:
				if (type instanceof TimestampType) {
					long d = divide(utcTs, TimeUnit.DAY.multiplier);
					return DateTimeUtils.unixDateExtract(range, d);
				} else if (type instanceof DateType) {
					return divide(utcTs, TimeUnit.DAY.multiplier);
				} else {
					// TODO support it
					throw new TableException(type + " is unsupported now.");
				}
			case DECADE:
				// TODO support it
				throw new TableException("DECADE is unsupported now.");
			case EPOCH:
				// TODO support it
				throw new TableException("EPOCH is unsupported now.");
			default:
				// fall through
		}

		long res = mod(utcTs, getFactory(startUnit));
		res = divide(res, startUnit.multiplier);
		return res;

	}

	private static long divide(long res, BigDecimal value) {
		if (value.equals(BigDecimal.ONE)) {
			return res;
		} else if (value.compareTo(BigDecimal.ONE) < 0 && value.signum() == 1) {
			BigDecimal reciprocal = BigDecimal.ONE.divide(value, RoundingMode.UNNECESSARY);
			return reciprocal.multiply(BigDecimal.valueOf(res)).longValue();
		} else {
			return res / value.longValue();
		}
	}

	private static long mod(long res, BigDecimal value) {
		if (value.equals(BigDecimal.ONE)) {
			return res;
		} else {
			return res % value.longValue();
		}
	}

	private static BigDecimal getFactory(TimeUnit unit) {
		switch (unit) {
			case DAY: return BigDecimal.ONE;
			case HOUR: return TimeUnit.DAY.multiplier;
			case MINUTE: return TimeUnit.HOUR.multiplier;
			case SECOND: return TimeUnit.MINUTE.multiplier;
			case YEAR: return BigDecimal.ONE;
			case MONTH: return TimeUnit.YEAR.multiplier;
			case QUARTER: return TimeUnit.YEAR.multiplier;
			case DECADE:
			case CENTURY:
			case MILLENNIUM:
				return BigDecimal.ONE;
			default:
				throw new IllegalArgumentException("Invalid start unit.");
		}
	}

	// --------------------------------------------------------------------------------------------
	// Floor/Ceil
	// --------------------------------------------------------------------------------------------

	public static long timestampFloor(TimeUnitRange range, long ts) {
		return timestampFloor(range, ts, UTC_ZONE);
	}

	public static long timestampFloor(TimeUnitRange range, long ts, TimeZone tz) {
		// assume that we are at UTC timezone, just for algorithm performance
		long offset = tz.getOffset(ts);
		long utcTs = ts + offset;

		switch (range) {
			case HOUR:
				return floor(utcTs, MILLIS_PER_HOUR) - offset;
			case DAY:
				return floor(utcTs, MILLIS_PER_DAY) - offset;
			case MONTH:
			case YEAR:
			case QUARTER:
				int days = (int) (utcTs / MILLIS_PER_DAY + EPOCH_JULIAN);
				return julianDateFloor(range, days, true) * MILLIS_PER_DAY - offset;
			default:
				// for MINUTE and SECONDS etc...,
				// it is more effective to use arithmetic Method
				throw new AssertionError(range);
		}
	}

	public static long timestampCeil(TimeUnitRange range, long ts) {
		return timestampCeil(range, ts, UTC_ZONE);
	}

	/**
	 * Keep the algorithm consistent with Calcite DateTimeUtils.julianDateFloor, but here
	 * we take time zone into account.
	 */
	public static long timestampCeil(TimeUnitRange range, long ts, TimeZone tz) {
		// assume that we are at UTC timezone, just for algorithm performance
		long offset = tz.getOffset(ts);
		long utcTs = ts + offset;

		switch (range) {
			case HOUR:
				return ceil(utcTs, MILLIS_PER_HOUR) - offset;
			case DAY:
				return ceil(utcTs, MILLIS_PER_DAY) - offset;
			case MONTH:
			case YEAR:
			case QUARTER:
				int days = (int) (utcTs / MILLIS_PER_DAY + EPOCH_JULIAN);
				return julianDateFloor(range, days, false) * MILLIS_PER_DAY - offset;
			default:
				// for MINUTE and SECONDS etc...,
				// it is more effective to use arithmetic Method
				throw new AssertionError(range);
		}
	}

	private static long floor(long a, long b) {
		long r = a % b;
		if (r < 0) {
			return a - r - b;
		} else {
			return a - r;
		}
	}

	private static long ceil(long a, long b) {
		long r = a % b;
		if (r > 0) {
			return a - r + b;
		} else {
			return a - r;
		}
	}

	private static long julianDateFloor(TimeUnitRange range, int julian, boolean floor) {
		// Algorithm the book "Astronomical Algorithms" by Jean Meeus, 1998
		int b = 0;
		int c = 0;
		if (julian > 2299160) {
			int a = julian + 32044;
			b = (4 * a + 3) / 146097;
			c = a - b * 146097 / 4;
		}
		else {
			b = 0;
			c = julian + 32082;
		}
		int d = (4 * c + 3) / 1461;
		int e = c - (1461 * d) / 4;
		int m = (5 * e + 2) / 153;
		int day = e - (153 * m + 2) / 5 + 1;
		int month = m + 3 - 12 * (m / 10);
		int quarter = (month + 2) / 3;
		int year = b * 100 + d - 4800 + (m / 10);
		switch (range) {
			case YEAR:
				if (!floor && (month > 1 || day > 1)) {
					year += 1;
				}
				return DateTimeUtils.ymdToUnixDate(year, 1, 1);
			case MONTH:
				if (!floor && day > 1) {
					month += 1;
				}
				return DateTimeUtils.ymdToUnixDate(year, month, 1);
			case QUARTER:
				if (!floor && (month > 1 || day > 1)) {
					quarter += 1;
				}
				return DateTimeUtils.ymdToUnixDate(year, quarter * 3 - 2, 1);
			default:
				throw new AssertionError(range);
		}
	}

	// --------------------------------------------------------------------------------------------
	// DATE DIFF/ADD
	// --------------------------------------------------------------------------------------------

	/**
	 * NOTE:
	 * (1). JDK relies on the operating system clock for time.
	 * Each operating system has its own method of handling date changes such as
	 * leap seconds(e.g. OS will slow down the  clock to accommodate for this).
	 * (2). DST(Daylight Saving Time) is a legal issue, governments changed it
	 * over time. Some days are NOT exactly 24 hours long, it could be 23/25 hours
	 * long on the first or last day of daylight saving time.
	 * JDK can handle DST correctly.
	 * TODO:
	 *       carefully written algorithm can improve the performance
	 */
	public static int dateDiff(long t1, long t2, TimeZone tz) {
		ZoneId zoneId = tz.toZoneId();
		LocalDate ld1 = Instant.ofEpochMilli(t1).atZone(zoneId).toLocalDate();
		LocalDate ld2 = Instant.ofEpochMilli(t2).atZone(zoneId).toLocalDate();
		return (int) ChronoUnit.DAYS.between(ld2, ld1);
	}

	public static int dateDiff(String t1Str, long t2, TimeZone tz) {
		long t1 = parseToTimeMillis(t1Str, tz);
		return dateDiff(t1, t2, tz);
	}

	public static int dateDiff(long t1, String t2Str, TimeZone tz) {
		long t2 = parseToTimeMillis(t2Str, tz);
		return dateDiff(t1, t2, tz);
	}

	public static int dateDiff(String t1Str, String t2Str, TimeZone tz) {
		long t1 = parseToTimeMillis(t1Str, tz);
		long t2 = parseToTimeMillis(t2Str, tz);
		return dateDiff(t1, t2, tz);
	}

	public static int dateDiff(long t1, long t2) {
		return dateDiff(t1, t2, UTC_ZONE);
	}

	public static int dateDiff(String t1Str, long t2) {
		return dateDiff(t1Str, t2, UTC_ZONE);
	}

	public static int dateDiff(long t1, String t2Str) {
		return dateDiff(t1, t2Str, UTC_ZONE);
	}

	public static int dateDiff(String t1Str, String t2Str) {
		return dateDiff(t1Str, t2Str, UTC_ZONE);
	}

	/**
	 * Do subtraction on date string.
	 *
	 * @param dateStr formatted date string.
	 * @param days days count you want to subtract.
	 * @param tz time zone of the date time string
	 * @return datetime string.
	 */
	public static String dateSub(String dateStr, int days, TimeZone tz) {
		long ts = parseToTimeMillis(dateStr, tz);
		if (ts == Long.MIN_VALUE) {
			return null;
		}
		return dateSub(ts, days, tz);
	}

	/**
	 * Do subtraction on date string.
	 *
	 * @param ts    the timestamp.
	 * @param days days count you want to subtract.
	 * @param tz time zone of the date time string
	 * @return datetime string.
	 */
	public static String dateSub(long ts, int days, TimeZone tz) {
		ZoneId zoneId = tz.toZoneId();
		Instant instant = Instant.ofEpochMilli(ts);
		ZonedDateTime zdt = ZonedDateTime.ofInstant(instant, zoneId);
		long resultTs = zdt.minusDays(days).toInstant().toEpochMilli();
		return dateFormat(resultTs, DATE_FORMAT_STRING, tz);
	}

	public static String dateSub(String dateStr, int days) {
		return dateSub(dateStr, days, UTC_ZONE);
	}

	public static String dateSub(long ts, int days) {
		return dateSub(ts, days, UTC_ZONE);
	}

	/**
	 * Do addition on date string.
	 *
	 * @param dateStr formatted date string.
	 * @param days days count you want to add.
	 * @return datetime string.
	 */
	public static String dateAdd(String dateStr, int days, TimeZone tz) {
		long ts = parseToTimeMillis(dateStr, tz);
		if (ts == Long.MIN_VALUE) {
			return null;
		}
		return dateAdd(ts, days, tz);
	}

	/**
	 * Do addition on timestamp.
	 *
	 * @param ts    the timestamp.
	 * @param days  days count you want to add.
	 * @return datetime string.
	 */
	public static String dateAdd(long ts, int days, TimeZone tz) {
		ZoneId zoneId = tz.toZoneId();
		Instant instant = Instant.ofEpochMilli(ts);
		ZonedDateTime zdt = ZonedDateTime.ofInstant(instant, zoneId);
		long resultTs = zdt.plusDays(days).toInstant().toEpochMilli();
		return dateFormat(resultTs, DATE_FORMAT_STRING, tz);
	}

	public static String dateAdd(String dateStr, int days) {
		return dateAdd(dateStr, days, UTC_ZONE);
	}

	public static String dateAdd(long ts, int days) {
		return dateAdd(ts, days, UTC_ZONE);
	}

	// --------------------------------------------------------------------------------------------
	// UNIX TIME
	// --------------------------------------------------------------------------------------------

	public static long fromTimestamp(long ts) {
		return ts;
	}

	/**
	 * Returns current timestamp(count by seconds).
	 *
	 * @return current timestamp.
	 */
	public static long now() {
		return System.currentTimeMillis() / 1000;
	}

	/**
	 * Returns current timestamp(count by seconds) with offset.
	 *
	 * @param offset value(count by seconds).
	 * @return current timestamp with offset.
	 */
	public static long now(long offset) {
		return now() + offset;
	}

	/**
	 * Convert unix timestamp (seconds since '1970-01-01 00:00:00' UTC) to datetime string
	 * in the "yyyy-MM-dd HH:mm:ss" format.
	 */
	public static String fromUnixtime(long unixtime, TimeZone tz) {
		return fromUnixtime(unixtime, TIMESTAMP_FORMAT_STRING, tz);

	}

	/**
	 * Convert unix timestamp (seconds since '1970-01-01 00:00:00' UTC) to datetime string
	 * in the given format.
	 */
	public static String fromUnixtime(long unixtime, String format, TimeZone tz) {
		SimpleDateFormat formatter = FORMATTER_CACHE.get(format);
		formatter.setTimeZone(tz);
		Date date = new Date(unixtime * 1000);
		try {
			return formatter.format(date);
		} catch (Exception e) {
			LOG.error("Exception when formatting.", e);
			return null;
		}
	}

	public static String fromUnixtime(double unixtime, TimeZone tz) {
		return fromUnixtime((long) unixtime, tz);
	}

	public static String fromUnixtime(Decimal unixtime, TimeZone tz) {
		return fromUnixtime(Decimal.castToLong(unixtime), tz);
	}

	public static String fromUnixtime(long unixtime) {
		return fromUnixtime(unixtime, UTC_ZONE);

	}

	public static String fromUnixtime(long unixtime, String format) {
		return fromUnixtime(unixtime, format, UTC_ZONE);
	}

	public static String fromUnixtime(double unixtime) {
		return fromUnixtime(unixtime, UTC_ZONE);
	}

	public static String fromUnixtime(Decimal unixtime) {
		return fromUnixtime(unixtime, UTC_ZONE);
	}

	/**
	 * Returns a Unix timestamp in seconds since '1970-01-01 00:00:00' UTC as an unsigned
	 * integer.
	 */
	public static long unixTimestamp() {
		return System.currentTimeMillis() / 1000;
	}

	/**
	 * Returns the value of the argument as an unsigned integer in seconds since
	 * '1970-01-01 00:00:00' UTC.
	 */
	public static long unixTimestamp(String dateStr, TimeZone tz) {
		return unixTimestamp(dateStr, TIMESTAMP_FORMAT_STRING, tz);
	}

	/**
	 * Returns the value of the argument as an unsigned integer in seconds since
	 * '1970-01-01 00:00:00' UTC.
	 */
	public static long unixTimestamp(String dateStr, String format, TimeZone tz) {
		long ts = parseToTimeMillis(dateStr, format, tz);
		if (ts == Long.MIN_VALUE) {
			return Long.MIN_VALUE;
		} else {
			// return the seconds
			return ts / 1000;
		}
	}

	public static long unixTimestamp(String dateStr) {
		return unixTimestamp(dateStr, UTC_ZONE);
	}

	public static long unixTimestamp(String dateStr, String format) {
		return unixTimestamp(dateStr, format, UTC_ZONE);
	}

	/**
	 * Returns the value of the timestamp to seconds since '1970-01-01 00:00:00' UTC.
	 */
	public static long unixTimestamp(long ts) {
		return ts / 1000;
	}

	public static LocalDate unixDateToLocalDate(int date) {
		return julianToLocalDate(date + EPOCH_JULIAN);
	}

	private static LocalDate julianToLocalDate(int julian) {
		// this shifts the epoch back to astronomical year -4800 instead of the
		// start of the Christian era in year AD 1 of the proleptic Gregorian
		// calendar.
		int j = julian + 32044;
		int g = j / 146097;
		int dg = j % 146097;
		int c = (dg / 36524 + 1) * 3 / 4;
		int dc = dg - c * 36524;
		int b = dc / 1461;
		int db = dc % 1461;
		int a = (db / 365 + 1) * 3 / 4;
		int da = db - a * 365;

		// integer number of full years elapsed since March 1, 4801 BC
		int y = g * 400 + c * 100 + b * 4 + a;
		// integer number of full months elapsed since the last March 1
		int m = (da * 5 + 308) / 153 - 2;
		// number of days elapsed since day 1 of the month
		int d = da - (m + 4) * 153 / 5 + 122;
		int year = y - 4800 + (m + 2) / 12;
		int month = (m + 2) % 12 + 1;
		int day = d + 1;
		return LocalDate.of(year, month, day);
	}

	public static int localDateToUnixDate(LocalDate date) {
		return ymdToUnixDate(date.getYear(), date.getMonthValue(), date.getDayOfMonth());
	}

	private static int ymdToUnixDate(int year, int month, int day) {
		final int julian = ymdToJulian(year, month, day);
		return julian - EPOCH_JULIAN;
	}

	private static int ymdToJulian(int year, int month, int day) {
		int a = (14 - month) / 12;
		int y = year + 4800 - a;
		int m = month + 12 * a - 3;
		return day + (153 * m + 2) / 5
				+ 365 * y
				+ y / 4
				- y / 100
				+ y / 400
				- 32045;
	}

	public static LocalTime unixTimeToLocalTime(int time) {
		int h = time / 3600000;
		int time2 = time % 3600000;
		int m = time2 / 60000;
		int time3 = time2 % 60000;
		int s = time3 / 1000;
		int ms = time3 % 1000;
		return LocalTime.of(h, m, s, ms * 1000_000);
	}

	public static int localTimeToUnixDate(LocalTime time) {
		return time.getHour() * (int) MILLIS_PER_HOUR
				+ time.getMinute() * (int) MILLIS_PER_MINUTE
				+ time.getSecond() * (int) MILLIS_PER_SECOND
				+ time.getNano() / 1000_000;
	}

	public static LocalDateTime unixTimestampToLocalDateTime(long timestamp) {
		int date = (int) (timestamp / MILLIS_PER_DAY);
		int time = (int) (timestamp % MILLIS_PER_DAY);
		if (time < 0) {
			--date;
			time += MILLIS_PER_DAY;
		}
		LocalDate localDate = unixDateToLocalDate(date);
		LocalTime localTime = unixTimeToLocalTime(time);
		return LocalDateTime.of(localDate, localTime);
	}

	public static long localDateTimeToUnixTimestamp(LocalDateTime dateTime) {
		return unixTimestamp(
				dateTime.getYear(), dateTime.getMonthValue(), dateTime.getDayOfMonth(),
				dateTime.getHour(), dateTime.getMinute(), dateTime.getSecond(),
				dateTime.getNano() / 1000_000);
	}

	private static long unixTimestamp(int year, int month, int day, int hour,
			int minute, int second, int mills) {
		final int date = ymdToUnixDate(year, month, day);
		return (long) date * MILLIS_PER_DAY
				+ (long) hour * MILLIS_PER_HOUR
				+ (long) minute * MILLIS_PER_MINUTE
				+ (long) second * MILLIS_PER_SECOND
				+ mills;
	}

	public static long timestampToTimestampWithLocalZone(long ts, TimeZone tz) {
		return unixTimestampToLocalDateTime(ts).atZone(tz.toZoneId()).toInstant().toEpochMilli();
	}

	public static long timestampWithLocalZoneToTimestamp(long ts, TimeZone tz) {
		return localDateTimeToUnixTimestamp(
				LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), tz.toZoneId()));
	}

	public static long timestampWithLocalZoneToDate(long ts, TimeZone tz) {
		return localDateToUnixDate(LocalDateTime.ofInstant(
				Instant.ofEpochMilli(ts), tz.toZoneId()).toLocalDate());
	}

	public static long timestampWithLocalZoneToTime(long ts, TimeZone tz) {
		return localTimeToUnixDate(LocalDateTime.ofInstant(
				Instant.ofEpochMilli(ts), tz.toZoneId()).toLocalTime());
	}

	public static long dateToTimestampWithLocalZone(int date, TimeZone tz) {
		return LocalDateTime.of(unixDateToLocalDate(date), LocalTime.MIDNIGHT)
				.atZone(tz.toZoneId()).toInstant().toEpochMilli();
	}

	public static long timeToTimestampWithLocalZone(int time, TimeZone tz) {
		return unixTimestampToLocalDateTime(time).atZone(tz.toZoneId()).toInstant().toEpochMilli();
	}

	private static boolean isInteger(String s) {
		boolean isInt = s.length() > 0;
		for (int i = 0; i < s.length(); i++) {
			if (s.charAt(i) < '0' || s.charAt(i) > '9') {
				isInt = false;
				break;
			}
		}
		return isInt;
	}

	private static boolean isLeapYear(int s) {
		return s % 400 == 0 || (s % 4 == 0 && s % 100 != 0);
	}

	private static boolean isIllegalDate(int y, int m, int d) {
		int[] monthOf31Days = new int[]{1, 3, 5, 7, 8, 10, 12};
		if (y < 0 || y > 9999 || m < 1 || m > 12 || d < 1 || d > 31) {
			return false;
		}
		if (m == 2 && d > 28) {
			if (!(isLeapYear(y) && d == 29)) {
				return false;
			}
		}
		if (d == 31) {
			for (int i: monthOf31Days) {
				if (i == m) {
					return true;
				}
			}
			return false;
		}
		return true;
	}

	public static Integer dateStringToUnixDate(String s) {
		// allow timestamp str to date, e.g. 2017-12-12 09:30:00.0
		int ws1 = s.indexOf(" ");
		if (ws1 > 0) {
			s = s.substring(0, ws1);
		}
		int hyphen1 = s.indexOf('-');
		int y;
		int m;
		int d;
		if (hyphen1 < 0) {
			if (!isInteger(s.trim())) {
				return null;
			}
			y = Integer.parseInt(s.trim());
			m = 1;
			d = 1;
		} else {
			if (!isInteger(s.substring(0, hyphen1).trim())) {
				return null;
			}
			y = Integer.parseInt(s.substring(0, hyphen1).trim());
			final int hyphen2 = s.indexOf('-', hyphen1 + 1);
			if (hyphen2 < 0) {
				if (!isInteger(s.substring(hyphen1 + 1).trim())) {
					return null;
				}
				m = Integer.parseInt(s.substring(hyphen1 + 1).trim());
				d = 1;
			} else {
				if (!isInteger(s.substring(hyphen1 + 1, hyphen2).trim())) {
					return null;
				}
				m = Integer.parseInt(s.substring(hyphen1 + 1, hyphen2).trim());
				if (!isInteger(s.substring(hyphen2 + 1).trim())) {
					return null;
				}
				d = Integer.parseInt(s.substring(hyphen2 + 1).trim());
			}
		}
		if (!isIllegalDate(y, m, d)) {
			return null;
		}
		return DateTimeUtils.ymdToUnixDate(y, m, d);
	}

	public static Integer timeStringToUnixDate(String v) {
		return timeStringToUnixDate(v, 0);
	}

	public static Integer timeStringToUnixDate(String v, int start) {
		final int colon1 = v.indexOf(':', start);
		//timezone hh:mm:ss[.ssssss][[+|-]hh:mm:ss]
		//refer https://www.w3.org/TR/NOTE-datetime
		int timezoneHour;
		int timezoneMinute;
		int hour;
		int minute;
		int second;
		int milli;
		int operator = -1;
		int end = v.length();
		int timezone = v.indexOf('-', start);
		if (timezone < 0) {
			timezone = v.indexOf('+', start);
			operator = 1;
		}
		if (timezone < 0) {
			timezoneHour = 0;
			timezoneMinute = 0;
		} else {
			end = timezone;
			final int colon3 = v.indexOf(':', timezone);
			if (colon3 < 0) {
				if (!isInteger(v.substring(timezone + 1).trim())) {
					return null;
				}
				timezoneHour = Integer.parseInt(v.substring(timezone + 1).trim());
				timezoneMinute = 0;
			} else {
				if (!isInteger(v.substring(timezone + 1, colon3).trim())) {
					return null;
				}
				timezoneHour = Integer.parseInt(v.substring(timezone + 1, colon3).trim());
				if (!isInteger(v.substring(colon3 + 1).trim())) {
					return null;
				}
				timezoneMinute = Integer.parseInt(v.substring(colon3 + 1).trim());
			}
		}
		if (colon1 < 0) {
			if (!isInteger(v.substring(start, end).trim())) {
				return null;
			}
			hour = Integer.parseInt(v.substring(start, end).trim());
			minute = 1;
			second = 1;
			milli = 0;
		} else {
			if (!isInteger(v.substring(start, colon1).trim())) {
				return null;
			}
			hour = Integer.parseInt(v.substring(start, colon1).trim());
			final int colon2 = v.indexOf(':', colon1 + 1);
			if (colon2 < 0) {
				if (!isInteger(v.substring(colon1 + 1, end).trim())) {
					return null;
				}
				minute = Integer.parseInt(v.substring(colon1 + 1, end).trim());
				second = 1;
				milli = 0;
			} else {
				if (!isInteger(v.substring(colon1 + 1, colon2).trim())) {
					return null;
				}
				minute = Integer.parseInt(v.substring(colon1 + 1, colon2).trim());
				int dot = v.indexOf('.', colon2);
				if (dot < 0) {
					if (!isInteger(v.substring(colon2 + 1, end).trim())) {
						return null;
					}
					second = Integer.parseInt(v.substring(colon2 + 1, end).trim());
					milli = 0;
				} else {
					if (!isInteger(v.substring(colon2 + 1, dot).trim())) {
						return null;
					}
					second = Integer.parseInt(v.substring(colon2 + 1, dot).trim());
					milli = parseFraction(v.substring(dot + 1, end).trim(), 100);
				}
			}
		}
		hour += operator * timezoneHour;
		minute += operator * timezoneMinute;
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

}
