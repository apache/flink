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
import org.apache.flink.table.type.InternalType;
import org.apache.flink.table.type.InternalTypes;

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
		return internalToDate(v, UTC_ZONE);
	}

	/**
	 * Converts the internal representation of a SQL DATE (int) to the Java
	 * type used for UDF parameters ({@link java.sql.Date}) with the given TimeZone.
	 *
	 * <p>The internal int represents the days since January 1, 1970. When we convert it
	 * to {@link java.sql.Date} (time milliseconds since January 1, 1970, 00:00:00 GMT),
	 * we need a TimeZone.
	 */
	public static java.sql.Date internalToDate(int v, TimeZone tz) {
		// note that, in this case, can't handle Daylight Saving Time
		final long t = v * MILLIS_PER_DAY;
		return new java.sql.Date(t - tz.getOffset(t));
	}

	/** Converts the internal representation of a SQL TIME (int) to the Java
	 * type used for UDF parameters ({@link java.sql.Time}). */
	public static java.sql.Time internalToTime(int v) {
		return internalToTime(v, UTC_ZONE);
	}

	/**
	 * Converts the internal representation of a SQL TIME (int) to the Java
	 * type used for UDF parameters ({@link java.sql.Time}).
	 *
	 * <p>The internal int represents the seconds since "00:00:00". When we convert it to
	 * {@link java.sql.Time} (time milliseconds since January 1, 1970, 00:00:00 GMT),
	 * we need a TimeZone.
	 */
	public static java.sql.Time internalToTime(int v, TimeZone tz) {
		// note that, in this case, can't handle Daylight Saving Time
		return new java.sql.Time(v - tz.getOffset(v));
	}

	/** Converts the internal representation of a SQL TIMESTAMP (long) to the Java
	 * type used for UDF parameters ({@link java.sql.Timestamp}).
	 *
	 * <p>The internal long represents the time milliseconds since January 1, 1970, 00:00:00 GMT.
	 * So we don't need to take TimeZone into account.
	 */
	public static java.sql.Timestamp internalToTimestamp(long v) {
		return new java.sql.Timestamp(v);
	}

	/** Converts the Java type used for UDF parameters of SQL DATE type
	 * ({@link java.sql.Date}) to internal representation (int).
	 *
	 * <p>Converse of {@link #internalToDate(int)}. */
	public static int dateToInternal(java.sql.Date date) {
		return dateToInternal(date, UTC_ZONE);
	}

	/** Converts the Java type used for UDF parameters of SQL DATE type
	 * ({@link java.sql.Date}) to internal representation (int).
	 *
	 * <p>Converse of {@link #internalToDate(int)}. */
	public static int dateToInternal(java.sql.Date date, TimeZone tz) {
		long ts = date.getTime() + tz.getOffset(date.getTime());
		return (int) (ts / MILLIS_PER_DAY);
	}

	/** Converts the Java type used for UDF parameters of SQL TIME type
	 * ({@link java.sql.Time}) to internal representation (int).
	 *
	 * <p>Converse of {@link #internalToTime(int)}. */
	public static int timeToInternal(java.sql.Time time) {
		return timeToInternal(time, UTC_ZONE);
	}

	/** Converts the Java type used for UDF parameters of SQL TIME type
	 * ({@link java.sql.Time}) to internal representation (int).
	 *
	 * <p>Converse of {@link #internalToTime(int)}. */
	public static int timeToInternal(java.sql.Time time, TimeZone tz) {
		long ts = time.getTime() + tz.getOffset(time.getTime());
		return (int) (ts % MILLIS_PER_DAY);
	}

	/** Converts the Java type used for UDF parameters of SQL TIMESTAMP type
	 * ({@link java.sql.Timestamp}) to internal representation (long).
	 *
	 * <p>Converse of {@link #internalToTimestamp(long)}. */
	public static long timestampToInternal(java.sql.Timestamp ts) {
		return ts.getTime();
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
		if (length == 21) {
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
		return dateFormatTz(toTimestampTz(dateStr, format, tzFrom), tzTo);
	}

	public static String convertTz(String dateStr, String tzFrom, String tzTo) {
		// use yyyy-MM-dd HH:mm:ss as default
		return convertTz(dateStr, TIMESTAMP_FORMAT_STRING, tzFrom, tzTo);
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

	public static long extractFromDate(TimeUnitRange range, long ts) {
		return convertExtract(range, ts, InternalTypes.DATE, TimeZone.getTimeZone("UTC"));
	}

	public static long unixTimeExtract(TimeUnitRange range, int ts) {
		return DateTimeUtils.unixTimeExtract(range, ts);
	}

	public static long extractFromTimestamp(TimeUnitRange range, long ts, TimeZone tz) {
		return convertExtract(range, ts, InternalTypes.TIMESTAMP, tz);
	}

	private static long convertExtract(TimeUnitRange range, long ts, InternalType type, TimeZone tz) {
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
				if (type == InternalTypes.TIMESTAMP) {
					long d = divide(utcTs, TimeUnit.DAY.multiplier);
					return DateTimeUtils.unixDateExtract(range, d);
				} else if (type == InternalTypes.DATE) {
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

	/**
	 * Returns the value of the timestamp to seconds since '1970-01-01 00:00:00' UTC.
	 */
	public static long unixTimestamp(long ts) {
		return ts / 1000;
	}
}
