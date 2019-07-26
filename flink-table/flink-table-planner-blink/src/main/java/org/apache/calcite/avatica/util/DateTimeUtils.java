/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.avatica.util;

import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

/*
 * THIS FILE HAS BEEN COPIED FROM THE APACHE CALCITE PROJECT UNTIL CALCITE-1884, CALCITE-3199 IS FIXED.
 * (Modified line: 839)
 */

/**
 * Utility functions for datetime types: date, time, timestamp.
 *
 * <p>Used by the JDBC driver.
 *
 * <p>TODO: review methods for performance. Due to allocations required, it may
 * be preferable to introduce a "formatter" with the required state.
 */
public class DateTimeUtils {
	/** The julian date of the epoch, 1970-01-01. */
	public static final int EPOCH_JULIAN = 2440588;

	private DateTimeUtils() {}

	//~ Static fields/initializers ---------------------------------------------

	/** The SimpleDateFormat string for ISO dates, "yyyy-MM-dd". */
	public static final String DATE_FORMAT_STRING = "yyyy-MM-dd";

	/** The SimpleDateFormat string for ISO times, "HH:mm:ss". */
	public static final String TIME_FORMAT_STRING = "HH:mm:ss";

	/** The SimpleDateFormat string for ISO timestamps, "yyyy-MM-dd HH:mm:ss". */
	public static final String TIMESTAMP_FORMAT_STRING =
		DATE_FORMAT_STRING + " " + TIME_FORMAT_STRING;

	/** The GMT time zone.
	 *
	 * @deprecated Use {@link #UTC_ZONE} */
	@Deprecated // to be removed before 2.0
	public static final TimeZone GMT_ZONE = TimeZone.getTimeZone("GMT");

	/** The UTC time zone. */
	public static final TimeZone UTC_ZONE = TimeZone.getTimeZone("UTC");

	/** The Java default time zone. */
	public static final TimeZone DEFAULT_ZONE = TimeZone.getDefault();

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

	/**
	 * Calendar set to the epoch (1970-01-01 00:00:00 UTC). Useful for
	 * initializing other values. Calendars are not immutable, so be careful not
	 * to screw up this object for everyone else.
	 */
	public static final Calendar ZERO_CALENDAR;

	static {
		ZERO_CALENDAR = Calendar.getInstance(DateTimeUtils.UTC_ZONE, Locale.ROOT);
		ZERO_CALENDAR.setTimeInMillis(0);
	}

	//~ Methods ----------------------------------------------------------------

	/**
	 * Parses a string using {@link SimpleDateFormat} and a given pattern. This
	 * method parses a string at the specified parse position and if successful,
	 * updates the parse position to the index after the last character used.
	 * The parsing is strict and requires months to be less than 12, days to be
	 * less than 31, etc.
	 *
	 * @param s       string to be parsed
	 * @param dateFormat Date format
	 * @param tz      time zone in which to interpret string. Defaults to the Java
	 *                default time zone
	 * @param pp      position to start parsing from
	 * @return a Calendar initialized with the parsed value, or null if parsing
	 * failed. If returned, the Calendar is configured to the GMT time zone.
	 */
	private static Calendar parseDateFormat(String s, DateFormat dateFormat,
											TimeZone tz, ParsePosition pp) {
		if (tz == null) {
			tz = DEFAULT_ZONE;
		}
		Calendar ret = Calendar.getInstance(tz, Locale.ROOT);
		dateFormat.setCalendar(ret);
		dateFormat.setLenient(false);

		final Date d = dateFormat.parse(s, pp);
		if (null == d) {
			return null;
		}
		ret.setTime(d);
		ret.setTimeZone(UTC_ZONE);
		return ret;
	}

	@Deprecated // to be removed before 2.0
	public static Calendar parseDateFormat(String s, String pattern,
										   TimeZone tz) {
		return parseDateFormat(s, new SimpleDateFormat(pattern, Locale.ROOT), tz);
	}

	/**
	 * Parses a string using {@link SimpleDateFormat} and a given pattern. The
	 * entire string must match the pattern specified.
	 *
	 * @param s       string to be parsed
	 * @param dateFormat Date format
	 * @param tz      time zone in which to interpret string. Defaults to the Java
	 *                default time zone
	 * @return a Calendar initialized with the parsed value, or null if parsing
	 * failed. If returned, the Calendar is configured to the UTC time zone.
	 */
	public static Calendar parseDateFormat(String s, DateFormat dateFormat,
										   TimeZone tz) {
		ParsePosition pp = new ParsePosition(0);
		Calendar ret = parseDateFormat(s, dateFormat, tz, pp);
		if (pp.getIndex() != s.length()) {
			// Didn't consume entire string - not good
			return null;
		}
		return ret;
	}

	@Deprecated // to be removed before 2.0
	public static PrecisionTime parsePrecisionDateTimeLiteral(
		String s,
		String pattern,
		TimeZone tz) {
		assert pattern != null;
		return parsePrecisionDateTimeLiteral(s,
			new SimpleDateFormat(pattern, Locale.ROOT), tz, 3);
	}

	/**
	 * Parses a string using {@link SimpleDateFormat} and a given pattern, and
	 * if present, parses a fractional seconds component. The fractional seconds
	 * component must begin with a decimal point ('.') followed by numeric
	 * digits. The precision is rounded to a maximum of 3 digits of fractional
	 * seconds precision (to obtain milliseconds).
	 *
	 * @param s       string to be parsed
	 * @param dateFormat Date format
	 * @param tz      time zone in which to interpret string. Defaults to the
	 *                local time zone
	 * @return a {@link DateTimeUtils.PrecisionTime PrecisionTime} initialized
	 * with the parsed value, or null if parsing failed. The PrecisionTime
	 * contains a GMT Calendar and a precision.
	 */
	public static PrecisionTime parsePrecisionDateTimeLiteral(String s,
															  DateFormat dateFormat, TimeZone tz, int maxPrecision) {
		final ParsePosition pp = new ParsePosition(0);
		final Calendar cal = parseDateFormat(s, dateFormat, tz, pp);
		if (cal == null) {
			return null; // Invalid date/time format
		}

		// Note: the Java SimpleDateFormat 'S' treats any number after
		// the decimal as milliseconds. That means 12:00:00.9 has 9
		// milliseconds and 12:00:00.9999 has 9999 milliseconds.
		int p = 0;
		String secFraction = "";
		if (pp.getIndex() < s.length()) {
			// Check to see if rest is decimal portion
			if (s.charAt(pp.getIndex()) != '.') {
				return null;
			}

			// Skip decimal sign
			pp.setIndex(pp.getIndex() + 1);

			// Parse decimal portion
			if (pp.getIndex() < s.length()) {
				secFraction = s.substring(pp.getIndex());
				if (!secFraction.matches("\\d+")) {
					return null;
				}
				NumberFormat nf = NumberFormat.getIntegerInstance(Locale.ROOT);
				Number num = nf.parse(s, pp);
				if ((num == null) || (pp.getIndex() != s.length())) {
					// Invalid decimal portion
					return null;
				}

				// Determine precision - only support prec 3 or lower
				// (milliseconds) Higher precisions are quietly rounded away
				p = secFraction.length();
				if (maxPrecision >= 0) {
					// If there is a maximum precision, ignore subsequent digits
					p = Math.min(maxPrecision, p);
					secFraction = secFraction.substring(0, p);
				}

				// Calculate milliseconds
				String millis = secFraction;
				if (millis.length() > 3) {
					millis = secFraction.substring(0, 3);
				}
				while (millis.length() < 3) {
					millis = millis + "0";
				}

				int ms = Integer.valueOf(millis);
				cal.add(Calendar.MILLISECOND, ms);
			}
		}

		assert pp.getIndex() == s.length();
		return new PrecisionTime(cal, secFraction, p);
	}

	/**
	 * Gets the active time zone based on a Calendar argument
	 */
	public static TimeZone getTimeZone(Calendar cal) {
		if (cal == null) {
			return DEFAULT_ZONE;
		}
		return cal.getTimeZone();
	}

	/**
	 * Checks if the date/time format is valid
	 *
	 * @param pattern {@link SimpleDateFormat}  pattern
	 * @throws IllegalArgumentException if the given pattern is invalid
	 */
	public static void checkDateFormat(String pattern) {
		new SimpleDateFormat(pattern, Locale.ROOT);
	}

	/**
	 * Creates a new date formatter with Farrago specific options. Farrago
	 * parsing is strict and does not allow values such as day 0, month 13, etc.
	 *
	 * @param format {@link SimpleDateFormat}  pattern
	 */
	public static SimpleDateFormat newDateFormat(String format) {
		SimpleDateFormat sdf = new SimpleDateFormat(format, Locale.ROOT);
		sdf.setLenient(false);
		return sdf;
	}

	/** Helper for CAST({timestamp} AS VARCHAR(n)). */
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

	private static void unixTimeToString(StringBuilder buf, int time,
										 int precision) {
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

	/** Helper for CAST({date} AS VARCHAR(n)). */
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
			c = a - b *146097 / 4;
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

	public static String intervalYearMonthToString(int v, TimeUnitRange range) {
		final StringBuilder buf = new StringBuilder();
		if (v >= 0) {
			buf.append('+');
		} else {
			buf.append('-');
			v = -v;
		}
		final int y;
		final int m;
		switch (range) {
			case YEAR:
				v = roundUp(v, 12);
				y = v / 12;
				buf.append(y);
				break;
			case YEAR_TO_MONTH:
				y = v / 12;
				buf.append(y);
				buf.append('-');
				m = v % 12;
				number(buf, m, 2);
				break;
			case MONTH:
				m = v;
				buf.append(m);
				break;
			default:
				throw new AssertionError(range);
		}
		return buf.toString();
	}

	public static StringBuilder number(StringBuilder buf, int v, int n) {
		for (int k = digitCount(v); k < n; k++) {
			buf.append('0');
		}
		return buf.append(v);
	}

	public static int digitCount(int v) {
		for (int n = 1; true; n++) {
			v /= 10;
			if (v == 0) {
				return n;
			}
		}
	}

	private static int roundUp(int dividend, int divisor) {
		int remainder = dividend % divisor;
		dividend -= remainder;
		if (remainder * 2 > divisor) {
			dividend += divisor;
		}
		return dividend;
	}

	/** Cheap, unsafe, long power. power(2, 3) returns 8. */
	public static long powerX(long a, long b) {
		long x = 1;
		while (b > 0) {
			x *= a;
			--b;
		}
		return x;
	}

	public static String intervalDayTimeToString(long v, TimeUnitRange range,
												 int scale) {
		final StringBuilder buf = new StringBuilder();
		if (v >= 0) {
			buf.append('+');
		} else {
			buf.append('-');
			v = -v;
		}
		final long ms;
		final long s;
		final long m;
		final long h;
		final long d;
		switch (range) {
			case DAY_TO_SECOND:
				v = roundUp(v, powerX(10, 3 - scale));
				ms = v % 1000;
				v /= 1000;
				s = v % 60;
				v /= 60;
				m = v % 60;
				v /= 60;
				h = v % 24;
				v /= 24;
				d = v;
				buf.append((int) d);
				buf.append(' ');
				number(buf, (int) h, 2);
				buf.append(':');
				number(buf, (int) m, 2);
				buf.append(':');
				number(buf, (int) s, 2);
				fraction(buf, scale, ms);
				break;
			case DAY_TO_MINUTE:
				v = roundUp(v, 1000 * 60);
				v /= 1000;
				v /= 60;
				m = v % 60;
				v /= 60;
				h = v % 24;
				v /= 24;
				d = v;
				buf.append((int) d);
				buf.append(' ');
				number(buf, (int) h, 2);
				buf.append(':');
				number(buf, (int) m, 2);
				break;
			case DAY_TO_HOUR:
				v = roundUp(v, 1000 * 60 * 60);
				v /= 1000;
				v /= 60;
				v /= 60;
				h = v % 24;
				v /= 24;
				d = v;
				buf.append((int) d);
				buf.append(' ');
				number(buf, (int) h, 2);
				break;
			case DAY:
				v = roundUp(v, 1000 * 60 * 60 * 24);
				d = v / (1000 * 60 * 60 * 24);
				buf.append((int) d);
				break;
			case HOUR:
				v = roundUp(v, 1000 * 60 * 60);
				v /= 1000;
				v /= 60;
				v /= 60;
				h = v;
				buf.append((int) h);
				break;
			case HOUR_TO_MINUTE:
				v = roundUp(v, 1000 * 60);
				v /= 1000;
				v /= 60;
				m = v % 60;
				v /= 60;
				h = v;
				buf.append((int) h);
				buf.append(':');
				number(buf, (int) m, 2);
				break;
			case HOUR_TO_SECOND:
				v = roundUp(v, powerX(10, 3 - scale));
				ms = v % 1000;
				v /= 1000;
				s = v % 60;
				v /= 60;
				m = v % 60;
				v /= 60;
				h = v;
				buf.append((int) h);
				buf.append(':');
				number(buf, (int) m, 2);
				buf.append(':');
				number(buf, (int) s, 2);
				fraction(buf, scale, ms);
				break;
			case MINUTE_TO_SECOND:
				v = roundUp(v, powerX(10, 3 - scale));
				ms = v % 1000;
				v /= 1000;
				s = v % 60;
				v /= 60;
				m = v;
				buf.append((int) m);
				buf.append(':');
				number(buf, (int) s, 2);
				fraction(buf, scale, ms);
				break;
			case MINUTE:
				v = roundUp(v, 1000 * 60);
				v /= 1000;
				v /= 60;
				m = v;
				buf.append((int) m);
				break;
			case SECOND:
				v = roundUp(v, powerX(10, 3 - scale));
				ms = v % 1000;
				v /= 1000;
				s = v;
				buf.append((int) s);
				fraction(buf, scale, ms);
				break;
			default:
				throw new AssertionError(range);
		}
		return buf.toString();
	}

	/**
	 * Rounds a dividend to the nearest divisor.
	 * For example roundUp(31, 10) yields 30; roundUp(37, 10) yields 40.
	 * @param dividend Number to be divided
	 * @param divisor Number to divide by
	 * @return Rounded dividend
	 */
	private static long roundUp(long dividend, long divisor) {
		long remainder = dividend % divisor;
		dividend -= remainder;
		if (remainder * 2 > divisor) {
			dividend += divisor;
		}
		return dividend;
	}

	private static void fraction(StringBuilder buf, int scale, long ms) {
		if (scale > 0) {
			buf.append('.');
			long v1 = scale == 3 ? ms
				: scale == 2 ? ms / 10
				: scale == 1 ? ms / 100
				: 0;
			number(buf, (int) v1, scale);
		}
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
			d = dateStringToUnixDate(s);
			t = 0;
		}
		return d * MILLIS_PER_DAY + t;
	}

	public static long unixDateExtract(TimeUnitRange range, long date) {
		return julianExtract(range, (int) date + EPOCH_JULIAN);
	}

	private static int julianExtract(TimeUnitRange range, int julian) {
		// Algorithm the book "Astronomical Algorithms" by Jean Meeus, 1998
		int b, c;
		if (julian > 2299160) {
			int a = julian + 32044;
			b = (4 * a + 3) / 146097;
			c = a - b *146097 / 4;
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

		switch (range) {
			case YEAR:
				return year;
			case QUARTER:
				return (month + 2) / 3;
			case MONTH:
				return month;
			case DAY:
				return day;
			case DOW:
				return (int) floorMod(julian + 1, 7) + 1; // sun=1, sat=7
			case WEEK:
				long fmofw = firstMondayOfFirstWeek(year);
				if (julian < fmofw) {
					fmofw = firstMondayOfFirstWeek(year - 1);
				}
				return (int) (julian - fmofw) / 7 + 1;
			case DOY:
				final long janFirst = ymdToJulian(year, 1, 1);
				return (int) (julian - janFirst) + 1;
			case CENTURY:
				return year > 0
					? (year + 99) / 100
					: (year - 99) / 100;
			case MILLENNIUM:
				return year > 0
					? (year + 999) / 1000
					: (year - 999) / 1000;
			default:
				throw new AssertionError(range);
		}
	}

	/** Returns the first day of the first week of a year.
	 * Per ISO-8601 it is the Monday of the week that contains Jan 4,
	 * or equivalently, it is a Monday between Dec 29 and Jan 4.
	 * Sometimes it is in the year before the given year. */
	private static long firstMondayOfFirstWeek(int year) {
		final long janFirst = ymdToJulian(year, 1, 1);
		final long janFirstDow = floorMod(janFirst + 1, 7); // sun=0, sat=6
		return janFirst + (11 - janFirstDow) % 7 - 3;
	}

	/** Extracts a time unit from a UNIX date (milliseconds since epoch). */
	public static int unixTimestampExtract(TimeUnitRange range,
										   long timestamp) {
		return unixTimeExtract(range, (int) floorMod(timestamp, MILLIS_PER_DAY));
	}

	/** Extracts a time unit from a time value (milliseconds since midnight). */
	public static int unixTimeExtract(TimeUnitRange range, int time) {
		assert time >= 0;
		assert time < MILLIS_PER_DAY;
		switch (range) {
			case HOUR:
				return time / (int) MILLIS_PER_HOUR;
			case MINUTE:
				final int minutes = time / (int) MILLIS_PER_MINUTE;
				return minutes % 60;
			case SECOND:
				final int seconds = time / (int) MILLIS_PER_SECOND;
				return seconds % 60;
			default:
				throw new AssertionError(range);
		}
	}

	/** Resets to zero the "time" part of a timestamp. */
	public static long resetTime(long timestamp) {
		int date = (int) (timestamp / MILLIS_PER_DAY);
		return (long) date * MILLIS_PER_DAY;
	}

	/** Resets to epoch (1970-01-01) the "date" part of a timestamp. */
	public static long resetDate(long timestamp) {
		return floorMod(timestamp, MILLIS_PER_DAY);
	}

	public static long unixTimestampFloor(TimeUnitRange range, long timestamp) {
		int date = (int) (timestamp / MILLIS_PER_DAY);
		final int f = julianDateFloor(range, date + EPOCH_JULIAN, true);
		return (long) f * MILLIS_PER_DAY;
	}

	public static long unixDateFloor(TimeUnitRange range, long date) {
		return julianDateFloor(range, (int) date + EPOCH_JULIAN, true);
	}

	public static long unixTimestampCeil(TimeUnitRange range, long timestamp) {
		int date = (int) (timestamp / MILLIS_PER_DAY);
		final int f = julianDateFloor(range, date + EPOCH_JULIAN, false);
		return (long) f * MILLIS_PER_DAY;
	}

	public static long unixDateCeil(TimeUnitRange range, long date) {
		return julianDateFloor(range, (int) date + EPOCH_JULIAN, false);
	}

	private static int julianDateFloor(TimeUnitRange range, int julian,
									   boolean floor) {
		// Algorithm the book "Astronomical Algorithms" by Jean Meeus, 1998
		int b, c;
		if (julian > 2299160) {
			int a = julian + 32044;
			b = (4 * a + 3) / 146097;
			c = a - b *146097 / 4;
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

		switch (range) {
			case YEAR:
				if (!floor && (month > 1 || day > 1)) {
					++year;
				}
				return ymdToUnixDate(year, 1, 1);
			case MONTH:
				if (!floor && day > 1) {
					++month;
				}
				return ymdToUnixDate(year, month, 1);
			default:
				throw new AssertionError(range);
		}
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

	public static long unixTimestamp(int year, int month, int day, int hour,
									 int minute, int second) {
		final int date = ymdToUnixDate(year, month, day);
		return (long) date * MILLIS_PER_DAY
			+ (long) hour * MILLIS_PER_HOUR
			+ (long) minute * MILLIS_PER_MINUTE
			+ (long) second * MILLIS_PER_SECOND;
	}

	/** Adds a given number of months to a timestamp, represented as the number
	 * of milliseconds since the epoch. */
	public static long addMonths(long timestamp, int m) {
		final long millis =
			DateTimeUtils.floorMod(timestamp, DateTimeUtils.MILLIS_PER_DAY);
		timestamp -= millis;
		final long x =
			addMonths((int) (timestamp / DateTimeUtils.MILLIS_PER_DAY), m);
		return x * DateTimeUtils.MILLIS_PER_DAY + millis;
	}

	/** Adds a given number of months to a date, represented as the number of
	 * days since the epoch. */
	public static int addMonths(int date, int m) {
		int y0 = (int) DateTimeUtils.unixDateExtract(TimeUnitRange.YEAR, date);
		int m0 = (int) DateTimeUtils.unixDateExtract(TimeUnitRange.MONTH, date);
		int d0 = (int) DateTimeUtils.unixDateExtract(TimeUnitRange.DAY, date);
		int y = m / 12;
		y0 += y;
		m0 += m - y * 12;
		int last = lastDay(y0, m0);
		if (d0 > last) {
			d0 = 1;
			if (++m0 > 12) {
				m0 = 1;
				++y0;
			}
		}
		return DateTimeUtils.ymdToUnixDate(y0, m0, d0);
	}

	private static int lastDay(int y, int m) {
		switch (m) {
			case 2:
				return y % 4 == 0
					&& (y % 100 != 0
					|| y % 400 == 0)
					? 29 : 28;
			case 4:
			case 6:
			case 9:
			case 11:
				return 30;
			default:
				return 31;
		}
	}

	/** Finds the number of months between two dates, each represented as the
	 * number of days since the epoch. */
	public static int subtractMonths(int date0, int date1) {
		if (date0 < date1) {
			return -subtractMonths(date1, date0);
		}
		// Start with an estimate.
		// Since no month has more than 31 days, the estimate is <= the true value.
		int m = (date0 - date1) / 31;
		while (true) {
			int date2 = addMonths(date1, m);
			if (date2 >= date0) {
				return m;
			}
			int date3 = addMonths(date1, m + 1);
			if (date3 > date0) {
				return m;
			}
			++m;
		}
	}

	public static int subtractMonths(long t0, long t1) {
		final long millis0 =
			DateTimeUtils.floorMod(t0, DateTimeUtils.MILLIS_PER_DAY);
		final int d0 = (int) DateTimeUtils.floorDiv(t0 - millis0,
			DateTimeUtils.MILLIS_PER_DAY);
		final long millis1 =
			DateTimeUtils.floorMod(t1, DateTimeUtils.MILLIS_PER_DAY);
		final int d1 = (int) DateTimeUtils.floorDiv(t1 - millis1,
			DateTimeUtils.MILLIS_PER_DAY);
		int x = subtractMonths(d0, d1);
		final long d2 = addMonths(d1, x);
		if (d2 == d0 && millis0 < millis1) {
			--x;
		}
		return x;
	}

	/** Divide, rounding towards negative infinity. */
	public static long floorDiv(long x, long y) {
		long r = x / y;
		// if the signs are different and modulo not zero, round down
		if ((x ^ y) < 0 && (r * y != x)) {
			r--;
		}
		return r;
	}

	/** Modulo, always returning a non-negative result. */
	public static long floorMod(long x, long y) {
		return x - floorDiv(x, y) * y;
	}

	/** Creates an instance of {@link Calendar} in the root locale and UTC time
	 * zone. */
	public static Calendar calendar() {
		return Calendar.getInstance(UTC_ZONE, Locale.ROOT);
	}

	//~ Inner Classes ----------------------------------------------------------

	/**
	 * Helper class for {@link DateTimeUtils#parsePrecisionDateTimeLiteral}
	 */
	public static class PrecisionTime {
		private final Calendar cal;
		private final String fraction;
		private final int precision;

		public PrecisionTime(Calendar cal, String fraction, int precision) {
			this.cal = cal;
			this.fraction = fraction;
			this.precision = precision;
		}

		public Calendar getCalendar() {
			return cal;
		}

		public int getPrecision() {
			return precision;
		}

		public String getFraction() {
			return fraction;
		}
	}
}

// End DateTimeUtils.java
