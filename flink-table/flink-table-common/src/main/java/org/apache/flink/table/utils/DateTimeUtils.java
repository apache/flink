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

package org.apache.flink.table.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.TimestampType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;

/**
 * Utility functions for datetime types: date, time, timestamp.
 *
 * <p>These utils include:
 *
 * <ul>
 *   <li>{@code parse[type]}: methods for parsing strings to date/time/timestamp
 *   <li>{@code format[type]}: methods for formatting date/time/timestamp
 *   <li>{@code to[externalTypeName]} and {@code toInternal}: methods for converting values from
 *       internal date/time/timestamp types from/to java.sql or java.time types
 *   <li>Various operations on timestamp, including floor, ceil and extract
 *   <li>{@link TimeUnit} and {@link TimeUnitRange} enums
 * </ul>
 *
 * <p>Currently, this class is a bit messy because it includes a mix of functionalities both from
 * common and planner. We should strive to reduce the number of functionalities here, eventually
 * moving some methods closer to where they're needed. Connectors and formats should not use this
 * class, but rather if a functionality is necessary, it should be part of the public APIs of our
 * type system (e.g a new method in {@link TimestampData} or in {@link TimestampType}). Methods used
 * only by the planner should live inside the planner whenever is possible.
 */
@Internal
public class DateTimeUtils {

    private static final Logger LOG = LoggerFactory.getLogger(DateTimeUtils.class);

    /** The julian date of the epoch, 1970-01-01. */
    public static final int EPOCH_JULIAN = 2440588;

    /** The number of milliseconds in a second. */
    private static final long MILLIS_PER_SECOND = 1000L;

    /** The number of milliseconds in a minute. */
    private static final long MILLIS_PER_MINUTE = 60000L;

    /** The number of milliseconds in an hour. */
    private static final long MILLIS_PER_HOUR = 3600000L; // = 60 * 60 * 1000

    /**
     * The number of milliseconds in a day.
     *
     * <p>This is the modulo 'mask' used when converting TIMESTAMP values to DATE and TIME values.
     */
    public static final long MILLIS_PER_DAY = 86400000L; // = 24 * 60 * 60 * 1000

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
    public static final TimeZone LOCAL_TZ = TimeZone.getDefault();

    /** The valid minimum epoch milliseconds ('0000-01-01 00:00:00.000 UTC+0'). */
    private static final long MIN_EPOCH_MILLS = -62167219200000L;

    /** The valid minimum epoch seconds ('0000-01-01 00:00:00 UTC+0'). */
    private static final long MIN_EPOCH_SECONDS = -62167219200L;

    /** The valid maximum epoch milliseconds ('9999-12-31 23:59:59.999 UTC+0'). */
    private static final long MAX_EPOCH_MILLS = 253402300799999L;

    /** The valid maximum epoch seconds ('9999-12-31 23:59:59 UTC+0'). */
    private static final long MAX_EPOCH_SECONDS = 253402300799L;

    private static final DateTimeFormatter DEFAULT_TIMESTAMP_FORMATTER =
            new DateTimeFormatterBuilder()
                    .appendPattern("yyyy-[MM][M]-[dd][d]")
                    .optionalStart()
                    .appendPattern(" [HH][H]:[mm][m]:[ss][s]")
                    .appendFraction(NANO_OF_SECOND, 0, 9, true)
                    .optionalEnd()
                    .toFormatter();

    /**
     * A ThreadLocal cache map for SimpleDateFormat, because SimpleDateFormat is not thread-safe.
     * (string_format) => formatter
     */
    private static final ThreadLocalCache<String, SimpleDateFormat> FORMATTER_CACHE =
            ThreadLocalCache.of(SimpleDateFormat::new);

    /** A ThreadLocal cache map for DateTimeFormatter. (string_format) => formatter */
    private static final ThreadLocalCache<String, DateTimeFormatter> DATETIME_FORMATTER_CACHE =
            ThreadLocalCache.of(DateTimeFormatter::ofPattern);

    /** A ThreadLocal cache map for TimeZone. (string_zone_id) => TimeZone */
    private static final ThreadLocalCache<String, TimeZone> TIMEZONE_CACHE =
            ThreadLocalCache.of(TimeZone::getTimeZone);

    // --------------------------------------------------------------------------------------------
    // java.sql Date/Time/Timestamp --> internal data types
    // --------------------------------------------------------------------------------------------

    /**
     * Converts the internal representation of a SQL DATE (int) to the Java type used for UDF
     * parameters ({@link java.sql.Date}).
     */
    public static java.sql.Date toSQLDate(int v) {
        // note that, in this case, can't handle Daylight Saving Time
        final long t = v * MILLIS_PER_DAY;
        return new java.sql.Date(t - LOCAL_TZ.getOffset(t));
    }

    /**
     * Converts the internal representation of a SQL TIME (int) to the Java type used for UDF
     * parameters ({@link java.sql.Time}).
     */
    public static java.sql.Time toSQLTime(int v) {
        // note that, in this case, can't handle Daylight Saving Time
        return new java.sql.Time(v - LOCAL_TZ.getOffset(v));
    }

    /**
     * Converts the internal representation of a SQL TIMESTAMP (long) to the Java type used for UDF
     * parameters ({@link java.sql.Timestamp}).
     */
    public static java.sql.Timestamp toSQLTimestamp(long v) {
        return new java.sql.Timestamp(v - LOCAL_TZ.getOffset(v));
    }

    /**
     * Converts the Java type used for UDF parameters of SQL DATE type ({@link java.sql.Date}) to
     * internal representation (int).
     *
     * <p>Converse of {@link #toSQLDate(int)}.
     */
    public static int toInternal(java.sql.Date date) {
        long ts = date.getTime() + LOCAL_TZ.getOffset(date.getTime());
        return (int) (ts / MILLIS_PER_DAY);
    }

    /**
     * Converts the Java type used for UDF parameters of SQL TIME type ({@link java.sql.Time}) to
     * internal representation (int).
     *
     * <p>Converse of {@link #toSQLTime(int)}.
     */
    public static int toInternal(java.sql.Time time) {
        long ts = time.getTime() + LOCAL_TZ.getOffset(time.getTime());
        return (int) (ts % MILLIS_PER_DAY);
    }

    /**
     * Converts the Java type used for UDF parameters of SQL TIMESTAMP type ({@link
     * java.sql.Timestamp}) to internal representation (long).
     *
     * <p>Converse of {@link #toSQLTimestamp(long)}.
     */
    public static long toInternal(java.sql.Timestamp ts) {
        long time = ts.getTime();
        return time + LOCAL_TZ.getOffset(time);
    }

    // --------------------------------------------------------------------------------------------
    // Java 8 time conversion
    // --------------------------------------------------------------------------------------------

    public static LocalDate toLocalDate(int date) {
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

    public static int toInternal(LocalDate date) {
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
        return day + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 32045;
    }

    public static LocalTime toLocalTime(int time) {
        int h = time / 3600000;
        int time2 = time % 3600000;
        int m = time2 / 60000;
        int time3 = time2 % 60000;
        int s = time3 / 1000;
        int ms = time3 % 1000;
        return LocalTime.of(h, m, s, ms * 1000_000);
    }

    public static int toInternal(LocalTime time) {
        return time.getHour() * (int) MILLIS_PER_HOUR
                + time.getMinute() * (int) MILLIS_PER_MINUTE
                + time.getSecond() * (int) MILLIS_PER_SECOND
                + time.getNano() / 1000_000;
    }

    public static LocalDateTime toLocalDateTime(long timestamp) {
        int date = (int) (timestamp / MILLIS_PER_DAY);
        int time = (int) (timestamp % MILLIS_PER_DAY);
        if (time < 0) {
            --date;
            time += MILLIS_PER_DAY;
        }
        LocalDate localDate = toLocalDate(date);
        LocalTime localTime = toLocalTime(time);
        return LocalDateTime.of(localDate, localTime);
    }

    public static long toTimestampMillis(LocalDateTime dateTime) {
        return unixTimestamp(
                dateTime.getYear(),
                dateTime.getMonthValue(),
                dateTime.getDayOfMonth(),
                dateTime.getHour(),
                dateTime.getMinute(),
                dateTime.getSecond(),
                dateTime.getNano() / 1000_000);
    }

    private static long unixTimestamp(
            int year, int month, int day, int hour, int minute, int second, int mills) {
        final int date = ymdToUnixDate(year, month, day);
        return (long) date * MILLIS_PER_DAY
                + (long) hour * MILLIS_PER_HOUR
                + (long) minute * MILLIS_PER_MINUTE
                + (long) second * MILLIS_PER_SECOND
                + mills;
    }

    // --------------------------------------------------------------------------------------------
    // Numeric -> Timestamp conversion
    // --------------------------------------------------------------------------------------------

    public static TimestampData toTimestampData(long v, int precision) {
        switch (precision) {
            case 0:
                if (MIN_EPOCH_SECONDS <= v && v <= MAX_EPOCH_SECONDS) {
                    return timestampDataFromEpochMills(v * MILLIS_PER_SECOND);
                } else {
                    return null;
                }
            case 3:
                return timestampDataFromEpochMills(v);
            default:
                throw new TableException(
                        "The precision value '"
                                + precision
                                + "' for function "
                                + "TO_TIMESTAMP_LTZ(numeric, precision) is unsupported,"
                                + " the supported value is '0' for second or '3' for millisecond.");
        }
    }

    public static TimestampData toTimestampData(double v, int precision) {
        switch (precision) {
            case 0:
                if (MIN_EPOCH_SECONDS <= v && v <= MAX_EPOCH_SECONDS) {
                    return timestampDataFromEpochMills((long) (v * MILLIS_PER_SECOND));
                } else {
                    return null;
                }
            case 3:
                return timestampDataFromEpochMills((long) v);
            default:
                throw new TableException(
                        "The precision value '"
                                + precision
                                + "' for function "
                                + "TO_TIMESTAMP_LTZ(numeric, precision) is unsupported,"
                                + " the supported value is '0' for second or '3' for millisecond.");
        }
    }

    public static TimestampData toTimestampData(DecimalData v, int precision) {
        long epochMills;
        switch (precision) {
            case 0:
                epochMills =
                        v.toBigDecimal().setScale(0, RoundingMode.DOWN).longValue()
                                * MILLIS_PER_SECOND;
                return timestampDataFromEpochMills(epochMills);
            case 3:
                epochMills = toMillis(v);
                return timestampDataFromEpochMills(epochMills);
            default:
                throw new TableException(
                        "The precision value '"
                                + precision
                                + "' for function "
                                + "TO_TIMESTAMP_LTZ(numeric, precision) is unsupported,"
                                + " the supported value is '0' for second or '3' for millisecond.");
        }
    }

    private static TimestampData timestampDataFromEpochMills(long epochMills) {
        if (MIN_EPOCH_MILLS <= epochMills && epochMills <= MAX_EPOCH_MILLS) {
            return TimestampData.fromEpochMillis(epochMills);
        }
        return null;
    }

    private static long toMillis(DecimalData v) {
        return v.toBigDecimal().setScale(0, RoundingMode.DOWN).longValue();
    }

    // --------------------------------------------------------------------------------------------
    // Parsing functions
    // --------------------------------------------------------------------------------------------

    public static TimestampData parseTimestampData(String dateStr) throws DateTimeException {
        // Precision is hardcoded to match signature of TO_TIMESTAMP
        //  https://issues.apache.org/jira/browse/FLINK-14925
        return parseTimestampData(dateStr, 3);
    }

    public static TimestampData parseTimestampData(String dateStr, int precision)
            throws DateTimeException {
        return TimestampData.fromLocalDateTime(
                fromTemporalAccessor(DEFAULT_TIMESTAMP_FORMATTER.parse(dateStr), precision));
    }

    public static TimestampData parseTimestampData(String dateStr, int precision, TimeZone timeZone)
            throws DateTimeException {
        return TimestampData.fromInstant(
                fromTemporalAccessor(DEFAULT_TIMESTAMP_FORMATTER.parse(dateStr), precision)
                        .atZone(timeZone.toZoneId())
                        .toInstant());
    }

    public static TimestampData parseTimestampData(String dateStr, String format) {
        DateTimeFormatter formatter = DATETIME_FORMATTER_CACHE.get(format);

        try {
            TemporalAccessor accessor = formatter.parse(dateStr);
            // Precision is hardcoded to match signature of TO_TIMESTAMP
            //  https://issues.apache.org/jira/browse/FLINK-14925
            LocalDateTime ldt = fromTemporalAccessor(accessor, 3);
            return TimestampData.fromLocalDateTime(ldt);
        } catch (DateTimeParseException e) {
            // fall back to support cases like '1999-9-10 05:20:10' or '1999-9-10'
            try {
                dateStr = dateStr.trim();
                int space = dateStr.indexOf(' ');
                if (space >= 0) {
                    Timestamp ts = Timestamp.valueOf(dateStr);
                    return TimestampData.fromTimestamp(ts);
                } else {
                    java.sql.Date dt = java.sql.Date.valueOf(dateStr);
                    return TimestampData.fromLocalDateTime(
                            LocalDateTime.of(dt.toLocalDate(), LocalTime.MIDNIGHT));
                }
            } catch (IllegalArgumentException ie) {
                return null;
            }
        }
    }

    /**
     * This is similar to {@link LocalDateTime#from(TemporalAccessor)}, but it's less strict and
     * introduces default values.
     */
    private static LocalDateTime fromTemporalAccessor(TemporalAccessor accessor, int precision) {
        // complement year with 1970
        int year = accessor.isSupported(YEAR) ? accessor.get(YEAR) : 1970;
        // complement month with 1
        int month = accessor.isSupported(MONTH_OF_YEAR) ? accessor.get(MONTH_OF_YEAR) : 1;
        // complement day with 1
        int day = accessor.isSupported(DAY_OF_MONTH) ? accessor.get(DAY_OF_MONTH) : 1;
        // complement hour with 0
        int hour = accessor.isSupported(HOUR_OF_DAY) ? accessor.get(HOUR_OF_DAY) : 0;
        // complement minute with 0
        int minute = accessor.isSupported(MINUTE_OF_HOUR) ? accessor.get(MINUTE_OF_HOUR) : 0;
        // complement second with 0
        int second = accessor.isSupported(SECOND_OF_MINUTE) ? accessor.get(SECOND_OF_MINUTE) : 0;
        // complement nano_of_second with 0
        int nanoOfSecond = accessor.isSupported(NANO_OF_SECOND) ? accessor.get(NANO_OF_SECOND) : 0;

        if (precision == 0) {
            nanoOfSecond = 0;
        } else if (precision != 9) {
            nanoOfSecond = (int) floor(nanoOfSecond, powerX(10, 9 - precision));
        }

        return LocalDateTime.of(year, month, day, hour, minute, second, nanoOfSecond);
    }

    /**
     * Parse date time string to timestamp based on the given time zone and format. Returns null if
     * parsing failed.
     *
     * @param dateStr the date time string
     * @param format date time string format
     * @param tz the time zone
     */
    private static long parseTimestampMillis(String dateStr, String format, TimeZone tz)
            throws ParseException {
        SimpleDateFormat formatter = FORMATTER_CACHE.get(format);
        formatter.setTimeZone(tz);
        return formatter.parse(dateStr).getTime();
    }

    /**
     * Parse date time string to timestamp based on the given time zone string and format. Returns
     * null if parsing failed.
     *
     * @param dateStr the date time string
     * @param tzStr the time zone id string
     */
    private static long parseTimestampTz(String dateStr, String tzStr) throws ParseException {
        TimeZone tz = TIMEZONE_CACHE.get(tzStr);
        return parseTimestampMillis(dateStr, DateTimeUtils.TIMESTAMP_FORMAT_STRING, tz);
    }

    /** Returns the epoch days since 1970-01-01. */
    public static int parseDate(String dateStr, String fromFormat) {
        // It is OK to use UTC, we just want get the epoch days
        // TODO  use offset, better performance
        long ts = internalParseTimestampMillis(dateStr, fromFormat, TimeZone.getTimeZone("UTC"));
        ZoneId zoneId = ZoneId.of("UTC");
        Instant instant = Instant.ofEpochMilli(ts);
        ZonedDateTime zdt = ZonedDateTime.ofInstant(instant, zoneId);
        return ymdToUnixDate(zdt.getYear(), zdt.getMonthValue(), zdt.getDayOfMonth());
    }

    public static Integer parseDate(String s) {
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
        return ymdToUnixDate(y, m, d);
    }

    public static Integer parseTime(String v) {
        final int start = 0;
        final int colon1 = v.indexOf(':', start);
        // timezone hh:mm:ss[.ssssss][[+|-]hh:mm:ss]
        // refer https://www.w3.org/TR/NOTE-datetime
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
            minute = 0;
            second = 0;
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
                second = 0;
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
                    milli = parseFraction(v.substring(dot + 1, end).trim());
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

    /**
     * Parses a fraction, multiplying the first character by {@code multiplier}, the second
     * character by {@code multiplier / 10}, the third character by {@code multiplier / 100}, and so
     * forth.
     *
     * <p>For example, {@code parseFraction("1234", 100)} yields {@code 123}.
     */
    private static int parseFraction(String v) {
        int multiplier = 100;
        int r = 0;
        for (int i = 0; i < v.length(); i++) {
            char c = v.charAt(i);
            int x = c < '0' || c > '9' ? 0 : (c - '0');
            r += multiplier * x;
            if (multiplier < 10) {
                // We're at the last digit. Check for rounding.
                if (i + 1 < v.length() && v.charAt(i + 1) >= '5') {
                    ++r;
                }
                break;
            }
            multiplier /= 10;
        }
        return r;
    }

    // --------------------------------------------------------------------------------------------
    // Format
    // --------------------------------------------------------------------------------------------

    public static String formatTimestamp(TimestampData ts, String format) {
        return formatTimestamp(ts, format, ZoneId.of("UTC"));
    }

    public static String formatTimestamp(TimestampData ts, String format, TimeZone zone) {
        return formatTimestamp(ts, format, zone.toZoneId());
    }

    private static String formatTimestamp(TimestampData ts, int precision) {
        LocalDateTime ldt = ts.toLocalDateTime();

        String fraction = pad(9, ldt.getNano());
        while (fraction.length() > precision && fraction.endsWith("0")) {
            fraction = fraction.substring(0, fraction.length() - 1);
        }

        StringBuilder ymdhms =
                ymdhms(
                        new StringBuilder(),
                        ldt.getYear(),
                        ldt.getMonthValue(),
                        ldt.getDayOfMonth(),
                        ldt.getHour(),
                        ldt.getMinute(),
                        ldt.getSecond());

        if (fraction.length() > 0) {
            ymdhms.append(".").append(fraction);
        }

        return ymdhms.toString();
    }

    public static String formatTimestamp(TimestampData ts, TimeZone tz, int precision) {
        return formatTimestamp(timestampWithLocalZoneToTimestamp(ts, tz), precision);
    }

    private static String formatTimestamp(TimestampData ts, String format, ZoneId zoneId) {
        DateTimeFormatter formatter = DATETIME_FORMATTER_CACHE.get(format);
        Instant instant = ts.toInstant();
        return LocalDateTime.ofInstant(instant, zoneId).format(formatter);
    }

    public static String formatTimestampMillis(long ts, String format, TimeZone tz) {
        SimpleDateFormat formatter = FORMATTER_CACHE.get(format);
        formatter.setTimeZone(tz);
        Date dateTime = new Date(ts);
        return formatter.format(dateTime);
    }

    public static String formatTimestampString(
            String dateStr, String fromFormat, String toFormat, TimeZone tz) {
        SimpleDateFormat fromFormatter = FORMATTER_CACHE.get(fromFormat);
        fromFormatter.setTimeZone(tz);
        SimpleDateFormat toFormatter = FORMATTER_CACHE.get(toFormat);
        toFormatter.setTimeZone(tz);
        try {
            return toFormatter.format(fromFormatter.parse(dateStr));
        } catch (ParseException e) {
            LOG.error(
                    "Exception when formatting: '"
                            + dateStr
                            + "' from: '"
                            + fromFormat
                            + "' to: '"
                            + toFormat
                            + "'",
                    e);
            return null;
        }
    }

    public static String formatTimestampString(String dateStr, String toFormat, TimeZone tz) {
        // use yyyy-MM-dd HH:mm:ss as default
        return formatTimestampString(dateStr, TIMESTAMP_FORMAT_STRING, toFormat, tz);
    }

    public static String formatTimestampString(String dateStr, String toFormat) {
        return formatTimestampString(dateStr, toFormat, UTC_ZONE);
    }

    public static String formatTimestampMillis(int time, int precision) {
        final StringBuilder buf = new StringBuilder(8 + (precision > 0 ? precision + 1 : 0));
        formatTimestampMillis(buf, time, precision);
        return buf.toString();
    }

    private static void formatTimestampMillis(StringBuilder buf, int time, int precision) {
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

    /** Helper for CAST({date} AS VARCHAR(n)). */
    public static String formatDate(int date) {
        final StringBuilder buf = new StringBuilder(10);
        formatDate(buf, date);
        return buf.toString();
    }

    private static void formatDate(StringBuilder buf, int date) {
        julianToString(buf, date + EPOCH_JULIAN);
    }

    private static void julianToString(StringBuilder buf, int julian) {
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
        int4(buf, year);
        buf.append('-');
        int2(buf, month);
        buf.append('-');
        int2(buf, day);
    }

    public static String formatIntervalYearMonth(int v) {
        final StringBuilder buf = new StringBuilder();
        if (v >= 0) {
            buf.append('+');
        } else {
            buf.append('-');
            v = -v;
        }
        final int y = v / 12;
        final int m = v % 12;
        buf.append(y);
        buf.append('-');
        number(buf, m, 2);
        return buf.toString();
    }

    public static StringBuilder number(StringBuilder buf, int v, int n) {
        for (int k = digitCount(v); k < n; k++) {
            buf.append('0');
        }
        return buf.append(v);
    }

    private static int digitCount(int v) {
        for (int n = 1; ; n++) {
            v /= 10;
            if (v == 0) {
                return n;
            }
        }
    }

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
            long v1 = scale == 3 ? ms : scale == 2 ? ms / 10 : scale == 1 ? ms / 100 : 0;
            number(buf, (int) v1, scale);
        }
    }

    private static long powerX(long a, long b) {
        long x = 1;
        while (b > 0) {
            x *= a;
            --b;
        }
        return x;
    }

    public static String formatIntervalDayTime(long v) {
        final int scale = 3;
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
        return buf.toString();
    }

    private static long internalParseTimestampMillis(String dateStr, String format, TimeZone tz) {
        SimpleDateFormat formatter = FORMATTER_CACHE.get(format);
        formatter.setTimeZone(tz);
        try {
            Date date = formatter.parse(dateStr);
            return date.getTime();
        } catch (ParseException e) {
            LOG.error(
                    String.format(
                            "Exception when parsing datetime string '%s' in format '%s'",
                            dateStr, format),
                    e);
            return Long.MIN_VALUE;
        }
    }

    // --------------------------------------------------------------------------------------------
    // EXTRACT
    // --------------------------------------------------------------------------------------------

    private static final TimestampType REUSE_TIMESTAMP_TYPE = new TimestampType(9);

    public static long extractFromTimestamp(TimeUnitRange range, TimestampData ts, TimeZone tz) {
        return convertExtract(range, ts, REUSE_TIMESTAMP_TYPE, tz);
    }

    public static long extractFromDate(TimeUnitRange range, long date) {
        return extractFromDate(range, (int) date);
    }

    public static long extractFromDate(TimeUnitRange range, int date) {
        switch (range) {
            case EPOCH:
                return date * 86400L;
            default:
                return julianExtract(range, date + 2440588);
        }
    }

    private static int julianExtract(TimeUnitRange range, int julian) {
        int j = julian + 32044;
        int g = j / 146097;
        int dg = j % 146097;
        int c = (dg / '躬' + 1) * 3 / 4;
        int dc = dg - c * '躬';
        int b = dc / 1461;
        int db = dc % 1461;
        int a = (db / 365 + 1) * 3 / 4;
        int da = db - a * 365;
        int y = g * 400 + c * 100 + b * 4 + a;
        int m = (da * 5 + 308) / 153 - 2;
        int d = da - (m + 4) * 153 / 5 + 122;
        int year = y - 4800 + (m + 2) / 12;
        int month = (m + 2) % 12 + 1;
        int day = d + 1;
        switch (range) {
            case YEAR:
                return year;
            case YEAR_TO_MONTH:
            case DAY_TO_SECOND:
            case DAY_TO_MINUTE:
            case DAY_TO_HOUR:
            case HOUR:
            case HOUR_TO_MINUTE:
            case HOUR_TO_SECOND:
            case MINUTE_TO_SECOND:
            case MINUTE:
            case SECOND:
            case EPOCH:
            default:
                throw new AssertionError(range);
            case MONTH:
                return month;
            case DAY:
                return day;
            case ISOYEAR:
                int weekNumber = getIso8601WeekNumber(julian, year, month, day);
                if (weekNumber == 1 && month == 12) {
                    return year + 1;
                } else {
                    if (month == 1 && weekNumber > 50) {
                        return year - 1;
                    }

                    return year;
                }
            case QUARTER:
                return (month + 2) / 3;
            case DOW:
                return (int) floorMod((long) (julian + 1), 7L) + 1;
            case ISODOW:
                return (int) floorMod((long) julian, 7L) + 1;
            case WEEK:
                return getIso8601WeekNumber(julian, year, month, day);
            case DOY:
                long janFirst = (long) ymdToJulian(year, 1, 1);
                return (int) ((long) julian - janFirst) + 1;
            case DECADE:
                return year / 10;
            case CENTURY:
                return year > 0 ? (year + 99) / 100 : (year - 99) / 100;
            case MILLENNIUM:
                return year > 0 ? (year + 999) / 1000 : (year - 999) / 1000;
        }
    }

    private static long firstMondayOfFirstWeek(int year) {
        long janFirst = (long) ymdToJulian(year, 1, 1);
        long janFirstDow = floorMod(janFirst + 1L, 7L);
        return janFirst + (11L - janFirstDow) % 7L - 3L;
    }

    private static int getIso8601WeekNumber(int julian, int year, int month, int day) {
        long fmofw = firstMondayOfFirstWeek(year);
        if (month == 12 && day > 28) {
            return 31 - day + 4 > 7 - ((int) floorMod((long) julian, 7L) + 1)
                            && 31 - day + (int) (floorMod((long) julian, 7L) + 1L) >= 4
                    ? (int) ((long) julian - fmofw) / 7 + 1
                    : 1;
        } else if (month == 1 && day < 5) {
            return 4 - day <= 7 - ((int) floorMod((long) julian, 7L) + 1)
                            && day - (int) (floorMod((long) julian, 7L) + 1L) >= -3
                    ? 1
                    : (int) ((long) julian - firstMondayOfFirstWeek(year - 1)) / 7 + 1;
        } else {
            return (int) ((long) julian - fmofw) / 7 + 1;
        }
    }

    private static long floorDiv(long x, long y) {
        long r = x / y;
        if ((x ^ y) < 0L && r * y != x) {
            --r;
        }

        return r;
    }

    private static long floorMod(long x, long y) {
        return x - floorDiv(x, y) * y;
    }

    private static long convertExtract(
            TimeUnitRange range, TimestampData ts, LogicalType type, TimeZone tz) {
        TimeUnit startUnit = range.startUnit;
        long millisecond = ts.getMillisecond();
        int nanoOfMillisecond = ts.getNanoOfMillisecond();
        long offset = tz.getOffset(millisecond);
        long utcTs = millisecond + offset;

        switch (startUnit) {
            case MILLENNIUM:
            case CENTURY:
            case DECADE:
            case YEAR:
            case QUARTER:
            case MONTH:
            case DAY:
            case DOW:
            case DOY:
            case ISODOW:
            case ISOYEAR:
            case WEEK:
                if (type.is(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)) {
                    long d = divide(utcTs, TimeUnit.DAY.multiplier);
                    return extractFromDate(range, d);
                } else if (type.is(LogicalTypeRoot.DATE)) {
                    return divide(utcTs, TimeUnit.DAY.multiplier);
                } else {
                    // TODO support it
                    throw new TableException(startUnit + " for " + type + " is unsupported now.");
                }
            case EPOCH:
                if (type.isAnyOf(
                        LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE, LogicalTypeRoot.DATE)) {
                    return utcTs / 1000;
                } else {
                    // TODO support it
                    throw new TableException(startUnit + " for " + type + " is unsupported now.");
                }
            case MICROSECOND:
                if (type.is(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)) {
                    long millis = divide(mod(utcTs, getFactor(startUnit)), startUnit.multiplier);
                    int micros = nanoOfMillisecond / 1000;
                    return millis + micros;
                } else {
                    throw new TableException(startUnit + " for " + type + " is unsupported now.");
                }
            case NANOSECOND:
                if (type.is(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)) {
                    long millis = divide(mod(utcTs, getFactor(startUnit)), startUnit.multiplier);
                    return millis + nanoOfMillisecond;
                } else {
                    throw new TableException(startUnit + " for " + type + " is unsupported now.");
                }
            default:
                // fall through
        }

        long res = mod(utcTs, getFactor(startUnit));
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

    private static BigDecimal getFactor(TimeUnit unit) {
        switch (unit) {
            case DAY:
                return BigDecimal.ONE;
            case HOUR:
                return TimeUnit.DAY.multiplier;
            case MINUTE:
                return TimeUnit.HOUR.multiplier;
            case SECOND:
                return TimeUnit.MINUTE.multiplier;
            case MILLISECOND:
            case MICROSECOND:
            case NANOSECOND:
                return TimeUnit.SECOND.multiplier;
            case YEAR:
                return BigDecimal.ONE;
            case MONTH:
                return TimeUnit.YEAR.multiplier;
            case QUARTER:
                return TimeUnit.YEAR.multiplier;
            case DECADE:
            case CENTURY:
            case MILLENNIUM:
                return BigDecimal.ONE;
            default:
                throw new IllegalArgumentException("Invalid start unit.");
        }
    }

    // --------------------------------------------------------------------------------------------
    // Floor/Ceil/Convert tz
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
            case MILLENNIUM:
            case CENTURY:
            case DECADE:
            case MONTH:
            case YEAR:
            case QUARTER:
            case WEEK:
                int days = (int) (utcTs / MILLIS_PER_DAY + EPOCH_JULIAN);
                return julianDateFloor(range, days, true) * MILLIS_PER_DAY - offset;
            default:
                // for MINUTE and SECONDS etc...,
                // it is more effective to use arithmetic Method
                throw new AssertionError(range);
        }
    }

    /**
     * Keep the algorithm consistent with Calcite DateTimeUtils.julianDateFloor, but here we take
     * time zone into account.
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
            case MILLENNIUM:
            case CENTURY:
            case DECADE:
            case MONTH:
            case YEAR:
            case QUARTER:
            case WEEK:
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
        } else {
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
            case MILLENNIUM:
                return floor
                        ? ymdToUnixDate(1000 * ((year + 999) / 1000) - 999, 1, 1)
                        : ymdToUnixDate(1000 * ((year + 999) / 1000) + 1, 1, 1);
            case CENTURY:
                return floor
                        ? ymdToUnixDate(100 * ((year + 99) / 100) - 99, 1, 1)
                        : ymdToUnixDate(100 * ((year + 99) / 100) + 1, 1, 1);
            case DECADE:
                return floor
                        ? ymdToUnixDate(10 * (year / 10), 1, 1)
                        : ymdToUnixDate(10 * (1 + year / 10), 1, 1);
            case YEAR:
                if (!floor && (month > 1 || day > 1)) {
                    year += 1;
                }
                return ymdToUnixDate(year, 1, 1);
            case MONTH:
                if (!floor && day > 1) {
                    month += 1;
                }
                return ymdToUnixDate(year, month, 1);
            case QUARTER:
                if (!floor && (month > 1 || day > 1)) {
                    quarter += 1;
                }
                return ymdToUnixDate(year, quarter * 3 - 2, 1);
            case WEEK:
                int dow = (int) floorMod(julian + 1, 7); // sun=0, sat=6
                int offset = dow;
                if (!floor && offset > 0) {
                    offset -= 7;
                }
                return ymdToUnixDate(year, month, day) - offset;
            case DAY:
                int res = ymdToUnixDate(year, month, day);
                return floor ? res : res + 1;
            default:
                throw new AssertionError(range);
        }
    }

    /**
     * Convert datetime string from a time zone to another time zone.
     *
     * @param dateStr the date time string
     * @param tzFrom the original time zone
     * @param tzTo the target time zone
     */
    public static String convertTz(String dateStr, String tzFrom, String tzTo) {
        try {
            return formatTimestampTz(parseTimestampTz(dateStr, tzFrom), tzTo);
        } catch (ParseException e) {
            return null;
        }
    }

    private static String formatTimestampTz(long ts, String tzStr) {
        TimeZone tz = TIMEZONE_CACHE.get(tzStr);
        return formatTimestampMillis(ts, DateTimeUtils.TIMESTAMP_FORMAT_STRING, tz);
    }

    // --------------------------------------------------------------------------------------------
    // TIMESTAMP to  DATE/TIME utils
    // --------------------------------------------------------------------------------------------

    /**
     * Get date from a timestamp.
     *
     * @param ts the timestamp in milliseconds.
     * @return the date in days.
     */
    public static int timestampMillisToDate(long ts) {
        int days = (int) (ts / MILLIS_PER_DAY);
        if (days < 0) {
            days = days - 1;
        }
        return days;
    }

    /**
     * Get time from a timestamp.
     *
     * @param ts the timestamp in milliseconds.
     * @return the time in milliseconds.
     */
    public static int timestampMillisToTime(long ts) {
        return (int) (ts % MILLIS_PER_DAY);
    }

    // --------------------------------------------------------------------------------------------
    // UNIX TIME
    // --------------------------------------------------------------------------------------------

    public static long fromTimestamp(long ts) {
        return ts;
    }

    /**
     * Convert unix timestamp (seconds since '1970-01-01 00:00:00' UTC) to datetime string in the
     * "yyyy-MM-dd HH:mm:ss" format.
     */
    public static String formatUnixTimestamp(long unixtime, TimeZone tz) {
        return formatUnixTimestamp(unixtime, TIMESTAMP_FORMAT_STRING, tz);
    }

    /**
     * Convert unix timestamp (seconds since '1970-01-01 00:00:00' UTC) to datetime string in the
     * given format.
     */
    public static String formatUnixTimestamp(long unixtime, String format, TimeZone tz) {
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

    /**
     * Returns a Unix timestamp in seconds since '1970-01-01 00:00:00' UTC as an unsigned integer.
     */
    public static long unixTimestamp() {
        return System.currentTimeMillis() / 1000;
    }

    /** Returns the value of the timestamp to seconds since '1970-01-01 00:00:00' UTC. */
    public static long unixTimestamp(long ts) {
        return ts / 1000;
    }

    /**
     * Returns the value of the argument as an unsigned integer in seconds since '1970-01-01
     * 00:00:00' UTC.
     */
    public static long unixTimestamp(String dateStr, TimeZone tz) {
        return unixTimestamp(dateStr, TIMESTAMP_FORMAT_STRING, tz);
    }

    /**
     * Returns the value of the argument as an unsigned integer in seconds since '1970-01-01
     * 00:00:00' UTC.
     */
    public static long unixTimestamp(String dateStr, String format, TimeZone tz) {
        long ts = internalParseTimestampMillis(dateStr, format, tz);
        if (ts == Long.MIN_VALUE) {
            return Long.MIN_VALUE;
        } else {
            // return the seconds
            return ts / 1000;
        }
    }

    // --------------------------------------------------------------------------------------------
    // TIMESTAMP to TIMESTAMP_LTZ conversions
    // --------------------------------------------------------------------------------------------

    public static TimestampData timestampToTimestampWithLocalZone(TimestampData ts, TimeZone tz) {
        return TimestampData.fromInstant(ts.toLocalDateTime().atZone(tz.toZoneId()).toInstant());
    }

    public static TimestampData timestampWithLocalZoneToTimestamp(TimestampData ts, TimeZone tz) {
        return TimestampData.fromLocalDateTime(
                LocalDateTime.ofInstant(ts.toInstant(), tz.toZoneId()));
    }

    public static int timestampWithLocalZoneToDate(TimestampData ts, TimeZone tz) {
        return toInternal(
                LocalDateTime.ofInstant(Instant.ofEpochMilli(ts.getMillisecond()), tz.toZoneId())
                        .toLocalDate());
    }

    public static int timestampWithLocalZoneToTime(TimestampData ts, TimeZone tz) {
        return toInternal(
                LocalDateTime.ofInstant(Instant.ofEpochMilli(ts.getMillisecond()), tz.toZoneId())
                        .toLocalTime());
    }

    public static TimestampData dateToTimestampWithLocalZone(int date, TimeZone tz) {
        return TimestampData.fromInstant(
                LocalDateTime.of(toLocalDate(date), LocalTime.MIDNIGHT)
                        .atZone(tz.toZoneId())
                        .toInstant());
    }

    public static TimestampData timeToTimestampWithLocalZone(int time, TimeZone tz) {
        return TimestampData.fromInstant(toLocalDateTime(time).atZone(tz.toZoneId()).toInstant());
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
        int[] monthOf31Days = new int[] {1, 3, 5, 7, 8, 10, 12};
        if (y < 0 || y > 9999 || m < 1 || m > 12 || d < 1 || d > 31) {
            return false;
        }
        if (m == 2 && d > 28) {
            if (!(isLeapYear(y) && d == 29)) {
                return false;
            }
        }
        if (d == 31) {
            for (int i : monthOf31Days) {
                if (i == m) {
                    return true;
                }
            }
            return false;
        }
        return true;
    }

    private static String pad(int length, long v) {
        StringBuilder s = new StringBuilder(Long.toString(v));
        while (s.length() < length) {
            s.insert(0, "0");
        }
        return s.toString();
    }

    /** Appends hour:minute:second to a buffer; assumes they are valid. */
    private static StringBuilder hms(StringBuilder b, int h, int m, int s) {
        int2(b, h);
        b.append(':');
        int2(b, m);
        b.append(':');
        int2(b, s);
        return b;
    }

    /** Appends year-month-day and hour:minute:second to a buffer; assumes they are valid. */
    private static StringBuilder ymdhms(
            StringBuilder b, int year, int month, int day, int h, int m, int s) {
        ymd(b, year, month, day);
        b.append(' ');
        hms(b, h, m, s);
        return b;
    }

    /** Appends year-month-day to a buffer; assumes they are valid. */
    private static StringBuilder ymd(StringBuilder b, int year, int month, int day) {
        int4(b, year);
        b.append('-');
        int2(b, month);
        b.append('-');
        int2(b, day);
        return b;
    }

    private static void int4(StringBuilder buf, int i) {
        buf.append((char) ('0' + (i / 1000) % 10));
        buf.append((char) ('0' + (i / 100) % 10));
        buf.append((char) ('0' + (i / 10) % 10));
        buf.append((char) ('0' + i % 10));
    }

    public static TimestampData truncate(TimestampData ts, int precision) {
        String fraction = Integer.toString(ts.toLocalDateTime().getNano());
        if (fraction.length() <= precision) {
            return ts;
        } else {
            // need to truncate
            if (precision <= 3) {
                return TimestampData.fromEpochMillis(
                        zeroLastDigits(ts.getMillisecond(), 3 - precision));
            } else {
                return TimestampData.fromEpochMillis(
                        ts.getMillisecond(),
                        (int) zeroLastDigits(ts.getNanoOfMillisecond(), 9 - precision));
            }
        }
    }

    private static long zeroLastDigits(long l, int n) {
        long tenToTheN = (long) Math.pow(10, n);
        return (l / tenToTheN) * tenToTheN;
    }

    public static long unixDateCeil(TimeUnitRange range, long date) {
        return julianDateFloor(range, (int) date + 2440588, false);
    }

    public static long unixDateFloor(TimeUnitRange range, long date) {
        return julianDateFloor(range, (int) date + EPOCH_JULIAN, true);
    }

    public static long unixTimestampFloor(TimeUnitRange range, long timestamp) {
        int date = (int) (timestamp / MILLIS_PER_DAY);
        final long f = julianDateFloor(range, date + EPOCH_JULIAN, true);
        return f * MILLIS_PER_DAY;
    }

    public static long unixTimestampCeil(TimeUnitRange range, long timestamp) {
        int date = (int) (timestamp / MILLIS_PER_DAY);
        final long f = julianDateFloor(range, date + EPOCH_JULIAN, false);
        return f * MILLIS_PER_DAY;
    }

    // --------------------------------------------------------------------------------------------
    // ADD/REMOVE months
    // --------------------------------------------------------------------------------------------

    /**
     * Adds a given number of months to a timestamp, represented as the number of milliseconds since
     * the epoch.
     */
    public static long addMonths(long timestamp, int m) {
        final long millis = DateTimeUtils.floorMod(timestamp, DateTimeUtils.MILLIS_PER_DAY);
        timestamp -= millis;
        final long x = addMonths((int) (timestamp / DateTimeUtils.MILLIS_PER_DAY), m);
        return x * DateTimeUtils.MILLIS_PER_DAY + millis;
    }

    /**
     * Adds a given number of months to a date, represented as the number of days since the epoch.
     */
    public static int addMonths(int date, int m) {
        int y0 = (int) extractFromDate(TimeUnitRange.YEAR, date);
        int m0 = (int) extractFromDate(TimeUnitRange.MONTH, date);
        int d0 = (int) extractFromDate(TimeUnitRange.DAY, date);
        m0 += m;
        int deltaYear = (int) DateTimeUtils.floorDiv(m0, 12);
        y0 += deltaYear;
        m0 = (int) DateTimeUtils.floorMod(m0, 12);
        if (m0 == 0) {
            y0 -= 1;
            m0 += 12;
        }

        int last = lastDay(y0, m0);
        if (d0 > last) {
            d0 = last;
        }
        return ymdToUnixDate(y0, m0, d0);
    }

    private static int lastDay(int y, int m) {
        switch (m) {
            case 2:
                return y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) ? 29 : 28;
            case 4:
            case 6:
            case 9:
            case 11:
                return 30;
            default:
                return 31;
        }
    }

    /**
     * Finds the number of months between two dates, each represented as the number of days since
     * the epoch.
     */
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
        final long millis0 = DateTimeUtils.floorMod(t0, DateTimeUtils.MILLIS_PER_DAY);
        final int d0 = (int) DateTimeUtils.floorDiv(t0 - millis0, DateTimeUtils.MILLIS_PER_DAY);
        final long millis1 = DateTimeUtils.floorMod(t1, DateTimeUtils.MILLIS_PER_DAY);
        final int d1 = (int) DateTimeUtils.floorDiv(t1 - millis1, DateTimeUtils.MILLIS_PER_DAY);
        int x = subtractMonths(d0, d1);
        final long d2 = addMonths(d1, x);
        if (d2 == d0 && millis0 < millis1) {
            --x;
        }
        return x;
    }

    // --------------------------------------------------------------------------------------------
    // TimeUnit and TimeUnitRange enums
    // --------------------------------------------------------------------------------------------

    /**
     * Enumeration of time units used to construct an interval.
     *
     * <p>Only {@link #YEAR}, {@link #MONTH}, {@link #DAY}, {@link #HOUR}, {@link #MINUTE}, {@link
     * #SECOND} can be the unit of a SQL interval.
     *
     * <p>The others ({@link #QUARTER}, {@link #WEEK}, {@link #MILLISECOND}, {@link #DOW}, {@link
     * #DOY}, {@link #EPOCH}, {@link #DECADE}, {@link #CENTURY}, {@link #MILLENNIUM}, {@link
     * #MICROSECOND}, {@link #NANOSECOND}, {@link #ISODOW} and {@link #ISOYEAR}) are convenient to
     * use internally, when converting to and from UNIX timestamps. And also may be arguments to the
     * {@code EXTRACT}, {@code TIMESTAMPADD} and {@code TIMESTAMPDIFF} functions.
     */
    @Internal
    public enum TimeUnit {
        YEAR(true, ' ', BigDecimal.valueOf(12) /* months */, null),
        MONTH(true, '-', BigDecimal.ONE /* months */, BigDecimal.valueOf(12)),
        DAY(false, '-', BigDecimal.valueOf(MILLIS_PER_DAY), null),
        HOUR(false, ' ', BigDecimal.valueOf(MILLIS_PER_HOUR), BigDecimal.valueOf(24)),
        MINUTE(false, ':', BigDecimal.valueOf(MILLIS_PER_MINUTE), BigDecimal.valueOf(60)),
        SECOND(false, ':', BigDecimal.valueOf(MILLIS_PER_SECOND), BigDecimal.valueOf(60)),

        QUARTER(true, '*', BigDecimal.valueOf(3) /* months */, BigDecimal.valueOf(4)),
        ISOYEAR(true, ' ', BigDecimal.valueOf(12) /* months */, null),
        WEEK(false, '*', BigDecimal.valueOf(MILLIS_PER_DAY * 7), BigDecimal.valueOf(53)),
        MILLISECOND(false, '.', BigDecimal.ONE, BigDecimal.valueOf(1000)),
        MICROSECOND(false, '.', BigDecimal.ONE.scaleByPowerOfTen(-3), BigDecimal.valueOf(1000_000)),
        NANOSECOND(
                false, '.', BigDecimal.ONE.scaleByPowerOfTen(-6), BigDecimal.valueOf(1000_000_000)),
        DOW(false, '-', null, null),
        ISODOW(false, '-', null, null),
        DOY(false, '-', null, null),
        EPOCH(false, '*', null, null),
        DECADE(true, '*', BigDecimal.valueOf(120) /* months */, null),
        CENTURY(true, '*', BigDecimal.valueOf(1200) /* months */, null),
        MILLENNIUM(true, '*', BigDecimal.valueOf(12000) /* months */, null);

        public final boolean yearMonth;
        public final char separator;
        public final BigDecimal multiplier;
        private final BigDecimal limit;

        private static final TimeUnit[] CACHED_VALUES = values();

        TimeUnit(boolean yearMonth, char separator, BigDecimal multiplier, BigDecimal limit) {
            this.yearMonth = yearMonth;
            this.separator = separator;
            this.multiplier = multiplier;
            this.limit = limit;
        }

        /**
         * Returns the TimeUnit associated with an ordinal. The value returned is null if the
         * ordinal is not a member of the TimeUnit enumeration.
         */
        public static TimeUnit getValue(int ordinal) {
            return ordinal < 0 || ordinal >= CACHED_VALUES.length ? null : CACHED_VALUES[ordinal];
        }

        /**
         * Returns whether a given value is valid for a field of this time unit.
         *
         * @param field Field value
         * @return Whether value
         */
        public boolean isValidValue(BigDecimal field) {
            return field.compareTo(BigDecimal.ZERO) >= 0
                    && (limit == null || field.compareTo(limit) < 0);
        }
    }

    /**
     * A range of time units. The first is more significant than the other (e.g. year-to-day) or the
     * same as the other (e.g. month).
     */
    @Internal
    public enum TimeUnitRange {
        YEAR(TimeUnit.YEAR, null),
        YEAR_TO_MONTH(TimeUnit.YEAR, TimeUnit.MONTH),
        MONTH(TimeUnit.MONTH, null),
        DAY(TimeUnit.DAY, null),
        DAY_TO_HOUR(TimeUnit.DAY, TimeUnit.HOUR),
        DAY_TO_MINUTE(TimeUnit.DAY, TimeUnit.MINUTE),
        DAY_TO_SECOND(TimeUnit.DAY, TimeUnit.SECOND),
        HOUR(TimeUnit.HOUR, null),
        HOUR_TO_MINUTE(TimeUnit.HOUR, TimeUnit.MINUTE),
        HOUR_TO_SECOND(TimeUnit.HOUR, TimeUnit.SECOND),
        MINUTE(TimeUnit.MINUTE, null),
        MINUTE_TO_SECOND(TimeUnit.MINUTE, TimeUnit.SECOND),
        SECOND(TimeUnit.SECOND, null),

        // non-standard time units cannot participate in ranges
        ISOYEAR(TimeUnit.ISOYEAR, null),
        QUARTER(TimeUnit.QUARTER, null),
        WEEK(TimeUnit.WEEK, null),
        MILLISECOND(TimeUnit.MILLISECOND, null),
        MICROSECOND(TimeUnit.MICROSECOND, null),
        NANOSECOND(TimeUnit.NANOSECOND, null),
        DOW(TimeUnit.DOW, null),
        ISODOW(TimeUnit.ISODOW, null),
        DOY(TimeUnit.DOY, null),
        EPOCH(TimeUnit.EPOCH, null),
        DECADE(TimeUnit.DECADE, null),
        CENTURY(TimeUnit.CENTURY, null),
        MILLENNIUM(TimeUnit.MILLENNIUM, null);

        public final TimeUnit startUnit;
        public final TimeUnit endUnit;

        private static final Map<Pair<TimeUnit>, TimeUnitRange> MAP = createMap();

        /**
         * Creates a TimeUnitRange.
         *
         * @param startUnit Start time unit
         * @param endUnit End time unit
         */
        TimeUnitRange(TimeUnit startUnit, TimeUnit endUnit) {
            assert startUnit != null;
            this.startUnit = startUnit;
            this.endUnit = endUnit;
        }

        /**
         * Returns a {@code TimeUnitRange} with a given start and end unit.
         *
         * @param startUnit Start unit
         * @param endUnit End unit
         * @return Time unit range, or null if not valid
         */
        public static TimeUnitRange of(TimeUnit startUnit, TimeUnit endUnit) {
            return MAP.get(new Pair<>(startUnit, endUnit));
        }

        private static Map<Pair<TimeUnit>, TimeUnitRange> createMap() {
            Map<Pair<TimeUnit>, TimeUnitRange> map = new HashMap<>();
            for (TimeUnitRange value : values()) {
                map.put(new Pair<>(value.startUnit, value.endUnit), value);
            }
            return Collections.unmodifiableMap(map);
        }

        /** Whether this is in the YEAR-TO-MONTH family of intervals. */
        public boolean monthly() {
            return ordinal() <= MONTH.ordinal();
        }

        /**
         * Immutable pair of values of the same type.
         *
         * @param <E> the element type
         */
        private static class Pair<E> {
            final E left;
            final E right;

            private Pair(E left, E right) {
                this.left = left;
                this.right = right;
            }

            @Override
            public int hashCode() {
                int k = (left == null) ? 0 : left.hashCode();
                int k1 = (right == null) ? 0 : right.hashCode();
                return ((k << 4) | k) ^ k1;
            }

            @Override
            public boolean equals(Object obj) {
                return obj == this
                        || obj instanceof Pair
                                && Objects.equals(left, ((Pair) obj).left)
                                && Objects.equals(right, ((Pair) obj).right);
            }
        }
    }
}
