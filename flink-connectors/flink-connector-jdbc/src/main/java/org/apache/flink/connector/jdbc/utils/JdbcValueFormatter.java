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

package org.apache.flink.connector.jdbc.utils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Array;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

/** JDBC value formatter. */
public class JdbcValueFormatter {
    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

    private static final Map<Character, String> escapeMapping;

    static {
        Map<Character, String> map = new HashMap<>();
        map.put('\\', "\\\\");
        map.put('\n', "\\n");
        map.put('\t', "\\t");
        map.put('\b', "\\b");
        map.put('\f', "\\f");
        map.put('\r', "\\r");
        map.put('\0', "\\0");
        map.put('\'', "\\'");
        map.put('`', "\\`");
        escapeMapping = Collections.unmodifiableMap(map);
    }

    private static final ThreadLocal<SimpleDateFormat> dateFormat =
            ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd"));

    private static final ThreadLocal<SimpleDateFormat> dateTimeFormat =
            ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));

    private JdbcValueFormatter() {}

    private static String formatInt(int myInt) {
        return Integer.toString(myInt);
    }

    private static String formatFloat(float myFloat) {
        return Float.toString(myFloat);
    }

    private static String formatDouble(double myDouble) {
        return Double.toString(myDouble);
    }

    private static String formatLong(long myLong) {
        return Long.toString(myLong);
    }

    private static String formatBigDecimal(BigDecimal myBigDecimal) {
        return myBigDecimal != null ? myBigDecimal.toPlainString() : null;
    }

    private static String formatShort(short myShort) {
        return Short.toString(myShort);
    }

    private static String formatString(String myString) {
        return escape(myString);
    }

    private static String formatBoolean(boolean myBoolean) {
        return myBoolean ? "1" : "0";
    }

    private static String formatDate(Date date, TimeZone timeZone) {
        SimpleDateFormat formatter = getDateFormat();
        formatter.setTimeZone(timeZone);
        return formatter.format(date);
    }

    private static String formatTime(Time time, TimeZone timeZone) {
        return TIME_FORMATTER.format(
                Instant.ofEpochMilli(time.getTime()).atZone(timeZone.toZoneId()).toLocalTime());
    }

    private static String formatTimestamp(Timestamp time, TimeZone timeZone) {
        SimpleDateFormat formatter = getDateTimeFormat();
        formatter.setTimeZone(timeZone);
        StringBuilder formatted = new StringBuilder(formatter.format(time));
        if (time != null && time.getNanos() % 1000000 > 0) {
            formatted.append('.').append(time.getNanos());
        }
        return formatted.toString();
    }

    private static String formatBigInteger(BigInteger x) {
        return x.toString();
    }

    private static String formatLocalDate(LocalDate x) {
        return DATE_FORMATTER.format(x);
    }

    private static String formatLocalDateTime(LocalDateTime x) {
        return DATE_TIME_FORMATTER.format(x);
    }

    private static String formatLocalTime(LocalTime x) {
        return TIME_FORMATTER.format(x);
    }

    private static String formatOffsetTime(OffsetTime x) {
        return DateTimeFormatter.ISO_OFFSET_TIME.format(x);
    }

    private static String formatOffsetDateTime(OffsetDateTime x, TimeZone timeZone) {
        return DATE_TIME_FORMATTER.withZone(timeZone.toZoneId()).format(x);
    }

    public static String formatObject(Object x) {
        TimeZone timeZone = getTimeZone();

        String value = formatObject(x, timeZone, timeZone);

        if (value == null) {
            return null;
        } else {
            return needsQuoting(x) ? String.join("", "'", value, "'") : value;
        }
    }

    private static String formatObject(Object x, TimeZone dateTimeZone, TimeZone dateTimeTimeZone) {
        if (x instanceof String) {
            return formatString((String) x);
        } else if (x instanceof BigDecimal) {
            return formatBigDecimal((BigDecimal) x);
        } else if (x instanceof Short) {
            return formatShort((Short) x);
        } else if (x instanceof Integer) {
            return formatInt((Integer) x);
        } else if (x instanceof Long) {
            return formatLong((Long) x);
        } else if (x instanceof Float) {
            return formatFloat((Float) x);
        } else if (x instanceof Double) {
            return formatDouble((Double) x);
        } else if (x instanceof Date) {
            return formatDate((Date) x, dateTimeZone);
        } else if (x instanceof LocalDate) {
            return formatLocalDate((LocalDate) x);
        } else if (x instanceof Time) {
            return formatTime((Time) x, dateTimeTimeZone);
        } else if (x instanceof LocalTime) {
            return formatLocalTime((LocalTime) x);
        } else if (x instanceof OffsetTime) {
            return formatOffsetTime((OffsetTime) x);
        } else if (x instanceof Timestamp) {
            return formatTimestamp((Timestamp) x, dateTimeTimeZone);
        } else if (x instanceof LocalDateTime) {
            return formatLocalDateTime((LocalDateTime) x);
        } else if (x instanceof OffsetDateTime) {
            return formatOffsetDateTime((OffsetDateTime) x, dateTimeTimeZone);
        } else if (x instanceof Boolean) {
            return formatBoolean((Boolean) x);
        } else if (x instanceof BigInteger) {
            return formatBigInteger((BigInteger) x);
        } else {
            return null;
        }
    }

    private static boolean needsQuoting(Object o) {
        return o != null
                && !(o instanceof Array)
                && !(o instanceof Boolean)
                && !(o instanceof Collection)
                && !(o instanceof Map)
                && !(o instanceof Number)
                && !o.getClass().isArray();
    }

    private static SimpleDateFormat getDateFormat() {
        return dateFormat.get();
    }

    private static SimpleDateFormat getDateTimeFormat() {
        return dateTimeFormat.get();
    }

    private static TimeZone getTimeZone() {
        return TimeZone.getDefault();
    }

    private static String escape(String s) {
        if (s == null) {
            return "\\N";
        }

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);

            String escaped = escapeMapping.get(ch);
            if (escaped != null) {
                sb.append(escaped);
            } else {
                sb.append(ch);
            }
        }

        return sb.toString();
    }
}
