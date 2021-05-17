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

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.TimeZone;

/**
 * Utils to represent a LocalDateTime to String, considered the precision.
 *
 * <p>TODO://This class keep same SQL formats with {@code
 * org.apache.flink.table.runtime.functions.SqlDateTimeUtils} which used in Flink SQL codegen, The
 * two utils will be unified once FLINK-21456 finished.
 */
@Internal
public class TimestampStringUtils {

    private static final long MILLIS_PER_SECOND = 1000L;
    private static final long MILLIS_PER_MINUTE = 60000L;
    private static final long MILLIS_PER_HOUR = 3600000L;
    private static final long MILLIS_PER_DAY = 86400000L;

    /** The local time zone, used to deal {@link java.sql.Time} value. */
    private static final TimeZone LOCAL_TZ = TimeZone.getDefault();

    public static String timestampToString(LocalDateTime ldt, int precision) {
        String fraction = pad(9, (long) ldt.getNano());
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

    private static void int2(StringBuilder buf, int i) {
        buf.append((char) ('0' + (i / 10) % 10));
        buf.append((char) ('0' + i % 10));
    }

    /**
     * Cast TIME type value to VARCHAR(N), we use same SQL format with codegen in
     * org.apache.flink.table.runtime.functions.SqlDateTimeUtils.
     */
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

    public static int timeToInternal(java.sql.Time time) {
        long ts = time.getTime() + LOCAL_TZ.getOffset(time.getTime());
        return (int) (ts % MILLIS_PER_DAY);
    }

    public static int localTimeToUnixDate(LocalTime time) {
        return time.getHour() * (int) MILLIS_PER_HOUR
                + time.getMinute() * (int) MILLIS_PER_MINUTE
                + time.getSecond() * (int) MILLIS_PER_SECOND
                + time.getNano() / 1000_000;
    }
}
