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

/** Utils to represent a LocalDateTime to String, considered the precision. */
@Internal
public class TimestampStringUtils {

    // TODO this method is copied from org.apache.flink.table.runtime.functions.SqlDDateTimeUtils,
    // we can refactor these utils in the future
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
}
