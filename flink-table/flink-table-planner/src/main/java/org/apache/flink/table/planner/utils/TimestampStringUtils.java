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

package org.apache.flink.table.planner.utils;

import org.apache.flink.annotation.Internal;

import org.apache.calcite.util.TimestampString;

import java.time.LocalDateTime;

/** Utility functions for calcite's {@link TimestampString}. */
@Internal
public class TimestampStringUtils {

    /** Convert a {@link LocalDateTime} to a calcite's {@link TimestampString}. */
    public static TimestampString fromLocalDateTime(LocalDateTime ldt) {
        return new TimestampString(
                        ldt.getYear(),
                        ldt.getMonthValue(),
                        ldt.getDayOfMonth(),
                        ldt.getHour(),
                        ldt.getMinute(),
                        ldt.getSecond())
                .withNanos(ldt.getNano());
    }

    /** Convert a calcite's {@link TimestampString} to a {@link LocalDateTime}. */
    public static LocalDateTime toLocalDateTime(TimestampString timestampString) {
        final String v = timestampString.toString();
        final int year = Integer.parseInt(v.substring(0, 4));
        final int month = Integer.parseInt(v.substring(5, 7));
        final int day = Integer.parseInt(v.substring(8, 10));
        final int h = Integer.parseInt(v.substring(11, 13));
        final int m = Integer.parseInt(v.substring(14, 16));
        final int s = Integer.parseInt(v.substring(17, 19));
        final int nano = getNanosInSecond(v);
        return LocalDateTime.of(year, month, day, h, m, s, nano);
    }

    private static int getNanosInSecond(String v) {
        if (v.length() == 19) { // "1999-12-31 12:34:56"
            return 0;
        }

        // "1999-12-31 12:34:56.789123456"
        return Integer.parseInt(v.substring(20)) * (int) Math.pow(10, 9 - (v.length() - 20));
    }
}
