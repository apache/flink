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

package org.apache.flink.table.runtime.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.TimeZone;

/** Time util to deals window start and end in different timezone. */
@Internal
public class TimeWindowUtil {

    private static final ZoneId UTC_ZONE_ID = TimeZone.getTimeZone("UTC").toZoneId();

    private static final long SECONDS_PER_HOUR = 60 * 60L;

    private static final long MILLS_PER_HOUR = SECONDS_PER_HOUR * 1000L;

    /**
     * Convert a epoch mills to timestamp mills which can describe a locate date time.
     *
     * <p>For example: The timestamp string of epoch mills 5 in UTC+8 is 1970-01-01 08:00:05, the
     * timestamp mills is 8 * 60 * 60 * 100 + 5.
     *
     * @param epochMills the epoch mills.
     * @param shiftTimeZone the timezone that the given timestamp mills has been shifted.
     * @return the mills which can describe the local timestamp string in given timezone.
     */
    public static long toUtcTimestampMills(long epochMills, ZoneId shiftTimeZone) {
        if (UTC_ZONE_ID.equals(shiftTimeZone)) {
            return epochMills;
        }
        LocalDateTime localDateTime =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMills), shiftTimeZone);
        return localDateTime.atZone(UTC_ZONE_ID).toInstant().toEpochMilli();
    }

    /**
     * Get a timer time according to the timestamp mills and the given shift timezone.
     *
     * @param utcTimestampMills the timestamp mills.
     * @param shiftTimeZone the timezone that the given timestamp mills has been shifted.
     * @return the epoch mills.
     */
    public static long toEpochMillsForTimer(long utcTimestampMills, ZoneId shiftTimeZone) {
        if (shiftTimeZone.equals(UTC_ZONE_ID)) {
            return utcTimestampMills;
        }

        if (TimeZone.getTimeZone(shiftTimeZone).useDaylightTime()) {
            /*
             * return the larger epoch mills if the time is leaving the DST.
             *  eg. Los_Angeles has two timestamp 2021-11-07 01:00:00 when leaving DST.
             * <pre>
             *  2021-11-07 00:00:00 -> epoch0  = 1636268400000L;  2021-11-07 00:00:00
             *  2021-11-07 01:00:00 -> epoch1  = 1636272000000L;  the first local timestamp 2021-11-07 01:00:00
             *  2021-11-07 01:00:00 -> epoch2  = 1636275600000L;  back to local timestamp  2021-11-07 01:00:00
             *  2021-11-07 02:00:00 -> epoch3  = 1636279200000L;  2021-11-07 02:00:00
             *
             * we should use the epoch1 + 1 hour to register timer to ensure the two hours' data can
             * be fired properly.
             *
             * <pre>
             *  2021-11-07 00:00:00 => long epoch0 = 1636268400000L;
             *  2021-11-07 01:00:00 => long epoch1 = 1636272000000L; register 1636275600000L(epoch2)
             *  2021-11-07 02:00:00 => long epoch3 = 1636279200000L;
             */
            LocalDateTime utcTimestamp =
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(utcTimestampMills), UTC_ZONE_ID);
            long t1 = utcTimestamp.atZone(shiftTimeZone).toInstant().toEpochMilli();
            long t2 =
                    utcTimestamp
                            .plusSeconds(SECONDS_PER_HOUR)
                            .atZone(shiftTimeZone)
                            .toInstant()
                            .toEpochMilli();
            boolean hasTwoEpochs = t2 - t1 > MILLS_PER_HOUR;
            if (hasTwoEpochs) {
                return t1 + MILLS_PER_HOUR;
            } else {
                return t1;
            }
        }

        LocalDateTime utcTimestamp =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(utcTimestampMills), UTC_ZONE_ID);
        return utcTimestamp.atZone(shiftTimeZone).toInstant().toEpochMilli();
    }

    /**
     * Convert a timestamp mills with the given timezone to epoch mills.
     *
     * @param utcTimestampMills the timezone that the given timestamp mills has been shifted.
     * @param shiftTimeZone the timezone that the given timestamp mills has been shifted.
     * @return the epoch mills.
     */
    public static long toEpochMills(long utcTimestampMills, ZoneId shiftTimeZone) {
        if (UTC_ZONE_ID.equals(shiftTimeZone)) {
            return utcTimestampMills;
        }
        LocalDateTime utcTimestamp =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(utcTimestampMills), UTC_ZONE_ID);
        return utcTimestamp.atZone(shiftTimeZone).toInstant().toEpochMilli();
    }

    /**
     * Get the shifted timezone of window if the time attribute type is TIMESTAMP_LTZ, always
     * returns UTC timezone if the time attribute type is TIMESTAMP which means do not shift.
     */
    public static ZoneId getShiftTimeZone(LogicalType timeAttributeType, TableConfig tableConfig) {
        boolean needShiftTimeZone = timeAttributeType instanceof LocalZonedTimestampType;
        return needShiftTimeZone ? tableConfig.getLocalTimeZone() : UTC_ZONE_ID;
    }
}
