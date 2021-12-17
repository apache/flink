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

import static org.apache.flink.table.runtime.operators.window.TimeWindow.getWindowStartWithOffset;

/** Time util to deals window start and end in different timezone. */
@Internal
public class TimeWindowUtil {

    private static final ZoneId UTC_ZONE_ID = TimeZone.getTimeZone("UTC").toZoneId();

    private static final long SECONDS_PER_HOUR = 60 * 60L;

    private static final long MILLS_PER_HOUR = SECONDS_PER_HOUR * 1000L;

    /**
     * Convert a epoch mills to timestamp mills which can describe a locate date time.
     *
     * <p>For example: The timestamp string of epoch mills 5 in GMT+08:00 is 1970-01-01 08:00:05,
     * the timestamp mills is 8 * 60 * 60 * 1000 + 5.
     *
     * @param epochMills the epoch mills.
     * @param shiftTimeZone the timezone that the given timestamp mills has been shifted.
     * @return the mills which can describe the local timestamp string in given timezone.
     */
    public static long toUtcTimestampMills(long epochMills, ZoneId shiftTimeZone) {
        // Long.MAX_VALUE is a flag of max watermark, directly return it
        if (UTC_ZONE_ID.equals(shiftTimeZone) || Long.MAX_VALUE == epochMills) {
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
        // Long.MAX_VALUE is a flag of max watermark, directly return it
        if (UTC_ZONE_ID.equals(shiftTimeZone) || Long.MAX_VALUE == utcTimestampMills) {
            return utcTimestampMills;
        }

        if (TimeZone.getTimeZone(shiftTimeZone).useDaylightTime()) {
            /*
             * return the first skipped epoch mills as timer time if the time is coming the DST.
             *  eg. Los_Angele has no timestamp 2021-03-14 02:00:00 when coming DST.
             * <pre>
             *  2021-03-14 00:00:00 -> epoch1 = 1615708800000L;
             *  2021-03-14 01:00:00 -> epoch2 = 1615712400000L;
             *  2021-03-14 03:00:00 -> epoch3 = 1615716000000L;  skip one hour (2021-03-14 02:00:00)
             *  2021-03-14 04:00:00 -> epoch4 = 1615719600000L;
             *
             * we should use the epoch3 to register timer for window that end with
             *  [2021-03-14 02:00:00, 2021-03-14 03:00:00] to ensure the window can be fired
             *  immediately once the window passed.
             *
             * <pre>
             *  2021-03-14 00:00:00 -> epoch0 = 1615708800000L;
             *  2021-03-14 01:00:00 -> epoch1 = 1615712400000L;
             *  2021-03-14 02:00:00 -> epoch3 = 1615716000000L; register 1615716000000L(epoch3)
             *  2021-03-14 02:59:59 -> epoch3 = 1615719599000L; register 1615716000000L(epoch3)
             *  2021-03-14 03:00:00 -> epoch3 = 1615716000000L;
             */

            /*
             * return the larger epoch mills as timer time if the time is leaving the DST.
             *  eg. Los_Angeles has two timestamp 2021-11-07 01:00:00 when leaving DST.
             * <pre>
             *  2021-11-07 00:00:00 -> epoch0 = 1636268400000L;  2021-11-07 00:00:00
             *  2021-11-07 01:00:00 -> epoch1 = 1636272000000L;  the first local timestamp 2021-11-07 01:00:00
             *  2021-11-07 01:00:00 -> epoch2 = 1636275600000L;  back to local timestamp  2021-11-07 01:00:00
             *  2021-11-07 02:00:00 -> epoch3 = 1636279200000L;  2021-11-07 02:00:00
             *
             * we should use the epoch1 + 1 hour to register timer to ensure the two hours' data can
             * be fired properly.
             *
             * <pre>
             *  2021-11-07 00:00:00 -> epoch0 = 1636268400000L;
             *  2021-11-07 01:00:00 -> epoch1 = 1636272000000L; register 1636275600000L(epoch2)
             *  2021-11-07 02:00:00 -> epoch3 = 1636279200000L;
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

            boolean hasNoEpoch = t1 == t2;
            boolean hasTwoEpochs = t2 - t1 > MILLS_PER_HOUR;

            if (hasNoEpoch) {
                return t1 - t1 % MILLS_PER_HOUR;
            } else if (hasTwoEpochs) {
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
        // Long.MAX_VALUE is a flag of max watermark, directly return it
        if (UTC_ZONE_ID.equals(shiftTimeZone) || Long.MAX_VALUE == utcTimestampMills) {
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

    /**
     * Returns the window should fired or not on current progress.
     *
     * @param windowEnd the end of the time window.
     * @param currentProgress current progress of the window operator, it is processing time under
     *     proctime, it is watermark value under rowtime.
     * @param shiftTimeZone the shifted timezone of the time window.
     */
    public static boolean isWindowFired(
            long windowEnd, long currentProgress, ZoneId shiftTimeZone) {
        // Long.MAX_VALUE is a flag of min window end, directly return false
        if (windowEnd == Long.MAX_VALUE) {
            return false;
        }
        long windowTriggerTime = toEpochMillsForTimer(windowEnd - 1, shiftTimeZone);
        return currentProgress >= windowTriggerTime;
    }

    /** Method to get the next watermark to trigger window. */
    public static long getNextTriggerWatermark(
            long currentWatermark, long interval, ZoneId shiftTimezone, boolean useDayLightSaving) {
        if (currentWatermark == Long.MAX_VALUE) {
            return currentWatermark;
        }

        long triggerWatermark;
        // consider the DST timezone
        if (useDayLightSaving) {
            long utcWindowStart =
                    getWindowStartWithOffset(
                            toUtcTimestampMills(currentWatermark, shiftTimezone), 0L, interval);
            triggerWatermark = toEpochMillsForTimer(utcWindowStart + interval - 1, shiftTimezone);
        } else {
            long start = getWindowStartWithOffset(currentWatermark, 0L, interval);
            triggerWatermark = start + interval - 1;
        }

        if (triggerWatermark > currentWatermark) {
            return triggerWatermark;
        } else {
            return triggerWatermark + interval;
        }
    }
}
