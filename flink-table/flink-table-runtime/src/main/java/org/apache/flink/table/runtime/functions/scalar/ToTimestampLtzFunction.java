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

package org.apache.flink.table.runtime.functions.scalar;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.utils.DateTimeUtils;

import javax.annotation.Nullable;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.apache.flink.table.utils.DateTimeUtils.parseTimestampData;

/**
 * Implementation of {@link BuiltInFunctionDefinitions#TO_TIMESTAMP_LTZ}.
 *
 * <p>A function that converts various time formats to TIMESTAMP_LTZ type.
 *
 * <p>Supported function signatures:
 *
 * <ul>
 *   <li>{@code TO_TIMESTAMP_LTZ(numeric)} -> TIMESTAMP_LTZ(3) <br>
 *       Converts numeric epoch time in milliseconds to timestamp with local timezone
 *   <li>{@code TO_TIMESTAMP_LTZ(numeric, precision)} -> TIMESTAMP_LTZ(precision) <br>
 *       Converts numeric epoch time to timestamp with specified precision (0 as seconds, 3 as
 *       milliseconds)
 *   <li>{@code TO_TIMESTAMP_LTZ(timestamp)} -> TIMESTAMP_LTZ(3) <br>
 *       Parses string timestamp using default format 'yyyy-MM-dd HH:mm:ss'
 *   <li>{@code TO_TIMESTAMP_LTZ(timestamp, format)} -> TIMESTAMP_LTZ(3) <br>
 *       Parses string timestamp using input string of format
 *   <li>{@code TO_TIMESTAMP_LTZ(timestamp, format, timezone)} -> TIMESTAMP_LTZ(3) <br>
 *       Parses string timestamp using input strings of format and timezone
 * </ul>
 *
 * <p>Example:
 *
 * <pre>{@code
 * TO_TIMESTAMP_LTZ('2023-01-01 10:00:00')  // Parses string using default format
 * TO_TIMESTAMP_LTZ(1234567890123)  // Converts epoch milliseconds
 * TO_TIMESTAMP_LTZ(1234567890, 0)     // Converts epoch seconds
 * TO_TIMESTAMP_LTZ(1234567890123, 3)  // Converts epoch milliseconds
 * TO_TIMESTAMP_LTZ('2023-01-01 10:00:00')  // Parses string using default format
 * TO_TIMESTAMP_LTZ('2023-01-01T10:00:00', 'yyyy-MM-dd\'T\'HH:mm:ss') // Parses string using input format
 * TO_TIMESTAMP_LTZ('2023-01-01 10:00:00', 'yyyy-MM-dd HH:mm:ss', 'UTC') // Parses string using input format and timezone
 * }</pre>
 */
@Internal
public class ToTimestampLtzFunction extends BuiltInScalarFunction {

    private static final int DEFAULT_PRECISION = 3;

    public ToTimestampLtzFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.TO_TIMESTAMP_LTZ, context);
    }

    public @Nullable TimestampData eval(Number epoch, Integer precision) {
        if (epoch == null || precision == null) {
            return null;
        }
        if (epoch instanceof Float || epoch instanceof Double) {
            return DateTimeUtils.toTimestampData(epoch.doubleValue(), precision);
        }
        return DateTimeUtils.toTimestampData(epoch.longValue(), precision);
    }

    public @Nullable TimestampData eval(DecimalData epoch, Integer precision) {
        if (epoch == null || precision == null) {
            return null;
        }

        return DateTimeUtils.toTimestampData(epoch, precision);
    }

    public @Nullable TimestampData eval(Number epoch) {
        return eval(epoch, DEFAULT_PRECISION);
    }

    public @Nullable TimestampData eval(DecimalData epoch) {
        return eval(epoch, DEFAULT_PRECISION);
    }

    public @Nullable TimestampData eval(StringData timestamp) {
        if (timestamp == null) {
            return null;
        }

        return parseTimestampData(timestamp.toString());
    }

    public @Nullable TimestampData eval(StringData timestamp, StringData format) {
        if (timestamp == null || format == null) {
            return null;
        }

        return parseTimestampData(timestamp.toString(), format.toString());
    }

    public @Nullable TimestampData eval(
            StringData dateStr, StringData format, StringData timezone) {
        if (dateStr == null || format == null || timezone == null) {
            return null;
        }

        TimestampData ts = parseTimestampData(dateStr.toString(), format.toString());
        if (ts == null) {
            return null;
        }

        try {
            ZonedDateTime zoneDate = ts.toLocalDateTime().atZone(ZoneId.of(timezone.toString()));
            return TimestampData.fromInstant(zoneDate.toInstant());
        } catch (DateTimeException e) {
            return null;
        }
    }
}
