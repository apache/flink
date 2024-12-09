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
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.util.FlinkRuntimeException;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Implementation of {@link BuiltInFunctionDefinitions#TO_TIMESTAMP_LTZ}.
 *
 * Supported function signatures:
 *   TO_TIMESTAMP_LTZ(numeric, precision) -> TIMESTAMP_LTZ(3)</li>
 *   TO_TIMESTAMP_LTZ(string) -> TIMESTAMP_LTZ(3)</li>
 *   TO_TIMESTAMP_LTZ(string, format) -> TIMESTAMP_LTZ(3)</li>
 *   TO_TIMESTAMP_LTZ(string, format, timezone) -> TIMESTAMP_LTZ(3)</li>
 *   TO_TIMESTAMP_LTZ(numeric) -> TIMESTAMP_LTZ(3)</li>
 */
@Internal
public class ToTimestampLtzFunction extends BuiltInScalarFunction {

    private static final String DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final ZoneId UTC = ZoneId.of("UTC");

    public ToTimestampLtzFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.TO_TIMESTAMP_LTZ, context);
    }

    public TimestampData eval(Integer epoch, Integer precision) {
        if (epoch == null) {
            return null;
        }
        if (precision == 0) {
            return TimestampData.fromEpochMillis(epoch * 1000L);
        } else if (precision == 3) {
            return TimestampData.fromEpochMillis(epoch);
        } else {
            throw new FlinkRuntimeException("Unsupported precision: " + precision);
        }
    }

    // Parse timestamp with default format
    public TimestampData eval(String timestamp) {
        if (timestamp == null) {
            return null;
        }
        return parseTimestamp(timestamp, DEFAULT_FORMAT, UTC);
    }

    // Parse timestamp with custom format
    public TimestampData eval(String timestamp, String format) {
        if (timestamp == null) {
            return null;
        }
        if (format == null) {
            throw new FlinkRuntimeException("Format must not be null.");
        }
        return parseTimestamp(timestamp, format, UTC);
    }

    // Parse timestamp with format and timezone
    public TimestampData eval(String timestamp, String format, String timezone) {
        if (timestamp == null) {
            return null;
        }
        if (format == null) {
            throw new FlinkRuntimeException("Format must not be null.");
        }
        if (timezone == null) {
            throw new FlinkRuntimeException("Timezone must not be null.");
        }
        return parseTimestamp(timestamp, format, ZoneId.of(timezone));
    }

    private TimestampData parseTimestamp(String timestamp, String format, ZoneId zoneId) {
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
            LocalDateTime localDateTime = LocalDateTime.parse(timestamp, formatter);
            long epochMillis = localDateTime.atZone(zoneId).toInstant().toEpochMilli();
            return TimestampData.fromEpochMillis(epochMillis);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Failed to parse timestamp '%s' in format '%s' with timezone '%s'",
                            timestamp, format, zoneId),
                    e);
        }
    }
}
