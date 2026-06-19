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

package org.apache.flink.table.api.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.ValidationException;

import java.time.ZoneId;

/** Utilities around validation to keep {@link TableConfig} clean. */
@Internal
public class TableConfigValidation {

    /** Validates user configured time zone. */
    public static void validateTimeZone(String zone) {
        boolean isValid;
        try {
            // We enforce a zone string that is compatible with both java.util.TimeZone and
            // java.time.ZoneId to avoid bugs.
            // In general, advertising either TZDB ID, GMT+xx:xx, or UTC is the best we can do.
            isValid = java.util.TimeZone.getTimeZone(zone).toZoneId().equals(ZoneId.of(zone));
        } catch (Exception e) {
            isValid = false;
        }

        if (!isValid) {
            throw new ValidationException(
                    "Invalid time zone. The value should be a Time Zone Database (TZDB) ID "
                            + "such as 'America/Los_Angeles' to include daylight saving time. Fixed "
                            + "offsets are supported using 'GMT-03:00' or 'GMT+03:00'. Or use 'UTC' "
                            + "without time zone and daylight saving time.");
        }
    }

    private TableConfigValidation() {
        // No instantiation
    }
}
