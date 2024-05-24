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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.IntervalFreshness;

import org.apache.commons.lang3.math.NumberUtils;

import java.time.Duration;

/** Utilities to {@link IntervalFreshness}. */
@Internal
public class IntervalFreshnessUtils {

    private IntervalFreshnessUtils() {}

    @VisibleForTesting
    static void validateIntervalFreshness(IntervalFreshness intervalFreshness) {
        if (!NumberUtils.isParsable(intervalFreshness.getInterval())) {
            throw new ValidationException(
                    String.format(
                            "The interval freshness value '%s' is an illegal integer type value.",
                            intervalFreshness.getInterval()));
        }

        if (!NumberUtils.isDigits(intervalFreshness.getInterval())) {
            throw new ValidationException(
                    "The freshness interval currently only supports integer type values.");
        }
    }

    public static Duration convertFreshnessToDuration(IntervalFreshness intervalFreshness) {
        // validate the freshness value firstly
        validateIntervalFreshness(intervalFreshness);

        long interval = Long.parseLong(intervalFreshness.getInterval());
        switch (intervalFreshness.getTimeUnit()) {
            case DAY:
                return Duration.ofDays(interval);
            case HOUR:
                return Duration.ofHours(interval);
            case MINUTE:
                return Duration.ofMinutes(interval);
            case SECOND:
                return Duration.ofSeconds(interval);
            default:
                throw new ValidationException(
                        String.format(
                                "Unknown freshness time unit: %s.",
                                intervalFreshness.getTimeUnit()));
        }
    }
}
