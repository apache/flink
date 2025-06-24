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

    private static final String SECOND_CRON_EXPRESSION_TEMPLATE = "0/%s * * * * ? *";
    private static final String MINUTE_CRON_EXPRESSION_TEMPLATE = "0 0/%s * * * ? *";
    private static final String HOUR_CRON_EXPRESSION_TEMPLATE = "0 0 0/%s * * ? *";
    private static final String ONE_DAY_CRON_EXPRESSION_TEMPLATE = "0 0 0 * * ? *";

    private static final long SECOND_CRON_UPPER_BOUND = 60;
    private static final long MINUTE_CRON_UPPER_BOUND = 60;
    private static final long HOUR_CRON_UPPER_BOUND = 24;

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

    /**
     * This is an util method that is used to convert the freshness of materialized table to cron
     * expression in full refresh mode. Since freshness and cron expression cannot be converted
     * equivalently, there are currently only a limited patterns of freshness that can be converted
     * to cron expression.
     */
    public static String convertFreshnessToCron(IntervalFreshness intervalFreshness) {
        switch (intervalFreshness.getTimeUnit()) {
            case SECOND:
                return validateAndConvertCron(
                        intervalFreshness,
                        SECOND_CRON_UPPER_BOUND,
                        SECOND_CRON_EXPRESSION_TEMPLATE);
            case MINUTE:
                return validateAndConvertCron(
                        intervalFreshness,
                        MINUTE_CRON_UPPER_BOUND,
                        MINUTE_CRON_EXPRESSION_TEMPLATE);
            case HOUR:
                return validateAndConvertCron(
                        intervalFreshness, HOUR_CRON_UPPER_BOUND, HOUR_CRON_EXPRESSION_TEMPLATE);
            case DAY:
                return validateAndConvertDayCron(intervalFreshness);
            default:
                throw new ValidationException(
                        String.format(
                                "Unknown freshness time unit: %s.",
                                intervalFreshness.getTimeUnit()));
        }
    }

    private static String validateAndConvertCron(
            IntervalFreshness intervalFreshness, long cronUpperBound, String cronTemplate) {
        long interval = Long.parseLong(intervalFreshness.getInterval());
        IntervalFreshness.TimeUnit timeUnit = intervalFreshness.getTimeUnit();
        // Freshness must be less than cronUpperBound for corresponding time unit when convert it
        // to cron expression
        if (interval >= cronUpperBound) {
            throw new ValidationException(
                    String.format(
                            "In full refresh mode, freshness must be less than %s when the time unit is %s.",
                            cronUpperBound, timeUnit));
        }
        // Freshness must be factors of cronUpperBound for corresponding time unit
        if (cronUpperBound % interval != 0) {
            throw new ValidationException(
                    String.format(
                            "In full refresh mode, only freshness that are factors of %s are currently supported when the time unit is %s.",
                            cronUpperBound, timeUnit));
        }

        return String.format(cronTemplate, interval);
    }

    private static String validateAndConvertDayCron(IntervalFreshness intervalFreshness) {
        // Since the number of days in each month is different, only one day of freshness is
        // currently supported when the time unit is DAY
        long interval = Long.parseLong(intervalFreshness.getInterval());
        if (interval > 1) {
            throw new ValidationException(
                    "In full refresh mode, freshness must be 1 when the time unit is DAY.");
        }
        return ONE_DAY_CRON_EXPRESSION_TEMPLATE;
    }
}
