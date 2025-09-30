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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.IntervalFreshness;

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

    /**
     * Validates that the given freshness can be converted to a cron expression in full refresh
     * mode. Since freshness and cron expression cannot be converted equivalently, there are
     * currently only a limited patterns of freshness that are supported.
     *
     * @param intervalFreshness the freshness to validate
     * @throws ValidationException if the freshness cannot be converted to a valid cron expression
     */
    public static void validateFreshnessForCron(IntervalFreshness intervalFreshness) {
        switch (intervalFreshness.getTimeUnit()) {
            case SECOND:
                validateCronConstraints(intervalFreshness, SECOND_CRON_UPPER_BOUND);
                break;
            case MINUTE:
                validateCronConstraints(intervalFreshness, MINUTE_CRON_UPPER_BOUND);
                break;
            case HOUR:
                validateCronConstraints(intervalFreshness, HOUR_CRON_UPPER_BOUND);
                break;
            case DAY:
                validateDayConstraints(intervalFreshness);
                break;
            default:
                throw new ValidationException(
                        String.format(
                                "Unknown freshness time unit: %s.",
                                intervalFreshness.getTimeUnit()));
        }
    }

    /**
     * Converts the freshness of materialized table to cron expression in full refresh mode. The
     * freshness must first pass validation via {@link #validateFreshnessForCron}.
     *
     * @param intervalFreshness the freshness to convert
     * @return the corresponding cron expression
     * @throws ValidationException if the freshness cannot be converted to a valid cron expression
     */
    public static String convertFreshnessToCron(IntervalFreshness intervalFreshness) {
        // First validate that conversion is possible
        validateFreshnessForCron(intervalFreshness);

        // Then perform the conversion
        switch (intervalFreshness.getTimeUnit()) {
            case SECOND:
                return String.format(
                        SECOND_CRON_EXPRESSION_TEMPLATE, intervalFreshness.getIntervalInt());
            case MINUTE:
                return String.format(
                        MINUTE_CRON_EXPRESSION_TEMPLATE, intervalFreshness.getIntervalInt());
            case HOUR:
                return String.format(
                        HOUR_CRON_EXPRESSION_TEMPLATE, intervalFreshness.getIntervalInt());
            case DAY:
                return ONE_DAY_CRON_EXPRESSION_TEMPLATE;
            default:
                throw new ValidationException(
                        String.format(
                                "Unknown freshness time unit: %s.",
                                intervalFreshness.getTimeUnit()));
        }
    }

    private static void validateCronConstraints(
            IntervalFreshness intervalFreshness, long cronUpperBound) {
        int interval = intervalFreshness.getIntervalInt();
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
    }

    private static void validateDayConstraints(IntervalFreshness intervalFreshness) {
        // Since the number of days in each month is different, only one day of freshness is
        // currently supported when the time unit is DAY
        int interval = intervalFreshness.getIntervalInt();
        if (interval > 1) {
            throw new ValidationException(
                    "In full refresh mode, freshness must be 1 when the time unit is DAY.");
        }
    }
}
