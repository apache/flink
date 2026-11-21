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

package org.apache.flink.api.connector.source.util.ratelimit;

import org.apache.flink.annotation.Internal;

import java.util.List;

/**
 * Computes a new target source rate (records per second, across all subtasks) from a rolling window
 * of recent checkpoint durations, compared against fractions of the checkpoint interval. Below
 * {@code min} nothing is adjusted and above {@code max} the rate is throttled down immediately.
 * Within that band the average duration of the window is compared against {@code target}: above it
 * the rate is throttled down, otherwise it is ramped up toward the configured maximum.
 *
 * <p>If the checkpoint interval is unknown, the fractions cannot be resolved and the average
 * duration of the more recent half of the window is compared against the older half instead: a
 * rising trend throttles the rate down, while a flat or improving trend ramps the rate back up.
 *
 * <p>The rate is scaled off the actually observed throughput rather than the previously broadcast
 * target rate, so that the adjustment reacts to reality directly instead of compounding off a value
 * that may already be stale, converging faster.
 *
 * <p>This is a pure function with no coordinator dependencies, making it independently
 * unit-testable.
 */
@Internal
public class CheckpointDurationRateLimitCalculator implements RateLimitCalculator {
    private static final long serialVersionUID = 1L;

    private static final double MIN_RATE = 1.0;

    /**
     * @param recentDurationsMillis rolling window of checkpoint durations, oldest first.
     * @param currentThroughput the current observed throughput (records/sec, summed across
     *     subtasks) to scale the adjustment off of; callers should fall back to the last target
     *     rate when no throughput has been observed yet.
     * @param checkpointIntervalMillis the checkpoint interval the duration band is a fraction of;
     *     non-positive or unbounded values fall back to the trend comparison.
     * @param params the configured thresholds and factors.
     * @return the new target rate (records/sec), clamped to {@code [MIN_RATE,
     *     params.getMaxRecordsPerSecond()]}.
     */
    @Override
    public double computeNewRate(
            List<Long> recentDurationsMillis,
            double currentThroughput,
            long checkpointIntervalMillis,
            DynamicRateLimitParams params) {
        if (recentDurationsMillis.isEmpty()) {
            return currentThroughput;
        }

        final boolean shouldThrottle;
        if (hasKnownInterval(checkpointIntervalMillis)) {
            final long latestDurationMillis =
                    recentDurationsMillis.get(recentDurationsMillis.size() - 1);
            if (latestDurationMillis < params.getMinDurationFraction() * checkpointIntervalMillis) {
                return currentThroughput;
            }
            shouldThrottle =
                    latestDurationMillis
                                    > params.getMaxDurationFraction() * checkpointIntervalMillis
                            || average(recentDurationsMillis)
                                    > params.getTargetDurationFraction() * checkpointIntervalMillis;
        } else {
            if (recentDurationsMillis.size() < 2) {
                // not enough data points to establish a trend
                return currentThroughput;
            }
            shouldThrottle = computeTrend(recentDurationsMillis) > 0;
        }

        final double newRate;
        if (shouldThrottle) {
            newRate = currentThroughput * params.getThrottleDownFactor();
        } else {
            // ramp up from the observed throughput, capped at the configured max
            newRate =
                    Math.min(
                            currentThroughput * params.getRampUpFactor(),
                            params.getMaxRecordsPerSecond());
        }
        return Math.max(newRate, MIN_RATE);
    }

    private boolean hasKnownInterval(long checkpointIntervalMillis) {
        return checkpointIntervalMillis > 0 && checkpointIntervalMillis != Long.MAX_VALUE;
    }

    /**
     * Returns the relative change between the average of the older half and the average of the more
     * recent half of the window, e.g. {@code 0.1} means checkpoints got 10% slower.
     */
    private double computeTrend(List<Long> durationsMillis) {
        int mid = durationsMillis.size() / 2;
        double olderHalfAvg = average(durationsMillis.subList(0, mid));
        double recentHalfAvg = average(durationsMillis.subList(mid, durationsMillis.size()));
        if (olderHalfAvg == 0) {
            return recentHalfAvg == 0 ? 0 : Double.POSITIVE_INFINITY;
        }
        return (recentHalfAvg - olderHalfAvg) / olderHalfAvg;
    }

    private double average(List<Long> values) {
        return values.stream().mapToLong(Long::longValue).average().orElse(0);
    }
}
