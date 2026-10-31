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

import org.apache.flink.annotation.Public;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.SourceOptions;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * Configuration parameters for the dynamic, checkpoint-duration-driven source rate limit controlled
 * by the source coordinator.
 */
@Public
public final class DynamicRateLimitParams implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final DynamicRateLimitParams DISABLED =
            new DynamicRateLimitParams(
                    false, 0, Double.MAX_VALUE, 1.0, 1.0, 0, 0, Double.MAX_VALUE);

    private final boolean enabled;
    private final int rollingWindowSize;
    private final double maxRecordsPerSecond;
    private final double throttleDownFactor;
    private final double rampUpFactor;
    private final double minDurationFraction;
    private final double targetDurationFraction;
    private final double maxDurationFraction;
    private final RateLimitCalculator rateLimitCalculator;

    public DynamicRateLimitParams(
            boolean enabled,
            int rollingWindowSize,
            double maxRecordsPerSecond,
            double throttleDownFactor,
            double rampUpFactor,
            double minDurationFraction,
            double targetDurationFraction,
            double maxDurationFraction) {
        this(
                enabled,
                rollingWindowSize,
                maxRecordsPerSecond,
                throttleDownFactor,
                rampUpFactor,
                minDurationFraction,
                targetDurationFraction,
                maxDurationFraction,
                new CheckpointDurationRateLimitCalculator());
    }

    public DynamicRateLimitParams(
            boolean enabled,
            int rollingWindowSize,
            double maxRecordsPerSecond,
            double throttleDownFactor,
            double rampUpFactor,
            double minDurationFraction,
            double targetDurationFraction,
            double maxDurationFraction,
            RateLimitCalculator rateLimitCalculator) {
        this.enabled = enabled;
        this.rollingWindowSize = rollingWindowSize;
        this.maxRecordsPerSecond = maxRecordsPerSecond;
        this.throttleDownFactor = throttleDownFactor;
        this.rampUpFactor = rampUpFactor;
        Preconditions.checkArgument(
                minDurationFraction <= targetDurationFraction
                        && targetDurationFraction <= maxDurationFraction,
                "Expected min (%s) <= target (%s) <= max (%s)",
                minDurationFraction,
                targetDurationFraction,
                maxDurationFraction);
        this.targetDurationFraction = targetDurationFraction;
        this.minDurationFraction = minDurationFraction;
        this.maxDurationFraction = maxDurationFraction;
        this.rateLimitCalculator = Preconditions.checkNotNull(rateLimitCalculator);
    }

    public boolean isEnabled() {
        return enabled;
    }

    public int getRollingWindowSize() {
        return rollingWindowSize;
    }

    public double getMaxRecordsPerSecond() {
        return maxRecordsPerSecond;
    }

    public double getThrottleDownFactor() {
        return throttleDownFactor;
    }

    public double getRampUpFactor() {
        return rampUpFactor;
    }

    /** Fraction of the checkpoint interval below which the rate is left unchanged. */
    public double getMinDurationFraction() {
        return minDurationFraction;
    }

    /** Fraction of the checkpoint interval the checkpoint duration should settle at. */
    public double getTargetDurationFraction() {
        return targetDurationFraction;
    }

    /** Fraction of the checkpoint interval above which the rate is always throttled down. */
    public double getMaxDurationFraction() {
        return maxDurationFraction;
    }

    /** The strategy that computes the new global target rate after each checkpoint. */
    public RateLimitCalculator getRateLimitCalculator() {
        return rateLimitCalculator;
    }

    /** Builds a {@code DynamicRateLimitParams} from the {@link SourceOptions}. */
    public static DynamicRateLimitParams fromConfiguration(ReadableConfig config) {
        return fromConfiguration(config, Thread.currentThread().getContextClassLoader());
    }

    /**
     * Same as {@link #fromConfiguration(ReadableConfig)}, loading a custom calculator class with
     * the given class loader.
     */
    public static DynamicRateLimitParams fromConfiguration(
            ReadableConfig config, ClassLoader classLoader) {
        if (!config.get(SourceOptions.SOURCE_DYNAMIC_RATE_LIMIT_ENABLED)) {
            return DISABLED;
        }
        return new DynamicRateLimitParams(
                true,
                config.get(SourceOptions.SOURCE_DYNAMIC_RATE_LIMIT_ROLLING_WINDOW_SIZE),
                config.get(SourceOptions.SOURCE_DYNAMIC_RATE_LIMIT_MAX_RECORDS_PER_SECOND),
                config.get(SourceOptions.SOURCE_DYNAMIC_RATE_LIMIT_THROTTLE_DOWN_FACTOR),
                config.get(SourceOptions.SOURCE_DYNAMIC_RATE_LIMIT_RAMP_UP_FACTOR),
                config.get(SourceOptions.SOURCE_DYNAMIC_RATE_LIMIT_MIN),
                config.get(SourceOptions.SOURCE_DYNAMIC_RATE_LIMIT_TARGET),
                config.get(SourceOptions.SOURCE_DYNAMIC_RATE_LIMIT_MAX),
                RateLimitCalculatorFactory.fromConfiguration(config, classLoader));
    }
}
