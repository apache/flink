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

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Configuration options for the dynamic, checkpoint-duration-driven source rate limit that the
 * source coordinator can broadcast to source readers.
 */
@PublicEvolving
public class SourceOptions {

    public static final ConfigOption<Boolean> SOURCE_DYNAMIC_RATE_LIMIT_ENABLED =
            key("source.dynamic-rate-limit.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether the source coordinator should track the rolling trend of "
                                    + "checkpoint completion times and dynamically broadcast an "
                                    + "adjusted target rate limit to source readers that opt in via "
                                    + "RateLimiterStrategy#dynamic(double).");

    public static final ConfigOption<Integer> SOURCE_DYNAMIC_RATE_LIMIT_ROLLING_WINDOW_SIZE =
            key("source.dynamic-rate-limit.rolling-window-size")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "The number of most recent checkpoint durations to keep when "
                                    + "computing the checkpoint-duration trend.");

    public static final ConfigOption<Double> SOURCE_DYNAMIC_RATE_LIMIT_MAX_RECORDS_PER_SECOND =
            key("source.dynamic-rate-limit.max-records-per-second")
                    .doubleType()
                    .defaultValue(Double.MAX_VALUE)
                    .withDescription(
                            "The ceiling (records per second, across all subtasks) that the "
                                    + "dynamic rate limit is allowed to ramp back up to.");

    public static final ConfigOption<Double> SOURCE_DYNAMIC_RATE_LIMIT_THROTTLE_DOWN_FACTOR =
            key("source.dynamic-rate-limit.throttle-down-factor")
                    .doubleType()
                    .defaultValue(0.8)
                    .withDescription(
                            "The factor the current target rate is multiplied by when checkpoint "
                                    + "durations are trending up beyond the configured threshold, "
                                    + "e.g. 0.8 cuts the rate by 20%.");

    public static final ConfigOption<Double> SOURCE_DYNAMIC_RATE_LIMIT_RAMP_UP_FACTOR =
            key("source.dynamic-rate-limit.ramp-up-factor")
                    .doubleType()
                    .defaultValue(0.3)
                    .withDescription(
                            "The factor the current target rate is multiplied by when checkpoint "
                                    + "durations are flat or improving, capped at "
                                    + SOURCE_DYNAMIC_RATE_LIMIT_MAX_RECORDS_PER_SECOND.key()
                                    + ", e.g. 1.1 raises the rate by 30%.");

    public static final ConfigOption<Double> SOURCE_DYNAMIC_RATE_LIMIT_MIN =
            key("source.dynamic-rate-limit.min")
                    .doubleType()
                    .defaultValue(0.1)
                    .withDescription(
                            "Lower bound of the checkpoint duration band, as a fraction of the "
                                    + "checkpoint interval. While the latest checkpoint duration "
                                    + "is below this value the rate is left unchanged.");

    public static final ConfigOption<Double> SOURCE_DYNAMIC_RATE_LIMIT_TARGET =
            key("source.dynamic-rate-limit.target")
                    .doubleType()
                    .defaultValue(0.3)
                    .withDescription(
                            "The desired checkpoint duration, as a fraction of the checkpoint "
                                    + "interval, between "
                                    + SOURCE_DYNAMIC_RATE_LIMIT_MIN.key()
                                    + " and "
                                    + "source.dynamic-rate-limit.max. The rate is throttled down "
                                    + "while the average duration over the rolling window is "
                                    + "above it and ramped up otherwise. Only applies when the "
                                    + "checkpoint interval is known; otherwise the rate follows "
                                    + "the checkpoint duration trend.");

    public static final ConfigOption<Double> SOURCE_DYNAMIC_RATE_LIMIT_MAX =
            key("source.dynamic-rate-limit.max")
                    .doubleType()
                    .defaultValue(0.5)
                    .withDescription(
                            "Upper bound of the checkpoint duration band, as a fraction of the "
                                    + "checkpoint interval. While the latest checkpoint duration "
                                    + "is above this value the rate is throttled down "
                                    + "immediately.");

    public static final ConfigOption<String> SOURCE_DYNAMIC_RATE_LIMIT_CALCULATOR =
            key("source.dynamic-rate-limit.calculator")
                    .stringType()
                    .defaultValue("checkpoint-duration")
                    .withDescription(
                            "How the global target rate is derived after each checkpoint. "
                                    + "'checkpoint-duration' throttles down when checkpoint "
                                    + "durations trend up and ramps up otherwise. Any other value "
                                    + "is read as the fully qualified name of a "
                                    + "RateLimitCalculator with a public no-arg constructor.");

    private SourceOptions() {}
}
