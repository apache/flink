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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SourceOptions;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link DynamicRateLimitParams#fromConfiguration}. */
class DynamicRateLimitParamsTest {

    @Test
    void testDisabledByDefault() {
        DynamicRateLimitParams params =
                DynamicRateLimitParams.fromConfiguration(new Configuration());

        assertThat(params.isEnabled()).isFalse();
        assertThat(params).isSameAs(DynamicRateLimitParams.DISABLED);
    }

    @Test
    void testEnabledReadsAllConfiguredValues() {
        Configuration config = new Configuration();
        config.set(SourceOptions.SOURCE_DYNAMIC_RATE_LIMIT_ENABLED, true);
        config.set(SourceOptions.SOURCE_DYNAMIC_RATE_LIMIT_ROLLING_WINDOW_SIZE, 20);
        config.set(SourceOptions.SOURCE_DYNAMIC_RATE_LIMIT_MAX_RECORDS_PER_SECOND, 5000.0);
        config.set(SourceOptions.SOURCE_DYNAMIC_RATE_LIMIT_THROTTLE_DOWN_FACTOR, 0.7);
        config.set(SourceOptions.SOURCE_DYNAMIC_RATE_LIMIT_RAMP_UP_FACTOR, 1.2);
        config.set(SourceOptions.SOURCE_DYNAMIC_RATE_LIMIT_MIN, 0.05);
        config.set(SourceOptions.SOURCE_DYNAMIC_RATE_LIMIT_TARGET, 0.4);
        config.set(SourceOptions.SOURCE_DYNAMIC_RATE_LIMIT_MAX, 0.9);

        DynamicRateLimitParams params = DynamicRateLimitParams.fromConfiguration(config);

        assertThat(params.isEnabled()).isTrue();
        assertThat(params.getRollingWindowSize()).isEqualTo(20);
        assertThat(params.getMaxRecordsPerSecond()).isEqualTo(5000.0);
        assertThat(params.getThrottleDownFactor()).isEqualTo(0.7);
        assertThat(params.getRampUpFactor()).isEqualTo(1.2);
        assertThat(params.getMinDurationFraction()).isEqualTo(0.05);
        assertThat(params.getTargetDurationFraction()).isEqualTo(0.4);
        assertThat(params.getMaxDurationFraction()).isEqualTo(0.9);
    }

    @Test
    void testDefaultValuesAreUsedWhenEnabledWithoutOverrides() {
        Configuration config = new Configuration();
        config.set(SourceOptions.SOURCE_DYNAMIC_RATE_LIMIT_ENABLED, true);

        DynamicRateLimitParams params = DynamicRateLimitParams.fromConfiguration(config);

        assertThat(params.getRollingWindowSize())
                .isEqualTo(
                        SourceOptions.SOURCE_DYNAMIC_RATE_LIMIT_ROLLING_WINDOW_SIZE.defaultValue());
        assertThat(params.getMaxRecordsPerSecond())
                .isEqualTo(
                        SourceOptions.SOURCE_DYNAMIC_RATE_LIMIT_MAX_RECORDS_PER_SECOND
                                .defaultValue());
        assertThat(params.getThrottleDownFactor())
                .isEqualTo(
                        SourceOptions.SOURCE_DYNAMIC_RATE_LIMIT_THROTTLE_DOWN_FACTOR
                                .defaultValue());
        assertThat(params.getRampUpFactor())
                .isEqualTo(SourceOptions.SOURCE_DYNAMIC_RATE_LIMIT_RAMP_UP_FACTOR.defaultValue());
        assertThat(params.getMinDurationFraction())
                .isEqualTo(SourceOptions.SOURCE_DYNAMIC_RATE_LIMIT_MIN.defaultValue());
        assertThat(params.getTargetDurationFraction())
                .isEqualTo(SourceOptions.SOURCE_DYNAMIC_RATE_LIMIT_TARGET.defaultValue());
        assertThat(params.getMaxDurationFraction())
                .isEqualTo(SourceOptions.SOURCE_DYNAMIC_RATE_LIMIT_MAX.defaultValue());
    }

    @Test
    void testTargetOutsideMinMaxBandFails() {
        Configuration config = new Configuration();
        config.set(SourceOptions.SOURCE_DYNAMIC_RATE_LIMIT_ENABLED, true);
        config.set(SourceOptions.SOURCE_DYNAMIC_RATE_LIMIT_MIN, 0.2);
        config.set(SourceOptions.SOURCE_DYNAMIC_RATE_LIMIT_MAX, 0.6);

        config.set(SourceOptions.SOURCE_DYNAMIC_RATE_LIMIT_TARGET, 0.1);
        assertThatThrownBy(() -> DynamicRateLimitParams.fromConfiguration(config))
                .isInstanceOf(IllegalArgumentException.class);

        config.set(SourceOptions.SOURCE_DYNAMIC_RATE_LIMIT_TARGET, 0.7);
        assertThatThrownBy(() -> DynamicRateLimitParams.fromConfiguration(config))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
