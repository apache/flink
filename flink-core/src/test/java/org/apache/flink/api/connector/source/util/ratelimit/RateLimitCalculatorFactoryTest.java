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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RateLimitCalculatorFactory}. */
class RateLimitCalculatorFactoryTest {

    private static final ClassLoader CLASS_LOADER =
            RateLimitCalculatorFactoryTest.class.getClassLoader();

    @Test
    void testDefaultIsCheckpointDuration() {
        assertThat(RateLimitCalculatorFactory.fromConfiguration(new Configuration(), CLASS_LOADER))
                .isInstanceOf(CheckpointDurationRateLimitCalculator.class);
    }

    @Test
    void testBuiltInNameIsCaseAndWhitespaceInsensitive() {
        assertThat(RateLimitCalculatorFactory.create(" Checkpoint-Duration ", CLASS_LOADER))
                .isInstanceOf(CheckpointDurationRateLimitCalculator.class);
    }

    @Test
    void testCustomClassFromConfiguration() {
        final Configuration config = new Configuration();
        config.set(
                SourceOptions.SOURCE_DYNAMIC_RATE_LIMIT_CALCULATOR,
                FixedRateCalculator.class.getName());

        final RateLimitCalculator calculator =
                RateLimitCalculatorFactory.fromConfiguration(config, CLASS_LOADER);

        assertThat(calculator).isInstanceOf(FixedRateCalculator.class);
        assertThat(calculator.computeNewRate(List.of(), 1, 1, DynamicRateLimitParams.DISABLED))
                .isEqualTo(FixedRateCalculator.RATE);
    }

    @Test
    void testParamsCarryConfiguredCalculator() {
        final Configuration config = new Configuration();
        config.set(SourceOptions.SOURCE_DYNAMIC_RATE_LIMIT_ENABLED, true);
        config.set(
                SourceOptions.SOURCE_DYNAMIC_RATE_LIMIT_CALCULATOR,
                FixedRateCalculator.class.getName());

        assertThat(DynamicRateLimitParams.fromConfiguration(config).getRateLimitCalculator())
                .isInstanceOf(FixedRateCalculator.class);
    }

    @Test
    void testClassNotImplementingRateLimitCalculatorFails() {
        assertThatThrownBy(
                        () ->
                                RateLimitCalculatorFactory.create(
                                        String.class.getName(), CLASS_LOADER))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("source.dynamic-rate-limit.calculator")
                .hasMessageContaining(String.class.getName());
    }

    @Test
    void testUnknownClassFails() {
        assertThatThrownBy(() -> RateLimitCalculatorFactory.create("no.such.Class", CLASS_LOADER))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no.such.Class");
    }

    /** Calculator that always returns the same rate. */
    public static class FixedRateCalculator implements RateLimitCalculator {
        private static final long serialVersionUID = 1L;
        static final double RATE = 42.0;

        @Override
        public double computeNewRate(
                List<Long> recentDurationsMillis,
                double currentThroughput,
                long checkpointIntervalMillis,
                DynamicRateLimitParams params) {
            return RATE;
        }
    }
}
