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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RateAllocatorFactory}. */
class RateAllocatorFactoryTest {

    private static final ClassLoader CLASS_LOADER = RateAllocatorFactoryTest.class.getClassLoader();

    @Test
    void testDefaultIsDemandAware() {
        assertThat(RateAllocatorFactory.fromConfiguration(new Configuration(), CLASS_LOADER))
                .isInstanceOf(DemandAwareRateAllocator.class);
    }

    @Test
    void testBuiltInNameIsCaseAndWhitespaceInsensitive() {
        assertThat(RateAllocatorFactory.create(" Demand-Aware ", CLASS_LOADER))
                .isInstanceOf(DemandAwareRateAllocator.class);
    }

    @Test
    void testCustomClassFromConfiguration() {
        final Configuration config = new Configuration();
        config.set(
                SourceOptions.SOURCE_DYNAMIC_RATE_LIMIT_ALLOCATOR,
                EvenRateAllocator.class.getName());

        final RateAllocator allocator =
                RateAllocatorFactory.fromConfiguration(config, CLASS_LOADER);

        assertThat(allocator).isInstanceOf(EvenRateAllocator.class);
        assertThat(allocator.allocate(10, List.of(new SubtaskRateInfo(0, 1, 1))))
                .containsEntry(0, 10.0);
    }

    @Test
    void testParamsCarryConfiguredAllocator() {
        final Configuration config = new Configuration();
        config.set(SourceOptions.SOURCE_DYNAMIC_RATE_LIMIT_ENABLED, true);
        config.set(
                SourceOptions.SOURCE_DYNAMIC_RATE_LIMIT_ALLOCATOR,
                EvenRateAllocator.class.getName());

        assertThat(DynamicRateLimitParams.fromConfiguration(config).getRateAllocator())
                .isInstanceOf(EvenRateAllocator.class);
    }

    @Test
    void testClassNotImplementingRateAllocatorFails() {
        assertThatThrownBy(() -> RateAllocatorFactory.create(String.class.getName(), CLASS_LOADER))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("source.dynamic-rate-limit.allocator")
                .hasMessageContaining(String.class.getName());
    }

    @Test
    void testUnknownClassFails() {
        assertThatThrownBy(() -> RateAllocatorFactory.create("no.such.Class", CLASS_LOADER))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no.such.Class");
    }

    /** Allocator that splits the total evenly. */
    public static class EvenRateAllocator implements RateAllocator {
        private static final long serialVersionUID = 1L;

        @Override
        public Map<Integer, Double> allocate(double total, List<SubtaskRateInfo> subtasks) {
            final Map<Integer, Double> result = new HashMap<>();
            subtasks.forEach(s -> result.put(s.getSubtaskId(), total / subtasks.size()));
            return result;
        }
    }
}
