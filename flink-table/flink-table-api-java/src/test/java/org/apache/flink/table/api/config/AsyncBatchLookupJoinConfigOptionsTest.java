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

package org.apache.flink.table.api.config;

import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for async batch lookup join configuration options. */
class AsyncBatchLookupJoinConfigOptionsTest {

    @Test
    void testBatchEnabledDefaultValue() {
        Configuration config = new Configuration();
        boolean defaultValue =
                config.get(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_ENABLED);
        assertThat(defaultValue).isFalse();
    }

    @Test
    void testBatchEnabledConfiguration() {
        Configuration config = new Configuration();

        // Test setting to true
        config.set(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_ENABLED, true);
        assertThat(config.get(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_ENABLED))
                .isTrue();

        // Test setting to false
        config.set(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_ENABLED, false);
        assertThat(config.get(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_ENABLED))
                .isFalse();
    }

    @Test
    void testBatchSizeDefaultValue() {
        Configuration config = new Configuration();
        int defaultValue =
                config.get(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_SIZE);
        assertThat(defaultValue).isEqualTo(100);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 10, 50, 100, 500, 1000})
    void testBatchSizeValidValues(int batchSize) {
        Configuration config = new Configuration();
        config.set(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_SIZE, batchSize);
        assertThat(config.get(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_SIZE))
                .isEqualTo(batchSize);
    }

    @Test
    void testBatchSizeInvalidValues() {
        Configuration config = new Configuration();

        // Test negative values
        assertThatThrownBy(
                        () ->
                                config.set(
                                        OptimizerConfigOptions
                                                .TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_SIZE,
                                        -1))
                .isInstanceOf(IllegalArgumentException.class);

        // Test zero
        assertThatThrownBy(
                        () ->
                                config.set(
                                        OptimizerConfigOptions
                                                .TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_SIZE,
                                        0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testFlushIntervalDefaultValue() {
        Configuration config = new Configuration();
        long defaultValue =
                config.get(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_FLUSH_MILLIS);
        assertThat(defaultValue).isEqualTo(2000L);
    }

    @ParameterizedTest
    @ValueSource(longs = {100L, 500L, 1000L, 2000L, 5000L, 10000L})
    void testFlushIntervalValidValues(long flushInterval) {
        Configuration config = new Configuration();
        config.set(
                OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_FLUSH_MILLIS,
                flushInterval);
        assertThat(
                        config.get(
                                OptimizerConfigOptions
                                        .TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_FLUSH_MILLIS))
                .isEqualTo(flushInterval);
    }

    @Test
    void testFlushIntervalInvalidValues() {
        Configuration config = new Configuration();

        // Test negative values
        assertThatThrownBy(
                        () ->
                                config.set(
                                        OptimizerConfigOptions
                                                .TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_FLUSH_MILLIS,
                                        -1L))
                .isInstanceOf(IllegalArgumentException.class);

        // Test zero
        assertThatThrownBy(
                        () ->
                                config.set(
                                        OptimizerConfigOptions
                                                .TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_FLUSH_MILLIS,
                                        0L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testConfigurationKeys() {
        // Test that configuration keys are correctly defined
        assertThat(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_ENABLED.key())
                .isEqualTo("table.optimizer.dim-lookup-join.batch.enabled");

        assertThat(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_SIZE.key())
                .isEqualTo("table.optimizer.dim-lookup-join.batch.size");

        assertThat(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_FLUSH_MILLIS.key())
                .isEqualTo("table.optimizer.dim-lookup-join.batch.flush.millis");
    }

    @Test
    void testConfigurationDescriptions() {
        // Test that configuration descriptions are not empty
        assertThat(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_ENABLED
                                .description())
                .isNotNull();

        assertThat(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_SIZE.description())
                .isNotNull();

        assertThat(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_FLUSH_MILLIS
                                .description())
                .isNotNull();
    }

    @Test
    void testConfigurationTypes() {
        // Test that configuration options have correct types
        // Note: ConfigOption.getClazz() is not public, so we test through the configuration API
        Configuration config = new Configuration();

        config.set(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_ENABLED, true);
        Object value1 =
                config.get(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_ENABLED);
        assertThat(value1).isInstanceOf(Boolean.class);

        config.set(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_SIZE, 100);
        Object value2 =
                config.get(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_SIZE);
        assertThat(value2).isInstanceOf(Integer.class);

        config.set(
                OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_FLUSH_MILLIS, 2000L);
        Object value3 =
                config.get(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_FLUSH_MILLIS);
        assertThat(value3).isInstanceOf(Long.class);
    }

    @Test
    void testCompleteConfiguration() {
        Configuration config = new Configuration();

        // Set all async batch lookup join options
        config.set(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_ENABLED, true);
        config.set(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_SIZE, 50);
        config.set(
                OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_FLUSH_MILLIS, 1500L);

        // Verify all values
        assertThat(config.get(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_ENABLED))
                .isTrue();
        assertThat(config.get(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_SIZE))
                .isEqualTo(50);
        assertThat(
                        config.get(
                                OptimizerConfigOptions
                                        .TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_FLUSH_MILLIS))
                .isEqualTo(1500L);
    }

    @Test
    void testConfigurationOverride() {
        Configuration config = new Configuration();

        // Set initial values
        config.set(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_ENABLED, false);
        config.set(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_SIZE, 10);
        config.set(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_FLUSH_MILLIS, 500L);

        // Override values
        config.set(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_ENABLED, true);
        config.set(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_SIZE, 200);
        config.set(
                OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_FLUSH_MILLIS, 3000L);

        // Verify overridden values
        assertThat(config.get(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_ENABLED))
                .isTrue();
        assertThat(config.get(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_SIZE))
                .isEqualTo(200);
        assertThat(
                        config.get(
                                OptimizerConfigOptions
                                        .TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_FLUSH_MILLIS))
                .isEqualTo(3000L);
    }

    @Test
    void testBoundaryValues() {
        Configuration config = new Configuration();

        // Test minimum valid values
        config.set(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_SIZE, 1);
        config.set(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_FLUSH_MILLIS, 1L);

        assertThat(config.get(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_SIZE))
                .isEqualTo(1);
        assertThat(
                        config.get(
                                OptimizerConfigOptions
                                        .TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_FLUSH_MILLIS))
                .isEqualTo(1L);

        // Test large valid values
        config.set(
                OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_SIZE,
                Integer.MAX_VALUE);
        config.set(
                OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_FLUSH_MILLIS,
                Long.MAX_VALUE);

        assertThat(config.get(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_SIZE))
                .isEqualTo(Integer.MAX_VALUE);
        assertThat(
                        config.get(
                                OptimizerConfigOptions
                                        .TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_FLUSH_MILLIS))
                .isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void testConfigurationSerialization() {
        Configuration config = new Configuration();

        // Set configuration values
        config.set(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_ENABLED, true);
        config.set(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_SIZE, 75);
        config.set(
                OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_FLUSH_MILLIS, 2500L);

        // Convert to properties and back
        Configuration newConfig = new Configuration();
        config.toMap().forEach((key, value) -> newConfig.setString(key, value));

        // Verify values are preserved
        assertThat(
                        newConfig.get(
                                OptimizerConfigOptions
                                        .TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_ENABLED))
                .isTrue();
        assertThat(newConfig.get(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_SIZE))
                .isEqualTo(75);
        assertThat(
                        newConfig.get(
                                OptimizerConfigOptions
                                        .TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_FLUSH_MILLIS))
                .isEqualTo(2500L);
    }
}
