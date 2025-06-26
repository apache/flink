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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.core.execution.CheckpointingMode;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link CheckpointingOptions}. */
class CheckpointingOptionsTest {

    @Test
    void testIsCheckpointingEnabled() {
        // Test with no checkpointing interval configured
        Configuration emptyConfig = new Configuration();
        assertThat(CheckpointingOptions.isCheckpointingEnabled(emptyConfig))
                .as("Checkpointing should be disabled when no interval is configured")
                .isFalse();

        // Test with checkpointing interval set to 0
        Configuration zeroIntervalConfig = new Configuration();
        zeroIntervalConfig.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ZERO);
        assertThat(CheckpointingOptions.isCheckpointingEnabled(zeroIntervalConfig))
                .as("Checkpointing should be disabled when interval is 0")
                .isFalse();

        // Test with negative checkpointing interval
        Configuration negativeIntervalConfig = new Configuration();
        negativeIntervalConfig.set(
                CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofMillis(-1000));
        assertThat(CheckpointingOptions.isCheckpointingEnabled(negativeIntervalConfig))
                .as("Checkpointing should be disabled when interval is negative")
                .isFalse();

        // Test with positive checkpointing interval
        Configuration positiveIntervalConfig = new Configuration();
        positiveIntervalConfig.set(
                CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(5));
        assertThat(CheckpointingOptions.isCheckpointingEnabled(positiveIntervalConfig))
                .as("Checkpointing should be enabled when interval is positive")
                .isTrue();

        // Test with very small positive interval (10 millisecond)
        Configuration smallIntervalConfig = new Configuration();
        smallIntervalConfig.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofMillis(10));
        assertThat(CheckpointingOptions.isCheckpointingEnabled(smallIntervalConfig))
                .as("Checkpointing should be enabled when interval is 1 millisecond")
                .isTrue();

        // Test with RUNTIME_MODE set to BATCH - should always return false
        Configuration batchModeConfig = new Configuration();
        batchModeConfig.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(5));
        batchModeConfig.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        assertThat(CheckpointingOptions.isCheckpointingEnabled(batchModeConfig))
                .as(
                        "Checkpointing should be disabled when runtime mode is BATCH, even with valid interval")
                .isFalse();

        // Test with RUNTIME_MODE set to STREAMING - should depend on interval
        Configuration streamingModeConfig = new Configuration();
        streamingModeConfig.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(5));
        streamingModeConfig.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.STREAMING);
        assertThat(CheckpointingOptions.isCheckpointingEnabled(streamingModeConfig))
                .as(
                        "Checkpointing should be enabled when runtime mode is STREAMING with valid interval")
                .isTrue();

        // Test with RUNTIME_MODE set to STREAMING but no interval
        Configuration streamingModeNoIntervalConfig = new Configuration();
        streamingModeNoIntervalConfig.set(
                ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.STREAMING);
        assertThat(CheckpointingOptions.isCheckpointingEnabled(streamingModeNoIntervalConfig))
                .as(
                        "Checkpointing should be disabled when runtime mode is STREAMING but no interval configured")
                .isFalse();

        // Test with RUNTIME_MODE set to AUTOMATIC - should depend on interval
        Configuration automaticModeConfig = new Configuration();
        automaticModeConfig.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(5));
        automaticModeConfig.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.AUTOMATIC);
        assertThat(CheckpointingOptions.isCheckpointingEnabled(automaticModeConfig))
                .as(
                        "Checkpointing should be enabled when runtime mode is AUTOMATIC with valid interval")
                .isTrue();
    }

    @Test
    void testGetCheckpointingMode() {
        // Test when checkpointing is disabled - should return AT_LEAST_ONCE
        Configuration disabledConfig = new Configuration();
        assertThat(CheckpointingOptions.getCheckpointingMode(disabledConfig))
                .as("Should return AT_LEAST_ONCE when checkpointing is disabled")
                .isEqualTo(CheckpointingMode.AT_LEAST_ONCE);

        // Test when checkpointing is enabled with default mode
        Configuration enabledDefaultConfig = new Configuration();
        enabledDefaultConfig.set(
                CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(5));
        assertThat(CheckpointingOptions.getCheckpointingMode(enabledDefaultConfig))
                .as("Should return EXACTLY_ONCE as default when checkpointing is enabled")
                .isEqualTo(CheckpointingMode.EXACTLY_ONCE);

        // Test when checkpointing is enabled with EXACTLY_ONCE explicitly set
        Configuration exactlyOnceConfig = new Configuration();
        exactlyOnceConfig.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(5));
        exactlyOnceConfig.set(
                CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE,
                CheckpointingMode.EXACTLY_ONCE);
        assertThat(CheckpointingOptions.getCheckpointingMode(exactlyOnceConfig))
                .as("Should return EXACTLY_ONCE when explicitly configured")
                .isEqualTo(CheckpointingMode.EXACTLY_ONCE);

        // Test when checkpointing is enabled with AT_LEAST_ONCE explicitly set
        Configuration atLeastOnceConfig = new Configuration();
        atLeastOnceConfig.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(5));
        atLeastOnceConfig.set(
                CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE,
                CheckpointingMode.AT_LEAST_ONCE);
        assertThat(CheckpointingOptions.getCheckpointingMode(atLeastOnceConfig))
                .as("Should return AT_LEAST_ONCE when explicitly configured")
                .isEqualTo(CheckpointingMode.AT_LEAST_ONCE);

        // Test when RUNTIME_MODE is BATCH - should return AT_LEAST_ONCE regardless of other
        // settings
        Configuration batchModeConfig = new Configuration();
        batchModeConfig.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(5));
        batchModeConfig.set(
                CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE,
                CheckpointingMode.EXACTLY_ONCE);
        batchModeConfig.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        assertThat(CheckpointingOptions.getCheckpointingMode(batchModeConfig))
                .as(
                        "Should return AT_LEAST_ONCE when runtime mode is BATCH, regardless of other settings")
                .isEqualTo(CheckpointingMode.AT_LEAST_ONCE);

        // Test when RUNTIME_MODE is STREAMING - should follow normal logic
        Configuration streamingModeConfig = new Configuration();
        streamingModeConfig.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(5));
        streamingModeConfig.set(
                CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE,
                CheckpointingMode.EXACTLY_ONCE);
        streamingModeConfig.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.STREAMING);
        assertThat(CheckpointingOptions.getCheckpointingMode(streamingModeConfig))
                .as(
                        "Should return EXACTLY_ONCE when runtime mode is STREAMING and explicitly configured")
                .isEqualTo(CheckpointingMode.EXACTLY_ONCE);
    }

    @Test
    void testIsUnalignedCheckpointEnabled() {
        // Test when checkpointing mode is AT_LEAST_ONCE - should always return false
        Configuration atLeastOnceConfig = new Configuration();
        atLeastOnceConfig.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(5));
        atLeastOnceConfig.set(
                CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE,
                CheckpointingMode.AT_LEAST_ONCE);
        atLeastOnceConfig.set(CheckpointingOptions.ENABLE_UNALIGNED, true);
        assertThat(CheckpointingOptions.isUnalignedCheckpointEnabled(atLeastOnceConfig))
                .as("Unaligned checkpoints should be disabled when mode is AT_LEAST_ONCE")
                .isFalse();

        // Test when checkpointing mode is EXACTLY_ONCE and ENABLE_UNALIGNED is false (default)
        Configuration exactlyOnceDefaultConfig = new Configuration();
        exactlyOnceDefaultConfig.set(
                CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(5));
        exactlyOnceDefaultConfig.set(
                CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE,
                CheckpointingMode.EXACTLY_ONCE);
        assertThat(CheckpointingOptions.isUnalignedCheckpointEnabled(exactlyOnceDefaultConfig))
                .as(
                        "Unaligned checkpoints should be disabled by default even with EXACTLY_ONCE mode")
                .isFalse();

        // Test when checkpointing mode is EXACTLY_ONCE and ENABLE_UNALIGNED is explicitly false
        Configuration exactlyOnceFalseConfig = new Configuration();
        exactlyOnceFalseConfig.set(
                CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(5));
        exactlyOnceFalseConfig.set(
                CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE,
                CheckpointingMode.EXACTLY_ONCE);
        exactlyOnceFalseConfig.set(CheckpointingOptions.ENABLE_UNALIGNED, false);
        assertThat(CheckpointingOptions.isUnalignedCheckpointEnabled(exactlyOnceFalseConfig))
                .as("Unaligned checkpoints should be disabled when explicitly set to false")
                .isFalse();

        // Test when checkpointing mode is EXACTLY_ONCE and ENABLE_UNALIGNED is true
        Configuration exactlyOnceTrueConfig = new Configuration();
        exactlyOnceTrueConfig.set(
                CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(5));
        exactlyOnceTrueConfig.set(
                CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE,
                CheckpointingMode.EXACTLY_ONCE);
        exactlyOnceTrueConfig.set(CheckpointingOptions.ENABLE_UNALIGNED, true);
        assertThat(CheckpointingOptions.isUnalignedCheckpointEnabled(exactlyOnceTrueConfig))
                .as(
                        "Unaligned checkpoints should be enabled when mode is EXACTLY_ONCE and explicitly enabled")
                .isTrue();

        // Test when checkpointing is disabled - should return false
        Configuration disabledConfig = new Configuration();
        disabledConfig.set(CheckpointingOptions.ENABLE_UNALIGNED, true);
        assertThat(CheckpointingOptions.isUnalignedCheckpointEnabled(disabledConfig))
                .as("Unaligned checkpoints should be disabled when checkpointing is disabled")
                .isFalse();

        // Test when RUNTIME_MODE is BATCH - should always return false
        Configuration batchModeConfig = new Configuration();
        batchModeConfig.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(5));
        batchModeConfig.set(
                CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE,
                CheckpointingMode.EXACTLY_ONCE);
        batchModeConfig.set(CheckpointingOptions.ENABLE_UNALIGNED, true);
        batchModeConfig.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        assertThat(CheckpointingOptions.isUnalignedCheckpointEnabled(batchModeConfig))
                .as("Unaligned checkpoints should be disabled when runtime mode is BATCH")
                .isFalse();
    }

    @Test
    void testIsUnalignedCheckpointInterruptibleTimersEnabled() {
        // Test when unaligned checkpoints are disabled - should always return false
        Configuration disabledUnalignedConfig = new Configuration();
        disabledUnalignedConfig.set(
                CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(5));
        disabledUnalignedConfig.set(
                CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE,
                CheckpointingMode.EXACTLY_ONCE);
        disabledUnalignedConfig.set(CheckpointingOptions.ENABLE_UNALIGNED, false);
        disabledUnalignedConfig.set(
                CheckpointingOptions.ENABLE_UNALIGNED_INTERRUPTIBLE_TIMERS, true);
        assertThat(
                        CheckpointingOptions.isUnalignedCheckpointInterruptibleTimersEnabled(
                                disabledUnalignedConfig))
                .as(
                        "Interruptible timers should be disabled when unaligned checkpoints are disabled")
                .isFalse();

        // Test when unaligned checkpoints are enabled but interruptible timers are disabled
        Configuration enabledUnalignedDisabledTimersConfig = new Configuration();
        enabledUnalignedDisabledTimersConfig.set(
                CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(5));
        enabledUnalignedDisabledTimersConfig.set(
                CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE,
                CheckpointingMode.EXACTLY_ONCE);
        enabledUnalignedDisabledTimersConfig.set(CheckpointingOptions.ENABLE_UNALIGNED, true);
        enabledUnalignedDisabledTimersConfig.set(
                CheckpointingOptions.ENABLE_UNALIGNED_INTERRUPTIBLE_TIMERS, false);
        assertThat(
                        CheckpointingOptions.isUnalignedCheckpointInterruptibleTimersEnabled(
                                enabledUnalignedDisabledTimersConfig))
                .as("Interruptible timers should be disabled when explicitly set to false")
                .isFalse();

        // Test when unaligned checkpoints are enabled and interruptible timers are enabled
        Configuration enabledBothConfig = new Configuration();
        enabledBothConfig.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(5));
        enabledBothConfig.set(
                CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE,
                CheckpointingMode.EXACTLY_ONCE);
        enabledBothConfig.set(CheckpointingOptions.ENABLE_UNALIGNED, true);
        enabledBothConfig.set(CheckpointingOptions.ENABLE_UNALIGNED_INTERRUPTIBLE_TIMERS, true);
        assertThat(
                        CheckpointingOptions.isUnalignedCheckpointInterruptibleTimersEnabled(
                                enabledBothConfig))
                .as(
                        "Interruptible timers should be enabled when both unaligned checkpoints and interruptible timers are enabled")
                .isTrue();

        // Test when checkpointing mode is AT_LEAST_ONCE - should return false
        Configuration atLeastOnceConfig = new Configuration();
        atLeastOnceConfig.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(5));
        atLeastOnceConfig.set(
                CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE,
                CheckpointingMode.AT_LEAST_ONCE);
        atLeastOnceConfig.set(CheckpointingOptions.ENABLE_UNALIGNED, true);
        atLeastOnceConfig.set(CheckpointingOptions.ENABLE_UNALIGNED_INTERRUPTIBLE_TIMERS, true);
        assertThat(
                        CheckpointingOptions.isUnalignedCheckpointInterruptibleTimersEnabled(
                                atLeastOnceConfig))
                .as(
                        "Interruptible timers should be disabled when checkpointing mode is AT_LEAST_ONCE")
                .isFalse();

        // Test when checkpointing is disabled - should return false
        Configuration checkpointingDisabledConfig = new Configuration();
        checkpointingDisabledConfig.set(CheckpointingOptions.ENABLE_UNALIGNED, true);
        checkpointingDisabledConfig.set(
                CheckpointingOptions.ENABLE_UNALIGNED_INTERRUPTIBLE_TIMERS, true);
        assertThat(
                        CheckpointingOptions.isUnalignedCheckpointInterruptibleTimersEnabled(
                                checkpointingDisabledConfig))
                .as("Interruptible timers should be disabled when checkpointing is disabled")
                .isFalse();

        // Test when RUNTIME_MODE is BATCH - should return false
        Configuration batchModeConfig = new Configuration();
        batchModeConfig.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(5));
        batchModeConfig.set(
                CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE,
                CheckpointingMode.EXACTLY_ONCE);
        batchModeConfig.set(CheckpointingOptions.ENABLE_UNALIGNED, true);
        batchModeConfig.set(CheckpointingOptions.ENABLE_UNALIGNED_INTERRUPTIBLE_TIMERS, true);
        batchModeConfig.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        assertThat(
                        CheckpointingOptions.isUnalignedCheckpointInterruptibleTimersEnabled(
                                batchModeConfig))
                .as("Interruptible timers should be disabled when runtime mode is BATCH")
                .isFalse();
    }
}
