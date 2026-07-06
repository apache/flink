/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io.recovery;

import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor;
import org.apache.flink.runtime.checkpoint.RescaleMappings;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptorUtil.mappings;
import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptorUtil.rescalingDescriptor;
import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptorUtil.set;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RecordFilterContext}. */
class RecordFilterContextTest {

    @TempDir private Path tempDir;

    private String[] tmpDirs() {
        return new String[] {tempDir.toString()};
    }

    @Test
    void testDisabledContextHasNoGates() {
        RecordFilterContext disabled = RecordFilterContext.disabled(tmpDirs());
        assertThat(disabled.getNumberOfGates()).isEqualTo(0);
        assertThat(disabled.isCheckpointingDuringRecoveryEnabled()).isFalse();
        assertThat(disabled.getTmpDirectories()).containsExactly(tempDir.toString());
    }

    @Test
    void testGetInputConfigReturnsCorrectConfig() {
        RecordFilterContext.InputFilterConfig config =
                new RecordFilterContext.InputFilterConfig(
                        LongSerializer.INSTANCE, new ForwardPartitioner<>(), 4);

        RecordFilterContext context =
                new RecordFilterContext(
                        new RecordFilterContext.InputFilterConfig[] {config},
                        InflightDataRescalingDescriptor.NO_RESCALE,
                        0,
                        128,
                        tmpDirs(),
                        true,
                        MemoryManager.DEFAULT_PAGE_SIZE);

        assertThat(context.getNumberOfGates()).isEqualTo(1);
        assertThat(context.getInputConfig(0)).isSameAs(config);
        assertThat(context.getSubtaskIndex()).isEqualTo(0);
        assertThat(context.getMaxParallelism()).isEqualTo(128);
        assertThat(context.isCheckpointingDuringRecoveryEnabled()).isTrue();
    }

    @Test
    void testGetInputConfigThrowsForInvalidIndex() {
        RecordFilterContext context =
                new RecordFilterContext(
                        new RecordFilterContext.InputFilterConfig[0],
                        InflightDataRescalingDescriptor.NO_RESCALE,
                        0,
                        128,
                        tmpDirs(),
                        false,
                        MemoryManager.DEFAULT_PAGE_SIZE);

        assertThatThrownBy(() -> context.getInputConfig(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> context.getInputConfig(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testEnabledContextRejectsNullOrEmptyTmpDirectories() {
        // When checkpointing-during-recovery is enabled, the spilling path needs spill
        // directories, so null/empty tmpDirectories are rejected.
        assertThatThrownBy(
                        () ->
                                new RecordFilterContext(
                                        new RecordFilterContext.InputFilterConfig[0],
                                        InflightDataRescalingDescriptor.NO_RESCALE,
                                        0,
                                        128,
                                        null,
                                        true,
                                        MemoryManager.DEFAULT_PAGE_SIZE))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(
                        () ->
                                new RecordFilterContext(
                                        new RecordFilterContext.InputFilterConfig[0],
                                        InflightDataRescalingDescriptor.NO_RESCALE,
                                        0,
                                        128,
                                        new String[0],
                                        true,
                                        MemoryManager.DEFAULT_PAGE_SIZE))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testDisabledContextToleratesNullOrEmptyTmpDirectories() {
        // A disabled context never spills, so it needs no spill directories: null/empty are
        // tolerated and normalized to an empty array.
        RecordFilterContext fromNull =
                new RecordFilterContext(
                        new RecordFilterContext.InputFilterConfig[0],
                        InflightDataRescalingDescriptor.NO_RESCALE,
                        0,
                        128,
                        null,
                        false,
                        MemoryManager.DEFAULT_PAGE_SIZE);
        assertThat(fromNull.getTmpDirectories()).isEmpty();

        RecordFilterContext fromEmpty =
                new RecordFilterContext(
                        new RecordFilterContext.InputFilterConfig[0],
                        InflightDataRescalingDescriptor.NO_RESCALE,
                        0,
                        128,
                        new String[0],
                        false,
                        MemoryManager.DEFAULT_PAGE_SIZE);
        assertThat(fromEmpty.getTmpDirectories()).isEmpty();
    }

    @Test
    void testIsAmbiguousWhenDisabled() {
        // Create a rescaling descriptor with an ambiguous subtask (oldSubtask 0 is ambiguous)
        RescaleMappings mapping = mappings(new int[] {0});
        InflightDataRescalingDescriptor descriptor =
                rescalingDescriptor(new int[] {0}, new RescaleMappings[] {mapping}, set(0));

        // When checkpointingDuringRecoveryEnabled is false, isAmbiguous should always return false
        RecordFilterContext context =
                new RecordFilterContext(
                        new RecordFilterContext.InputFilterConfig[0],
                        descriptor,
                        0,
                        128,
                        tmpDirs(),
                        false,
                        MemoryManager.DEFAULT_PAGE_SIZE);

        assertThat(context.isAmbiguous(0, 0)).isFalse();
    }

    @Test
    void testIsAmbiguousWhenEnabled() {
        // Create a rescaling descriptor with an ambiguous subtask (oldSubtask 0 is ambiguous)
        RescaleMappings mapping = mappings(new int[] {0});
        InflightDataRescalingDescriptor descriptor =
                rescalingDescriptor(new int[] {0}, new RescaleMappings[] {mapping}, set(0));

        // When checkpointingDuringRecoveryEnabled is true, isAmbiguous follows rescalingDescriptor
        RecordFilterContext context =
                new RecordFilterContext(
                        new RecordFilterContext.InputFilterConfig[0],
                        descriptor,
                        0,
                        128,
                        tmpDirs(),
                        true,
                        MemoryManager.DEFAULT_PAGE_SIZE);

        assertThat(context.isAmbiguous(0, 0)).isTrue();
    }

    @Test
    void testIsAmbiguousForNonAmbiguousSubtask() {
        // Create a rescaling descriptor where oldSubtask 0 is ambiguous but oldSubtask 1 is not
        RescaleMappings mapping = mappings(new int[] {0});
        InflightDataRescalingDescriptor descriptor =
                rescalingDescriptor(new int[] {0, 1}, new RescaleMappings[] {mapping}, set(0));

        RecordFilterContext context =
                new RecordFilterContext(
                        new RecordFilterContext.InputFilterConfig[0],
                        descriptor,
                        0,
                        128,
                        tmpDirs(),
                        true,
                        MemoryManager.DEFAULT_PAGE_SIZE);

        // oldSubtask 0 is ambiguous
        assertThat(context.isAmbiguous(0, 0)).isTrue();
        // oldSubtask 1 is NOT in the ambiguous set
        assertThat(context.isAmbiguous(0, 1)).isFalse();
    }

    @Test
    void testMemorySegmentSizeExposedAndValidated() {
        RecordFilterContext context =
                new RecordFilterContext(
                        new RecordFilterContext.InputFilterConfig[0],
                        InflightDataRescalingDescriptor.NO_RESCALE,
                        0,
                        128,
                        tmpDirs(),
                        false,
                        MemoryManager.DEFAULT_PAGE_SIZE * 2);

        assertThat(context.getMemorySegmentSize()).isEqualTo(MemoryManager.DEFAULT_PAGE_SIZE * 2);

        // Non-positive sizes are rejected.
        assertThatThrownBy(
                        () ->
                                new RecordFilterContext(
                                        new RecordFilterContext.InputFilterConfig[0],
                                        InflightDataRescalingDescriptor.NO_RESCALE,
                                        0,
                                        128,
                                        tmpDirs(),
                                        false,
                                        0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                new RecordFilterContext(
                                        new RecordFilterContext.InputFilterConfig[0],
                                        InflightDataRescalingDescriptor.NO_RESCALE,
                                        0,
                                        128,
                                        tmpDirs(),
                                        false,
                                        -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testInputFilterConfigGetters() {
        ForwardPartitioner<Long> partitioner = new ForwardPartitioner<>();
        RecordFilterContext.InputFilterConfig config =
                new RecordFilterContext.InputFilterConfig(LongSerializer.INSTANCE, partitioner, 4);

        assertThat(config.getTypeSerializer()).isSameAs(LongSerializer.INSTANCE);
        assertThat(config.getPartitioner()).isSameAs(partitioner);
        assertThat(config.getNumberOfChannels()).isEqualTo(4);
    }

    @Test
    void testMultipleGateConfigs() {
        RecordFilterContext.InputFilterConfig config0 =
                new RecordFilterContext.InputFilterConfig(
                        LongSerializer.INSTANCE, new ForwardPartitioner<>(), 2);
        RecordFilterContext.InputFilterConfig config1 =
                new RecordFilterContext.InputFilterConfig(
                        LongSerializer.INSTANCE, new ForwardPartitioner<>(), 4);

        RecordFilterContext context =
                new RecordFilterContext(
                        new RecordFilterContext.InputFilterConfig[] {config0, config1},
                        InflightDataRescalingDescriptor.NO_RESCALE,
                        1,
                        256,
                        tmpDirs(),
                        false,
                        MemoryManager.DEFAULT_PAGE_SIZE);

        assertThat(context.getNumberOfGates()).isEqualTo(2);
        assertThat(context.getInputConfig(0)).isSameAs(config0);
        assertThat(context.getInputConfig(1)).isSameAs(config1);
        assertThat(context.getInputConfig(0).getNumberOfChannels()).isEqualTo(2);
        assertThat(context.getInputConfig(1).getNumberOfChannels()).isEqualTo(4);
    }
}
