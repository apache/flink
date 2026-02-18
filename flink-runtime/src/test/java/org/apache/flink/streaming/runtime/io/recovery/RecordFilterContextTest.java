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
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;

import org.junit.jupiter.api.Test;

import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptorUtil.mappings;
import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptorUtil.rescalingDescriptor;
import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptorUtil.set;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RecordFilterContext}. */
class RecordFilterContextTest {

    @Test
    void testDisabledContextHasNoGates() {
        RecordFilterContext disabled = RecordFilterContext.disabled();
        assertThat(disabled.getNumberOfGates()).isEqualTo(0);
        assertThat(disabled.isCheckpointingDuringRecoveryEnabled()).isFalse();
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
                        new String[] {"/tmp"},
                        true);

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
                        null,
                        false);

        assertThatThrownBy(() -> context.getInputConfig(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> context.getInputConfig(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testNullTmpDirectoriesConvertedToEmptyArray() {
        RecordFilterContext context =
                new RecordFilterContext(
                        new RecordFilterContext.InputFilterConfig[0],
                        InflightDataRescalingDescriptor.NO_RESCALE,
                        0,
                        128,
                        null,
                        false);

        assertThat(context.getTmpDirectories()).isNotNull().isEmpty();
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
                        null,
                        false);

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
                        null,
                        true);

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
                        null,
                        true);

        // oldSubtask 0 is ambiguous
        assertThat(context.isAmbiguous(0, 0)).isTrue();
        // oldSubtask 1 is NOT in the ambiguous set
        assertThat(context.isAmbiguous(0, 1)).isFalse();
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
                        new String[] {"/tmp"},
                        false);

        assertThat(context.getNumberOfGates()).isEqualTo(2);
        assertThat(context.getInputConfig(0)).isSameAs(config0);
        assertThat(context.getInputConfig(1)).isSameAs(config1);
        assertThat(context.getInputConfig(0).getNumberOfChannels()).isEqualTo(2);
        assertThat(context.getInputConfig(1).getNumberOfChannels()).isEqualTo(4);
    }
}
