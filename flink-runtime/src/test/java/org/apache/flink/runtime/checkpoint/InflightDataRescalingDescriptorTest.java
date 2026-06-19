/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor.InflightDataGateOrPartitionRescalingDescriptor;
import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor.InflightDataGateOrPartitionRescalingDescriptor.MappingType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link InflightDataRescalingDescriptor}. */
class InflightDataRescalingDescriptorTest {

    @Test
    void testNoStateDescriptorReturnsEmptyOldSubtaskInstances() {
        InflightDataGateOrPartitionRescalingDescriptor noStateDescriptor =
                InflightDataGateOrPartitionRescalingDescriptor.NO_STATE;

        assertThat(noStateDescriptor.getOldSubtaskInstances()).isEqualTo(new int[0]);
    }

    @Test
    void testNoStateDescriptorReturnsSymmetricIdentity() {
        InflightDataGateOrPartitionRescalingDescriptor noStateDescriptor =
                InflightDataGateOrPartitionRescalingDescriptor.NO_STATE;

        assertThat(noStateDescriptor.getRescaleMappings())
                .isEqualTo(RescaleMappings.SYMMETRIC_IDENTITY);
    }

    @Test
    void testNoStateDescriptorIsIdentity() {
        InflightDataGateOrPartitionRescalingDescriptor noStateDescriptor =
                InflightDataGateOrPartitionRescalingDescriptor.NO_STATE;

        assertThat(noStateDescriptor.isIdentity()).isTrue();
    }

    @Test
    void testRegularDescriptorDoesNotThrow() {
        int[] oldSubtasks = new int[] {0, 1, 2};
        RescaleMappings mappings =
                RescaleMappings.of(Arrays.stream(new int[][] {{0}, {1}, {2}}), 3);

        InflightDataGateOrPartitionRescalingDescriptor descriptor =
                new InflightDataGateOrPartitionRescalingDescriptor(
                        oldSubtasks, mappings, Collections.emptySet(), MappingType.RESCALING);

        // Should not throw
        assertThat(descriptor.getOldSubtaskInstances()).isEqualTo(oldSubtasks);
        assertThat(descriptor.getRescaleMappings()).isEqualTo(mappings);
        assertThat(descriptor.isIdentity()).isFalse();
    }

    @Test
    void testIdentityDescriptor() {
        int[] oldSubtasks = new int[] {0};
        RescaleMappings mappings = RescaleMappings.identity(1, 1);

        InflightDataGateOrPartitionRescalingDescriptor descriptor =
                new InflightDataGateOrPartitionRescalingDescriptor(
                        oldSubtasks, mappings, Collections.emptySet(), MappingType.IDENTITY);

        assertThat(descriptor.isIdentity()).isTrue();
        assertThat(descriptor.getOldSubtaskInstances()).isEqualTo(oldSubtasks);
        assertThat(descriptor.getRescaleMappings()).isEqualTo(mappings);
    }

    @Test
    void testInflightDataRescalingDescriptorWithNoStateDescriptor() {
        // Create a descriptor array with NO_STATE descriptor
        InflightDataGateOrPartitionRescalingDescriptor[] descriptors =
                new InflightDataGateOrPartitionRescalingDescriptor[] {
                    InflightDataGateOrPartitionRescalingDescriptor.NO_STATE,
                    new InflightDataGateOrPartitionRescalingDescriptor(
                            new int[] {0, 1},
                            RescaleMappings.of(Arrays.stream(new int[][] {{0}, {1}}), 2),
                            Collections.emptySet(),
                            MappingType.RESCALING)
                };

        InflightDataRescalingDescriptor rescalingDescriptor =
                new InflightDataRescalingDescriptor(descriptors);

        // First gate/partition has NO_STATE - should return empty array and SYMMETRIC_IDENTITY
        assertThat(rescalingDescriptor.getOldSubtaskIndexes(0)).isEqualTo(new int[0]);
        assertThat(rescalingDescriptor.getChannelMapping(0))
                .isEqualTo(RescaleMappings.SYMMETRIC_IDENTITY);

        // Second gate/partition has normal state
        assertThat(rescalingDescriptor.getOldSubtaskIndexes(1)).isEqualTo(new int[] {0, 1});
        assertThat(rescalingDescriptor.getChannelMapping(1)).isNotNull();
    }
}
