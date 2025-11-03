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

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor;
import org.apache.flink.runtime.checkpoint.RescaleMappings;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;

import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptorUtil.mappings;
import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptorUtil.to;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link ResultSubpartitionDistributor}. */
class ResultSubpartitionDistributorTest {

    private static final int DEFAULT_PARTITION_INDEX = 0;
    private static final int DEFAULT_SUBPARTITION_INDEX = 0;

    private InflightDataRescalingDescriptor createIdentityMapping() {
        return new InflightDataRescalingDescriptor(
                new InflightDataRescalingDescriptor.InflightDataGateOrPartitionRescalingDescriptor
                        [] {
                    new InflightDataRescalingDescriptor
                            .InflightDataGateOrPartitionRescalingDescriptor(
                            new int[] {1},
                            RescaleMappings.identity(1, 1),
                            new HashSet<>(),
                            InflightDataRescalingDescriptor
                                    .InflightDataGateOrPartitionRescalingDescriptor.MappingType
                                    .IDENTITY)
                });
    }

    private InflightDataRescalingDescriptor createRescalingMapping(RescaleMappings mappings) {
        return new InflightDataRescalingDescriptor(
                new InflightDataRescalingDescriptor.InflightDataGateOrPartitionRescalingDescriptor
                        [] {
                    new InflightDataRescalingDescriptor
                            .InflightDataGateOrPartitionRescalingDescriptor(
                            new int[] {2},
                            mappings,
                            new HashSet<>(),
                            InflightDataRescalingDescriptor
                                    .InflightDataGateOrPartitionRescalingDescriptor.MappingType
                                    .RESCALING)
                });
    }

    @Test
    void testGetMappedSubpartitionsIdentityMapping() {
        InflightDataRescalingDescriptor identityMapping = createIdentityMapping();
        ResultSubpartitionDistributor distributor =
                new ResultSubpartitionDistributor(identityMapping);
        ResultSubpartitionInfo inputInfo =
                new ResultSubpartitionInfo(DEFAULT_PARTITION_INDEX, DEFAULT_SUBPARTITION_INDEX);

        List<ResultSubpartitionInfo> mappedSubpartitions =
                distributor.getMappedSubpartitions(inputInfo);

        // Identity mapping should preserve original indices
        assertThat(mappedSubpartitions).hasSize(1);
        assertThat(mappedSubpartitions.get(0).getPartitionIdx()).isEqualTo(DEFAULT_PARTITION_INDEX);
        assertThat(mappedSubpartitions.get(0).getSubPartitionIdx())
                .isEqualTo(DEFAULT_SUBPARTITION_INDEX);
    }

    @Test
    void testGetMappedSubpartitionsRescaling() {
        // Test rescaling scenario where one input maps to multiple outputs
        RescaleMappings rescaleMappings = mappings(to(0, 1));
        InflightDataRescalingDescriptor rescalingMapping = createRescalingMapping(rescaleMappings);
        ResultSubpartitionDistributor distributor =
                new ResultSubpartitionDistributor(rescalingMapping);
        ResultSubpartitionInfo inputInfo =
                new ResultSubpartitionInfo(DEFAULT_PARTITION_INDEX, DEFAULT_SUBPARTITION_INDEX);

        List<ResultSubpartitionInfo> mappedSubpartitions =
                distributor.getMappedSubpartitions(inputInfo);

        // Rescaling preserves partition index but may change subpartition mapping
        assertThat(mappedSubpartitions).isNotEmpty();
        assertThat(mappedSubpartitions)
                .allMatch(info -> info.getPartitionIdx() == DEFAULT_PARTITION_INDEX);
    }

    @Test
    void testMappingCacheConsistency() {
        // Verify caching behavior to ensure performance optimization
        InflightDataRescalingDescriptor identityMapping = createIdentityMapping();
        ResultSubpartitionDistributor distributor =
                new ResultSubpartitionDistributor(identityMapping);
        ResultSubpartitionInfo inputInfo =
                new ResultSubpartitionInfo(DEFAULT_PARTITION_INDEX, DEFAULT_SUBPARTITION_INDEX);

        List<ResultSubpartitionInfo> firstCall = distributor.getMappedSubpartitions(inputInfo);
        List<ResultSubpartitionInfo> secondCall = distributor.getMappedSubpartitions(inputInfo);

        // Cache should return same instance
        assertThat(firstCall).isEqualTo(secondCall).isSameAs(secondCall);
    }

    @Test
    void testInvalidMappingThrowsException() {
        // Test error handling when mapping configuration is inconsistent
        RescaleMappings mappingsWithNoTarget = mappings();
        InflightDataRescalingDescriptor invalidMapping =
                createRescalingMapping(mappingsWithNoTarget);
        ResultSubpartitionDistributor distributor =
                new ResultSubpartitionDistributor(invalidMapping);
        ResultSubpartitionInfo inputInfo =
                new ResultSubpartitionInfo(DEFAULT_PARTITION_INDEX, DEFAULT_SUBPARTITION_INDEX);

        assertThatThrownBy(() -> distributor.getMappedSubpartitions(inputInfo))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Recovered a buffer from old")
                .hasMessageContaining("that has no mapping in");
    }
}
