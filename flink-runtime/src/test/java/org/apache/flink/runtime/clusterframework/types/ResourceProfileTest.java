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

package org.apache.flink.runtime.clusterframework.types;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.api.common.resources.ExternalResource;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.testutils.CommonTestUtils;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.runtime.clusterframework.types.ResourceProfile.MAX_CPU_CORE_NUMBER_TO_LOG;
import static org.apache.flink.runtime.clusterframework.types.ResourceProfile.MAX_MEMORY_SIZE_TO_LOG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/** Tests for the {@link ResourceProfile}. */
class ResourceProfileTest {
    private static final MemorySize TOO_LARGE_MEMORY =
            MAX_MEMORY_SIZE_TO_LOG.add(MemorySize.ofMebiBytes(10));
    private static final String EXTERNAL_RESOURCE_NAME = "gpu";

    @Test
    void testAllFieldsNoLessThanProfile() {
        final ResourceProfile rp1 =
                ResourceProfile.newBuilder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .build();
        final ResourceProfile rp2 =
                ResourceProfile.newBuilder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(200)
                        .setTaskOffHeapMemoryMB(200)
                        .setManagedMemoryMB(200)
                        .build();
        final ResourceProfile rp3 =
                ResourceProfile.newBuilder()
                        .setCpuCores(2.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .build();
        final ResourceProfile rp4 =
                ResourceProfile.newBuilder()
                        .setCpuCores(2.0)
                        .setTaskHeapMemoryMB(200)
                        .setTaskOffHeapMemoryMB(200)
                        .setManagedMemoryMB(200)
                        .build();

        assertThat(rp1.allFieldsNoLessThan(rp2)).isFalse();
        assertThat(rp2.allFieldsNoLessThan(rp1)).isTrue();

        assertThat(rp1.allFieldsNoLessThan(rp3)).isFalse();
        assertThat(rp3.allFieldsNoLessThan(rp1)).isTrue();

        assertThat(rp2.allFieldsNoLessThan(rp3)).isFalse();
        assertThat(rp3.allFieldsNoLessThan(rp2)).isFalse();

        assertThat(rp4.allFieldsNoLessThan(rp1)).isTrue();
        assertThat(rp4.allFieldsNoLessThan(rp2)).isTrue();
        assertThat(rp4.allFieldsNoLessThan(rp3)).isTrue();
        assertThat(rp4.allFieldsNoLessThan(rp4)).isTrue();

        final ResourceProfile rp5 =
                ResourceProfile.newBuilder()
                        .setCpuCores(2.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .build();
        assertThat(rp4.allFieldsNoLessThan(rp5)).isFalse();

        ResourceSpec rs1 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 2.2))
                        .build();
        ResourceSpec rs2 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.1))
                        .build();

        assertThat(rp1.allFieldsNoLessThan(ResourceProfile.fromResourceSpec(rs1))).isFalse();
        assertThat(
                        ResourceProfile.fromResourceSpec(rs1)
                                .allFieldsNoLessThan(ResourceProfile.fromResourceSpec(rs2)))
                .isTrue();
        assertThat(
                        ResourceProfile.fromResourceSpec(rs2)
                                .allFieldsNoLessThan(ResourceProfile.fromResourceSpec(rs1)))
                .isFalse();
    }

    @Test
    void testUnknownNoLessThanUnknown() {
        assertThat(ResourceProfile.UNKNOWN.allFieldsNoLessThan(ResourceProfile.UNKNOWN)).isTrue();
    }

    @Test
    void testMatchRequirement() {
        final ResourceProfile resource1 =
                ResourceProfile.newBuilder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .build();
        final ResourceProfile resource2 =
                ResourceProfile.newBuilder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.0))
                        .build();
        final ResourceProfile requirement1 = ResourceProfile.UNKNOWN;
        final ResourceProfile requirement2 =
                ResourceProfile.newBuilder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .build();
        final ResourceProfile requirement3 =
                ResourceProfile.newBuilder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.0))
                        .build();

        assertThat(resource1.isMatching(requirement1)).isTrue();
        assertThat(resource1.isMatching(requirement2)).isTrue();
        assertThat(resource1.isMatching(requirement3)).isFalse();

        assertThat(resource2.isMatching(requirement1)).isTrue();
        assertThat(resource2.isMatching(requirement2)).isFalse();
        assertThat(resource2.isMatching(requirement3)).isTrue();
    }

    @Test
    void testEquals() {
        ResourceSpec rs1 = ResourceSpec.newBuilder(1.0, 100).build();
        ResourceSpec rs2 = ResourceSpec.newBuilder(1.0, 100).build();
        assertThat(ResourceProfile.fromResourceSpec(rs2))
                .isEqualTo(ResourceProfile.fromResourceSpec(rs1));

        ResourceSpec rs3 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 2.2))
                        .build();
        ResourceSpec rs4 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.1))
                        .build();
        assertThat(ResourceProfile.fromResourceSpec(rs4))
                .isNotEqualTo(ResourceProfile.fromResourceSpec(rs3));

        ResourceSpec rs5 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 2.2))
                        .build();
        MemorySize networkMemory = MemorySize.ofMebiBytes(100);
        assertThat(ResourceProfile.fromResourceSpec(rs5, networkMemory))
                .isEqualTo(ResourceProfile.fromResourceSpec(rs3, networkMemory));

        final ResourceProfile rp1 =
                ResourceProfile.newBuilder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .build();
        final ResourceProfile rp2 =
                ResourceProfile.newBuilder()
                        .setCpuCores(1.1)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .build();
        final ResourceProfile rp3 =
                ResourceProfile.newBuilder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(110)
                        .setTaskOffHeapMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .build();
        final ResourceProfile rp4 =
                ResourceProfile.newBuilder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(110)
                        .setManagedMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .build();
        final ResourceProfile rp5 =
                ResourceProfile.newBuilder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setManagedMemoryMB(110)
                        .setNetworkMemoryMB(100)
                        .build();
        final ResourceProfile rp6 =
                ResourceProfile.newBuilder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setManagedMemoryMB(110)
                        .setNetworkMemoryMB(100)
                        .build();
        final ResourceProfile rp7 =
                ResourceProfile.newBuilder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNetworkMemoryMB(110)
                        .build();
        final ResourceProfile rp8 =
                ResourceProfile.newBuilder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .build();

        assertThat(rp2).isNotEqualTo(rp1);
        assertThat(rp3).isNotEqualTo(rp1);
        assertThat(rp4).isNotEqualTo(rp1);
        assertThat(rp5).isNotEqualTo(rp1);
        assertThat(rp6).isNotEqualTo(rp1);
        assertThat(rp7).isNotEqualTo(rp1);
        assertThat(rp8).isEqualTo(rp1);
    }

    @Test
    void testGet() {
        ResourceSpec rs =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.6))
                        .build();
        ResourceProfile rp = ResourceProfile.fromResourceSpec(rs, MemorySize.ofMebiBytes(50));

        assertThat(rp.getCpuCores()).isEqualTo(new CPUResource(1.0));
        assertThat(rp.getTotalMemory().getMebiBytes()).isEqualTo(150);
        assertThat(rp.getOperatorsMemory().getMebiBytes()).isEqualTo(100);
        assertThat(rp.getExtendedResources().get(EXTERNAL_RESOURCE_NAME))
                .isEqualTo(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.6));
    }

    @Test
    void testMerge() {
        final ResourceProfile rp1 =
                ResourceProfile.newBuilder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .build();
        final ResourceProfile rp2 =
                ResourceProfile.newBuilder()
                        .setCpuCores(2.0)
                        .setTaskHeapMemoryMB(200)
                        .setTaskOffHeapMemoryMB(200)
                        .setManagedMemoryMB(200)
                        .setNetworkMemoryMB(200)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 2.0))
                        .build();

        final ResourceProfile rp1MergeRp1 =
                ResourceProfile.newBuilder()
                        .setCpuCores(2.0)
                        .setTaskHeapMemoryMB(200)
                        .setTaskOffHeapMemoryMB(200)
                        .setManagedMemoryMB(200)
                        .setNetworkMemoryMB(200)
                        .build();
        final ResourceProfile rp1MergeRp2 =
                ResourceProfile.newBuilder()
                        .setCpuCores(3.0)
                        .setTaskHeapMemoryMB(300)
                        .setTaskOffHeapMemoryMB(300)
                        .setManagedMemoryMB(300)
                        .setNetworkMemoryMB(300)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 2.0))
                        .build();
        final ResourceProfile rp2MergeRp2 =
                ResourceProfile.newBuilder()
                        .setCpuCores(4.0)
                        .setTaskHeapMemoryMB(400)
                        .setTaskOffHeapMemoryMB(400)
                        .setManagedMemoryMB(400)
                        .setNetworkMemoryMB(400)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 4.0))
                        .build();

        assertThat(rp1.merge(rp1)).isEqualTo(rp1MergeRp1);
        assertThat(rp1.merge(rp2)).isEqualTo(rp1MergeRp2);
        assertThat(rp2.merge(rp1)).isEqualTo(rp1MergeRp2);
        assertThat(rp2.merge(rp2)).isEqualTo(rp2MergeRp2);

        assertThat(rp1.merge(ResourceProfile.UNKNOWN)).isEqualTo(ResourceProfile.UNKNOWN);
        assertThat(ResourceProfile.UNKNOWN.merge(rp1)).isEqualTo(ResourceProfile.UNKNOWN);
        assertThat(ResourceProfile.UNKNOWN.merge(ResourceProfile.UNKNOWN))
                .isEqualTo(ResourceProfile.UNKNOWN);
        assertThat(rp1.merge(ResourceProfile.ANY)).isEqualTo(ResourceProfile.ANY);
        assertThat(ResourceProfile.ANY.merge(rp1)).isEqualTo(ResourceProfile.ANY);
        assertThat(ResourceProfile.ANY.merge(ResourceProfile.ANY)).isEqualTo(ResourceProfile.ANY);
    }

    @Test
    void testMergeWithOverflow() {
        final CPUResource largeDouble = new CPUResource(Double.MAX_VALUE - 1.0);
        final MemorySize largeMemory = MemorySize.MAX_VALUE.subtract(MemorySize.parse("100m"));

        final ResourceProfile rp1 =
                ResourceProfile.newBuilder()
                        .setCpuCores(3.0)
                        .setTaskHeapMemoryMB(300)
                        .setTaskOffHeapMemoryMB(300)
                        .setManagedMemoryMB(300)
                        .setNetworkMemoryMB(300)
                        .build();
        final ResourceProfile rp2 =
                ResourceProfile.newBuilder()
                        .setCpuCores(largeDouble)
                        .setTaskHeapMemory(largeMemory)
                        .setTaskOffHeapMemory(largeMemory)
                        .setManagedMemory(largeMemory)
                        .setNetworkMemory(largeMemory)
                        .build();

        List<ArithmeticException> exceptions = new ArrayList<>();
        try {
            rp2.merge(rp2);
        } catch (ArithmeticException e) {
            exceptions.add(e);
        }
        try {
            rp2.merge(rp1);
        } catch (ArithmeticException e) {
            exceptions.add(e);
        }
        try {
            rp1.merge(rp2);
        } catch (ArithmeticException e) {
            exceptions.add(e);
        }
        assertThat(exceptions).hasSize(3);
    }

    @Test
    void testSubtract() {
        final ResourceProfile rp1 =
                ResourceProfile.newBuilder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .build();
        final ResourceProfile rp2 =
                ResourceProfile.newBuilder()
                        .setCpuCores(2.0)
                        .setTaskHeapMemoryMB(200)
                        .setTaskOffHeapMemoryMB(200)
                        .setManagedMemoryMB(200)
                        .setNetworkMemoryMB(200)
                        .build();
        final ResourceProfile rp3 =
                ResourceProfile.newBuilder()
                        .setCpuCores(3.0)
                        .setTaskHeapMemoryMB(300)
                        .setTaskOffHeapMemoryMB(300)
                        .setManagedMemoryMB(300)
                        .setNetworkMemoryMB(300)
                        .build();

        assertThat(rp3.subtract(rp2)).isEqualTo(rp1);
        assertThat(rp2.subtract(rp1)).isEqualTo(rp1);

        assertThatExceptionOfType(IllegalArgumentException.class)
                .as("The subtract should failed due to trying to subtract a larger resource")
                .isThrownBy(() -> rp1.subtract(rp2));

        assertThat(ResourceProfile.ANY.subtract(rp3)).isEqualTo(ResourceProfile.ANY);
        assertThat(ResourceProfile.ANY.subtract(ResourceProfile.ANY))
                .isEqualTo(ResourceProfile.ANY);
        assertThat(rp3.subtract(ResourceProfile.ANY)).isEqualTo(ResourceProfile.ANY);

        assertThat(ResourceProfile.UNKNOWN.subtract(rp3)).isEqualTo(ResourceProfile.UNKNOWN);
        assertThat(rp3.subtract(ResourceProfile.UNKNOWN)).isEqualTo(ResourceProfile.UNKNOWN);
        assertThat(ResourceProfile.UNKNOWN.subtract(ResourceProfile.UNKNOWN))
                .isEqualTo(ResourceProfile.UNKNOWN);
    }

    @Test
    void testSubtractWithInfValues() {
        // Does not equals to ANY since it has extended resources.
        final ResourceProfile rp1 =
                ResourceProfile.newBuilder()
                        .setCpuCores(Double.MAX_VALUE)
                        .setTaskHeapMemoryMB(Integer.MAX_VALUE)
                        .setTaskOffHeapMemoryMB(Integer.MAX_VALUE)
                        .setManagedMemoryMB(Integer.MAX_VALUE)
                        .setNetworkMemoryMB(Integer.MAX_VALUE)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 4.0))
                        .build();
        final ResourceProfile rp2 =
                ResourceProfile.newBuilder()
                        .setCpuCores(2.0)
                        .setTaskHeapMemoryMB(200)
                        .setTaskOffHeapMemoryMB(200)
                        .setManagedMemoryMB(200)
                        .setNetworkMemoryMB(200)
                        .build();

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> rp2.subtract(rp1));
    }

    @Test
    void testMultiply() {
        final int by = 3;
        final ResourceProfile rp1 =
                ResourceProfile.newBuilder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.0))
                        .build();

        ResourceProfile rp2 = rp1;
        for (int i = 1; i < by; ++i) {
            rp2 = rp2.merge(rp1);
        }

        assertThat(rp1.multiply(by)).isEqualTo(rp2);
    }

    @Test
    void testMultiplyZero() {
        final ResourceProfile rp1 =
                ResourceProfile.newBuilder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.0))
                        .build();

        assertThat(rp1.multiply(0)).isEqualTo(ResourceProfile.ZERO);
    }

    @Test
    public void testMultiplyNegative() {
        final ResourceProfile rp =
                ResourceProfile.newBuilder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.0))
                        .build();

        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> rp.multiply(-2));
    }

    @Test
    void testFromSpecWithSerializationCopy() throws Exception {
        final ResourceSpec copiedSpec =
                CommonTestUtils.createCopySerializable(ResourceSpec.UNKNOWN);
        final ResourceProfile profile = ResourceProfile.fromResourceSpec(copiedSpec);

        assertThat(profile).isEqualTo(ResourceProfile.fromResourceSpec(ResourceSpec.UNKNOWN));
    }

    @Test
    void testSingletonPropertyOfUnknown() throws Exception {
        final ResourceProfile copiedProfile =
                CommonTestUtils.createCopySerializable(ResourceProfile.UNKNOWN);

        assertThat(copiedProfile).isSameAs(ResourceProfile.UNKNOWN);
    }

    @Test
    void testSingletonPropertyOfAny() throws Exception {
        final ResourceProfile copiedProfile =
                CommonTestUtils.createCopySerializable(ResourceProfile.ANY);

        assertThat(copiedProfile).isSameAs(ResourceProfile.ANY);
    }

    @Test
    void doesNotIncludeCPUAndMemoryInToStringIfTheyAreTooLarge() {
        double tooLargeCpuCount = MAX_CPU_CORE_NUMBER_TO_LOG.doubleValue() + 1.0;
        ResourceProfile resourceProfile = createResourceProfile(tooLargeCpuCount, TOO_LARGE_MEMORY);
        assertThat(resourceProfile.toString())
                .doesNotContain("cpuCores=")
                .doesNotContain("taskHeapMemory=");
    }

    @Test
    void includesCPUAndMemoryInToStringIfTheyAreBelowThreshold() {
        ResourceProfile resourceProfile = createResourceProfile(1.0, MemorySize.ofMebiBytes(4));
        assertThat(resourceProfile.toString()).contains("cpuCores=").contains("taskHeapMemory=");
    }

    @Test
    void testZeroExtendedResourceFromConstructor() {
        final ResourceProfile resourceProfile =
                ResourceProfile.newBuilder()
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 0.0))
                        .build();
        assertThat(resourceProfile.getExtendedResources()).isEmpty();
    }

    @Test
    void testZeroExtendedResourceFromSubtract() {
        final ResourceProfile resourceProfile =
                ResourceProfile.newBuilder()
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.0))
                        .build();
        assertThat(resourceProfile.subtract(resourceProfile).getExtendedResources()).isEmpty();
    }

    private static ResourceProfile createResourceProfile(double cpu, MemorySize taskHeapMemory) {
        return ResourceProfile.newBuilder()
                .setCpuCores(cpu)
                .setTaskHeapMemory(taskHeapMemory)
                .build();
    }
}
