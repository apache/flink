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

package org.apache.flink.api.common.operators;

import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.api.common.resources.ExternalResource;
import org.apache.flink.core.testutils.CommonTestUtils;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for ResourceSpec class, including its all public api: isValid, lessThanOrEqual, equals,
 * hashCode and merge.
 */
class ResourceSpecTest {
    private static final String EXTERNAL_RESOURCE_NAME = "gpu";

    @Test
    void testLessThanOrEqualWhenBothSpecified() {
        ResourceSpec rs1 = ResourceSpec.newBuilder(1.0, 100).build();
        ResourceSpec rs2 = ResourceSpec.newBuilder(1.0, 100).build();
        assertThat(rs1.lessThanOrEqual(rs2)).isTrue();
        assertThat(rs2.lessThanOrEqual(rs1)).isTrue();

        ResourceSpec rs3 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.1))
                        .build();
        assertThat(rs1.lessThanOrEqual(rs3)).isTrue();
        assertThat(rs3.lessThanOrEqual(rs1)).isFalse();

        ResourceSpec rs4 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 2.2))
                        .build();
        assertThat(rs4.lessThanOrEqual(rs3)).isFalse();
        assertThat(rs3.lessThanOrEqual(rs4)).isTrue();
    }

    @Test
    void testLessThanOrEqualWhenBothUnknown() {
        assertThat(ResourceSpec.UNKNOWN.lessThanOrEqual(ResourceSpec.UNKNOWN)).isTrue();
    }

    @Test
    void testLessThanOrEqualWhenUnknownWithSpecified() {
        assertThatThrownBy(
                        () ->
                                ResourceSpec.UNKNOWN.lessThanOrEqual(
                                        ResourceSpec.newBuilder(1.0, 100).build()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testLessThanOrEqualWhenSpecifiedWithUnknown() {
        // final ResourceSpec rs1 = ResourceSpec.newBuilder(1.0, 100).build();
        // assertThat(rs1.lessThanOrEqual(ResourceSpec.UNKNOWN)).isTrue();

        assertThatThrownBy(
                        () ->
                                ResourceSpec.newBuilder(1.0, 100)
                                        .build()
                                        .lessThanOrEqual(ResourceSpec.UNKNOWN))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testEquals() {
        ResourceSpec rs1 = ResourceSpec.newBuilder(1.0, 100).build();
        ResourceSpec rs2 = ResourceSpec.newBuilder(1.0, 100).build();
        assertThat(rs2).isEqualTo(rs1);
        assertThat(rs1).isEqualTo(rs2);

        ResourceSpec rs3 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 2.2))
                        .build();
        ResourceSpec rs4 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        assertThat(rs4).isNotEqualTo(rs3);

        ResourceSpec rs5 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 2.2))
                        .build();
        assertThat(rs5).isEqualTo(rs3);
    }

    @Test
    void testHashCode() {
        ResourceSpec rs1 = ResourceSpec.newBuilder(1.0, 100).build();
        ResourceSpec rs2 = ResourceSpec.newBuilder(1.0, 100).build();
        assertThat(rs2).hasSameHashCodeAs(rs1);

        ResourceSpec rs3 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 2.2))
                        .build();
        ResourceSpec rs4 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        assertThat(rs4.hashCode()).isNotEqualTo(rs3.hashCode());

        ResourceSpec rs5 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 2.2))
                        .build();
        assertThat(rs5).hasSameHashCodeAs(rs3);
    }

    @Test
    void testMerge() {
        ResourceSpec rs1 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.1))
                        .build();
        ResourceSpec rs2 = ResourceSpec.newBuilder(1.0, 100).build();

        ResourceSpec rs3 = rs1.merge(rs2);
        assertThat(rs3.getCpuCores()).isEqualTo(new CPUResource(2.0));
        assertThat(rs3.getTaskHeapMemory().getMebiBytes()).isEqualTo(200);
        assertThat(rs3.getExtendedResource(EXTERNAL_RESOURCE_NAME).get())
                .isEqualTo(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.1));

        ResourceSpec rs4 = rs1.merge(rs3);
        assertThat(rs4.getExtendedResource(EXTERNAL_RESOURCE_NAME).get())
                .isEqualTo(new ExternalResource(EXTERNAL_RESOURCE_NAME, 2.2));
    }

    @Test
    void testSerializable() throws Exception {
        ResourceSpec rs1 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.1))
                        .build();

        ResourceSpec rs2 = CommonTestUtils.createCopySerializable(rs1);
        assertThat(rs2).isEqualTo(rs1);
    }

    @Test
    void testMergeThisUnknown() {
        final ResourceSpec spec1 = ResourceSpec.UNKNOWN;
        final ResourceSpec spec2 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.1))
                        .build();

        final ResourceSpec merged = spec1.merge(spec2);

        assertThat(merged).isEqualTo(ResourceSpec.UNKNOWN);
    }

    @Test
    void testMergeOtherUnknown() {
        final ResourceSpec spec1 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.1))
                        .build();
        final ResourceSpec spec2 = ResourceSpec.UNKNOWN;

        final ResourceSpec merged = spec1.merge(spec2);

        assertThat(merged).isEqualTo(ResourceSpec.UNKNOWN);
    }

    @Test
    void testMergeBothUnknown() {
        final ResourceSpec spec1 = ResourceSpec.UNKNOWN;
        final ResourceSpec spec2 = ResourceSpec.UNKNOWN;

        final ResourceSpec merged = spec1.merge(spec2);

        assertThat(merged).isEqualTo(ResourceSpec.UNKNOWN);
    }

    @Test
    void testMergeWithSerializationCopy() throws Exception {
        final ResourceSpec spec1 = CommonTestUtils.createCopySerializable(ResourceSpec.UNKNOWN);
        final ResourceSpec spec2 = CommonTestUtils.createCopySerializable(ResourceSpec.UNKNOWN);

        final ResourceSpec merged = spec1.merge(spec2);

        assertThat(merged).isEqualTo(ResourceSpec.UNKNOWN);
    }

    @Test
    void testSingletonPropertyOfUnknown() throws Exception {
        final ResourceSpec copiedSpec =
                CommonTestUtils.createCopySerializable(ResourceSpec.UNKNOWN);

        assertThat(copiedSpec).isSameAs(ResourceSpec.UNKNOWN);
    }

    @Test
    void testSubtract() {
        final ResourceSpec rs1 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.1))
                        .build();
        final ResourceSpec rs2 =
                ResourceSpec.newBuilder(0.2, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 0.5))
                        .build();

        final ResourceSpec subtracted = rs1.subtract(rs2);
        assertThat(subtracted.getCpuCores()).isEqualTo(new CPUResource(0.8));
        assertThat(subtracted.getTaskHeapMemory().getMebiBytes()).isZero();
        assertThat(subtracted.getExtendedResource(EXTERNAL_RESOURCE_NAME).get())
                .isEqualTo(new ExternalResource(EXTERNAL_RESOURCE_NAME, 0.6));
    }

    @Test
    void testSubtractOtherHasLargerResources() {
        final ResourceSpec rs1 = ResourceSpec.newBuilder(1.0, 100).build();
        final ResourceSpec rs2 = ResourceSpec.newBuilder(0.2, 200).build();

        assertThatThrownBy(() -> rs1.subtract(rs2)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testSubtractThisUnknown() {
        final ResourceSpec rs1 = ResourceSpec.UNKNOWN;
        final ResourceSpec rs2 =
                ResourceSpec.newBuilder(0.2, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 0.5))
                        .build();

        final ResourceSpec subtracted = rs1.subtract(rs2);
        assertThat(subtracted).isEqualTo(ResourceSpec.UNKNOWN);
    }

    @Test
    void testSubtractOtherUnknown() {
        final ResourceSpec rs1 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.1))
                        .build();
        final ResourceSpec rs2 = ResourceSpec.UNKNOWN;

        final ResourceSpec subtracted = rs1.subtract(rs2);
        assertThat(subtracted).isEqualTo(ResourceSpec.UNKNOWN);
    }

    @Test
    void testZeroExtendedResourceFromConstructor() {
        final ResourceSpec resourceSpec =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 0))
                        .build();
        assertThat(resourceSpec.getExtendedResources()).isEmpty();
    }

    @Test
    void testZeroExtendedResourceFromSubtract() {
        final ResourceSpec resourceSpec =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.0))
                        .build();

        assertThat(resourceSpec.subtract(resourceSpec).getExtendedResources()).isEmpty();
    }
}
