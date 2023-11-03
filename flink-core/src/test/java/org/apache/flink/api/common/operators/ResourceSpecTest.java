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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;

/**
 * Tests for ResourceSpec class, including its all public api: isValid, lessThanOrEqual, equals,
 * hashCode and merge.
 */
public class ResourceSpecTest {
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
    public void testLessThanOrEqualWhenUnknownWithSpecified() {
        final ResourceSpec rs1 = ResourceSpec.newBuilder(1.0, 100).build();
        assertThatThrownBy(() -> ResourceSpec.UNKNOWN.lessThanOrEqual(rs1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testLessThanOrEqualWhenSpecifiedWithUnknown() {
        final ResourceSpec rs1 = ResourceSpec.newBuilder(1.0, 100).build();
        assertThatThrownBy(() -> rs1.lessThanOrEqual(ResourceSpec.UNKNOWN))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testEquals() throws Exception {
        ResourceSpec rs1 = ResourceSpec.newBuilder(1.0, 100).build();
        ResourceSpec rs2 = ResourceSpec.newBuilder(1.0, 100).build();
        assertThat(rs1).isEqualTo(rs2);
        assertThat(rs2).isEqualTo(rs1);

        ResourceSpec rs3 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 2.2))
                        .build();
        ResourceSpec rs4 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        assertNotEquals(rs3, rs4);

        ResourceSpec rs5 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 2.2))
                        .build();
        assertThat(rs3).isEqualTo(rs5);
    }

    @Test
    void testHashCode() throws Exception {
        ResourceSpec rs1 = ResourceSpec.newBuilder(1.0, 100).build();
        ResourceSpec rs2 = ResourceSpec.newBuilder(1.0, 100).build();
        assertThat(rs1.hashCode()).isEqualTo(rs2.hashCode());

        ResourceSpec rs3 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 2.2))
                        .build();
        ResourceSpec rs4 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        assertNotEquals(rs3.hashCode(), rs4.hashCode());

        ResourceSpec rs5 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 2.2))
                        .build();
        assertThat(rs3.hashCode()).isEqualTo(rs5.hashCode());
    }

    @Test
    void testMerge() throws Exception {
        ResourceSpec rs1 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.1))
                        .build();
        ResourceSpec rs2 = ResourceSpec.newBuilder(1.0, 100).build();

        ResourceSpec rs3 = rs1.merge(rs2);
        assertThat(new CPUResource(2.0)).isEqualTo(rs3.getCpuCores());
        assertThat(200).isEqualTo(rs3.getTaskHeapMemory().getMebiBytes());
        assertThat(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.1))
                .isEqualTo(rs3.getExtendedResource(EXTERNAL_RESOURCE_NAME).get());

        ResourceSpec rs4 = rs1.merge(rs3);
        assertThat(new ExternalResource(EXTERNAL_RESOURCE_NAME, 2.2))
                .isEqualTo(rs4.getExtendedResource(EXTERNAL_RESOURCE_NAME).get());
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
    void testMergeThisUnknown() throws Exception {
        final ResourceSpec spec1 = ResourceSpec.UNKNOWN;
        final ResourceSpec spec2 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.1))
                        .build();

        final ResourceSpec merged = spec1.merge(spec2);

        assertThat(merged).isEqualTo(ResourceSpec.UNKNOWN);
    }

    @Test
    void testMergeOtherUnknown() throws Exception {
        final ResourceSpec spec1 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.1))
                        .build();
        final ResourceSpec spec2 = ResourceSpec.UNKNOWN;

        final ResourceSpec merged = spec1.merge(spec2);

        assertThat(merged).isEqualTo(ResourceSpec.UNKNOWN);
    }

    @Test
    void testMergeBothUnknown() throws Exception {
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

        assertSame(ResourceSpec.UNKNOWN, copiedSpec);
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
        assertThat(subtracted.getTaskHeapMemory().getMebiBytes()).isEqualTo(0);
        assertThat(subtracted.getExtendedResource(EXTERNAL_RESOURCE_NAME).get())
                .isEqualTo(new ExternalResource(EXTERNAL_RESOURCE_NAME, 0.6));
    }

    @Test
    public void testSubtractOtherHasLargerResources() {
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
        assertThat(0).isEqualTo(resourceSpec.getExtendedResources().size());
    }

    @Test
    void testZeroExtendedResourceFromSubtract() {
        final ResourceSpec resourceSpec =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.0))
                        .build();

        assertThat(0).isEqualTo(resourceSpec.subtract(resourceSpec).getExtendedResources().size());
    }
}
