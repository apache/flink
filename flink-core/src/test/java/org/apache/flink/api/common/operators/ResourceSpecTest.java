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
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Tests for ResourceSpec class, including its all public api: isValid, lessThanOrEqual, equals,
 * hashCode and merge.
 */
public class ResourceSpecTest extends TestLogger {
    private static final String EXTERNAL_RESOURCE_NAME = "gpu";

    @Test
    public void testLessThanOrEqualWhenBothSpecified() {
        ResourceSpec rs1 = ResourceSpec.newBuilder(1.0, 100).build();
        ResourceSpec rs2 = ResourceSpec.newBuilder(1.0, 100).build();
        assertTrue(rs1.lessThanOrEqual(rs2));
        assertTrue(rs2.lessThanOrEqual(rs1));

        ResourceSpec rs3 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.1))
                        .build();
        assertTrue(rs1.lessThanOrEqual(rs3));
        assertFalse(rs3.lessThanOrEqual(rs1));

        ResourceSpec rs4 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 2.2))
                        .build();
        assertFalse(rs4.lessThanOrEqual(rs3));
        assertTrue(rs3.lessThanOrEqual(rs4));
    }

    @Test
    public void testLessThanOrEqualWhenBothUnknown() {
        assertTrue(ResourceSpec.UNKNOWN.lessThanOrEqual(ResourceSpec.UNKNOWN));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLessThanOrEqualWhenUnknownWithSpecified() {
        final ResourceSpec rs1 = ResourceSpec.newBuilder(1.0, 100).build();
        assertTrue(ResourceSpec.UNKNOWN.lessThanOrEqual(rs1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLessThanOrEqualWhenSpecifiedWithUnknown() {
        final ResourceSpec rs1 = ResourceSpec.newBuilder(1.0, 100).build();
        assertTrue(rs1.lessThanOrEqual(ResourceSpec.UNKNOWN));
    }

    @Test
    public void testEquals() throws Exception {
        ResourceSpec rs1 = ResourceSpec.newBuilder(1.0, 100).build();
        ResourceSpec rs2 = ResourceSpec.newBuilder(1.0, 100).build();
        assertEquals(rs1, rs2);
        assertEquals(rs2, rs1);

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
        assertEquals(rs3, rs5);
    }

    @Test
    public void testHashCode() throws Exception {
        ResourceSpec rs1 = ResourceSpec.newBuilder(1.0, 100).build();
        ResourceSpec rs2 = ResourceSpec.newBuilder(1.0, 100).build();
        assertEquals(rs1.hashCode(), rs2.hashCode());

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
        assertEquals(rs3.hashCode(), rs5.hashCode());
    }

    @Test
    public void testMerge() throws Exception {
        ResourceSpec rs1 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.1))
                        .build();
        ResourceSpec rs2 = ResourceSpec.newBuilder(1.0, 100).build();

        ResourceSpec rs3 = rs1.merge(rs2);
        assertEquals(new CPUResource(2.0), rs3.getCpuCores());
        assertEquals(200, rs3.getTaskHeapMemory().getMebiBytes());
        assertEquals(
                new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.1),
                rs3.getExtendedResource(EXTERNAL_RESOURCE_NAME).get());

        ResourceSpec rs4 = rs1.merge(rs3);
        assertEquals(
                new ExternalResource(EXTERNAL_RESOURCE_NAME, 2.2),
                rs4.getExtendedResource(EXTERNAL_RESOURCE_NAME).get());
    }

    @Test
    public void testSerializable() throws Exception {
        ResourceSpec rs1 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.1))
                        .build();

        ResourceSpec rs2 = CommonTestUtils.createCopySerializable(rs1);
        assertEquals(rs1, rs2);
    }

    @Test
    public void testMergeThisUnknown() throws Exception {
        final ResourceSpec spec1 = ResourceSpec.UNKNOWN;
        final ResourceSpec spec2 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.1))
                        .build();

        final ResourceSpec merged = spec1.merge(spec2);

        assertEquals(ResourceSpec.UNKNOWN, merged);
    }

    @Test
    public void testMergeOtherUnknown() throws Exception {
        final ResourceSpec spec1 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.1))
                        .build();
        final ResourceSpec spec2 = ResourceSpec.UNKNOWN;

        final ResourceSpec merged = spec1.merge(spec2);

        assertEquals(ResourceSpec.UNKNOWN, merged);
    }

    @Test
    public void testMergeBothUnknown() throws Exception {
        final ResourceSpec spec1 = ResourceSpec.UNKNOWN;
        final ResourceSpec spec2 = ResourceSpec.UNKNOWN;

        final ResourceSpec merged = spec1.merge(spec2);

        assertEquals(ResourceSpec.UNKNOWN, merged);
    }

    @Test
    public void testMergeWithSerializationCopy() throws Exception {
        final ResourceSpec spec1 = CommonTestUtils.createCopySerializable(ResourceSpec.UNKNOWN);
        final ResourceSpec spec2 = CommonTestUtils.createCopySerializable(ResourceSpec.UNKNOWN);

        final ResourceSpec merged = spec1.merge(spec2);

        assertEquals(ResourceSpec.UNKNOWN, merged);
    }

    @Test
    public void testSingletonPropertyOfUnknown() throws Exception {
        final ResourceSpec copiedSpec =
                CommonTestUtils.createCopySerializable(ResourceSpec.UNKNOWN);

        assertSame(ResourceSpec.UNKNOWN, copiedSpec);
    }

    @Test
    public void testSubtract() {
        final ResourceSpec rs1 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.1))
                        .build();
        final ResourceSpec rs2 =
                ResourceSpec.newBuilder(0.2, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 0.5))
                        .build();

        final ResourceSpec subtracted = rs1.subtract(rs2);
        assertEquals(new CPUResource(0.8), subtracted.getCpuCores());
        assertEquals(0, subtracted.getTaskHeapMemory().getMebiBytes());
        assertEquals(
                new ExternalResource(EXTERNAL_RESOURCE_NAME, 0.6),
                subtracted.getExtendedResource(EXTERNAL_RESOURCE_NAME).get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSubtractOtherHasLargerResources() {
        final ResourceSpec rs1 = ResourceSpec.newBuilder(1.0, 100).build();
        final ResourceSpec rs2 = ResourceSpec.newBuilder(0.2, 200).build();

        rs1.subtract(rs2);
    }

    @Test
    public void testSubtractThisUnknown() {
        final ResourceSpec rs1 = ResourceSpec.UNKNOWN;
        final ResourceSpec rs2 =
                ResourceSpec.newBuilder(0.2, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 0.5))
                        .build();

        final ResourceSpec subtracted = rs1.subtract(rs2);
        assertEquals(ResourceSpec.UNKNOWN, subtracted);
    }

    @Test
    public void testSubtractOtherUnknown() {
        final ResourceSpec rs1 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.1))
                        .build();
        final ResourceSpec rs2 = ResourceSpec.UNKNOWN;

        final ResourceSpec subtracted = rs1.subtract(rs2);
        assertEquals(ResourceSpec.UNKNOWN, subtracted);
    }

    @Test
    public void testZeroExtendedResourceFromConstructor() {
        final ResourceSpec resourceSpec =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 0))
                        .build();
        assertEquals(resourceSpec.getExtendedResources().size(), 0);
    }

    @Test
    public void testZeroExtendedResourceFromSubtract() {
        final ResourceSpec resourceSpec =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.0))
                        .build();

        assertEquals(resourceSpec.subtract(resourceSpec).getExtendedResources().size(), 0);
    }
}
