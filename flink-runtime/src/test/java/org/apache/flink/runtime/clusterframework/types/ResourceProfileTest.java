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
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matcher;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.runtime.clusterframework.types.ResourceProfile.MAX_CPU_CORE_NUMBER_TO_LOG;
import static org.apache.flink.runtime.clusterframework.types.ResourceProfile.MAX_MEMORY_SIZE_TO_LOG;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for the {@link ResourceProfile}. */
public class ResourceProfileTest extends TestLogger {
    private static final MemorySize TOO_LARGE_MEMORY =
            MAX_MEMORY_SIZE_TO_LOG.add(MemorySize.ofMebiBytes(10));
    private static final String EXTERNAL_RESOURCE_NAME = "gpu";

    @Test
    public void testAllFieldsNoLessThanProfile() {
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

        assertFalse(rp1.allFieldsNoLessThan(rp2));
        assertTrue(rp2.allFieldsNoLessThan(rp1));

        assertFalse(rp1.allFieldsNoLessThan(rp3));
        assertTrue(rp3.allFieldsNoLessThan(rp1));

        assertFalse(rp2.allFieldsNoLessThan(rp3));
        assertFalse(rp3.allFieldsNoLessThan(rp2));

        assertTrue(rp4.allFieldsNoLessThan(rp1));
        assertTrue(rp4.allFieldsNoLessThan(rp2));
        assertTrue(rp4.allFieldsNoLessThan(rp3));
        assertTrue(rp4.allFieldsNoLessThan(rp4));

        final ResourceProfile rp5 =
                ResourceProfile.newBuilder()
                        .setCpuCores(2.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .build();
        assertFalse(rp4.allFieldsNoLessThan(rp5));

        ResourceSpec rs1 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 2.2))
                        .build();
        ResourceSpec rs2 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.1))
                        .build();

        assertFalse(rp1.allFieldsNoLessThan(ResourceProfile.fromResourceSpec(rs1)));
        assertTrue(
                ResourceProfile.fromResourceSpec(rs1)
                        .allFieldsNoLessThan(ResourceProfile.fromResourceSpec(rs2)));
        assertFalse(
                ResourceProfile.fromResourceSpec(rs2)
                        .allFieldsNoLessThan(ResourceProfile.fromResourceSpec(rs1)));
    }

    @Test
    public void testUnknownNoLessThanUnknown() {
        assertTrue(ResourceProfile.UNKNOWN.allFieldsNoLessThan(ResourceProfile.UNKNOWN));
    }

    @Test
    public void testMatchRequirement() {
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

        assertTrue(resource1.isMatching(requirement1));
        assertTrue(resource1.isMatching(requirement2));
        assertFalse(resource1.isMatching(requirement3));

        assertTrue(resource2.isMatching(requirement1));
        assertFalse(resource2.isMatching(requirement2));
        assertTrue(resource2.isMatching(requirement3));
    }

    @Test
    public void testEquals() {
        ResourceSpec rs1 = ResourceSpec.newBuilder(1.0, 100).build();
        ResourceSpec rs2 = ResourceSpec.newBuilder(1.0, 100).build();
        assertEquals(ResourceProfile.fromResourceSpec(rs1), ResourceProfile.fromResourceSpec(rs2));

        ResourceSpec rs3 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 2.2))
                        .build();
        ResourceSpec rs4 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.1))
                        .build();
        assertNotEquals(
                ResourceProfile.fromResourceSpec(rs3), ResourceProfile.fromResourceSpec(rs4));

        ResourceSpec rs5 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 2.2))
                        .build();
        MemorySize networkMemory = MemorySize.ofMebiBytes(100);
        assertEquals(
                ResourceProfile.fromResourceSpec(rs3, networkMemory),
                ResourceProfile.fromResourceSpec(rs5, networkMemory));

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

        assertNotEquals(rp1, rp2);
        assertNotEquals(rp1, rp3);
        assertNotEquals(rp1, rp4);
        assertNotEquals(rp1, rp5);
        assertNotEquals(rp1, rp6);
        assertNotEquals(rp1, rp7);
        assertEquals(rp1, rp8);
    }

    @Test
    public void testGet() {
        ResourceSpec rs =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.6))
                        .build();
        ResourceProfile rp = ResourceProfile.fromResourceSpec(rs, MemorySize.ofMebiBytes(50));

        assertEquals(new CPUResource(1.0), rp.getCpuCores());
        assertEquals(150, rp.getTotalMemory().getMebiBytes());
        assertEquals(100, rp.getOperatorsMemory().getMebiBytes());
        assertEquals(
                new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.6),
                rp.getExtendedResources().get(EXTERNAL_RESOURCE_NAME));
    }

    @Test
    public void testMerge() {
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

        assertEquals(rp1MergeRp1, rp1.merge(rp1));
        assertEquals(rp1MergeRp2, rp1.merge(rp2));
        assertEquals(rp1MergeRp2, rp2.merge(rp1));
        assertEquals(rp2MergeRp2, rp2.merge(rp2));

        assertEquals(ResourceProfile.UNKNOWN, rp1.merge(ResourceProfile.UNKNOWN));
        assertEquals(ResourceProfile.UNKNOWN, ResourceProfile.UNKNOWN.merge(rp1));
        assertEquals(
                ResourceProfile.UNKNOWN, ResourceProfile.UNKNOWN.merge(ResourceProfile.UNKNOWN));
        assertEquals(ResourceProfile.ANY, rp1.merge(ResourceProfile.ANY));
        assertEquals(ResourceProfile.ANY, ResourceProfile.ANY.merge(rp1));
        assertEquals(ResourceProfile.ANY, ResourceProfile.ANY.merge(ResourceProfile.ANY));
    }

    @Test
    public void testMergeWithOverflow() {
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
        assertEquals(3, exceptions.size());
    }

    @Test
    public void testSubtract() {
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

        assertEquals(rp1, rp3.subtract(rp2));
        assertEquals(rp1, rp2.subtract(rp1));

        try {
            rp1.subtract(rp2);
            fail("The subtract should failed due to trying to subtract a larger resource");
        } catch (IllegalArgumentException ex) {
            // Ignore ex.
        }

        assertEquals(ResourceProfile.ANY, ResourceProfile.ANY.subtract(rp3));
        assertEquals(ResourceProfile.ANY, ResourceProfile.ANY.subtract(ResourceProfile.ANY));
        assertEquals(ResourceProfile.ANY, rp3.subtract(ResourceProfile.ANY));

        assertEquals(ResourceProfile.UNKNOWN, ResourceProfile.UNKNOWN.subtract(rp3));
        assertEquals(ResourceProfile.UNKNOWN, rp3.subtract(ResourceProfile.UNKNOWN));
        assertEquals(
                ResourceProfile.UNKNOWN, ResourceProfile.UNKNOWN.subtract(ResourceProfile.UNKNOWN));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSubtractWithInfValues() {
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

        rp2.subtract(rp1);
    }

    @Test
    public void testMultiply() {
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

        assertEquals(rp2, rp1.multiply(by));
    }

    @Test
    public void testMultiplyZero() {
        final ResourceProfile rp1 =
                ResourceProfile.newBuilder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.0))
                        .build();

        assertEquals(ResourceProfile.ZERO, rp1.multiply(0));
    }

    @Test(expected = IllegalArgumentException.class)
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
        rp.multiply(-2);
    }

    @Test
    public void testFromSpecWithSerializationCopy() throws Exception {
        final ResourceSpec copiedSpec =
                CommonTestUtils.createCopySerializable(ResourceSpec.UNKNOWN);
        final ResourceProfile profile = ResourceProfile.fromResourceSpec(copiedSpec);

        assertEquals(ResourceProfile.fromResourceSpec(ResourceSpec.UNKNOWN), profile);
    }

    @Test
    public void testSingletonPropertyOfUnknown() throws Exception {
        final ResourceProfile copiedProfile =
                CommonTestUtils.createCopySerializable(ResourceProfile.UNKNOWN);

        assertSame(ResourceProfile.UNKNOWN, copiedProfile);
    }

    @Test
    public void testSingletonPropertyOfAny() throws Exception {
        final ResourceProfile copiedProfile =
                CommonTestUtils.createCopySerializable(ResourceProfile.ANY);

        assertSame(ResourceProfile.ANY, copiedProfile);
    }

    @Test
    public void doesNotIncludeCPUAndMemoryInToStringIfTheyAreTooLarge() {
        double tooLargeCpuCount = MAX_CPU_CORE_NUMBER_TO_LOG.doubleValue() + 1.0;
        ResourceProfile resourceProfile = createResourceProfile(tooLargeCpuCount, TOO_LARGE_MEMORY);
        assertThat(
                resourceProfile.toString(),
                allOf(not(containsCPUCores()), not(containsTaskHeapMemory())));
    }

    @Test
    public void includesCPUAndMemoryInToStringIfTheyAreBelowThreshold() {
        ResourceProfile resourceProfile = createResourceProfile(1.0, MemorySize.ofMebiBytes(4));
        assertThat(resourceProfile.toString(), allOf(containsCPUCores(), containsTaskHeapMemory()));
    }

    @Test
    public void testZeroExtendedResourceFromConstructor() {
        final ResourceProfile resourceProfile =
                ResourceProfile.newBuilder()
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 0.0))
                        .build();
        assertEquals(resourceProfile.getExtendedResources().size(), 0);
    }

    @Test
    public void testZeroExtendedResourceFromSubtract() {
        final ResourceProfile resourceProfile =
                ResourceProfile.newBuilder()
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.0))
                        .build();
        assertEquals(resourceProfile.subtract(resourceProfile).getExtendedResources().size(), 0);
    }

    private Matcher<String> containsTaskHeapMemory() {
        return containsString("taskHeapMemory=");
    }

    private Matcher<String> containsCPUCores() {
        return containsString("cpuCores=");
    }

    private static ResourceProfile createResourceProfile(double cpu, MemorySize taskHeapMemory) {
        return ResourceProfile.newBuilder()
                .setCpuCores(cpu)
                .setTaskHeapMemory(taskHeapMemory)
                .build();
    }
}
