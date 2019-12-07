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
import org.apache.flink.api.common.resources.GPUResource;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.testutils.CommonTestUtils;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link ResourceProfile}.
 */
public class ResourceProfileTest {

	@Test
	public void testMatchRequirement() {
		final ResourceProfile rp1 = ResourceProfile.newBuilder()
			.setCpuCores(1.0)
			.setTaskHeapMemoryMB(100)
			.setTaskOffHeapMemoryMB(100)
			.setManagedMemoryMB(100)
			.build();
		final ResourceProfile rp2 = ResourceProfile.newBuilder()
			.setCpuCores(1.0)
			.setTaskHeapMemoryMB(200)
			.setTaskOffHeapMemoryMB(200)
			.setManagedMemoryMB(200)
			.build();
		final ResourceProfile rp3 = ResourceProfile.newBuilder()
			.setCpuCores(2.0)
			.setTaskHeapMemoryMB(100)
			.setTaskOffHeapMemoryMB(100)
			.setManagedMemoryMB(100)
			.build();
		final ResourceProfile rp4 = ResourceProfile.newBuilder()
			.setCpuCores(2.0)
			.setTaskHeapMemoryMB(200)
			.setTaskOffHeapMemoryMB(200)
			.setManagedMemoryMB(200)
			.build();

		assertFalse(rp1.isMatching(rp2));
		assertTrue(rp2.isMatching(rp1));

		assertFalse(rp1.isMatching(rp3));
		assertTrue(rp3.isMatching(rp1));

		assertFalse(rp2.isMatching(rp3));
		assertFalse(rp3.isMatching(rp2));

		assertTrue(rp4.isMatching(rp1));
		assertTrue(rp4.isMatching(rp2));
		assertTrue(rp4.isMatching(rp3));
		assertTrue(rp4.isMatching(rp4));

		final ResourceProfile rp5 = ResourceProfile.newBuilder()
			.setCpuCores(2.0)
			.setTaskHeapMemoryMB(100)
			.setTaskOffHeapMemoryMB(100)
			.setManagedMemoryMB(100)
			.setShuffleMemoryMB(100)
			.build();
		assertFalse(rp4.isMatching(rp5));

		ResourceSpec rs1 = ResourceSpec.newBuilder(1.0, 100).
				setGPUResource(2.2).
				build();
		ResourceSpec rs2 = ResourceSpec.newBuilder(1.0, 100).
				setGPUResource(1.1).
				build();

		assertFalse(rp1.isMatching(ResourceProfile.fromResourceSpec(rs1)));
		assertTrue(ResourceProfile.fromResourceSpec(rs1).isMatching(ResourceProfile.fromResourceSpec(rs2)));
		assertFalse(ResourceProfile.fromResourceSpec(rs2).isMatching(ResourceProfile.fromResourceSpec(rs1)));
	}

	@Test
	public void testUnknownMatchesUnknown() {
		assertTrue(ResourceProfile.UNKNOWN.isMatching(ResourceProfile.UNKNOWN));
	}

	@Test
	public void testEquals() {
		ResourceSpec rs1 = ResourceSpec.newBuilder(1.0, 100).build();
		ResourceSpec rs2 = ResourceSpec.newBuilder(1.0, 100).build();
		assertEquals(ResourceProfile.fromResourceSpec(rs1), ResourceProfile.fromResourceSpec(rs2));

		ResourceSpec rs3 = ResourceSpec.newBuilder(1.0, 100).
				setGPUResource(2.2).
				build();
		ResourceSpec rs4 = ResourceSpec.newBuilder(1.0, 100).
				setGPUResource(1.1).
				build();
		assertNotEquals(ResourceProfile.fromResourceSpec(rs3), ResourceProfile.fromResourceSpec(rs4));

		ResourceSpec rs5 = ResourceSpec.newBuilder(1.0, 100).
				setGPUResource(2.2).
				build();
		MemorySize networkMemory = MemorySize.parse(100 + "m");
		assertEquals(ResourceProfile.fromResourceSpec(rs3, networkMemory), ResourceProfile.fromResourceSpec(rs5, networkMemory));

		final ResourceProfile rp1 = ResourceProfile.newBuilder()
			.setCpuCores(1.0)
			.setTaskHeapMemoryMB(100)
			.setTaskOffHeapMemoryMB(100)
			.setManagedMemoryMB(100)
			.setShuffleMemoryMB(100)
			.build();
		final ResourceProfile rp2 = ResourceProfile.newBuilder()
			.setCpuCores(1.1)
			.setTaskHeapMemoryMB(100)
			.setTaskOffHeapMemoryMB(100)
			.setManagedMemoryMB(100)
			.setShuffleMemoryMB(100)
			.build();
		final ResourceProfile rp3 = ResourceProfile.newBuilder()
			.setCpuCores(1.0)
			.setTaskHeapMemoryMB(110)
			.setTaskOffHeapMemoryMB(100)
			.setManagedMemoryMB(100)
			.setShuffleMemoryMB(100)
			.build();
		final ResourceProfile rp4 = ResourceProfile.newBuilder()
			.setCpuCores(1.0)
			.setTaskHeapMemoryMB(100)
			.setTaskOffHeapMemoryMB(110)
			.setManagedMemoryMB(100)
			.setShuffleMemoryMB(100)
			.build();
		final ResourceProfile rp5 = ResourceProfile.newBuilder()
			.setCpuCores(1.0)
			.setTaskHeapMemoryMB(100)
			.setTaskOffHeapMemoryMB(100)
			.setManagedMemoryMB(110)
			.setShuffleMemoryMB(100)
			.build();
		final ResourceProfile rp6 = ResourceProfile.newBuilder()
			.setCpuCores(1.0)
			.setTaskHeapMemoryMB(100)
			.setTaskOffHeapMemoryMB(100)
			.setManagedMemoryMB(110)
			.setShuffleMemoryMB(100)
			.build();
		final ResourceProfile rp7 = ResourceProfile.newBuilder()
			.setCpuCores(1.0)
			.setTaskHeapMemoryMB(100)
			.setTaskOffHeapMemoryMB(100)
			.setManagedMemoryMB(100)
			.setShuffleMemoryMB(110)
			.build();
		final ResourceProfile rp8 = ResourceProfile.newBuilder()
			.setCpuCores(1.0)
			.setTaskHeapMemoryMB(100)
			.setTaskOffHeapMemoryMB(100)
			.setManagedMemoryMB(100)
			.setShuffleMemoryMB(100)
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
		ResourceSpec rs = ResourceSpec.newBuilder(1.0, 100).
				setGPUResource(1.6).
				build();
		ResourceProfile rp = ResourceProfile.fromResourceSpec(rs, MemorySize.parse(50 + "m"));

		assertEquals(new CPUResource(1.0), rp.getCpuCores());
		assertEquals(150, rp.getTotalMemory().getMebiBytes());
		assertEquals(100, rp.getOperatorsMemory().getMebiBytes());
		assertEquals(new GPUResource(1.6), rp.getExtendedResources().get(GPUResource.NAME));
	}

	@Test
	public void testMerge() {
		final ResourceProfile rp1 = ResourceProfile.newBuilder()
			.setCpuCores(1.0)
			.setTaskHeapMemoryMB(100)
			.setTaskOffHeapMemoryMB(100)
			.setManagedMemoryMB(100)
			.setShuffleMemoryMB(100)
			.build();
		final ResourceProfile rp2 = ResourceProfile.newBuilder()
			.setCpuCores(2.0)
			.setTaskHeapMemoryMB(200)
			.setTaskOffHeapMemoryMB(200)
			.setManagedMemoryMB(200)
			.setShuffleMemoryMB(200)
			.addExtendedResource("gpu", new GPUResource(2.0))
			.build();

		final ResourceProfile rp1MergeRp1 = ResourceProfile.newBuilder()
			.setCpuCores(2.0)
			.setTaskHeapMemoryMB(200)
			.setTaskOffHeapMemoryMB(200)
			.setManagedMemoryMB(200)
			.setShuffleMemoryMB(200)
			.build();
		final ResourceProfile rp1MergeRp2 = ResourceProfile.newBuilder()
			.setCpuCores(3.0)
			.setTaskHeapMemoryMB(300)
			.setTaskOffHeapMemoryMB(300)
			.setManagedMemoryMB(300)
			.setShuffleMemoryMB(300)
			.addExtendedResource("gpu", new GPUResource(2.0))
			.build();
		final ResourceProfile rp2MergeRp2 = ResourceProfile.newBuilder()
			.setCpuCores(4.0)
			.setTaskHeapMemoryMB(400)
			.setTaskOffHeapMemoryMB(400)
			.setManagedMemoryMB(400)
			.setShuffleMemoryMB(400)
			.addExtendedResource("gpu", new GPUResource(4.0))
			.build();

		assertEquals(rp1MergeRp1, rp1.merge(rp1));
		assertEquals(rp1MergeRp2, rp1.merge(rp2));
		assertEquals(rp1MergeRp2, rp2.merge(rp1));
		assertEquals(rp2MergeRp2, rp2.merge(rp2));

		assertEquals(ResourceProfile.UNKNOWN, rp1.merge(ResourceProfile.UNKNOWN));
		assertEquals(ResourceProfile.UNKNOWN, ResourceProfile.UNKNOWN.merge(rp1));
		assertEquals(ResourceProfile.UNKNOWN, ResourceProfile.UNKNOWN.merge(ResourceProfile.UNKNOWN));
		assertEquals(ResourceProfile.ANY, rp1.merge(ResourceProfile.ANY));
		assertEquals(ResourceProfile.ANY, ResourceProfile.ANY.merge(rp1));
		assertEquals(ResourceProfile.ANY, ResourceProfile.ANY.merge(ResourceProfile.ANY));
	}

	@Test
	public void testMergeWithOverflow() {
		final CPUResource largeDouble = new CPUResource(Double.MAX_VALUE - 1.0);
		final MemorySize largeMemory = MemorySize.MAX_VALUE.subtract(MemorySize.parse("100m"));

		final ResourceProfile rp1 = ResourceProfile.newBuilder()
			.setCpuCores(3.0)
			.setTaskHeapMemoryMB(300)
			.setTaskOffHeapMemoryMB(300)
			.setManagedMemoryMB(300)
			.setShuffleMemoryMB(300)
			.build();
		final ResourceProfile rp2 = ResourceProfile.newBuilder()
			.setCpuCores(largeDouble)
			.setTaskHeapMemory(largeMemory)
			.setTaskOffHeapMemory(largeMemory)
			.setManagedMemory(largeMemory)
			.setShuffleMemory(largeMemory)
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
		final ResourceProfile rp1 = ResourceProfile.newBuilder()
			.setCpuCores(1.0)
			.setTaskHeapMemoryMB(100)
			.setTaskOffHeapMemoryMB(100)
			.setManagedMemoryMB(100)
			.setShuffleMemoryMB(100)
			.build();
		final ResourceProfile rp2 = ResourceProfile.newBuilder()
			.setCpuCores(2.0)
			.setTaskHeapMemoryMB(200)
			.setTaskOffHeapMemoryMB(200)
			.setManagedMemoryMB(200)
			.setShuffleMemoryMB(200)
			.build();
		final ResourceProfile rp3 = ResourceProfile.newBuilder()
			.setCpuCores(3.0)
			.setTaskHeapMemoryMB(300)
			.setTaskOffHeapMemoryMB(300)
			.setManagedMemoryMB(300)
			.setShuffleMemoryMB(300)
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
		assertEquals(ResourceProfile.UNKNOWN, ResourceProfile.UNKNOWN.subtract(ResourceProfile.UNKNOWN));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSubtractWithInfValues() {
		// Does not equals to ANY since it has extended resources.
		final ResourceProfile rp1 = ResourceProfile.newBuilder()
			.setCpuCores(Double.MAX_VALUE)
			.setTaskHeapMemoryMB(Integer.MAX_VALUE)
			.setTaskOffHeapMemoryMB(Integer.MAX_VALUE)
			.setManagedMemoryMB(Integer.MAX_VALUE)
			.setShuffleMemoryMB(Integer.MAX_VALUE)
			.addExtendedResource("gpu", new GPUResource(4.0))
			.build();
		final ResourceProfile rp2 = ResourceProfile.newBuilder()
			.setCpuCores(2.0)
			.setTaskHeapMemoryMB(200)
			.setTaskOffHeapMemoryMB(200)
			.setManagedMemoryMB(200)
			.setShuffleMemoryMB(200)
			.build();

		rp2.subtract(rp1);
	}

	@Test
	public void testFromSpecWithSerializationCopy() throws Exception {
		final ResourceSpec copiedSpec = CommonTestUtils.createCopySerializable(ResourceSpec.UNKNOWN);
		final ResourceProfile profile = ResourceProfile.fromResourceSpec(copiedSpec);

		assertEquals(ResourceProfile.fromResourceSpec(ResourceSpec.UNKNOWN), profile);
	}

	@Test
	public void testSingletonPropertyOfUnknown() throws Exception {
		final ResourceProfile copiedProfile = CommonTestUtils.createCopySerializable(ResourceProfile.UNKNOWN);

		assertSame(ResourceProfile.UNKNOWN, copiedProfile);
	}

	@Test
	public void testSingletonPropertyOfAny() throws Exception {
		final ResourceProfile copiedProfile = CommonTestUtils.createCopySerializable(ResourceProfile.ANY);

		assertSame(ResourceProfile.ANY, copiedProfile);
	}
}
