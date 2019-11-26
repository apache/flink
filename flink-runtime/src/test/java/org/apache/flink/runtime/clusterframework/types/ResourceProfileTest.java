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
import java.util.Collections;
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
		ResourceProfile rp1 = new ResourceProfile(1.0, 100, 100, 100, 0, 0, Collections.emptyMap());
		ResourceProfile rp2 = new ResourceProfile(1.0, 200, 200, 200, 0, 0, Collections.emptyMap());
		ResourceProfile rp3 = new ResourceProfile(2.0, 100, 100, 100, 0, 0, Collections.emptyMap());
		ResourceProfile rp4 = new ResourceProfile(2.0, 200, 200, 200, 0, 0, Collections.emptyMap());

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

		ResourceProfile rp5 = new ResourceProfile(2.0, 100, 100, 100, 100, 100, null);
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

		ResourceProfile rp1 = new ResourceProfile(1.0, 100, 100, 100, 100, 100, Collections.emptyMap());
		ResourceProfile rp2 = new ResourceProfile(1.1, 100, 100, 100, 100, 100, Collections.emptyMap());
		ResourceProfile rp3 = new ResourceProfile(1.0, 110, 100, 100, 100, 100, Collections.emptyMap());
		ResourceProfile rp4 = new ResourceProfile(1.0, 100, 110, 100, 100, 100, Collections.emptyMap());
		ResourceProfile rp5 = new ResourceProfile(1.0, 100, 100, 110, 100, 100, Collections.emptyMap());
		ResourceProfile rp6 = new ResourceProfile(1.0, 100, 100, 100, 110, 100, Collections.emptyMap());
		ResourceProfile rp7 = new ResourceProfile(1.0, 100, 100, 100, 100, 110, Collections.emptyMap());
		ResourceProfile rp8 = new ResourceProfile(1.0, 100, 100, 100, 100, 100, Collections.emptyMap());

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
		ResourceProfile rp1 = new ResourceProfile(1.0, 100, 100, 100, 100, 100, Collections.emptyMap());
		ResourceProfile rp2 = new ResourceProfile(2.0, 200, 200, 200, 200, 200,
			Collections.singletonMap("gpu", new GPUResource(2.0)));

		ResourceProfile rp1MergeRp1 = new ResourceProfile(2.0, 200, 200, 200, 200, 200,
			Collections.emptyMap());
		ResourceProfile rp1MergeRp2 = new ResourceProfile(3.0, 300, 300, 300, 300, 300,
			Collections.singletonMap("gpu", new GPUResource(2.0)));
		ResourceProfile rp2MergeRp2 = new ResourceProfile(4.0, 400, 400, 400, 400, 400,
			Collections.singletonMap("gpu", new GPUResource(4.0)));

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

		ResourceProfile rp1 = new ResourceProfile(3.0, 300, 300, 300, 300, 300, Collections.emptyMap());
		ResourceProfile rp2 = new ResourceProfile(largeDouble, largeMemory, largeMemory, largeMemory, largeMemory, largeMemory, Collections.emptyMap());

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
		ResourceProfile rp1 = new ResourceProfile(1.0, 100, 100, 100, 100, 100, Collections.emptyMap());
		ResourceProfile rp2 = new ResourceProfile(2.0, 200, 200, 200, 200, 200, Collections.emptyMap());
		ResourceProfile rp3 = new ResourceProfile(3.0, 300, 300, 300, 300, 300, Collections.emptyMap());

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
		ResourceProfile rp1 = new ResourceProfile(Double.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE,
			Integer.MAX_VALUE, Collections.singletonMap("gpu", new GPUResource(4.0)));
		ResourceProfile rp2 = new ResourceProfile(2.0, 200, 200, 200, 200, 200,
			Collections.emptyMap());

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
