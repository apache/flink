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
import org.apache.flink.api.common.resources.CommonExtendedResource;
import org.apache.flink.api.common.resources.GPUResource;
import org.apache.flink.api.common.resources.Resource;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ResourceProfileTest {

	@Test
	public void testMatchRequirement() throws Exception {
		ResourceProfile rp1 = new ResourceProfile(1.0, 100, 100, 100, 0, Collections.emptyMap());
		ResourceProfile rp2 = new ResourceProfile(1.0, 200, 200, 200, 0, Collections.emptyMap());
		ResourceProfile rp3 = new ResourceProfile(2.0, 100, 100, 100, 0, Collections.emptyMap());
		ResourceProfile rp4 = new ResourceProfile(2.0, 200, 200, 200, 0, Collections.emptyMap());

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

		ResourceProfile rp5 = new ResourceProfile(2.0, 100, 100, 100, 100, null);
		assertFalse(rp4.isMatching(rp5));

		ResourceSpec rs1 = ResourceSpec.newBuilder().
				setCpuCores(1.0).
				setHeapMemoryInMB(100).
				setGPUResource(2.2).
				build();
		ResourceSpec rs2 = ResourceSpec.newBuilder().
				setCpuCores(1.0).
				setHeapMemoryInMB(100).
				setGPUResource(1.1).
				build();

		assertFalse(rp1.isMatching(ResourceProfile.fromResourceSpec(rs1, 0)));
		assertTrue(ResourceProfile.fromResourceSpec(rs1, 0).isMatching(ResourceProfile.fromResourceSpec(rs2, 0)));
		assertFalse(ResourceProfile.fromResourceSpec(rs2, 0).isMatching(ResourceProfile.fromResourceSpec(rs1, 0)));

		assertEquals(0, rp5.getManagedMemoryInMB());
		assertEquals(0, rp5.getFloatingManagedMemoryInMB());

		ResourceSpec rs3 = ResourceSpec.newBuilder().
				setCpuCores(1.0).
				setHeapMemoryInMB(100).
				addExtendedResource(new CommonExtendedResource(ResourceSpec.FLOATING_MANAGED_MEMORY_NAME, 200)).
				build();
		ResourceSpec rs4 = ResourceSpec.newBuilder().
				setCpuCores(1.0).
				setHeapMemoryInMB(100).
				addExtendedResource(new CommonExtendedResource(ResourceSpec.FLOATING_MANAGED_MEMORY_NAME, 400)).
				build();
		assertTrue(ResourceProfile.fromResourceSpec(rs3, 0).isMatching(ResourceProfile.fromResourceSpec(rs4, 0)));
	}

	@Test
	public void testUnknownMatchesUnknown() {
		assertTrue(ResourceProfile.UNKNOWN.isMatching(ResourceProfile.UNKNOWN));
	}

	@Test
	public void testEquals() throws Exception {
		ResourceSpec rs1 = ResourceSpec.newBuilder().setCpuCores(1.0).setHeapMemoryInMB(100).build();
		ResourceSpec rs2 = ResourceSpec.newBuilder().setCpuCores(1.0).setHeapMemoryInMB(100).build();
		assertTrue(ResourceProfile.fromResourceSpec(rs1, 0).equals(ResourceProfile.fromResourceSpec(rs2, 0)));

		ResourceSpec rs3 = ResourceSpec.newBuilder().
				setCpuCores(1.0).
				setHeapMemoryInMB(100).
				setGPUResource(2.2).
				build();
		ResourceSpec rs4 = ResourceSpec.newBuilder().
				setCpuCores(1.0).
				setHeapMemoryInMB(100).
				setGPUResource(1.1).
				build();
		assertFalse(ResourceProfile.fromResourceSpec(rs3, 0).equals(ResourceProfile.fromResourceSpec(rs4, 0)));

		ResourceSpec rs5 = ResourceSpec.newBuilder().
				setCpuCores(1.0).
				setHeapMemoryInMB(100).
				setGPUResource(2.2).
				build();
		assertTrue(ResourceProfile.fromResourceSpec(rs3, 100).equals(ResourceProfile.fromResourceSpec(rs5, 100)));
	}

	@Test
	public void testCompareTo() throws Exception {
		ResourceSpec rs1 = ResourceSpec.newBuilder().setCpuCores(1.0).setHeapMemoryInMB(100).build();
		ResourceSpec rs2 = ResourceSpec.newBuilder().setCpuCores(1.0).setHeapMemoryInMB(100).build();
		assertEquals(0, ResourceProfile.fromResourceSpec(rs1, 0).compareTo(ResourceProfile.fromResourceSpec(rs2, 0)));

		ResourceSpec rs3 = ResourceSpec.newBuilder().
				setCpuCores(1.0).
				setHeapMemoryInMB(100).
				setGPUResource(2.2).
				build();
		assertEquals(-1, ResourceProfile.fromResourceSpec(rs1,  0).compareTo(ResourceProfile.fromResourceSpec(rs3, 0)));
		assertEquals(1, ResourceProfile.fromResourceSpec(rs3, 0).compareTo(ResourceProfile.fromResourceSpec(rs1, 0)));

		ResourceSpec rs4 = ResourceSpec.newBuilder().
				setCpuCores(1.0).
				setHeapMemoryInMB(100).
				setGPUResource(1.1).
				build();
		assertEquals(1, ResourceProfile.fromResourceSpec(rs3, 0).compareTo(ResourceProfile.fromResourceSpec(rs4, 0)));
		assertEquals(-1, ResourceProfile.fromResourceSpec(rs4, 0).compareTo(ResourceProfile.fromResourceSpec(rs3, 0)));


		ResourceSpec rs5 = ResourceSpec.newBuilder().
				setCpuCores(1.0).
				setHeapMemoryInMB(100).
				setGPUResource(2.2).
				build();
		assertEquals(0, ResourceProfile.fromResourceSpec(rs3, 0).compareTo(ResourceProfile.fromResourceSpec(rs5, 0)));
	}

	@Test
	public void testGet() throws Exception {
		ResourceSpec rs = ResourceSpec.newBuilder().
				setCpuCores(1.0).
				setHeapMemoryInMB(100).
				setGPUResource(1.6).
				build();
		ResourceProfile rp = ResourceProfile.fromResourceSpec(rs, 50);

		assertEquals(1.0, rp.getCpuCores(), 0.000001);
		assertEquals(150, rp.getMemoryInMB());
		assertEquals(100, rp.getOperatorsMemoryInMB());
		assertEquals(1.6, rp.getExtendedResources().get(ResourceSpec.GPU_NAME).getValue(), 0.000001);
	}

	@Test
	public void testMinus() {
		ResourceProfile rs1 = new ResourceProfile(
			3,
			300,
			302,
			303,
			304,
			new HashMap<String, Resource>() {{
				this.put("extend_1", new GPUResource(305));
				this.put("extend_2", new GPUResource(306));
			}}
		);

		ResourceProfile rs2 = new ResourceProfile(
			3,
			200,
			202,
			203,
			204,
			new HashMap<String, Resource>() {{
				this.put("extend_1", new GPUResource(205));
			}}
		);

		ResourceProfile result = rs1.minus(rs2);
		assertEquals(result, new ResourceProfile(
			0,
			100,
			100,
			100,
			100,
			new HashMap<String, Resource>() {{
				this.put("extend_1", new GPUResource(100));
				// Should be ignored.
				this.put("extend_2", new GPUResource(306));
			}})
		);
	}

	@Test
	public void testMinusWithUnknownExtendedResource() {
		ResourceProfile rs1 = new ResourceProfile(
			3,
			300,
			302,
			303,
			304,
			new HashMap<String, Resource>() {{
				this.put("extend_1", new GPUResource(305));
				this.put("extend_2", new GPUResource(306));
			}}
		);

		ResourceProfile rs2 = new ResourceProfile(
			3,
			200,
			202,
			203,
			204,
			new HashMap<String, Resource>() {{
				this.put("extend_1", new GPUResource(205));
				this.put("extend_3", new GPUResource(105));
			}}
		);

		boolean exceptionCaught = false;
		try {
			ResourceProfile result = rs1.minus(rs2);
		} catch (IllegalArgumentException e) {
			exceptionCaught = true;
		}

		assertTrue("Minus with non-existent extended resource should cause IllegalArgumentException",
			exceptionCaught);
	}

	@Test
	public void testMerge() {
		ResourceProfile rs1 = new ResourceProfile(
			3,
			300,
			301,
			302,
			303,
			new HashMap<String, Resource>() {{
				this.put("extend_1", new GPUResource(305));
				this.put("extend_2", new GPUResource(306));
			}});

		ResourceProfile rs2 = new ResourceProfile(
			2,
			200,
			201,
			202,
			203,
			new HashMap<String, Resource>() {{
				this.put("extend_1", new GPUResource(225));
				this.put("extend_3", new GPUResource(321));
			}}
		);

		ResourceProfile result = rs1.merge(rs2);
		assertEquals(result, new ResourceProfile(
			5,
			500,
			502,
			504,
			506,
			new HashMap<String, Resource>() {{
				this.put("extend_1", new GPUResource(530));
				this.put("extend_2", new GPUResource(306));
				this.put("extend_3", new GPUResource(321));
			}})
		);
	}

	@Test
	public void testAddTo() {
		ResourceProfile rs1 = new ResourceProfile(
			3,
			300,
			301,
			302,
			303,
			new HashMap<String, Resource>() {{
				this.put("extend_1", new GPUResource(305));
				this.put("extend_2", new GPUResource(306));
			}});

		ResourceProfile rs2 = new ResourceProfile(
			2,
			200,
			201,
			202,
			203,
			new HashMap<String, Resource>() {{
				this.put("extend_1", new GPUResource(225));
				this.put("extend_3", new GPUResource(321));
			}}
		);

		rs1.addTo(rs2);
		assertEquals(new ResourceProfile(
			5,
			500,
			502,
			504,
			506,
			new HashMap<String, Resource>() {{
				this.put("extend_1", new GPUResource(530));
				this.put("extend_2", new GPUResource(306));
				this.put("extend_3", new GPUResource(321));
			}}), rs1
		);
	}

	@Test
	public void testMultiply() {
		ResourceProfile rs1 = new ResourceProfile(
			3,
			300,
			301,
			302,
			303,
			new HashMap<String, Resource>() {{
				this.put("extend_1", new GPUResource(305));
				this.put("extend_2", new GPUResource(306));
			}});

		ResourceProfile result = rs1.multiply(4);
		assertEquals(new ResourceProfile(
			3 * 4,
			300 * 4,
			301 * 4,
			302 * 4,
			303 * 4,
			new HashMap<String, Resource>() {{
				this.put("extend_1", new GPUResource(305 * 4));
				this.put("extend_2", new GPUResource(306 * 4));
			}}), result
		);
	}
}
