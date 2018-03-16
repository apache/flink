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

import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for ResourceSpec class, including its all public api: isValid, lessThanOrEqual, equals, hashCode and merge.
 */
public class ResourceSpecTest extends TestLogger {

	@Test
	public void testIsValid() throws Exception {
		ResourceSpec rs = ResourceSpec.newBuilder().setCpuCores(1.0).setHeapMemoryInMB(100).build();
		assertTrue(rs.isValid());

		rs = ResourceSpec.newBuilder().
				setCpuCores(1.0).
				setHeapMemoryInMB(100).
				setGPUResource(1).
				build();
		assertTrue(rs.isValid());

		rs = ResourceSpec.newBuilder().
				setCpuCores(1.0).
				setHeapMemoryInMB(100).
				setGPUResource(-1).
				build();
		assertFalse(rs.isValid());
	}

	@Test
	public void testLessThanOrEqual() throws Exception {
		ResourceSpec rs1 = ResourceSpec.newBuilder().setCpuCores(1.0).setHeapMemoryInMB(100).build();
		ResourceSpec rs2 = ResourceSpec.newBuilder().setCpuCores(1.0).setHeapMemoryInMB(100).build();
		assertTrue(rs1.lessThanOrEqual(rs2));
		assertTrue(rs2.lessThanOrEqual(rs1));

		ResourceSpec rs3 = ResourceSpec.newBuilder().
				setCpuCores(1.0).
				setHeapMemoryInMB(100).
				setGPUResource(1.1).
				build();
		assertTrue(rs1.lessThanOrEqual(rs3));
		assertFalse(rs3.lessThanOrEqual(rs1));

		ResourceSpec rs4 = ResourceSpec.newBuilder().
				setCpuCores(1.0).
				setHeapMemoryInMB(100).
				setGPUResource(2.2).
				build();
		assertFalse(rs4.lessThanOrEqual(rs3));
		assertTrue(rs3.lessThanOrEqual(rs4));
	}

	@Test
	public void testEquals() throws Exception {
		ResourceSpec rs1 = ResourceSpec.newBuilder().setCpuCores(1.0).setHeapMemoryInMB(100).build();
		ResourceSpec rs2 = ResourceSpec.newBuilder().setCpuCores(1.0).setHeapMemoryInMB(100).build();
		assertTrue(rs1.equals(rs2));
		assertTrue(rs2.equals(rs1));

		ResourceSpec rs3 = ResourceSpec.newBuilder().
				setCpuCores(1.0).
				setHeapMemoryInMB(100).
				setGPUResource(2.2).
				build();
		ResourceSpec rs4 = ResourceSpec.newBuilder().
				setCpuCores(1.0).
				setHeapMemoryInMB(100).
				setGPUResource(1).
				build();
		assertFalse(rs3.equals(rs4));

		ResourceSpec rs5 = ResourceSpec.newBuilder().
				setCpuCores(1.0).
				setHeapMemoryInMB(100).
				setGPUResource(2.2).
				build();
		assertTrue(rs3.equals(rs5));
	}

	@Test
	public void testHashCode() throws Exception {
		ResourceSpec rs1 = ResourceSpec.newBuilder().setCpuCores(1.0).setHeapMemoryInMB(100).build();
		ResourceSpec rs2 = ResourceSpec.newBuilder().setCpuCores(1.0).setHeapMemoryInMB(100).build();
		assertEquals(rs1.hashCode(), rs2.hashCode());

		ResourceSpec rs3 = ResourceSpec.newBuilder().
				setCpuCores(1.0).
				setHeapMemoryInMB(100).
				setGPUResource(2.2).
				build();
		ResourceSpec rs4 = ResourceSpec.newBuilder().
				setCpuCores(1.0).
				setHeapMemoryInMB(100).
				setGPUResource(1).
				build();
		assertFalse(rs3.hashCode() == rs4.hashCode());

		ResourceSpec rs5 = ResourceSpec.newBuilder().
				setCpuCores(1.0).
				setHeapMemoryInMB(100).
				setGPUResource(2.2).
				build();
		assertEquals(rs3.hashCode(), rs5.hashCode());
	}

	@Test
	public void testMerge() throws Exception {
		ResourceSpec rs1 = ResourceSpec.newBuilder().
				setCpuCores(1.0).
				setHeapMemoryInMB(100).
				setGPUResource(1.1).
				build();
		ResourceSpec rs2 = ResourceSpec.newBuilder().setCpuCores(1.0).setHeapMemoryInMB(100).build();

		ResourceSpec rs3 = rs1.merge(rs2);
		assertEquals(1.1, rs3.getGPUResource(), 0.000001);

		ResourceSpec rs4 = rs1.merge(rs3);
		assertEquals(2.2, rs4.getGPUResource(), 0.000001);
	}

	@Test
	public void testSerializable() throws Exception {
		ResourceSpec rs1 = ResourceSpec.newBuilder().
				setCpuCores(1.0).
				setHeapMemoryInMB(100).
				setGPUResource(1.1).
				build();
		byte[] buffer = InstantiationUtil.serializeObject(rs1);
		ResourceSpec rs2 = InstantiationUtil.deserializeObject(buffer, ClassLoader.getSystemClassLoader());
		assertEquals(rs1, rs2);
	}
}
