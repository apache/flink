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
import static org.junit.Assert.fail;

/**
 * Tests for ResourceSpec class, including its all public api: isValid, lessThanOrEqual, equals, hashCode and merge.
 */
public class ResourceSpecTest extends TestLogger {

	@Test
	public void testIsValid() throws Exception {
		ResourceSpec rs = new ResourceSpec(1.0, 100);
		assertTrue(rs.isValid());

		rs = new ResourceSpec(1.0, 100, new ResourceSpec.GPUResource(1));
		assertTrue(rs.isValid());

		rs = new ResourceSpec(1.0, 100, new ResourceSpec.GPUResource(-1));
		assertFalse(rs.isValid());
	}

	@Test
	public void testLessThanOrEqual() throws Exception {
		ResourceSpec rs1 = new ResourceSpec(1.0, 100);
		ResourceSpec rs2 = new ResourceSpec(1.0, 100);
		assertTrue(rs1.lessThanOrEqual(rs2));
		assertTrue(rs2.lessThanOrEqual(rs1));

		rs2 = new ResourceSpec(1.0, 100, new ResourceSpec.FPGAResource(1));
		assertTrue(rs1.lessThanOrEqual(rs2));
		assertFalse(rs2.lessThanOrEqual(rs1));

		ResourceSpec rs3 = new ResourceSpec(1.0, 100, new ResourceSpec.FPGAResource(2));
		assertFalse(rs3.lessThanOrEqual(rs2));
		assertTrue(rs2.lessThanOrEqual(rs3));

		ResourceSpec rs4 = new ResourceSpec(1.0, 100,
				new ResourceSpec.FPGAResource(1),
				new ResourceSpec.GPUResource( 1));
		assertFalse(rs3.lessThanOrEqual(rs4));
		assertFalse(rs4.lessThanOrEqual(rs3));
	}

	@Test
	public void testEquals() throws Exception {
		ResourceSpec rs1 = new ResourceSpec(1.0, 100);
		ResourceSpec rs2 = new ResourceSpec(1.0, 100);
		assertTrue(rs1.equals(rs2));
		assertTrue(rs2.equals(rs1));

		ResourceSpec rs3 = new ResourceSpec(1.0, 100, new ResourceSpec.FPGAResource(2.2));
		ResourceSpec rs4 = new ResourceSpec(1.0, 100, new ResourceSpec.FPGAResource( 1));
		assertFalse(rs3.equals(rs4));

		ResourceSpec rs5 = new ResourceSpec(1.0, 100, new ResourceSpec.FPGAResource(2.2));
		assertTrue(rs3.equals(rs5));

		ResourceSpec rs6 = new ResourceSpec(1.0, 100,
				new ResourceSpec.FPGAResource(2),
				new ResourceSpec.GPUResource( 0.5));
		ResourceSpec rs7 = new ResourceSpec(1.0, 100,
				new ResourceSpec.FPGAResource( 2),
				new ResourceSpec.GPUResource(0.5, ResourceSpec.ResourceAggregateType.AGGREGATE_TYPE_MAX));
		assertFalse(rs6.equals(rs7));
	}

	@Test
	public void testHashCode() throws Exception {
		ResourceSpec rs1 = new ResourceSpec(1.0, 100);
		ResourceSpec rs2 = new ResourceSpec(1.0, 100);
		assertEquals(rs1.hashCode(), rs2.hashCode());

		ResourceSpec rs3 = new ResourceSpec(1.0, 100, new ResourceSpec.FPGAResource(2.2));
		ResourceSpec rs4 = new ResourceSpec(1.0, 100, new ResourceSpec.FPGAResource(1));
		assertFalse(rs3.hashCode() == rs4.hashCode());

		ResourceSpec rs5 = new ResourceSpec(1.0, 100, new ResourceSpec.FPGAResource( 2.2));
		assertEquals(rs3.hashCode(), rs5.hashCode());

		ResourceSpec rs6 = new ResourceSpec(1.0, 100,
				new ResourceSpec.FPGAResource( 2),
				new ResourceSpec.GPUResource(0.5));
		ResourceSpec rs7 = new ResourceSpec(1.0, 100,
				new ResourceSpec.FPGAResource(2),
				new ResourceSpec.GPUResource(0.5, ResourceSpec.ResourceAggregateType.AGGREGATE_TYPE_MAX));
		assertFalse(rs6.hashCode() == rs7.hashCode());
	}

	@Test
	public void testMerge() throws Exception {
		ResourceSpec rs1 = new ResourceSpec(1.0, 100, new ResourceSpec.FPGAResource(1.1));
		ResourceSpec rs2 = new ResourceSpec(1.0, 100);

		ResourceSpec rs3 = rs1.merge(rs2);
		assertEquals(1.1, rs3.getExtendedResources().get("FPGA").doubleValue(), 0.000001);

		ResourceSpec rs4 = rs1.merge(rs3);
		assertEquals(2.2, rs4.getExtendedResources().get("FPGA").doubleValue(), 0.000001);

		ResourceSpec rs6 = new ResourceSpec(1.0, 100,
				new ResourceSpec.FPGAResource(2),
				new ResourceSpec.GPUResource(0.5));
		ResourceSpec rs5 = rs6.merge(rs2);
		assertEquals(2, rs5.getExtendedResources().get("FPGA").doubleValue(), 0.000001);
		assertEquals(0.5, rs5.getExtendedResources().get("GPU").doubleValue(), 0.000001);

		ResourceSpec rs7 = new ResourceSpec(1.0, 100,
				new ResourceSpec.GPUResource( 0.5, ResourceSpec.ResourceAggregateType.AGGREGATE_TYPE_MAX));
		try {
			rs6.merge(rs7);
			fail("Merge with different aggregate type should fail");
		} catch (IllegalArgumentException ignored) {
		}

		ResourceSpec rs8 = new ResourceSpec(1.0, 100,
				new ResourceSpec.FPGAResource(2),
				new ResourceSpec.GPUResource(1.5, ResourceSpec.ResourceAggregateType.AGGREGATE_TYPE_MAX));
		ResourceSpec rs9 = rs8.merge(rs7);
		assertEquals(2, rs9.getExtendedResources().get("FPGA").doubleValue(), 0.000001);
		assertEquals(1.5, rs9.getExtendedResources().get("GPU").doubleValue(), 0.000001);

	}

	@Test
	public void testSerializable() throws Exception {
		ResourceSpec rs = new ResourceSpec(1.0, 100, new ResourceSpec.FPGAResource(1.1));
		byte[] buffer = InstantiationUtil.serializeObject(rs);
		InstantiationUtil.deserializeObject(buffer, ClassLoader.getSystemClassLoader());
	}
}
