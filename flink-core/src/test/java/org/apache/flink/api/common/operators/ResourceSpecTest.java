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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ResourceSpecTest {

	@Test
	public void testIsValid() throws Exception {
		ResourceSpec rs = new ResourceSpec(1.0, 100);
		assertTrue(rs.isValid());

		rs = new ResourceSpec(1.0, 100, new ResourceSpec.Resource("GPU", 1));
		assertTrue(rs.isValid());

		rs = new ResourceSpec(1.0, 100, new ResourceSpec.Resource("GPU", -1));
		assertFalse(rs.isValid());
	}

	@Test
	public void testLessThanOrEqual() throws Exception {
		ResourceSpec rs1 = new ResourceSpec(1.0, 100);
		ResourceSpec rs2 = new ResourceSpec(1.0, 100);
		assertTrue(rs1.lessThanOrEqual(rs2));
		assertTrue(rs2.lessThanOrEqual(rs1));

		rs2 = new ResourceSpec(1.0, 100, new ResourceSpec.Resource("FPGA", 1));
		assertTrue(rs1.lessThanOrEqual(rs2));
		assertFalse(rs2.lessThanOrEqual(rs1));

		rs1 = new ResourceSpec(1.0, 100, new ResourceSpec.Resource("FPGA", 2));
		assertFalse(rs1.lessThanOrEqual(rs2));
		assertTrue(rs2.lessThanOrEqual(rs1));

		rs2 = new ResourceSpec(1.0, 100,
				new ResourceSpec.Resource("FPGA", 1),
				new ResourceSpec.Resource("GPU", 1));
		assertFalse(rs1.lessThanOrEqual(rs2));
		assertFalse(rs2.lessThanOrEqual(rs1));
	}

	@Test
	public void testEquals() throws Exception {
		ResourceSpec rs1 = new ResourceSpec(1.0, 100);
		ResourceSpec rs2 = new ResourceSpec(1.0, 100);
		assertTrue(rs1.equals(rs2));
		assertTrue(rs2.equals(rs1));

		rs1 = new ResourceSpec(1.0, 100, new ResourceSpec.Resource("FPGA", 2.2));
		rs2 = new ResourceSpec(1.0, 100, new ResourceSpec.Resource("FPGA", 1));
		assertFalse(rs1.equals(rs2));

		rs2 = new ResourceSpec(1.0, 100, new ResourceSpec.Resource("FPGA", 2.2));
		assertTrue(rs1.equals(rs2));

		rs1 = new ResourceSpec(1.0, 100,
				new ResourceSpec.Resource("FPGA", 2),
				new ResourceSpec.Resource("GPU", 0.5));
		rs2 = new ResourceSpec(1.0, 100,
				new ResourceSpec.Resource("FPGA", 2),
				new ResourceSpec.Resource("GPU", ResourceSpec.ResourceAggregateType.AGGREGATE_TYPE_MAX, 0.5));
		assertFalse(rs1.equals(rs2));
	}

	@Test
	public void testHashCode() throws Exception {
		ResourceSpec rs1 = new ResourceSpec(1.0, 100);
		ResourceSpec rs2 = new ResourceSpec(1.0, 100);
		assertEquals(rs1.hashCode(), rs2.hashCode());

		rs1 = new ResourceSpec(1.0, 100, new ResourceSpec.Resource("FPGA", 2.2));
		rs2 = new ResourceSpec(1.0, 100, new ResourceSpec.Resource("FPGA", 1));
		assertFalse(rs1.hashCode() == rs2.hashCode());

		rs2 = new ResourceSpec(1.0, 100, new ResourceSpec.Resource("FPGA", 2.2));
		assertEquals(rs1.hashCode(), rs2.hashCode());

		rs1 = new ResourceSpec(1.0, 100,
				new ResourceSpec.Resource("FPGA", 2),
				new ResourceSpec.Resource("GPU", 0.5));
		rs2 = new ResourceSpec(1.0, 100,
				new ResourceSpec.Resource("FPGA", 2),
				new ResourceSpec.Resource("GPU", ResourceSpec.ResourceAggregateType.AGGREGATE_TYPE_MAX, 0.5));
		assertFalse(rs1.hashCode() == rs2.hashCode());
	}

	@Test
	public void testMerge() throws Exception {
		ResourceSpec rs1 = new ResourceSpec(1.0, 100, new ResourceSpec.Resource("FPGA", 1.1));
		ResourceSpec rs2 = new ResourceSpec(1.0, 100);

		ResourceSpec rs3 = rs1.merge(rs2);
		assertEquals(1.1, rs3.getExtendedResources().get("FPGA").doubleValue(), 0.000001);

		ResourceSpec rs4 = rs1.merge(rs3);
		assertEquals(2.2, rs4.getExtendedResources().get("FPGA").doubleValue(), 0.000001);

		rs1 = new ResourceSpec(1.0, 100,
				new ResourceSpec.Resource("FPGA", 2),
				new ResourceSpec.Resource("GPU", 0.5));
		ResourceSpec rs5 = rs1.merge(rs2);
		assertEquals(2, rs5.getExtendedResources().get("FPGA").doubleValue(), 0.000001);
		assertEquals(0.5, rs5.getExtendedResources().get("GPU").doubleValue(), 0.000001);

		rs2 = new ResourceSpec(1.0, 100,
				new ResourceSpec.Resource("GPU", ResourceSpec.ResourceAggregateType.AGGREGATE_TYPE_MAX, 0.5));
		try {
			rs1.merge(rs2);
			fail("Merge with different aggregate type should fail");
		} catch (IllegalArgumentException e) {
		}

		rs1 = new ResourceSpec(1.0, 100,
				new ResourceSpec.Resource("FPGA", 2),
				new ResourceSpec.Resource("GPU", ResourceSpec.ResourceAggregateType.AGGREGATE_TYPE_MAX, 1.5));
		ResourceSpec rs6 = rs1.merge(rs2);
		assertEquals(2, rs6.getExtendedResources().get("FPGA").doubleValue(), 0.000001);
		assertEquals(1.5, rs6.getExtendedResources().get("GPU").doubleValue(), 0.000001);

	}

}
