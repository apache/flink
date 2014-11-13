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

package org.apache.flink.compiler.dataproperties;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.compiler.dag.OptimizerNode;
import org.junit.Test;
import org.mockito.Matchers;

public class GlobalPropertiesPushdownTest {

	@Test
	public void testAnyPartitioningPushedDown() {
		try {
			RequestedGlobalProperties req = new RequestedGlobalProperties();
			req.setAnyPartitioning(new FieldSet(3, 1));
			
			RequestedGlobalProperties preserved = req.filterByNodesConstantSet(getAllPreservingNode(), 0);
			assertEquals(PartitioningProperty.ANY_PARTITIONING, preserved.getPartitioning());
			assertTrue(preserved.getPartitionedFields().isValidSubset(new FieldSet(1, 3)));
			
			RequestedGlobalProperties nonPreserved = req.filterByNodesConstantSet(getNonePreservingNode(), 0);
			assertTrue(nonPreserved == null || nonPreserved.isTrivial());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testHashPartitioningPushedDown() {
		try {
			RequestedGlobalProperties req = new RequestedGlobalProperties();
			req.setHashPartitioned(new FieldSet(3, 1));
			
			RequestedGlobalProperties preserved = req.filterByNodesConstantSet(getAllPreservingNode(), 0);
			assertEquals(PartitioningProperty.HASH_PARTITIONED, preserved.getPartitioning());
			assertTrue(preserved.getPartitionedFields().isValidSubset(new FieldSet(1, 3)));
			
			RequestedGlobalProperties nonPreserved = req.filterByNodesConstantSet(getNonePreservingNode(), 0);
			assertTrue(nonPreserved == null || nonPreserved.isTrivial());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testCustomPartitioningNotPushedDown() {
		try {
			RequestedGlobalProperties req = new RequestedGlobalProperties();
			req.setCustomPartitioned(new FieldSet(3, 1), new MockPartitioner());
			
			RequestedGlobalProperties pushedDown = req.filterByNodesConstantSet(getAllPreservingNode(), 0);
			assertTrue(pushedDown == null || pushedDown.isTrivial());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testForcedReblancingNotPushedDown() {
		try {
			RequestedGlobalProperties req = new RequestedGlobalProperties();
			req.setForceRebalancing();
			
			RequestedGlobalProperties pushedDown = req.filterByNodesConstantSet(getAllPreservingNode(), 0);
			assertTrue(pushedDown == null || pushedDown.isTrivial());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	private static OptimizerNode getAllPreservingNode() {
		OptimizerNode node = mock(OptimizerNode.class);
		when(node.isFieldConstant(Matchers.anyInt(), Matchers.anyInt())).thenReturn(true);
		return node;
	}
	
	private static OptimizerNode getNonePreservingNode() {
		OptimizerNode node = mock(OptimizerNode.class);
		when(node.isFieldConstant(Matchers.anyInt(), Matchers.anyInt())).thenReturn(false);
		return node;
	}
}
