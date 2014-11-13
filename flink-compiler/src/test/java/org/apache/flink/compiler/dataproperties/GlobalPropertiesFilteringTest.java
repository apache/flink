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
import static org.mockito.Mockito.*;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.compiler.dag.OptimizerNode;
import org.junit.Test;
import org.mockito.Matchers;

public class GlobalPropertiesFilteringTest {

	@Test
	public void testCustomPartitioningPreserves() {
		try {
			Partitioner<?> partitioner = new MockPartitioner();
			
			GlobalProperties gp = new GlobalProperties();
			gp.setCustomPartitioned(new FieldList(2, 3), partitioner);
			
			OptimizerNode node = mock(OptimizerNode.class);
			when(node.isFieldConstant(Matchers.anyInt(), Matchers.anyInt())).thenReturn(true);
			
			GlobalProperties filtered = gp.filterByNodesConstantSet(node, 0);
			
			assertTrue(filtered.isPartitionedOnFields(new FieldSet(2, 3)));
			assertEquals(PartitioningProperty.CUSTOM_PARTITIONING, filtered.getPartitioning());
			assertEquals(partitioner, filtered.getCustomPartitioner());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
