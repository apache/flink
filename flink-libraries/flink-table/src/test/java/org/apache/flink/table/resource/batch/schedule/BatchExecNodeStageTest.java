/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.resource.batch.schedule;

import org.apache.flink.table.plan.nodes.exec.RowBatchExecNode;
import org.apache.flink.table.plan.nodes.physical.batch.BatchPhysicalRel;
import org.apache.flink.table.resource.batch.BatchExecNodeStage;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for BatchExecNodeStage.
 */
public class BatchExecNodeStageTest {

	@Test
	public void testDependStages() {
		RowBatchExecNode node = mock(RowBatchExecNode.class);
		BatchPhysicalRel rel = mock(BatchPhysicalRel.class);
		when(node.getFlinkPhysicalRel()).thenReturn(rel);
		BatchExecNodeStage nodeStage = new BatchExecNodeStage(node, 0);
		BatchExecNodeStage nodeStage1 = new BatchExecNodeStage(node, 1);
		nodeStage.addDependStage(nodeStage1, BatchExecNodeStage.DependType.DATA_TRIGGER);
		assertEquals(1, nodeStage.getAllDependStageList().size());
		BatchExecNodeStage nodeStage2 = new BatchExecNodeStage(node, 2);
		nodeStage.addDependStage(nodeStage2, BatchExecNodeStage.DependType.PRIORITY);
		assertEquals(2, nodeStage.getAllDependStageList().size());
		BatchExecNodeStage nodeStage3 = new BatchExecNodeStage(node, 3);
		nodeStage.addDependStage(nodeStage3, BatchExecNodeStage.DependType.DATA_TRIGGER);
		assertEquals(3, nodeStage.getAllDependStageList().size());
		nodeStage.removeDependStage(nodeStage2);
		assertEquals(2, nodeStage.getAllDependStageList().size());
	}
}
