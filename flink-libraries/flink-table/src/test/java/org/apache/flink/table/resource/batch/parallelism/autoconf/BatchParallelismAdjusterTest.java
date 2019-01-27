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

package org.apache.flink.table.resource.batch.parallelism.autoconf;

import org.apache.flink.table.plan.nodes.exec.BatchExecNode;
import org.apache.flink.table.plan.nodes.exec.ExecNode;
import org.apache.flink.table.plan.nodes.exec.NodeResource;
import org.apache.flink.table.plan.nodes.physical.batch.BatchPhysicalRel;
import org.apache.flink.table.resource.batch.BatchExecNodeStage;
import org.apache.flink.table.resource.batch.NodeRunningUnit;
import org.apache.flink.table.resource.batch.parallelism.ShuffleStage;

import org.junit.Before;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link BatchParallelismAdjuster}.
 */
public class BatchParallelismAdjusterTest {

	private double totalCpu = 100;
	private List<BatchExecNode<?>> nodeList;
	private Map<BatchExecNode<?>, Set<NodeRunningUnit>> nodeRunningUnitMap;
	private Map<ExecNode<?, ?>, ShuffleStage> nodeShuffleStageMap;

	@Before
	public void setUp() {
		nodeRunningUnitMap = new LinkedHashMap<>();
		nodeShuffleStageMap = new LinkedHashMap<>();
	}

	@Test
	public void testNotAdjustParallelism() {
		createNodeList(5);
		putSameRunningUnit(0, 1, 2, 3, 4);

		putSameShuffleStage(0, 1);
		setShuffleStageParallelism(0, 4);
		putSameShuffleStage(2, 3);
		setShuffleStageParallelism(2, 5);
		putSameShuffleStage(4);
		setShuffleStageParallelism(4, 7);

		setNodeCpu(0, 0.5);
		setNodeCpu(1, 0.5);
		setNodeCpu(2, 0.5);
		setNodeCpu(3, 0.5);
		setNodeCpu(4, 0.5);

		BatchParallelismAdjuster.adjustParallelism(totalCpu, nodeRunningUnitMap, nodeShuffleStageMap);
		assertEquals(4, nodeShuffleStageMap.get(nodeList.get(0)).getParallelism());
		assertEquals(4, nodeShuffleStageMap.get(nodeList.get(1)).getParallelism());
		assertEquals(5, nodeShuffleStageMap.get(nodeList.get(2)).getParallelism());
		assertEquals(5, nodeShuffleStageMap.get(nodeList.get(3)).getParallelism());
		assertEquals(7, nodeShuffleStageMap.get(nodeList.get(4)).getParallelism());
	}

	@Test
	public void testOverlap() {
		createNodeList(9);
		putSameRunningUnit(0, 1, 2, 3, 4, 5);
		putSameRunningUnit(3, 6);
		putSameRunningUnit(6, 7, 8);
		putSameRunningUnit(7, 8, 5);

		putSameShuffleStage(0, 1);
		setShuffleStageParallelism(0, 200);
		putSameShuffleStage(2, 3);
		setShuffleStageParallelism(2, 6000);
		putSameShuffleStage(4, 5);
		setShuffleStageParallelism(4, 7000);
		putSameShuffleStage(6, 7);
		setShuffleStageParallelism(6, 8000);
		putSameShuffleStage(8);
		setShuffleStageParallelism(8, 4000);

		setNodeCpu(0, 0.2);
		setNodeCpu(1, 0.3);
		setNodeCpu(2, 0.4);
		setNodeCpu(3, 0.5);
		setNodeCpu(4, 0.6);
		setNodeCpu(5, 0.55);
		setNodeCpu(6, 0.45);
		setNodeCpu(7, 0.35);
		setNodeCpu(8, 0.25);

		BatchParallelismAdjuster.adjustParallelism(totalCpu, nodeRunningUnitMap, nodeShuffleStageMap);
		assertEquals(3, nodeShuffleStageMap.get(nodeList.get(0)).getParallelism());
		assertEquals(3, nodeShuffleStageMap.get(nodeList.get(1)).getParallelism());
		assertEquals(82, nodeShuffleStageMap.get(nodeList.get(2)).getParallelism());
		assertEquals(82, nodeShuffleStageMap.get(nodeList.get(3)).getParallelism());
		assertEquals(79, nodeShuffleStageMap.get(nodeList.get(4)).getParallelism());
		assertEquals(79, nodeShuffleStageMap.get(nodeList.get(5)).getParallelism());
		assertEquals(91, nodeShuffleStageMap.get(nodeList.get(6)).getParallelism());
		assertEquals(91, nodeShuffleStageMap.get(nodeList.get(7)).getParallelism());
		assertEquals(46, nodeShuffleStageMap.get(nodeList.get(8)).getParallelism());
	}

	@Test
	public void testAllInARunningUnit() {
		createNodeList(5);
		putSameRunningUnit(0, 1, 2, 3, 4);

		putSameShuffleStage(0, 1);
		setShuffleStageParallelism(0, 1000);
		putSameShuffleStage(2, 3);
		setShuffleStageParallelism(2, 2000);
		putSameShuffleStage(4);
		setShuffleStageParallelism(4, 5000);

		setNodeCpu(0, 0.5);
		setNodeCpu(1, 0.5);
		setNodeCpu(2, 0.5);
		setNodeCpu(3, 0.5);
		setNodeCpu(4, 0.5);

		BatchParallelismAdjuster.adjustParallelism(totalCpu, nodeRunningUnitMap, nodeShuffleStageMap);
		assertEquals(25, nodeShuffleStageMap.get(nodeList.get(0)).getParallelism());
		assertEquals(25, nodeShuffleStageMap.get(nodeList.get(1)).getParallelism());
		assertEquals(50, nodeShuffleStageMap.get(nodeList.get(2)).getParallelism());
		assertEquals(50, nodeShuffleStageMap.get(nodeList.get(3)).getParallelism());
		assertEquals(125, nodeShuffleStageMap.get(nodeList.get(4)).getParallelism());
	}

	@Test
	public void testAllInAShuffleStage() {
		createNodeList(5);
		putSameRunningUnit(0, 1);
		putSameRunningUnit(1, 2);
		putSameRunningUnit(2, 3, 4);

		putSameShuffleStage(0, 1, 2, 3, 4);
		setShuffleStageParallelism(0, Integer.MAX_VALUE);

		setNodeCpu(0, 0.2);
		setNodeCpu(1, 0.3);
		setNodeCpu(2, 0.4);
		setNodeCpu(3, 0.5);
		setNodeCpu(4, 0.6);

		BatchParallelismAdjuster.adjustParallelism(totalCpu, nodeRunningUnitMap, nodeShuffleStageMap);

		assertEquals(166, nodeShuffleStageMap.get(nodeList.get(0)).getParallelism());
		assertEquals(166, nodeShuffleStageMap.get(nodeList.get(1)).getParallelism());
		assertEquals(166, nodeShuffleStageMap.get(nodeList.get(2)).getParallelism());
		assertEquals(166, nodeShuffleStageMap.get(nodeList.get(3)).getParallelism());
		assertEquals(166, nodeShuffleStageMap.get(nodeList.get(4)).getParallelism());
	}

	private void putSameRunningUnit(int... indexes) {
		NodeRunningUnit runningUnit = new NodeRunningUnit();
		for (int index : indexes) {
			BatchExecNode<?> node = nodeList.get(index);
			runningUnit.addNodeStage(new BatchExecNodeStage(node, 0));
		}
		for (int index : indexes) {
			BatchExecNode<?> node = nodeList.get(index);
			nodeRunningUnitMap.computeIfAbsent(node, k->new LinkedHashSet<>()).add(runningUnit);
		}
	}

	private void setShuffleStageParallelism(int nodeIndex, int parallelism) {
		nodeShuffleStageMap.get(nodeList.get(nodeIndex)).setParallelism(parallelism, false);
	}

	public void putSameShuffleStage(int... indexes) {
		ShuffleStage shuffleStage = new ShuffleStage();
		for (int index : indexes) {
			BatchExecNode<?> node = nodeList.get(index);
			shuffleStage.addNode(node);
		}
		for (int index : indexes) {
			BatchExecNode<?> node = nodeList.get(index);
			nodeShuffleStageMap.put(node, shuffleStage);
		}
	}

	private void setNodeCpu(int index, double cpu) {
		nodeList.get(index).getResource().setCpu(cpu);
	}

	private void createNodeList(int num) {
		nodeList = new LinkedList<>();
		for (int i = 0; i < num; i++) {
			BatchExecNode<?> node = mock(BatchExecNode.class);
			BatchPhysicalRel rel = mock(BatchPhysicalRel.class);
			when(node.getFlinkPhysicalRel()).thenReturn(rel);
			when(node.toString()).thenReturn("id: " + i);
			nodeList.add(node);
			NodeResource resource = new NodeResource();
			when(node.getResource()).thenReturn(resource);
		}
	}
}
