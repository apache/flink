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

package org.apache.flink.table.plan.nodes.resource.batch.parallelism;

import org.apache.flink.table.plan.nodes.exec.ExecNode;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecCalc;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExchange;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecTableSourceScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecUnion;
import org.apache.flink.table.plan.nodes.resource.MockNodeTestBase;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

/**
 * Test for {@link ShuffleStageGenerator}.
 */
public class ShuffleStageGeneratorTest extends MockNodeTestBase {

	private Map<ExecNode<?, ?>, Integer> finalParallelismNodeMap;

	@Before
	public void setUp() {
		finalParallelismNodeMap = new HashMap<>();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testGenerateShuffleStags() {
		/**
		 *
		 *    0, Source     1, Source
		 *             \     /
		 *             2, Union
		 *             /     \
		 *        3, Calc   4, Calc
		 *           |        |
		 *    5, Exchange    6, Exchange
		 *            \      /
		 *              7, Join
		 *               |
		 *              8, Calc
		 */
		createNodeList(9);
		updateNode(2, mock(BatchExecUnion.class));
		updateNode(5, mock(BatchExecExchange.class));
		updateNode(6, mock(BatchExecExchange.class));
		connect(2, 0, 1);
		connect(3, 2);
		connect(4, 2);
		connect(5, 3);
		connect(6, 4);
		connect(7, 5, 6);
		connect(8, 7);

		Map<ExecNode<?, ?>, ShuffleStage> nodeShuffleStageMap = ShuffleStageGenerator.generate(Arrays.asList(nodeList.get(8)), finalParallelismNodeMap);

		assertSameShuffleStage(nodeShuffleStageMap, 7, 8);
		assertSameShuffleStage(nodeShuffleStageMap, 0, 1, 3, 4);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testMultiOutput() {
		/**
		 *
		 *    0, Source     2, Source  4, Source   6, Source
		 *       |            |         |             |
		 *    1, Calc       3, Calc    5, Calc     7, Exchange
		 *            \     /      \   /       \    /
		 *            8, Join     9, Join     10, Join
		 *             \          /       \    /
		 *              \   12, Exchange  \   /
		 *               \      /         \  /
		 *                 11, Join      13, Union
		 *                         \      |
		 *                15, Exchange   14, Calc
		 *                           \   /
		 *                           16, Join
		 */
		createNodeList(17);
		updateNode(7, mock(BatchExecExchange.class));
		updateNode(12, mock(BatchExecExchange.class));
		updateNode(13, mock(BatchExecUnion.class));
		updateNode(15, mock(BatchExecExchange.class));
		connect(1, 0);
		connect(3, 2);
		connect(5, 4);
		connect(7, 6);
		connect(8, 1, 3);
		connect(9, 3, 5);
		connect(10, 5, 7);
		connect(12, 9);
		connect(11, 8, 12);
		connect(13, 9, 10);
		connect(14, 13);
		connect(15, 11);
		connect(16, 15, 14);

		Map<ExecNode<?, ?>, ShuffleStage> nodeShuffleStageMap = ShuffleStageGenerator.generate(Arrays.asList(nodeList.get(16)), finalParallelismNodeMap);

		assertSameShuffleStage(nodeShuffleStageMap, 0, 1, 8, 3, 2, 9, 5, 4, 10, 11, 14, 16);
		assertSameShuffleStage(nodeShuffleStageMap, 6);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testWithFinalParallelism() {
		/**
		 *
		 *    0, Source    2, Source
		 *       |            |
		 *    1, Calc      3, Calc  6, Source
		 *             \     /     /
		 *               4, Union
		 *                 |
		 *               5, Calc
		 */
		createNodeList(7);
		ExecNode<?, ?> scan0 = mock(BatchExecTableSourceScan.class);
		ExecNode<?, ?>scan1 = mock(BatchExecTableSourceScan.class);
		updateNode(0, scan0);
		finalParallelismNodeMap.put(scan0, 10);
		updateNode(2, scan1);
		finalParallelismNodeMap.put(scan1, 11);
		updateNode(4, mock(BatchExecUnion.class));
		updateNode(5, mock(BatchExecCalc.class));
		updateNode(6, mock(BatchExecTableSourceScan.class));
		connect(1, 0);
		connect(3, 2);
		connect(4, 1, 3, 6);
		connect(5, 4);

		Map<ExecNode<?, ?>, ShuffleStage> nodeShuffleStageMap = ShuffleStageGenerator.generate(Arrays.asList(nodeList.get(5)), finalParallelismNodeMap);

		assertSameShuffleStage(nodeShuffleStageMap, 0, 1);
		assertSameShuffleStage(nodeShuffleStageMap, 2, 3, 6, 5);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testWithFinalParallelism1() {
		/**
		 *
		 *    0, Source    2, Source
		 *       |            |
		 *    1, Calc      3, Calc
		 *             \     /
		 *               4, Union
		 *                 |
		 *               5, Calc
		 */
		createNodeList(7);
		ExecNode<?, ?> scan0 = mock(BatchExecTableSourceScan.class);
		ExecNode<?, ?> scan1 = mock(BatchExecTableSourceScan.class);
		updateNode(0, scan0);
		finalParallelismNodeMap.put(scan0, 10);
		updateNode(2, scan1);
		finalParallelismNodeMap.put(scan1, 11);
		updateNode(4, mock(BatchExecUnion.class));
		ExecNode<?, ?> calc = mock(BatchExecCalc.class);
		updateNode(5, calc);
		finalParallelismNodeMap.put(calc, 12);
		connect(1, 0);
		connect(3, 2);
		connect(4, 1, 3);
		connect(5, 4);

		Map<ExecNode<?, ?>, ShuffleStage> nodeShuffleStageMap = ShuffleStageGenerator.generate(Arrays.asList(nodeList.get(5)), finalParallelismNodeMap);

		assertSameShuffleStage(nodeShuffleStageMap, 0, 1);
		assertSameShuffleStage(nodeShuffleStageMap, 2, 3);
		assertSameShuffleStage(nodeShuffleStageMap, 5);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testWithFinalParallelism2() {
		/**
		 *
		 *    0, Source    2, Source
		 *       |            |
		 *       |         3, Exchange
		 *       |            |
		 *    1, Calc      4, Calc
		 *             \     /
		 *               5, Union
		 *                 |
		 *               6, Calc
		 */
		createNodeList(7);
		ExecNode<?, ?> scan0 = mock(BatchExecTableSourceScan.class);
		ExecNode<?, ?> scan1 = mock(BatchExecTableSourceScan.class);
		updateNode(0, scan0);
		finalParallelismNodeMap.put(scan0, 10);
		updateNode(2, scan1);
		finalParallelismNodeMap.put(scan1, 11);
		updateNode(3, mock(BatchExecExchange.class));
		ExecNode<?, ?> calc = mock(BatchExecCalc.class);
		updateNode(4, calc);
		finalParallelismNodeMap.put(calc, 1);
		updateNode(5, mock(BatchExecUnion.class));
		connect(1, 0);
		connect(3, 2);
		connect(4, 3);
		connect(5, 1, 4);
		connect(6, 5);

		Map<ExecNode<?, ?>, ShuffleStage> nodeShuffleStageMap = ShuffleStageGenerator.generate(Arrays.asList(nodeList.get(6)), finalParallelismNodeMap);

		assertSameShuffleStage(nodeShuffleStageMap, 0, 1, 6);
		assertSameShuffleStage(nodeShuffleStageMap, 2);
		assertSameShuffleStage(nodeShuffleStageMap, 4);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testWithFinalParallelism3() {
		/**
		 *
		 *    0, Source    2, Source
		 *       |            |
		 *    1, Calc      3, Calc  6, Source   7,Source
		 *             \     /     /             /
		 *               4, Union
		 *                 |
		 *               5, Calc
		 */
		createNodeList(8);
		ExecNode<?, ?> scan0 = mock(BatchExecTableSourceScan.class);
		ExecNode<?, ?> scan1 = mock(BatchExecTableSourceScan.class);
		updateNode(0, scan0);
		finalParallelismNodeMap.put(scan0, 11);
		updateNode(2, scan1);
		finalParallelismNodeMap.put(scan1, 5);
		ExecNode<?, ?> union4 = mock(BatchExecUnion.class);
		updateNode(4, union4);
		finalParallelismNodeMap.put(union4, 5);
		updateNode(5, mock(BatchExecCalc.class));
		updateNode(6, mock(BatchExecTableSourceScan.class));
		updateNode(7, mock(BatchExecTableSourceScan.class));
		connect(1, 0);
		connect(3, 2);
		connect(4, 1, 3, 6, 7);
		connect(5, 4);

		Map<ExecNode<?, ?>, ShuffleStage> nodeShuffleStageMap = ShuffleStageGenerator.generate(Arrays.asList(nodeList.get(5)), finalParallelismNodeMap);

		assertSameShuffleStage(nodeShuffleStageMap, 0, 1);
		assertSameShuffleStage(nodeShuffleStageMap, 2, 3, 6, 5, 7);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testWithFinalParallelism4() {
		/**
		 *
		 *    0, Source    2, Source
		 *       |            |
		 *    1, Calc      3, Calc
		 *             \     /
		 *               4, Union
		 *                 |
		 *               5, Calc
		 */
		createNodeList(6);
		ExecNode<?, ?> scan0 = mock(BatchExecTableSourceScan.class);
		ExecNode<?, ?> scan1 = mock(BatchExecTableSourceScan.class);
		updateNode(0, scan0);
		finalParallelismNodeMap.put(scan0, 11);
		updateNode(2, scan1);
		finalParallelismNodeMap.put(scan1, 5);
		ExecNode<?, ?> union4 = mock(BatchExecUnion.class);
		updateNode(4, union4);
		finalParallelismNodeMap.put(union4, 3);
		updateNode(5, mock(BatchExecCalc.class));
		connect(1, 0);
		connect(3, 2);
		connect(4, 1, 3);
		connect(5, 4);

		Map<ExecNode<?, ?>, ShuffleStage> nodeShuffleStageMap = ShuffleStageGenerator.generate(Arrays.asList(nodeList.get(5)), finalParallelismNodeMap);

		assertSameShuffleStage(nodeShuffleStageMap, 0, 1);
		assertSameShuffleStage(nodeShuffleStageMap, 2, 3);
		assertSameShuffleStage(nodeShuffleStageMap, 5);
	}

	private void assertSameShuffleStage(Map<ExecNode<?, ?>, ShuffleStage> nodeShuffleStageMap, int ... nodeIndexes) {
		Set<ExecNode<?, ?>> nodeSet = new HashSet<>();
		for (int index : nodeIndexes) {
			nodeSet.add(nodeList.get(index));
		}
		for (int index : nodeIndexes) {
			assertNotNull("shuffleStage should not be null. node index: " + index, nodeShuffleStageMap.get(nodeList.get(index)));
			assertEquals("node index: " + index, nodeSet, nodeShuffleStageMap.get(nodeList.get(index)).getExecNodeSet());
		}
	}
}

