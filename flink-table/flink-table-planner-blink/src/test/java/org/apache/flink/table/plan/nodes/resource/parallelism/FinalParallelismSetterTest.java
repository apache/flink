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

package org.apache.flink.table.plan.nodes.resource.parallelism;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.plan.nodes.exec.ExecNode;
import org.apache.flink.table.plan.nodes.resource.MockNodeTestBase;

import org.apache.calcite.rel.RelDistribution;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Test for FinalParallelismSetter.
 */
@SuppressWarnings("serial")
@RunWith(Parameterized.class)
public class FinalParallelismSetterTest extends MockNodeTestBase {

	private StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

	public FinalParallelismSetterTest(boolean isBatch) {
		super(isBatch);
	}

	@Before
	public void setUp() {
		sEnv.setParallelism(21);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testSource() {
		/**
		 *   0, Source   1, Source  2, Values  4, Source   5, Source
		 *            \      /      /            /           /
		 *             3, Union
		 */
		createNodeList(6);
		updateTableSource(0, 5);
		updateTableSource(1);
		updateValues(2);
		updateUnion(3);
		updateStreamScan(4, 7);
		updateStreamScan(5);
		connect(3, 0, 1, 2, 4, 5);
		Map<ExecNode<?, ?>, Integer> finalParallelismNodeMap = FinalParallelismSetter.calculate(sEnv, Collections.singletonList(nodeList.get(3)));
		assertEquals(3, finalParallelismNodeMap.size());
		assertEquals(5, nodeList.get(0).getResource().getMaxParallelism());
		assertEquals(1, finalParallelismNodeMap.get(nodeList.get(2)).intValue());
		assertEquals(7, finalParallelismNodeMap.get(nodeList.get(4)).intValue());
		assertEquals(21, finalParallelismNodeMap.get(nodeList.get(5)).intValue());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testExchange() {
		/**
		 *   0, Source    1, Source               10, Source
		 *        |         |                       |
		 *   2, Exchange  3, Exchange 8, Source  9, Exchange
		 *        |         |              \     /
		 *   4, Calc      5, Calc          7, Join
		 *         \         /              /
		 *          6, Union
		 */
		createNodeList(11);
		updateTableSource(0, 5);
		updateExchange(2, RelDistribution.Type.BROADCAST_DISTRIBUTED);
		updateExchange(3, RelDistribution.Type.SINGLETON);
		updateExchange(9, RelDistribution.Type.SINGLETON);
		connect(2, 0);
		connect(4, 2);
		connect(3, 1);
		connect(5, 3);
		connect(6, 4, 5, 7);
		connect(7, 8, 9);
		connect(9, 10);
		Map<ExecNode<?, ?>, Integer> finalParallelismNodeMap = FinalParallelismSetter.calculate(sEnv, Collections.singletonList(nodeList.get(6)));
		assertEquals(2, finalParallelismNodeMap.size());
		assertEquals(5, nodeList.get(0).getResource().getMaxParallelism());
		assertEquals(1, finalParallelismNodeMap.get(nodeList.get(5)).intValue());
		assertEquals(1, finalParallelismNodeMap.get(nodeList.get(7)).intValue());
	}

	@Parameterized.Parameters(name = "isBatch = {0}")
	public static Collection<Object[]> runMode() {
		return Arrays.asList(
				new Object[] { false, },
				new Object[] { true });
	}
}

