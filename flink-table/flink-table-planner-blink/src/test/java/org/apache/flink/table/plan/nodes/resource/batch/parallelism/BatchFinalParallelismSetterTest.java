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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.BatchTableEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.plan.nodes.exec.ExecNode;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecBoundedStreamScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExchange;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecTableSourceScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecUnion;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecValues;
import org.apache.flink.table.plan.nodes.resource.MockNodeTestBase;

import org.apache.calcite.rel.RelDistribution;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for BatchFinalParallelismSetter.
 */
public class BatchFinalParallelismSetterTest extends MockNodeTestBase {

	private StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
	private BatchTableEnvironment tEnv;

	@Before
	public void setUp() {
		tEnv = TableEnvironment.getBatchTableEnvironment(sEnv);
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
		BatchExecTableSourceScan scan0 = mock(BatchExecTableSourceScan.class);
		updateNode(0, scan0);
		when(((BatchExecTableSourceScan) nodeList.get(0)).getSourceTransformation(any()).getMaxParallelism()).thenReturn(5);
		updateNode(1, mock(BatchExecTableSourceScan.class));
		updateNode(2, mock(BatchExecValues.class));
		updateNode(3, mock(BatchExecUnion.class));
		updateNode(4, mock(BatchExecBoundedStreamScan.class));
		when(((BatchExecBoundedStreamScan) nodeList.get(4)).getSourceTransformation().getParallelism()).thenReturn(7);
		updateNode(5, mock(BatchExecBoundedStreamScan.class));
		connect(3, 0, 1, 2, 4, 5);
		Map<ExecNode<?, ?>, Integer> finalParallelismNodeMap = BatchFinalParallelismSetter.calculate(tEnv, Collections.singletonList(nodeList.get(3)));
		assertEquals(4, finalParallelismNodeMap.size());
		assertEquals(5, finalParallelismNodeMap.get(nodeList.get(0)).intValue());
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
		BatchExecTableSourceScan scan0 = mock(BatchExecTableSourceScan.class);
		updateNode(0, scan0);
		when(((BatchExecTableSourceScan) nodeList.get(0)).getSourceTransformation(any()).getMaxParallelism()).thenReturn(5);
		BatchExecExchange execExchange4 = mock(BatchExecExchange.class, RETURNS_DEEP_STUBS);
		when(execExchange4.getDistribution().getType()).thenReturn(RelDistribution.Type.BROADCAST_DISTRIBUTED);
		updateNode(2, execExchange4);
		BatchExecExchange execExchange3 = mock(BatchExecExchange.class, RETURNS_DEEP_STUBS);
		updateNode(3, execExchange3);
		when(execExchange3.getDistribution().getType()).thenReturn(RelDistribution.Type.SINGLETON);
		BatchExecExchange execExchange5 = mock(BatchExecExchange.class, RETURNS_DEEP_STUBS);
		updateNode(9, execExchange5);
		when(execExchange5.getDistribution().getType()).thenReturn(RelDistribution.Type.SINGLETON);
		connect(2, 0);
		connect(4, 2);
		connect(3, 1);
		connect(5, 3);
		connect(6, 4, 5, 7);
		connect(7, 8, 9);
		connect(9, 10);
		Map<ExecNode<?, ?>, Integer> finalParallelismNodeMap = BatchFinalParallelismSetter.calculate(tEnv, Collections.singletonList(nodeList.get(6)));
		assertEquals(3, finalParallelismNodeMap.size());
		assertEquals(5, finalParallelismNodeMap.get(nodeList.get(0)).intValue());
		assertEquals(1, finalParallelismNodeMap.get(nodeList.get(5)).intValue());
		assertEquals(1, finalParallelismNodeMap.get(nodeList.get(7)).intValue());
	}
}

