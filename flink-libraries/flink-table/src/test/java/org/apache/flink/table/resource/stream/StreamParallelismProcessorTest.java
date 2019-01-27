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

package org.apache.flink.table.resource.stream;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.table.api.TableConfigOptions;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecCalc;
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecDataStreamScan;
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecExchange;
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecTableSourceScan;
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecUnion;
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecValues;
import org.apache.flink.table.plan.nodes.process.DAGProcessContext;
import org.apache.flink.table.resource.MockNodeTestBase;

import org.apache.calcite.rel.RelDistribution;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link StreamParallelismProcessor}.
 */
public class StreamParallelismProcessorTest extends MockNodeTestBase {

	private StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
	private DAGProcessContext context;
	private StreamTableEnvironment tEnv;

	@Before
	public void setUp() {
		tEnv = TableEnvironment.getTableEnvironment(sEnv);
		context = new DAGProcessContext(tEnv);
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
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 10);
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_SOURCE_PARALLELISM, 30);
		createNodeList(6);
		StreamExecTableSourceScan scan0 = mock(StreamExecTableSourceScan.class);
		updateNode(0, scan0);
		when(((StreamExecTableSourceScan) nodeList.get(0)).getSourceTransformation(any()).getMaxParallelism()).thenReturn(5);
		updateNode(1, mock(StreamExecTableSourceScan.class));
		updateNode(2, mock(StreamExecValues.class));
		updateNode(3, mock(StreamExecUnion.class));
		updateNode(4, mock(StreamExecDataStreamScan.class));
		when(((StreamExecDataStreamScan) nodeList.get(4)).getSourceTransformation(any()).getParallelism()).thenReturn(7);
		updateNode(5, mock(StreamExecDataStreamScan.class));
		connect(3, 0, 1, 2, 4, 5);
		new StreamParallelismProcessor().process(Collections.singletonList(nodeList.get(3)), context);
		assertEquals(5, nodeList.get(0).getResource().getParallelism());
		assertEquals(30, nodeList.get(1).getResource().getParallelism());
		assertEquals(1, nodeList.get(2).getResource().getParallelism());
		assertEquals(7, nodeList.get(4).getResource().getParallelism());
		assertEquals(21, nodeList.get(5).getResource().getParallelism());
		assertEquals(10, nodeList.get(3).getResource().getParallelism());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testExchange() {
		/**
		 *   0, Source    1, Source
		 *        |         |
		 *   2, Exchange  3, Exchange
		 *        |         |
		 *   4, Calc      5, Calc
		 *         \         /
		 *          6, Union
		 */
		tEnv.getConfig().getConf().setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 10);
		createNodeList(7);
		StreamExecTableSourceScan scan0 = mock(StreamExecTableSourceScan.class);
		updateNode(0, scan0);
		when(((StreamExecTableSourceScan) nodeList.get(0)).getSourceTransformation(any()).getMaxParallelism()).thenReturn(5);
		StreamExecExchange execExchange4 = mock(StreamExecExchange.class, RETURNS_DEEP_STUBS);
		when(execExchange4.getDistribution().getType()).thenReturn(RelDistribution.Type.BROADCAST_DISTRIBUTED);
		updateNode(1, mock(StreamExecTableSourceScan.class));
		updateNode(2, execExchange4);
		StreamExecExchange execExchange3 = mock(StreamExecExchange.class, RETURNS_DEEP_STUBS);
		updateNode(3, execExchange3);
		when(execExchange3.getDistribution().getType()).thenReturn(RelDistribution.Type.SINGLETON);
		updateNode(4, mock(StreamExecCalc.class));
		updateNode(5, mock(StreamExecCalc.class));
		updateNode(6, mock(StreamExecUnion.class));
		connect(2, 0);
		connect(4, 2);
		connect(3, 1);
		connect(5, 3);
		connect(6, 4, 5);
		new StreamParallelismProcessor().process(Collections.singletonList(nodeList.get(6)), context);
		assertEquals(5, nodeList.get(0).getResource().getParallelism());
		assertEquals(10, nodeList.get(1).getResource().getParallelism());
		assertEquals(10, nodeList.get(2).getResource().getParallelism());
		assertEquals(10, nodeList.get(4).getResource().getParallelism());
		assertEquals(10, nodeList.get(5).getResource().getParallelism());
		assertEquals(1, nodeList.get(3).getResource().getParallelism());
		assertEquals(10, nodeList.get(6).getResource().getParallelism());
	}
}
