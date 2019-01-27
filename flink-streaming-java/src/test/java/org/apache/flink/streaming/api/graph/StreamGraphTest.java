/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.io.network.DataExchangeMode;
import org.apache.flink.runtime.operators.DamBehavior;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;
import org.apache.flink.util.OutputTag;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@code StreamGraph}.
 */
public class StreamGraphTest {

	@Test
	public void testDataExchangeMode() {
		StreamGraph streamGraph = new StreamGraph(new ExecutionConfig(),
			new CheckpointConfig(),
			3,
			1234L,
			DataPartitionerType.REBALANCE);

		streamGraph.addNode(999, null, null, null, null);
		streamGraph.addNode(888, null, null, null, null);
		streamGraph.addNode(777, null, null, null, null);
		streamGraph.addNode(11, null, null, null, null);
		streamGraph.addNode(22, null, null, null, null);
		streamGraph.addNode(33, null, null, null, null);
		streamGraph.addNode(44, null, null, null, null);
		streamGraph.addNode(55, null, null, null, null);
		streamGraph.addNode(66, null, null, null, null);

		streamGraph.addVirtualPartitionNode(888, 87, new ForwardPartitioner<>(), DataExchangeMode.BATCH);
		streamGraph.addVirtualPartitionNode(777, 76, new ForwardPartitioner<>(), null);

		streamGraph.addEdge(999, 11, 0);
		streamGraph.addEdge(999, 22, 0, DataExchangeMode.PIPELINED, null);
		streamGraph.addEdge(87, 33, 0);
		streamGraph.addEdge(87, 44, 0, DataExchangeMode.PIPELINED, null);
		streamGraph.addEdge(76, 55, 0);
		streamGraph.addEdge(76, 66, 0, DataExchangeMode.PIPELINED, null);

		assertEquals(DataExchangeMode.AUTO, streamGraph.getStreamEdges(999, 11).get(0).getDataExchangeMode());
		assertEquals(DataExchangeMode.PIPELINED, streamGraph.getStreamEdges(999, 22).get(0).getDataExchangeMode());
		assertEquals(DataExchangeMode.BATCH, streamGraph.getStreamEdges(888, 33).get(0).getDataExchangeMode());
		assertEquals(DataExchangeMode.BATCH, streamGraph.getStreamEdges(888, 44).get(0).getDataExchangeMode());
		assertEquals(DataExchangeMode.AUTO, streamGraph.getStreamEdges(777, 55).get(0).getDataExchangeMode());
		assertEquals(DataExchangeMode.AUTO, streamGraph.getStreamEdges(777, 66).get(0).getDataExchangeMode());
	}

	@Test
	public void testDataPartitionerType() {
		// case: set the default partitioner type to REBALANCE
		{
			StreamGraph streamGraph = new StreamGraph(new ExecutionConfig(),
				new CheckpointConfig(),
				3,
				1234L,
				DataPartitionerType.REBALANCE);

			streamGraph.addNode(999, null, null, null, null);
			streamGraph.addNode(888, null, null, null, null);
			streamGraph.addNode(777, null, null, null, null)
				.setParallelism(100);

			streamGraph.addEdge(999, 888, 0, null, null);
			streamGraph.addEdge(888, 777, 0, null, null);

			assertEquals(ForwardPartitioner.class, streamGraph.getStreamEdges(999, 888).get(0).getPartitioner().getClass());
			assertEquals(RebalancePartitioner.class, streamGraph.getStreamEdges(888, 777).get(0).getPartitioner().getClass());
		}

		// case: set the default partitioner type to RESCALE
		{
			StreamGraph streamGraph = new StreamGraph(new ExecutionConfig(),
				new CheckpointConfig(),
				3,
				1234L,
				DataPartitionerType.RESCALE);

			streamGraph.addNode(999, null, null, null, null);
			streamGraph.addNode(888, null, null, null, null);
			streamGraph.addNode(777, null, null, null, null)
				.setParallelism(100);

			streamGraph.addEdge(999, 888, 0);
			streamGraph.addEdge(888, 777, 0);

			assertEquals(ForwardPartitioner.class, streamGraph.getStreamEdges(999, 888).get(0).getPartitioner().getClass());
			assertEquals(RescalePartitioner.class, streamGraph.getStreamEdges(888, 777).get(0).getPartitioner().getClass());
		}
	}

	@Test
	public void testDamBehavior() {
		StreamGraph streamGraph = new StreamGraph(new ExecutionConfig(),
				new CheckpointConfig(),
				3,
				1234L,
				DataPartitionerType.REBALANCE);

		streamGraph.addNode(999, null, null, null, null);
		streamGraph.addNode(888, null, null, null, null);
		streamGraph.addNode(11, null, null, null, null);
		streamGraph.addNode(22, null, null, null, null);
		streamGraph.addNode(33, null, null, null, null);
		streamGraph.addNode(44, null, null, null, null);

		streamGraph.setMainOutputDamBehavior(999, DamBehavior.MATERIALIZING);
		streamGraph.addVirtualSideOutputNode(999, 98, new OutputTag<String>("test tag"){}, DamBehavior.FULL_DAM);

		streamGraph.addEdge(999, 11, 0);
		streamGraph.addEdge(98, 22, 0, null, DamBehavior.MATERIALIZING);
		streamGraph.addEdge(888, 33, 0);
		streamGraph.addEdge(888, 44, 0, null, DamBehavior.MATERIALIZING);

		assertEquals(DamBehavior.MATERIALIZING, streamGraph.getStreamEdges(999, 11).get(0).getDamBehavior());
		assertEquals(DamBehavior.FULL_DAM, streamGraph.getStreamEdges(999, 22).get(0).getDamBehavior());
		assertEquals(DamBehavior.PIPELINED, streamGraph.getStreamEdges(888, 33).get(0).getDamBehavior());
		assertEquals(DamBehavior.MATERIALIZING, streamGraph.getStreamEdges(888, 44).get(0).getDamBehavior());
	}
}
