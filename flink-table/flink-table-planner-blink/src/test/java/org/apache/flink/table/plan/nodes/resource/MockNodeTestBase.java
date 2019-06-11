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

package org.apache.flink.table.plan.nodes.resource;

import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.table.plan.nodes.exec.ExecNode;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecBoundedStreamScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecCalc;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExchange;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecTableSourceScan;

import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Base for test with mock node list.
 */
public class MockNodeTestBase {

	protected List<ExecNode> nodeList;

	protected void updateNode(int index, ExecNode<?, ?> node) {
		nodeList.set(index, node);
		NodeResource resource = new NodeResource();
		when(node.getResource()).thenReturn(resource);
		when(node.toString()).thenReturn("id: " + index);
		if (node instanceof BatchExecTableSourceScan) {
			StreamTransformation transformation = mock(StreamTransformation.class);
			when(((BatchExecTableSourceScan) node).getSourceTransformation(any())).thenReturn(transformation);
			when(transformation.getMaxParallelism()).thenReturn(-1);
		} else if (node instanceof BatchExecBoundedStreamScan) {
			StreamTransformation transformation = mock(StreamTransformation.class);
			when(((BatchExecBoundedStreamScan) node).getSourceTransformation()).thenReturn(transformation);
		} else if (node instanceof BatchExecExchange) {
			RelDistribution distribution = mock(RelDistribution.class);
			when(distribution.getType()).thenReturn(RelDistribution.Type.BROADCAST_DISTRIBUTED);
			when(((BatchExecExchange) node).getDistribution()).thenReturn(distribution);
		}
	}

	protected void createNodeList(int num) {
		nodeList = new LinkedList<>();
		for (int i = 0; i < num; i++) {
			ExecNode<?, ?>  node = mock(BatchExecCalc.class);
			when(node.getInputNodes()).thenReturn(new ArrayList<>());
			when(node.toString()).thenReturn("id: " + i);
			nodeList.add(node);
		}
	}

	protected void connect(int nodeIndex, int... inputNodeIndexes) {
		List<ExecNode<?, ?>> inputNodes = new ArrayList<>(inputNodeIndexes.length);
		for (int inputIndex : inputNodeIndexes) {
			ExecNode<?, ?> input = nodeList.get(inputIndex);
			inputNodes.add(input);
		}
		when(nodeList.get(nodeIndex).getInputNodes()).thenReturn(inputNodes);
		if (inputNodeIndexes.length == 1 && nodeList.get(nodeIndex) instanceof SingleRel) {
			when(((SingleRel) nodeList.get(nodeIndex)).getInput()).thenReturn((RelNode) nodeList.get(inputNodeIndexes[0]));
		} else if (inputNodeIndexes.length == 2 && nodeList.get(nodeIndex) instanceof BiRel) {
			when(((BiRel) nodeList.get(nodeIndex)).getLeft()).thenReturn((RelNode) nodeList.get(inputNodeIndexes[0]));
			when(((BiRel) nodeList.get(nodeIndex)).getRight()).thenReturn((RelNode) nodeList.get(inputNodeIndexes[1]));
		}
	}
}

