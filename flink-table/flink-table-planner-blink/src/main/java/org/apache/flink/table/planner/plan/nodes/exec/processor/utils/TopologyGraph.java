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

package org.apache.flink.table.planner.plan.nodes.exec.processor.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecBoundedStreamScan;
import org.apache.flink.table.planner.plan.nodes.exec.visitor.AbstractExecNodeExactlyOnceVisitor;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * A data structure storing the topological and input priority information of an {@link ExecNode} graph.
 */
@Internal
class TopologyGraph {

	private final Map<ExecNode<?>, TopologyNode> nodes;

	TopologyGraph(List<ExecNode<?>> roots) {
		this(roots, Collections.emptySet());
	}

	TopologyGraph(List<ExecNode<?>> roots, Set<ExecNode<?>> boundaries) {
		this.nodes = new HashMap<>();

		// we first link all edges in the original exec node graph
		AbstractExecNodeExactlyOnceVisitor visitor = new AbstractExecNodeExactlyOnceVisitor() {
			@Override
			protected void visitNode(ExecNode<?> node) {
				if (boundaries.contains(node)) {
					return;
				}
				for (ExecNode<?> input : node.getInputNodes()) {
					link(input, node);
				}
				visitInputs(node);
			}
		};
		roots.forEach(n -> n.accept(visitor));
	}

	/**
	 * Link an edge from `from` node to `to` node if no loop will occur after adding this edge.
	 * Returns if this edge is successfully added.
	 */
	boolean link(ExecNode<?> from, ExecNode<?> to) {
		TopologyNode fromNode = getOrCreateTopologyNode(from);
		TopologyNode toNode = getOrCreateTopologyNode(to);

		if (canReach(toNode, fromNode)) {
			// invalid edge, as `to` is the predecessor of `from`
			return false;
		} else {
			// link `from` and `to`
			fromNode.outputs.add(toNode);
			toNode.inputs.add(fromNode);
			return true;
		}
	}

	/**
	 * Remove the edge from `from` node to `to` node. If there is no edge between them then do nothing.
	 */
	void unlink(ExecNode<?> from, ExecNode<?> to) {
		TopologyNode fromNode = getOrCreateTopologyNode(from);
		TopologyNode toNode = getOrCreateTopologyNode(to);

		fromNode.outputs.remove(toNode);
		toNode.inputs.remove(fromNode);
	}

	/**
	 * Calculate the maximum distance of the currently added nodes from the nodes without inputs.
	 * The smallest distance is 0 (which are exactly the nodes without inputs) and the distances of
	 * other nodes are the largest distances in their inputs plus 1.
	 *
	 * <p>Distance of a node is defined as the number of edges one needs to go through from the
	 * nodes without inputs to this node.
	 */
	Map<ExecNode<?>, Integer> calculateMaximumDistance() {
		Map<ExecNode<?>, Integer> result = new HashMap<>();
		Map<TopologyNode, Integer> inputsVisitedMap = new HashMap<>();

		Queue<TopologyNode> queue = new LinkedList<>();
		for (TopologyNode node : nodes.values()) {
			if (node.inputs.size() == 0) {
				queue.offer(node);
			}
		}

		while (!queue.isEmpty()) {
			TopologyNode node = queue.poll();
			int dist = -1;
			for (TopologyNode input : node.inputs) {
				dist = Math.max(
						dist,
					Preconditions.checkNotNull(
						result.get(input.execNode),
						"The distance of an input node is not calculated. This is a bug."));
			}
			dist++;
			result.put(node.execNode, dist);

			for (TopologyNode output : node.outputs) {
				int inputsVisited = inputsVisitedMap.compute(output, (k, v) -> v == null ? 1 : v + 1);
				if (inputsVisited == output.inputs.size()) {
					queue.offer(output);
				}
			}
		}

		return result;
	}

	/**
	 * Make the distance of node A at least as far as node B by adding edges
	 * from all inputs of node B to node A.
	 */
	void makeAsFarAs(ExecNode<?> a, ExecNode<?> b) {
		TopologyNode nodeA = getOrCreateTopologyNode(a);
		TopologyNode nodeB = getOrCreateTopologyNode(b);

		for (TopologyNode input : nodeB.inputs) {
			link(input.execNode, nodeA.execNode);
		}
	}

	@VisibleForTesting
	boolean canReach(ExecNode<?> from, ExecNode<?> to) {
		TopologyNode fromNode = getOrCreateTopologyNode(from);
		TopologyNode toNode = getOrCreateTopologyNode(to);
		return canReach(fromNode, toNode);
	}

	private boolean canReach(TopologyNode from, TopologyNode to) {
		Set<TopologyNode> visited = new HashSet<>();
		visited.add(from);
		Queue<TopologyNode> queue = new LinkedList<>();
		queue.offer(from);

		while (!queue.isEmpty()) {
			TopologyNode node = queue.poll();
			if (to.equals(node)) {
				return true;
			}

			for (TopologyNode next : node.outputs) {
				if (visited.contains(next)) {
					continue;
				}
				visited.add(next);
				queue.offer(next);
			}
		}

		return false;
	}

	private TopologyNode getOrCreateTopologyNode(ExecNode<?> execNode) {
		// NOTE: We treat different `BatchExecBoundedStreamScan`s with same `DataStream` object as the same
		if (execNode instanceof BatchExecBoundedStreamScan) {
			DataStream<?> currentStream = ((BatchExecBoundedStreamScan) execNode).getDataStream();
			for (Map.Entry<ExecNode<?>, TopologyNode> entry : nodes.entrySet()) {
				ExecNode<?> key = entry.getKey();
				if (key instanceof BatchExecBoundedStreamScan) {
					DataStream<?> existingStream = ((BatchExecBoundedStreamScan) key).getDataStream();
					if (existingStream.equals(currentStream)) {
						return entry.getValue();
					}
				}
			}

			TopologyNode result = new TopologyNode(execNode);
			nodes.put(execNode, result);
			return result;
		} else {
			return nodes.computeIfAbsent(execNode, k -> new TopologyNode(execNode));
		}
	}

	/**
	 * A node in the {@link TopologyGraph}.
	 */
	private static class TopologyNode {
		private final ExecNode<?> execNode;
		private final Set<TopologyNode> inputs;
		private final Set<TopologyNode> outputs;

		private TopologyNode(ExecNode<?> execNode) {
			this.execNode = execNode;
			this.inputs = new HashSet<>();
			this.outputs = new HashSet<>();
		}
	}
}
