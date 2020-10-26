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

package org.apache.flink.table.planner.plan.reuse;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.transformations.ShuffleMode;
import org.apache.flink.table.planner.plan.nodes.exec.AbstractExecNodeExactlyOnceVisitor;
import org.apache.flink.table.planner.plan.nodes.exec.BatchExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecBoundedStreamScan;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecExchange;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistribution;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;

/**
 * This class contains algorithm to detect and resolve input priority conflict in an {@link ExecNode} graph.
 *
 * <p>Some batch operators (for example, hash join and nested loop join) have different priorities for their inputs.
 * When some operators are reused, a deadlock may occur due to the conflict in these priorities.
 *
 * <p>For example, consider the SQL query:
 * <pre>
 * WITH
 *   T1 AS (SELECT a, COUNT(*) AS cnt1 FROM x GROUP BY a),
 *   T2 AS (SELECT d, COUNT(*) AS cnt2 FROM y GROUP BY d)
 * SELECT * FROM
 *   (SELECT cnt1, cnt2 FROM T1 LEFT JOIN T2 ON a = d)
 *   UNION ALL
 *   (SELECT cnt1, cnt2 FROM T2 LEFT JOIN T1 ON d = a)
 * </pre>
 *
 * <p>When sub-plan reuse are enabled, we'll get the following physical plan:
 * <pre>
 * Union(all=[true], union=[cnt1, cnt2])
 * :- Calc(select=[CAST(cnt1) AS cnt1, cnt2])
 * :  +- HashJoin(joinType=[LeftOuterJoin], where=[=(a, d)], select=[a, cnt1, d, cnt2], build=[right])
 * :     :- HashAggregate(isMerge=[true], groupBy=[a], select=[a, Final_COUNT(count1$0) AS cnt1], reuse_id=[2])
 * :     :  +- Exchange(distribution=[hash[a]])
 * :     :     +- LocalHashAggregate(groupBy=[a], select=[a, Partial_COUNT(*) AS count1$0])
 * :     :        +- Calc(select=[a])
 * :     :           +- LegacyTableSourceScan(table=[[default_catalog, default_database, x, source: [TestTableSource(a, b, c)]]], fields=[a, b, c])
 * :     +- HashAggregate(isMerge=[true], groupBy=[d], select=[d, Final_COUNT(count1$0) AS cnt2], reuse_id=[1])
 * :        +- Exchange(distribution=[hash[d]])
 * :           +- LocalHashAggregate(groupBy=[d], select=[d, Partial_COUNT(*) AS count1$0])
 * :              +- Calc(select=[d])
 * :                 +- LegacyTableSourceScan(table=[[default_catalog, default_database, y, source: [TestTableSource(d, e, f)]]], fields=[d, e, f])
 * +- Calc(select=[cnt1, CAST(cnt2) AS cnt2])
 *    +- HashJoin(joinType=[LeftOuterJoin], where=[=(d, a)], select=[d, cnt2, a, cnt1], build=[right])
 *       :- Reused(reference_id=[1])
 *       +- Reused(reference_id=[2])
 * </pre>
 *
 * <p>Note that the first hash join needs to read all results from the hash aggregate whose reuse id is 1
 * before reading the results from the hash aggregate whose reuse id is 2, while the second hash join requires
 * the opposite. This physical plan will thus cause a deadlock.
 *
 * <p>This class maintains a topological graph in which an edge pointing from vertex A to vertex B indicates
 * that the results from vertex A need to be read before those from vertex B. A loop in the graph indicates
 * a deadlock, and we resolve such deadlock by inserting a {@link BatchExecExchange} with batch shuffle mode.
 *
 * <p>For a detailed explanation of the algorithm, see appendix of the
 * <a href="https://docs.google.com/document/d/1qKVohV12qn-bM51cBZ8Hcgp31ntwClxjoiNBUOqVHsI">design doc</a>.
 */
@Internal
public class InputPriorityConflictResolver {

	private final List<ExecNode<?, ?>> roots;

	private TopologyGraph graph;

	public InputPriorityConflictResolver(List<ExecNode<?, ?>> roots) {
		Preconditions.checkArgument(
			roots.stream().allMatch(root -> root instanceof BatchExecNode),
			"InputPriorityConflictResolver can only be used for batch jobs.");
		this.roots = roots;
	}

	public void detectAndResolve() {
		// build an initial topology graph
		graph = new TopologyGraph(roots);

		// check and resolve conflicts about input priorities
		AbstractExecNodeExactlyOnceVisitor inputPriorityVisitor = new AbstractExecNodeExactlyOnceVisitor() {
			@Override
			protected void visitNode(ExecNode<?, ?> node) {
				visitInputs(node);
				checkInputPriorities(node);
			}
		};
		roots.forEach(n -> n.accept(inputPriorityVisitor));
	}

	private void checkInputPriorities(ExecNode<?, ?> node) {
		// group inputs by input priorities
		TreeMap<Integer, List<Integer>> inputPriorityGroupMap = new TreeMap<>();
		Preconditions.checkState(
			node.getInputNodes().size() == node.getInputEdges().size(),
			"Number of inputs nodes does not equal to number of input edges for node " +
				node.getClass().getName() + ". This is a bug.");
		for (int i = 0; i < node.getInputEdges().size(); i++) {
			int priority = node.getInputEdges().get(i).getPriority();
			inputPriorityGroupMap.computeIfAbsent(priority, k -> new ArrayList<>()).add(i);
		}

		// add edges between neighboring priority groups
		List<List<Integer>> inputPriorityGroups = new ArrayList<>(inputPriorityGroupMap.values());
		for (int i = 0; i + 1 < inputPriorityGroups.size(); i++) {
			List<Integer> higherGroup = inputPriorityGroups.get(i);
			List<Integer> lowerGroup = inputPriorityGroups.get(i + 1);

			for (int higher : higherGroup) {
				for (int lower : lowerGroup) {
					addTopologyEdges(node, higher, lower);
				}
			}
		}
	}

	private void addTopologyEdges(ExecNode<?, ?> node, int higherInput, int lowerInput) {
		ExecNode<?, ?> higherNode = node.getInputNodes().get(higherInput);
		ExecNode<?, ?> lowerNode = node.getInputNodes().get(lowerInput);
		List<ExecNode<?, ?>> lowerAncestors = calculateAncestors(lowerNode);

		List<Tuple2<ExecNode<?, ?>, ExecNode<?, ?>>> linkedEdges = new ArrayList<>();
		for (ExecNode<?, ?> ancestor : lowerAncestors) {
			if (graph.link(higherNode, ancestor)) {
				linkedEdges.add(Tuple2.of(higherNode, ancestor));
			} else {
				// a conflict occurs, resolve it by adding a batch exchange
				// and revert all linked edges
				if (lowerNode instanceof BatchExecExchange) {
					BatchExecExchange exchange = (BatchExecExchange) lowerNode;
					exchange.setRequiredShuffleMode(ShuffleMode.BATCH);
				} else {
					node.replaceInputNode(lowerInput, (ExecNode) createExchange(node, lowerInput));
				}

				for (Tuple2<ExecNode<?, ?>, ExecNode<?, ?>> linkedEdge : linkedEdges) {
					graph.unlink(linkedEdge.f0, linkedEdge.f1);
				}
				return;
			}
		}
	}

	/**
	 * Find the ancestors by going through PIPELINED edges.
	 */
	@VisibleForTesting
	List<ExecNode<?, ?>> calculateAncestors(ExecNode<?, ?> node) {
		List<ExecNode<?, ?>> ret = new ArrayList<>();
		AbstractExecNodeExactlyOnceVisitor ancestorVisitor = new AbstractExecNodeExactlyOnceVisitor() {
			@Override
			protected void visitNode(ExecNode<?, ?> node) {
				List<ExecEdge> inputEdges = node.getInputEdges();
				boolean hasAncestor = false;

				for (int i = 0; i < inputEdges.size(); i++) {
					// we only go through PIPELINED edges
					if (inputEdges.get(i).getDamBehavior().stricterOrEqual(ExecEdge.DamBehavior.END_INPUT)) {
						continue;
					}
					hasAncestor = true;
					node.getInputNodes().get(i).accept(this);
				}

				if (!hasAncestor) {
					ret.add(node);
				}
			}
		};
		node.accept(ancestorVisitor);
		return ret;
	}

	private BatchExecExchange createExchange(ExecNode<?, ?> node, int idx) {
		RelNode inputRel = (RelNode) node.getInputNodes().get(idx);

		FlinkRelDistribution distribution;
		ExecEdge.RequiredShuffle requiredShuffle = node.getInputEdges().get(idx).getRequiredShuffle();
		if (requiredShuffle.getType() == ExecEdge.ShuffleType.HASH) {
			distribution = FlinkRelDistribution.hash(requiredShuffle.getKeys(), true);
		} else if (requiredShuffle.getType() == ExecEdge.ShuffleType.BROADCAST) {
			// should not occur
			throw new IllegalStateException(
				"Trying to resolve input priority conflict on broadcast side. This is not expected.");
		} else if (requiredShuffle.getType() == ExecEdge.ShuffleType.SINGLETON) {
			distribution = FlinkRelDistribution.SINGLETON();
		} else {
			distribution = FlinkRelDistribution.ANY();
		}

		BatchExecExchange exchange = new BatchExecExchange(
			inputRel.getCluster(),
			inputRel.getTraitSet().replace(distribution),
			inputRel,
			distribution);
		exchange.setRequiredShuffleMode(ShuffleMode.BATCH);
		return exchange;
	}

	/**
	 * A data structure storing the topological information of an {@link ExecNode} graph.
	 */
	@VisibleForTesting
	static class TopologyGraph {
		private final Map<ExecNode<?, ?>, TopologyNode> nodes;

		TopologyGraph(List<ExecNode<?, ?>> roots) {
			this.nodes = new HashMap<>();

			// we first link all edges in the original exec node graph
			AbstractExecNodeExactlyOnceVisitor visitor = new AbstractExecNodeExactlyOnceVisitor() {
				@Override
				protected void visitNode(ExecNode<?, ?> node) {
					for (ExecNode<?, ?> input : node.getInputNodes()) {
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
		boolean link(ExecNode<?, ?> from, ExecNode<?, ?> to) {
			TopologyNode fromNode = getTopologyNode(from);
			TopologyNode toNode = getTopologyNode(to);

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
		void unlink(ExecNode<?, ?> from, ExecNode<?, ?> to) {
			TopologyNode fromNode = getTopologyNode(from);
			TopologyNode toNode = getTopologyNode(to);

			fromNode.outputs.remove(toNode);
			toNode.inputs.remove(fromNode);
		}

		@VisibleForTesting
		boolean canReach(ExecNode<?, ?> from, ExecNode<?, ?> to) {
			TopologyNode fromNode = getTopologyNode(from);
			TopologyNode toNode = getTopologyNode(to);
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

		private TopologyNode getTopologyNode(ExecNode<?, ?> execNode) {
			// NOTE: We treat different `BatchExecBoundedStreamScan`s with same `DataStream` object as the same
			if (execNode instanceof BatchExecBoundedStreamScan) {
				DataStream<?> currentStream =
					((BatchExecBoundedStreamScan) execNode).boundedStreamTable().dataStream();
				for (Map.Entry<ExecNode<?, ?>, TopologyNode> entry : nodes.entrySet()) {
					ExecNode<?, ?> key = entry.getKey();
					if (key instanceof BatchExecBoundedStreamScan) {
						DataStream<?> existingStream =
							((BatchExecBoundedStreamScan) key).boundedStreamTable().dataStream();
						if (existingStream.equals(currentStream)) {
							return entry.getValue();
						}
					}
				}

				TopologyNode result = new TopologyNode();
				nodes.put(execNode, result);
				return result;
			} else {
				return nodes.computeIfAbsent(execNode, k -> new TopologyNode());
			}
		}
	}

	/**
	 * A node in the {@link TopologyGraph}.
	 */
	private static class TopologyNode {
		private final Set<TopologyNode> inputs = new HashSet<>();
		private final Set<TopologyNode> outputs = new HashSet<>();
	}
}
