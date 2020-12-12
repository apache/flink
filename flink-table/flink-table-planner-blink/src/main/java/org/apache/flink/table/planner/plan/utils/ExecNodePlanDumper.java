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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.table.planner.plan.nodes.calcite.LegacySink;
import org.apache.flink.table.planner.plan.nodes.calcite.Sink;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeVisitor;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeVisitorImpl;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An utility class for converting an exec node plan to a string as a tree style.
 */
public class ExecNodePlanDumper {

	/**
	 * Converts an {@link ExecNode} tree to a string as a tree style.
	 *
	 *  <p>The following tree of {@link ExecNode}
	 * <pre>{@code
	 *        Sink
	 *         |
	 *        Join
	 *      /      \
	 *  Filter1  Filter2
	 *     \     /
	 *     Project
	 *       |
	 *     Scan
	 * }</pre>
	 *
	 * <p>would be converted to the tree style as following:
	 * <pre>{@code
	 * Sink
	 * +- Join
	 *    :- Filter1
	 *    :  +- Project(reuse_id=[1])
	 *    :     +- Scan
	 *    +- Filter2
	 *       +- Reused(reference_id=[1])
	 * }
	 * }</pre>
	 *
	 * @param node the ExecNode to convert
	 * @return explain plan of ExecNode
	 */
	public static String treeToString(ExecNode<?> node) {
		return treeToString(node, new ArrayList<>(), false);
	}

	/**
	 * Converts an {@link ExecNode} tree to a string as a tree style.
	 *
	 * @param node the ExecNode to convert
	 * @param borders node sets that stop visit when meet them
	 * @param includingBorders Whether print the border nodes
	 * @return the plan of ExecNode
	 */
	public static String treeToString(ExecNode<?> node, List<ExecNode<?>> borders, boolean includingBorders) {
		checkNotNull(node, "node should not be null.");
		// convert to mutable list
		List<ExecNode<?>> borderList = new ArrayList<>(checkNotNull(borders, "borders should not be null."));
		TreeReuseInfo reuseInfo = new TreeReuseInfo(node, borderList);
		return doConvertTreeToString(node, reuseInfo, true, borderList, includingBorders);
	}

	/**
	 * Converts an {@link ExecNode} DAG to a string as a tree style.
	 *
	 * <p>The following DAG of {@link ExecNode}
	 * <pre>{@code
	 *     Sink1    Sink2
	 *      |        |
	 *   Filter3  Filter4
	 *       \     /
	 *        Join
	 *      /      \
	 *  Filter1  Filter2
	 *     \     /
	 *     Project
	 *       |
	 *     Scan
	 * }</pre>
	 *
	 * <p>would be converted to the tree style as following:
	 * <pre>{@code
	 * Join(reuse_id=[2])
	 * :- Filter1
	 * :  +- Project(reuse_id=[1])
	 * :     +- Scan
	 * +- Filter2
	 *    +- Reused(reference_id=[1])
	 *
	 * Sink1
	 * +- Filter3
	 *    +- Reused(reference_id=[2])
	 *
	 * Sink2
	 * +- Filter4
	 *    +- Reused(reference_id=[2])
	 * }</pre>
	 *
	 * @param nodes the ExecNodes to convert
	 * @return the plan of ExecNode
	 */
	public static String dagToString(List<ExecNode<?>> nodes) {
		Preconditions.checkArgument(nodes != null && !nodes.isEmpty(), "nodes should not be null or empty.");
		if (nodes.size() == 1) {
			return treeToString(nodes.get(0));
		}

		// nodes that stop visit when meet them
		final List<ExecNode<?>> stopVisitNodes = new ArrayList<>();
		final StringBuilder sb = new StringBuilder();
		final DagReuseInfo reuseInfo = new DagReuseInfo(nodes, new ArrayList<>());

		final ExecNodeVisitor visitor = new ExecNodeVisitorImpl() {
			@Override
			public void visit(ExecNode<?> node) {
				int visitedTimes = reuseInfo.addVisitedTimes(node);
				boolean isFirstVisit = visitedTimes == 1;
				if (isFirstVisit) {
					super.visit(node);
				}

				int reuseId = reuseInfo.getReuseId(node);
				boolean isReuseNode = reuseId >= 0;
				if (node instanceof LegacySink || node instanceof Sink || (isReuseNode && isFirstVisit)) {
					if (isReuseNode) {
						reuseInfo.setFirstVisited(node, true);
					}

					String reusePlan = doConvertTreeToString(node, reuseInfo, false, stopVisitNodes, false);
					sb.append(reusePlan).append(System.lineSeparator());

					if (isReuseNode) {
						// update visit info after the reuse node visited
						stopVisitNodes.add(node);
						reuseInfo.setFirstVisited(node, false);
					}
				}
			}
		};
		nodes.forEach(visitor::visit);

		if (sb.length() > 0) {
			// delete last line separator
			sb.deleteCharAt(sb.length() - 1);
		}
		return sb.toString();
	}

	private static String doConvertTreeToString(
			ExecNode<?> node,
			ReuseInfo reuseInfo,
			boolean updateVisitedTimes,
			List<ExecNode<?>> stopVisitNodes,
			boolean includingBorders) {
		StringBuilder sb = new StringBuilder();
		ExecNodeVisitor visitor = new ExecNodeStringTreeBuilder(
				sb, reuseInfo, updateVisitedTimes, stopVisitNodes, includingBorders);
		node.accept(visitor);
		return sb.toString();
	}

	/**
	 * A class that describe the reuse info for the given DAG or tree.
	 */
	private abstract static class ReuseInfo {
		// build reuse id
		private final ReuseIdBuilder reuseIdBuilder;
		// mapping node object to visited times
		protected final Map<ExecNode<?>, Integer> mapNodeToVisitedTimes;

		protected ReuseInfo(List<ExecNode<?>> nodes, List<ExecNode<?>> borders) {
			this.reuseIdBuilder = new ReuseIdBuilder(borders);
			nodes.forEach(reuseIdBuilder::visit);
			this.mapNodeToVisitedTimes = new IdentityHashMap<>();
		}

		/**
		 * Returns reuse id if the given node is a reuse node, else -1.
		 */
		Integer getReuseId(ExecNode<?> node) {
			return reuseIdBuilder.getReuseId(node);
		}

		/**
		 * Returns true if the given node is first visited, else false.
		 */
		abstract boolean isFirstVisited(ExecNode<?> node);

		/**
		 * Updates visited times for given node, return the new times.
		 */
		int addVisitedTimes(ExecNode<?> node) {
			return mapNodeToVisitedTimes.compute(node, (k, v) -> v == null ? 1 : v + 1);
		}
	}

	/**
	 * {@link ReuseInfo} for node tree.
	 */
	private static class TreeReuseInfo extends ReuseInfo {
		public TreeReuseInfo(ExecNode<?> node, List<ExecNode<?>> borders) {
			super(Collections.singletonList(node), borders);
		}

		@Override
		boolean isFirstVisited(ExecNode<?> node) {
			return mapNodeToVisitedTimes.get(node) == 1;
		}
	}

	/**
	 * {@link ReuseInfo} for node dag.
	 */
	private static class DagReuseInfo extends ReuseInfo {
		// mapping node object to first-visited flag
		private final Map<ExecNode<?>, Boolean> firstVisitedMap;

		public DagReuseInfo(List<ExecNode<?>> nodes, List<ExecNode<?>> borders) {
			super(nodes, borders);
			this.firstVisitedMap = new IdentityHashMap<>();
		}

		@Override
		boolean isFirstVisited(ExecNode<?> node) {
			return firstVisitedMap.getOrDefault(node, false);
		}

		void setFirstVisited(ExecNode<?> node, boolean visitedFlag) {
			firstVisitedMap.put(node, visitedFlag);
		}
	}

	/**
	 * Build reuse id in an ExecNode DAG or tree.
	 */
	private static class ReuseIdBuilder extends ExecNodeVisitorImpl {
		private final List<ExecNode<?>> borders;
		// visited node set
		private final Set<ExecNode<?>> visitedNodes = Collections.newSetFromMap(new IdentityHashMap<>());
		// mapping reuse node to its reuse id
		private final Map<ExecNode<?>, Integer> mapReuseNodeToReuseId = new IdentityHashMap<>();
		private final AtomicInteger reuseIdGenerator = new AtomicInteger(0);

		public ReuseIdBuilder(List<ExecNode<?>> borders) {
			this.borders = borders;
		}

		@Override
		public void visit(ExecNode<?> node) {
			if (borders.contains(node)) {
				return;
			}

			// if a node is visited more than once, this node is a reusable node
			if (visitedNodes.contains(node)) {
				if (!mapReuseNodeToReuseId.containsKey(node)) {
					int reuseId = reuseIdGenerator.incrementAndGet();
					mapReuseNodeToReuseId.put(node, reuseId);
				}
			} else {
				visitedNodes.add(node);
				super.visit(node);
			}
		}

		/**
		 * Returns reuse id if the given node is a reuse node (that means it has multiple outputs), else -1.
		 */
		public Integer getReuseId(ExecNode<?> node) {
			return mapReuseNodeToReuseId.getOrDefault(node, -1);
		}
	}

	/**
	 * Convert ExecNode tree to string as a tree style.
	 */
	private static class ExecNodeStringTreeBuilder extends ExecNodeVisitorImpl {
		private final StringBuilder sb;
		private final ReuseInfo reuseInfo;
		private final boolean updateVisitedTimes;
		private final List<ExecNode<?>> stopVisitNodes;
		private final boolean includingBorders;

		private List<Boolean> lastChildren = new ArrayList<>();
		private int depth = 0;

		private ExecNodeStringTreeBuilder(
				StringBuilder sb,
				ReuseInfo reuseInfo,
				boolean updateVisitedTimes,
				List<ExecNode<?>> stopVisitNodes,
				boolean includingBorders) {
			this.sb = sb;
			this.reuseInfo = reuseInfo;
			this.updateVisitedTimes = updateVisitedTimes;
			this.stopVisitNodes = stopVisitNodes;
			this.includingBorders = includingBorders;
		}

		@Override
		public void visit(ExecNode<?> node) {
			if (updateVisitedTimes) {
				reuseInfo.addVisitedTimes(node);
			}
			if (depth > 0) {
				lastChildren.subList(0, lastChildren.size() - 1).forEach(isLast -> sb.append(isLast ? "   " : ":  "));
				sb.append(lastChildren.get(lastChildren.size() - 1) ? "+- " : ":- ");
			}
			final int borderIndex = stopVisitNodes.indexOf(node);
			final boolean reachBorder = borderIndex >= 0;
			if (reachBorder && includingBorders) {
				sb.append("[#").append(borderIndex + 1).append("] ");
			}

			final int reuseId = reuseInfo.getReuseId(node);
			final boolean isReuseNode = reuseId >= 0;
			final boolean firstVisited = reuseInfo.isFirstVisited(node);

			if (isReuseNode && !firstVisited) {
				sb.append("Reused");
			} else {
				sb.append(node.getDesc());
			}

			if (isReuseNode) {
				if (firstVisited) {
					sb.append("(reuse_id=[").append(reuseId).append("])");
				} else {
					sb.append("(reference_id=[").append(reuseId).append("])");
				}
			}
			sb.append("\n");

			// whether visit input nodes of current node
			final boolean visitInputs = (firstVisited || !isReuseNode) && !stopVisitNodes.contains(node);
			final List<ExecNode<?>> inputNodes = node.getInputNodes();
			if (visitInputs && inputNodes.size() > 1) {
				inputNodes.subList(0, inputNodes.size() - 1).forEach(input -> {
					depth = depth + 1;
					lastChildren.add(false);
					input.accept(this);
					depth = depth - 1;
					lastChildren = lastChildren.subList(0, lastChildren.size() - 1);
				});
			}
			if (visitInputs && !inputNodes.isEmpty()) {
				depth = depth + 1;
				lastChildren.add(true);
				inputNodes.get(inputNodes.size() - 1).accept(this);
				depth = depth - 1;
				lastChildren = lastChildren.subList(0, lastChildren.size() - 1);
			}
		}
	}

	private ExecNodePlanDumper() {
	}
}
