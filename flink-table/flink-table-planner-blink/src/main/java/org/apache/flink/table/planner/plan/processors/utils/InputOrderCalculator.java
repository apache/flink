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

package org.apache.flink.table.planner.plan.processors.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.plan.nodes.exec.AbstractExecNodeExactlyOnceVisitor;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Subclass of the {@link InputPriorityGraphGenerator}.
 *
 * <p>This class only calculates the input order for the given boundary nodes
 * and will throw exception when a conflict is detected.
 */
@Internal
public class InputOrderCalculator extends InputPriorityGraphGenerator {

	private final Set<ExecNode<?>> boundaries;

	/**
	 * Create a {@link InputOrderCalculator} for the given {@link ExecNode} sub-graph.
	 *
	 * @param root            the output node of the sub-graph
	 * @param boundaries      the first layer of nodes on the input side of the sub-graph
	 * @param safeDamBehavior when checking for conflicts we'll ignore the edges with
	 *                        {@link ExecEdge.DamBehavior} stricter or equal than this
	 */
	public InputOrderCalculator(
			ExecNode<?> root,
			Set<ExecNode<?>> boundaries,
			ExecEdge.DamBehavior safeDamBehavior) {
		super(Collections.singletonList(root), boundaries, safeDamBehavior);
		this.boundaries = boundaries;
	}

	public Map<ExecNode<?>, Integer> calculate() {
		createTopologyGraph();

		// some boundaries node may be connected from the outside of the sub-graph,
		// which we cannot deduce by the above process,
		// so we need to check each pair of boundaries and see if they're related
		dealWithPossiblyRelatedBoundaries();
		Map<ExecNode<?>, Integer> distances = graph.calculateMaximumDistance();

		// extract only the distances of the boundaries and renumbering the distances
		// so that the smallest value starts from 0
		// the smaller the distance, the higher the priority
		Set<Integer> boundaryDistanceSet = new HashSet<>();
		for (ExecNode<?> boundary : boundaries) {
			boundaryDistanceSet.add(distances.getOrDefault(boundary, 0));
		}
		List<Integer> boundaryDistanceList = new ArrayList<>(boundaryDistanceSet);
		Collections.sort(boundaryDistanceList);

		Map<ExecNode<?>, Integer> results = new HashMap<>();
		for (ExecNode<?> boundary : boundaries) {
			results.put(boundary, boundaryDistanceList.indexOf(distances.get(boundary)));
		}
		return results;
	}

	private void dealWithPossiblyRelatedBoundaries() {
		List<ExecNode<?>> boundaries = new ArrayList<>(this.boundaries);
		for (int i = 0; i < boundaries.size(); i++) {
			ExecNode<?> boundaryA = boundaries.get(i);
			for (int j = i + 1; j < boundaries.size(); j++) {
				ExecNode<?> boundaryB = boundaries.get(j);
				// if boundaries are already comparable in the topology graph
				// we do not need to check them
				if (graph.canReach(boundaryA, boundaryB) || graph.canReach(boundaryB, boundaryA)) {
					continue;
				}
				dealWithPossiblyRelatedBoundaries(boundaryA, boundaryB);
			}
		}
	}

	private void dealWithPossiblyRelatedBoundaries(ExecNode<?> boundaryA, ExecNode<?> boundaryB) {
		Set<ExecNode<?>> ancestorsA = calculateAllAncestors(boundaryA);
		Set<ExecNode<?>> ancestorsB = calculateAllAncestors(boundaryB);
		if (checkPipelinedPath(boundaryA, ancestorsB)) {
			// boundary A and B are related, and there exists a path
			// which only goes through PIPELINED edges from their public ancestor to boundary A.
			// this means that the priority of boundary B should be at least as low as A
			graph.makeAsFarAs(boundaryB, boundaryA);
		}
		if (checkPipelinedPath(boundaryB, ancestorsA)) {
			// similar situation with above
			graph.makeAsFarAs(boundaryA, boundaryB);
		}
	}

	private static Set<ExecNode<?>> calculateAllAncestors(ExecNode<?> node) {
		Set<ExecNode<?>> ret = new HashSet<>();
		AbstractExecNodeExactlyOnceVisitor visitor = new AbstractExecNodeExactlyOnceVisitor() {
			@Override
			protected void visitNode(ExecNode<?> node) {
				ret.add(node);
				visitInputs(node);
			}
		};
		node.accept(visitor);
		return ret;
	}

	@VisibleForTesting
	static boolean checkPipelinedPath(ExecNode<?> node, Set<ExecNode<?>> goals) {
		PipelinedPathChecker checker = new PipelinedPathChecker(goals);
		node.accept(checker);
		return checker.res;
	}

	private static class PipelinedPathChecker extends AbstractExecNodeExactlyOnceVisitor {
		private final Set<ExecNode<?>> goals;
		private boolean res;

		private PipelinedPathChecker(Set<ExecNode<?>> goals) {
			this.goals = goals;
			this.res = false;
		}

		@Override
		protected void visitNode(ExecNode<?> node) {
			if (goals.contains(node)) {
				res = true;
				return;
			}

			List<ExecEdge> inputEdges = node.getInputEdges();
			for (int i = 0; i < inputEdges.size(); i++) {
				if (inputEdges.get(i).getDamBehavior().stricterOrEqual(ExecEdge.DamBehavior.END_INPUT)) {
					continue;
				}
				visit(node.getInputNodes().get(i));
				if (res) {
					return;
				}
			}
		}
	}

	@Override
	protected void resolveInputPriorityConflict(ExecNode<?> node, int higherInput, int lowerInput) {
		throw new IllegalStateException(
			"A conflict is detected. This is a bug. Please file an issue.\n" +
				"To work around this bug, please set " +
				OptimizerConfigOptions.TABLE_OPTIMIZER_MULTIPLE_INPUT_ENABLED.key() +
				" to false to disable multiple input operator.");
	}
}
