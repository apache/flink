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

	private final Set<ExecNode<?, ?>> boundaries;

	/**
	 * Create a {@link InputOrderCalculator} for the given {@link ExecNode} sub-graph.
	 *
	 * @param root            the output node of the sub-graph
	 * @param boundaries      the first layer of nodes on the input side of the sub-graph
	 * @param safeDamBehavior when checking for conflicts we'll ignore the edges with
	 *                        {@link ExecEdge.DamBehavior} stricter or equal than this
	 */
	public InputOrderCalculator(
			ExecNode<?, ?> root,
			Set<ExecNode<?, ?>> boundaries,
			ExecEdge.DamBehavior safeDamBehavior) {
		super(Collections.singletonList(root), boundaries, safeDamBehavior);
		this.boundaries = boundaries;
	}

	public Map<ExecNode<?, ?>, Integer> calculate() {
		createTopologyGraph();
		Map<ExecNode<?, ?>, Integer> distances = graph.calculateMaximumDistance();

		// extract only the distances of the boundaries and renumbering the distances
		// so that the smallest value starts from 0
		// the smaller the distance, the higher the priority
		Set<Integer> boundaryDistanceSet = new HashSet<>();
		for (ExecNode<?, ?> boundary : boundaries) {
			boundaryDistanceSet.add(distances.getOrDefault(boundary, 0));
		}
		List<Integer> boundaryDistanceList = new ArrayList<>(boundaryDistanceSet);
		Collections.sort(boundaryDistanceList);

		Map<ExecNode<?, ?>, Integer> results = new HashMap<>();
		for (ExecNode<?, ?> boundary : boundaries) {
			results.put(boundary, boundaryDistanceList.indexOf(distances.get(boundary)));
		}
		return results;
	}

	@Override
	protected void resolveInputPriorityConflict(ExecNode<?, ?> node, int higherInput, int lowerInput) {
		throw new IllegalStateException("A conflict is detected. This is unexpected.");
	}
}
