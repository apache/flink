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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.windowing.StreamDiscretizer;
import org.apache.flink.streaming.api.operators.windowing.WindowFlattener;
import org.apache.flink.streaming.api.operators.windowing.WindowMerger;
import org.apache.flink.streaming.runtime.partitioner.DistributePartitioner;

public class WindowingOptimizer {

	public static void optimizeGraph(StreamGraph streamGraph) {

		// Share common discrtizers
		setDiscretizerReuse(streamGraph);

		// Remove unnecessary merges before flatten operators
		removeMergeBeforeFlatten(streamGraph);
	}

	@SuppressWarnings("rawtypes")
	private static void removeMergeBeforeFlatten(StreamGraph streamGraph) {
		Set<Tuple2<Integer, StreamOperator<?, ?>>> operators = streamGraph.getOperators();
		List<Integer> flatteners = new ArrayList<Integer>();

		for (Tuple2<Integer, StreamOperator<?, ?>> entry : operators) {
			if (entry.f1 instanceof WindowFlattener) {
				flatteners.add(entry.f0);
			}
		}

		for (Integer flattenerID : flatteners) {
			// Flatteners should have exactly one input
			StreamNode input = streamGraph.getStreamNode(flattenerID).getInEdges().get(0)
					.getSourceVertex();

			// Check whether the flatten is applied after a merge
			if (input.getOperator() instanceof WindowMerger) {

				// Mergers should have exactly one input
				StreamNode mergeInput = input.getInEdges().get(0).getSourceVertex();

				// We connect the merge input to the flattener directly
				streamGraph.addEdge(mergeInput.getID(), flattenerID,
						new DistributePartitioner(true), 0, new ArrayList<String>());

				// If the merger is only connected to the flattener we delete it
				// completely, otherwise we only remove the edge
				if (input.getOutEdges().size() > 1) {
					streamGraph.removeEdge(streamGraph.getEdge(input.getID(), flattenerID));
				} else {
					streamGraph.removeVertex(input);
				}

				streamGraph.setParallelism(flattenerID, mergeInput.getParallelism());
			}
		}

	}

	private static void setDiscretizerReuse(StreamGraph streamGraph) {

		Set<Tuple2<Integer, StreamOperator<?, ?>>> operators = streamGraph.getOperators();
		List<Tuple2<Integer, StreamDiscretizer<?>>> discretizers = new ArrayList<Tuple2<Integer, StreamDiscretizer<?>>>();

		// Get the discretizers
		for (Tuple2<Integer, StreamOperator<?, ?>> entry : operators) {
			if (entry.f1 instanceof StreamDiscretizer) {
				discretizers.add(new Tuple2<Integer, StreamDiscretizer<?>>(entry.f0,
						(StreamDiscretizer<?>) entry.f1));
			}
		}

		List<Tuple2<StreamDiscretizer<?>, List<Integer>>> matchingDiscretizers = new ArrayList<Tuple2<StreamDiscretizer<?>, List<Integer>>>();

		for (Tuple2<Integer, StreamDiscretizer<?>> discretizer : discretizers) {
			boolean inMatching = false;
			for (Tuple2<StreamDiscretizer<?>, List<Integer>> matching : matchingDiscretizers) {
				Set<Integer> discretizerInEdges = new HashSet<Integer>(streamGraph.getStreamNode(
						discretizer.f0).getInEdgeIndices());
				Set<Integer> matchingInEdges = new HashSet<Integer>(streamGraph.getStreamNode(
						matching.f1.get(0)).getInEdgeIndices());

				if (discretizer.f1.equals(matching.f0)
						&& discretizerInEdges.equals(matchingInEdges)) {
					matching.f1.add(discretizer.f0);
					inMatching = true;
					break;
				}
			}
			if (!inMatching) {
				List<Integer> matchingNames = new ArrayList<Integer>();
				matchingNames.add(discretizer.f0);
				matchingDiscretizers.add(new Tuple2<StreamDiscretizer<?>, List<Integer>>(
						discretizer.f1, matchingNames));
			}
		}

		for (Tuple2<StreamDiscretizer<?>, List<Integer>> matching : matchingDiscretizers) {
			List<Integer> matchList = matching.f1;
			if (matchList.size() > 1) {
				Integer first = matchList.get(0);
				for (int i = 1; i < matchList.size(); i++) {
					replaceDiscretizer(streamGraph, matchList.get(i), first);
				}
			}
		}
	}

	private static void replaceDiscretizer(StreamGraph streamGraph, Integer toReplaceID,
			Integer replaceWithID) {
		// Convert to array to create a copy
		List<StreamEdge> outEdges = new ArrayList<StreamEdge>(streamGraph.getStreamNode(toReplaceID)
				.getOutEdges());

		int numOutputs = outEdges.size();

		// Reconnect outputs
		for (int i = 0; i < numOutputs; i++) {
			StreamEdge outEdge = outEdges.get(i);

			streamGraph.addEdge(replaceWithID, outEdge.getTargetID(), outEdge.getPartitioner(), 0,
					new ArrayList<String>());
		}

		// Remove the other discretizer
		streamGraph.removeVertex(streamGraph.getStreamNode(toReplaceID));
	}
}
