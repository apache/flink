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

package org.apache.flink.graph.library;

import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.gsa.ApplyFunction;
import org.apache.flink.graph.gsa.GatherFunction;
import org.apache.flink.graph.gsa.SumFunction;
import org.apache.flink.graph.gsa.Neighbor;
import org.apache.flink.types.NullValue;

/**
 * This is an implementation of the Connected Components algorithm, using a gather-sum-apply iteration.
 */
public class GSAConnectedComponents implements
	GraphAlgorithm<Long, Long, NullValue, Graph<Long, Long, NullValue>> {

	private Integer maxIterations;

	public GSAConnectedComponents(Integer maxIterations) {
		this.maxIterations = maxIterations;
	}

	@Override
	public Graph<Long, Long, NullValue> run(Graph<Long, Long, NullValue> graph) throws Exception {

		Graph<Long, Long, NullValue> undirectedGraph = graph.getUndirected();

		// initialize vertex values and run the Vertex Centric Iteration
		return undirectedGraph.runGatherSumApplyIteration(new GatherNeighborIds(), new SelectMinId(), new UpdateComponentId(),
				maxIterations);
	}

	// --------------------------------------------------------------------------------------------
	//  Connected Components UDFs
	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("serial")
	private static final class GatherNeighborIds extends GatherFunction<Long, NullValue, Long> {

		public Long gather(Neighbor<Long, NullValue> neighbor) {
			return neighbor.getNeighborValue();
		}
	};

	@SuppressWarnings("serial")
	private static final class SelectMinId extends SumFunction<Long, NullValue, Long> {

		public Long sum(Long newValue, Long currentValue) {
			return Math.min(newValue, currentValue);
		}
	};

	@SuppressWarnings("serial")
	private static final class UpdateComponentId extends ApplyFunction<Long, Long, Long> {

		public void apply(Long summedValue, Long origValue) {
			if (summedValue < origValue) {
				setResult(summedValue);
			}
		}
	}
}
