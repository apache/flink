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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.gsa.ApplyFunction;
import org.apache.flink.graph.gsa.GatherFunction;
import org.apache.flink.graph.gsa.SumFunction;
import org.apache.flink.graph.gsa.Neighbor;
import org.apache.flink.graph.utils.NullValueEdgeMapper;
import org.apache.flink.types.NullValue;

/**
 * This is an implementation of the Connected Components algorithm, using a gather-sum-apply iteration.
 * This implementation assumes that the vertices of the input Graph are initialized with unique, Long component IDs.
 * The result is a DataSet of vertices, where the vertex value corresponds to the assigned component ID.
 * 
 * @see org.apache.flink.graph.library.ConnectedComponents
 */
public class GSAConnectedComponents<K, EV> implements GraphAlgorithm<K, Long, EV, DataSet<Vertex<K, Long>>> {

	private Integer maxIterations;

	/**
	 * Creates an instance of the GSA Connected Components algorithm.
	 * The algorithm computes weakly connected components
	 * and converges when no vertex updates its component ID
	 * or when the maximum number of iterations has been reached.
	 * 
	 * @param maxIterations The maximum number of iterations to run.
	 */
	public GSAConnectedComponents(Integer maxIterations) {
		this.maxIterations = maxIterations;
	}

	@Override
	public DataSet<Vertex<K, Long>> run(Graph<K, Long, EV> graph) throws Exception {

		Graph<K, Long, NullValue> undirectedGraph = graph.mapEdges(new NullValueEdgeMapper<K, EV>())
				.getUndirected();

		// initialize vertex values and run the Vertex Centric Iteration
		return undirectedGraph.runGatherSumApplyIteration(
				new GatherNeighborIds(), new SelectMinId(), new UpdateComponentId<K>(),
				maxIterations).getVertices();
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
	private static final class UpdateComponentId<K> extends ApplyFunction<K, Long, Long> {

		public void apply(Long summedValue, Long origValue) {
			if (summedValue < origValue) {
				setResult(summedValue);
			}
		}
	}
}
