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

package org.apache.flink.graph.asm.degree.annotate.undirected;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.CachingGraphAlgorithm;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.degree.annotate.DegreeAnnotationFunctions.DegreeCount;
import org.apache.flink.graph.asm.degree.annotate.DegreeAnnotationFunctions.MapEdgeToSourceLabel;
import org.apache.flink.graph.asm.degree.annotate.DegreeAnnotationFunctions.MapEdgeToTargetLabel;
import org.apache.flink.types.LongValue;

/**
 * Annotates vertices of an undirected graph with the degree count.
 *
 * @param <K> graph label type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class VertexDegree<K, VV, EV>
extends CachingGraphAlgorithm<K, VV, EV, DataSet<Vertex<K,LongValue>>> {

	// Optional configuration
	private boolean reduceOnTargetLabel = false;

	private int parallelism = ExecutionConfig.PARALLELISM_UNKNOWN;

	/**
	 * The degree can be counted from either the edge source or target labels.
	 * By default the source labels are counted. Reducing on target labels may
	 * optimize the algorithm if the input edge list is sorted by target label.
	 *
	 * @param reduceOnTargetLabel set to {@code true} if the input edge list
	 *                            is sorted by target label
	 * @return this
	 */
	public VertexDegree<K,VV,EV> setReduceOnTargetLabel(boolean reduceOnTargetLabel) {
		this.reduceOnTargetLabel = reduceOnTargetLabel;

		return this;
	}

	/**
	 * Override the operator parallelism.
	 *
	 * @param parallelism operator parallelism
	 * @return this
	 */
	public VertexDegree<K,VV,EV> setParallelism(int parallelism) {
		this.parallelism = parallelism;

		return this;
	}

	@Override
	protected String getAlgorithmName() {
		return VertexDegree.class.getCanonicalName();
	}

	@Override
	public DataSet<Vertex<K,LongValue>> runInternal(Graph<K,VV,EV> input)
			throws Exception {
		MapFunction<Edge<K,EV>, Vertex<K,LongValue>> mapEdgeToLabel = reduceOnTargetLabel ?
			new MapEdgeToTargetLabel<K,EV>() : new MapEdgeToSourceLabel<K,EV>();

		// v
		DataSet<Vertex<K,LongValue>> vertexLabels = input
			.getEdges()
			.map(mapEdgeToLabel)
				.setParallelism(parallelism)
				.name("Map edge to vertex label");

		// v, deg(v)
		return vertexLabels
			.groupBy(0)
			.reduce(new DegreeCount<K>())
				.setParallelism(parallelism)
				.name("Degree count");
	}
}
