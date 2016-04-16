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

package org.apache.flink.graph.asm.degree.annotate.directed;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.CachingGraphAlgorithm;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.degree.annotate.DegreeAnnotationFunctions.DegreeCount;
import org.apache.flink.graph.asm.degree.annotate.DegreeAnnotationFunctions.JoinVertexWithVertexDegree;
import org.apache.flink.graph.asm.degree.annotate.DegreeAnnotationFunctions.MapEdgeToTargetLabel;
import org.apache.flink.types.LongValue;

/**
 * Annotates vertices of a directed graph with the in-degree count.
 *
 * @param <K> graph label type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class VertexInDegree<K, VV, EV>
extends CachingGraphAlgorithm<K, VV, EV, DataSet<Vertex<K,LongValue>>> {

	// Optional configuration
	private int parallelism = ExecutionConfig.PARALLELISM_UNKNOWN;

	/**
	 * Override the operator parallelism.
	 *
	 * @param parallelism operator parallelism
	 * @return this
	 */
	public VertexInDegree<K,VV,EV> setParallelism(int parallelism) {
		this.parallelism = parallelism;

		return this;
	}

	@Override
	protected String getAlgorithmName() {
		return VertexInDegree.class.getCanonicalName();
	}

	@Override
	public DataSet<Vertex<K,LongValue>> runInternal(Graph<K,VV,EV> input)
			throws Exception {
		// t
		DataSet<Vertex<K,LongValue>> targetLabels = input
			.getEdges()
			.map(new MapEdgeToTargetLabel<K,EV>())
				.setParallelism(parallelism)
				.name("Map edge to target label");

		// t, deg(t)
		DataSet<Vertex<K,LongValue>> targetDegree = targetLabels
			.groupBy(0)
			.reduce(new DegreeCount<K>())
				.setParallelism(parallelism)
				.name("Degree count");

		return input
			.getVertices()
			.leftOuterJoin(targetDegree)
			.where(0)
			.equalTo(0)
			.with(new JoinVertexWithVertexDegree<K,VV>())
				.setParallelism(parallelism)
				.name("Join vertices");
	}
}
