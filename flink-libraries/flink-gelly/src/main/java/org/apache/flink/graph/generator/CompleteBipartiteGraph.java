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

package org.apache.flink.graph.generator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.bipartite.BipartiteEdge;
import org.apache.flink.graph.bipartite.BipartiteGraph;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

/**
 * Generate a complete bipartate graph where every top node is connected to every bottom node.
 */
public class CompleteBipartiteGraph
	implements BipartiteGraphGenerator<LongValue, LongValue, NullValue, NullValue, NullValue> {

	// Required to create the DataSource
	private final ExecutionEnvironment env;

	// Required configuration
	private final long topVertexCount;
	private final long bottomVertexCount;

	private int parallelism = 1;

	public CompleteBipartiteGraph(ExecutionEnvironment env, long topVertexCount, long bottomVertexCount) {
		this.env = env;
		this.topVertexCount = topVertexCount;
		this.bottomVertexCount = bottomVertexCount;
	}

	@Override
	public BipartiteGraph<LongValue, LongValue, NullValue, NullValue, NullValue> generate() {
		DataSet<Vertex<LongValue, NullValue>> topVertices
			= GraphGeneratorUtils.vertexSequence(env, parallelism, topVertexCount);
		DataSet<Vertex<LongValue, NullValue>> bottomVertices
			= GraphGeneratorUtils.vertexSequence(env, parallelism, bottomVertexCount);

		DataSet<BipartiteEdge<LongValue, LongValue, NullValue>> edges = topVertices.cross(bottomVertices)
			.setParallelism(parallelism)
			.map(new EdgeGenerator())
			.setParallelism(parallelism);

		return BipartiteGraph.fromDataSet(topVertices, bottomVertices, edges, env);
	}

	@Override
	public BipartiteGraphGenerator<LongValue, LongValue, NullValue, NullValue, NullValue> setParallelism(int parallelism) {
		this.parallelism = parallelism;
		return this;
	}

	private static class EdgeGenerator
		implements MapFunction<Tuple2<Vertex<LongValue, NullValue>, Vertex<LongValue, NullValue>>, BipartiteEdge<LongValue, LongValue, NullValue>> {

		private BipartiteEdge<LongValue, LongValue, NullValue> edge = new BipartiteEdge<>();

		@Override
		public BipartiteEdge<LongValue, LongValue, NullValue> map(Tuple2<Vertex<LongValue, NullValue>, Vertex<LongValue, NullValue>> value) throws Exception {
			edge.setTopId(value.f0.getId());
			edge.setBottomId(value.f1.getId());
			return edge;
		}
	}
}
