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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.LongValueSequenceIterator;
import org.apache.flink.util.Preconditions;

/**
 * @see <a href="http://mathworld.wolfram.com/StarGraph.html">Star Graph at Wolfram MathWorld</a>
 */
public class StarGraph
extends GraphGeneratorBase<LongValue, NullValue, NullValue> {

	public static final int MINIMUM_VERTEX_COUNT = 2;

	// Required to create the DataSource
	private final ExecutionEnvironment env;

	// Required configuration
	private final long vertexCount;

	/**
	 * An undirected {@link Graph} with {@code n} vertices where the single
	 * central node has degree {@code n-1}, connecting to the other {@code n-1}
	 * vertices which have degree {@code 1}.
	 *
	 * @param env the Flink execution environment
	 * @param vertexCount number of vertices
	 */
	public StarGraph(ExecutionEnvironment env, long vertexCount) {
		Preconditions.checkArgument(vertexCount >= MINIMUM_VERTEX_COUNT,
			"Vertex count must be at least " + MINIMUM_VERTEX_COUNT);

		this.env = env;
		this.vertexCount = vertexCount;
	}

	@Override
	public Graph<LongValue, NullValue, NullValue> generate() {
		Preconditions.checkState(vertexCount >= 2);

		// Vertices
		DataSet<Vertex<LongValue, NullValue>> vertices = GraphGeneratorUtils.vertexSequence(env, parallelism, vertexCount);

		// Edges
		LongValueSequenceIterator iterator = new LongValueSequenceIterator(1, this.vertexCount - 1);

		DataSet<Edge<LongValue, NullValue>> edges = env
			.fromParallelCollection(iterator, LongValue.class)
				.setParallelism(parallelism)
				.name("Edge iterators")
			.flatMap(new LinkVertexToCenter())
				.setParallelism(parallelism)
				.name("Star graph edges");

		// Graph
		return Graph.fromDataSet(vertices, edges, env);
	}

	@ForwardedFields("*->f0")
	private static class LinkVertexToCenter
	implements FlatMapFunction<LongValue, Edge<LongValue, NullValue>> {

		private LongValue center = new LongValue(0);

		private Edge<LongValue, NullValue> centerToLeaf = new Edge<>(center, null, NullValue.getInstance());

		private Edge<LongValue, NullValue> leafToCenter = new Edge<>(null, center, NullValue.getInstance());

		@Override
		public void flatMap(LongValue leaf, Collector<Edge<LongValue, NullValue>> out)
				throws Exception {
			centerToLeaf.f1 = leaf;
			out.collect(centerToLeaf);

			leafToCenter.f0 = leaf;
			out.collect(leafToCenter);
		}
	}
}
