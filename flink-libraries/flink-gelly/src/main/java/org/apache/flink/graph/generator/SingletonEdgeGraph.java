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
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.LongValueSequenceIterator;
import org.apache.flink.util.Preconditions;

/**
 * A singleton-edge {@link Graph} contains one or more isolated two-paths.
 */
public class SingletonEdgeGraph
extends GraphGeneratorBase<LongValue, NullValue, NullValue> {

	public static final int MINIMUM_VERTEX_PAIR_COUNT = 1;

	// Required to create the DataSource
	private final ExecutionEnvironment env;

	// Required configuration
	private final long vertexPairCount;

	/**
	 * An undirected {@link Graph} containing one or more isolated two-paths.
	 * The in- and out-degree of every vertex is 1. For {@code n} vertices
	 * there are {@code n/2} components.
	 *
	 * @param env the Flink execution environment
	 * @param vertexPairCount number of pairs of vertices
	 */
	public SingletonEdgeGraph(ExecutionEnvironment env, long vertexPairCount) {
		Preconditions.checkArgument(vertexPairCount >= MINIMUM_VERTEX_PAIR_COUNT,
			"Vertex pair count must be at least " + MINIMUM_VERTEX_PAIR_COUNT);

		this.env = env;
		this.vertexPairCount = vertexPairCount;
	}

	@Override
	public Graph<LongValue, NullValue, NullValue> generate() {
		Preconditions.checkState(vertexPairCount > 0);

		// Vertices
		long vertexCount = 2 * vertexPairCount;

		DataSet<Vertex<LongValue, NullValue>> vertices = GraphGeneratorUtils.vertexSequence(env, parallelism, vertexCount);

		// Edges
		LongValueSequenceIterator iterator = new LongValueSequenceIterator(0, vertexCount - 1);

		DataSet<Edge<LongValue, NullValue>> edges = env
			.fromParallelCollection(iterator, LongValue.class)
				.setParallelism(parallelism)
				.name("Edge iterators")
			.map(new LinkVertexToSingletonNeighbor())
				.setParallelism(parallelism)
				.name("Complete graph edges");

		// Graph
		return Graph.fromDataSet(vertices, edges, env);
	}

	@ForwardedFields("*->f0")
	private static class LinkVertexToSingletonNeighbor
	implements MapFunction<LongValue, Edge<LongValue, NullValue>> {

		private LongValue source = new LongValue();

		private LongValue target = new LongValue();

		private Edge<LongValue, NullValue> edge = new Edge<>(source, target, NullValue.getInstance());

		@Override
		public Edge<LongValue, NullValue> map(LongValue value) throws Exception {
			long val = value.getValue();

			source.setValue(val);

			if (val % 2 == 0) {
				target.setValue(val + 1);
			} else {
				target.setValue(val - 1);
			}

			return edge;
		}
	}
}
