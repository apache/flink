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
 * Evenly graph means every {@link Vertex} in the {@link Graph} has the same degree.
 * when vertex degree is 0, {@link EmptyGraph} will be generated.
 * when vertex degree is vertex count - 1, {@link CompleteGraph} will be generated.
 */
public class EvenlyGraph
extends AbstractGraphGenerator<LongValue, NullValue, NullValue> {

	public static final int MINIMUM_VERTEX_COUNT = 1;

	public static final int MINIMUM_VERTEX_DEGREE = 0;

	// Required to create the DataSource
	private final ExecutionEnvironment env;

	// Required configuration
	private long vertexCount;

	private long vertexDegree;

	/**
	 * An undirected {@link Graph} whose vertices have the same degree.
	 *
	 * @param env the Flink execution environment
	 * @param vertexCount number of vertices
	 * @param vertexDegree degree of vertices
	 */
	public EvenlyGraph(ExecutionEnvironment env, long vertexCount,  long vertexDegree) {
		Preconditions.checkArgument(vertexCount >= MINIMUM_VERTEX_COUNT,
			"Vertex count must be at least " + MINIMUM_VERTEX_COUNT);
		Preconditions.checkArgument(vertexDegree >= MINIMUM_VERTEX_DEGREE,
				"Vertex degree must be at least " + MINIMUM_VERTEX_DEGREE);
		Preconditions.checkArgument(vertexCount % 2 == 0 || vertexDegree % 2 == 0,
				"Vertex degree must be even when vertex count is odd number");

		this.env = env;
		this.vertexCount = vertexCount;
		this.vertexDegree = vertexDegree;
	}

	@Override
	public Graph<LongValue, NullValue, NullValue> generate() {
		// Vertices
		DataSet<Vertex<LongValue, NullValue>> vertices = GraphGeneratorUtils.vertexSequence(env, parallelism, vertexCount);

		// Edges
		LongValueSequenceIterator iterator = new LongValueSequenceIterator(0, this.vertexCount - 1);

		DataSet<Edge<LongValue, NullValue>> edges = env
			.fromParallelCollection(iterator, LongValue.class)
				.setParallelism(parallelism)
				.name("Edge iterators")
			.flatMap(new LinkVertexToOpposite(vertexCount, vertexDegree))
				.setParallelism(parallelism)
				.name("Evenly graph edges");

		// Graph
		return Graph.fromDataSet(vertices, edges, env);
	}

	@ForwardedFields("*->f0")
	public class LinkVertexToOpposite
	implements FlatMapFunction<LongValue, Edge<LongValue, NullValue>> {

		private final long vertexCount;

		private final long vertexDegree;

		private LongValue target = new LongValue();

		private Edge<LongValue, NullValue> edge = new Edge<>(null, target, NullValue.getInstance());

		public LinkVertexToOpposite(long vertex_count, long vertexDegree) {
			this.vertexCount = vertex_count;
			this.vertexDegree = vertexDegree;
		}

		@Override
		public void flatMap(LongValue source, Collector<Edge<LongValue, NullValue>> out)
				throws Exception {
			// empty graph
			if (vertexDegree == 0) {
				return;
			}

			edge.f0 = source;

			// get opposite vertex's id
			long opposite = (source.getValue() + vertexCount / 2) % vertexCount;

			// link to opposite vertex if possible
			if (vertexCount % 2 == 0 && vertexDegree % 2 == 1) {
				target.setValue(opposite);
				out.collect(edge);
			}

			// link to vertices on both sides of opposite
			long initialOffset = (vertexCount + 1) % 2;
			long oneSideVertexCount = (vertexDegree - vertexCount % 2) / 2;
			for (long i = initialOffset; i <= oneSideVertexCount; i++) {
				long previous = (opposite - i + vertexCount) % vertexCount;
				long next = (opposite + i + vertexCount % 2) % vertexCount;

				target.setValue(previous);
				out.collect(edge);

				target.setValue(next);
				out.collect(edge);
			}
		}
	}
}
