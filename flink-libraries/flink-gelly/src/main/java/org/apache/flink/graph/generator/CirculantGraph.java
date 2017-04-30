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
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.LongValueSequenceIterator;
import org.apache.flink.util.Preconditions;

import java.util.HashSet;
import java.util.Set;

/*
 * @see <a href="http://mathworld.wolfram.com/CirculantGraph.html">Circulant Graph at Wolfram MathWorld</a>
 */
public class CirculantGraph
extends AbstractGraphGenerator<LongValue, NullValue, NullValue> {

	public static final int MINIMUM_VERTEX_COUNT = 1;

	public static final int MINIMUM_OFFSET = 1;

	// Required to create the DataSource
	private final ExecutionEnvironment env;

	// Required configuration
	private long vertexCount;

	private Set<Long> signedOffsets = new HashSet<>();

	/**
	 * An undirected {@link Graph} whose {@link Vertex} connects to targets appointed by an offset list.
	 *
	 * @param env the Flink execution environment
	 * @param vertexCount number of vertices
	 */
	public CirculantGraph(ExecutionEnvironment env, long vertexCount) {
		Preconditions.checkArgument(vertexCount >= MINIMUM_VERTEX_COUNT,
			"Vertex count must be at least " + MINIMUM_VERTEX_COUNT);

		this.env = env;
		this.vertexCount = vertexCount;
	}

	/**
	 * Required configuration for each offset of the graph.
	 *
	 * @param offset appoint the vertices' position should be linked by any vertex
	 * @return this
	 */
	public CirculantGraph addOffset(long offset) {
		long maxOffset = vertexCount / 2;
		Preconditions.checkArgument(offset >= MINIMUM_OFFSET,
			"Offset must be at least " + MINIMUM_OFFSET);
		Preconditions.checkArgument(offset <= maxOffset,
			"Offset must be at most " + maxOffset);

		// add sign, ignore negative max offset when vertex count is even
		signedOffsets.add(offset);
		if (!(vertexCount % 2 == 0 && offset == maxOffset)) {
			signedOffsets.add(-offset);
		}

		return this;
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
				.flatMap(new LinkVertexToOffsets(vertexCount, signedOffsets))
				.setParallelism(parallelism)
				.name("Circulant graph edges");

		// Graph
		return Graph.fromDataSet(vertices, edges, env);
	}

	@FunctionAnnotation.ForwardedFields("*->f0")
	private static class LinkVertexToOffsets
			implements FlatMapFunction<LongValue, Edge<LongValue, NullValue>> {

		private final long vertexCount;

		private final Set<Long> offsets;

		private LongValue target = new LongValue();

		private Edge<LongValue, NullValue> edge = new Edge<>(null, target, NullValue.getInstance());

		public LinkVertexToOffsets(long vertexCount, Set<Long> offsets) {
			this.vertexCount = vertexCount;
			this.offsets = offsets;
		}

		@Override
		public void flatMap(LongValue source, Collector<Edge<LongValue, NullValue>> out)
				throws Exception {
			edge.f0 = source;

			// link to offset vertex
			long index = source.getValue();
			for (long offset : offsets) {
				target.setValue((index + offset + vertexCount) % vertexCount);
				out.collect(edge);
			}
		}
	}
}
