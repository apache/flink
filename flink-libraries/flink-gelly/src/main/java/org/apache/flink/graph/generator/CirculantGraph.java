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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.LongValueSequenceIterator;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;

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

	private List<Tuple2<Long, Long>> startOffsetPairs = new ArrayList<>();

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
	 * @param startOffset first offset appointing the vertices' position should be linked by any vertex
	 * @param length offset in [startOffset, startOffset + length) will be added
	 * @return this
	 */
	public CirculantGraph addOffsets(long startOffset, long length) {
		long maxOffset = vertexCount / 2;

		Preconditions.checkArgument(startOffset >= MINIMUM_OFFSET,
			"Offset must be at least " + MINIMUM_OFFSET);
		Preconditions.checkArgument(startOffset + length - 1 <= maxOffset,
			"Offset must be at most " + maxOffset);

		// save startOffset and length pair
		startOffsetPairs.add(new Tuple2<>(startOffset, length));

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
				.flatMap(new LinkVertexToOffsets(vertexCount, startOffsetPairs))
				.setParallelism(parallelism)
				.name("Circulant graph edges");

		// Graph
		return Graph.fromDataSet(vertices, edges, env);
	}

	@FunctionAnnotation.ForwardedFields("*->f0")
	private static class LinkVertexToOffsets
			implements FlatMapFunction<LongValue, Edge<LongValue, NullValue>> {

		private final long vertexCount;

		private final List<Tuple2<Long, Long>> startOffsetPairs;

		private LongValue target = new LongValue();

		private Edge<LongValue, NullValue> edge = new Edge<>(null, target, NullValue.getInstance());

		public LinkVertexToOffsets(long vertexCount, List<Tuple2<Long, Long>> startOffsetPairs) {
			this.vertexCount = vertexCount;
			this.startOffsetPairs = startOffsetPairs;
		}

		@Override
		public void flatMap(LongValue source, Collector<Edge<LongValue, NullValue>> out)
				throws Exception {
			edge.f0 = source;

			long index = source.getValue();
			long maxOffset = vertexCount / 2;

			for (Tuple2<Long, Long> offsetPair : startOffsetPairs) {
				Long startOffset = offsetPair.f0;
				Long length = offsetPair.f1;

				for (int i = 0; i < length; i++) {
					long offset = startOffset + i;

					// add positive offset
					target.setValue((index + offset + vertexCount) % vertexCount);
					out.collect(edge);

					// add negative offset, ignore negative max offset when vertex count is even
					if (!(vertexCount % 2 == 0 && offset == maxOffset)) {
						target.setValue((index - offset + vertexCount) % vertexCount);
						out.collect(edge);
					}
				}
			}
		}
	}
}
