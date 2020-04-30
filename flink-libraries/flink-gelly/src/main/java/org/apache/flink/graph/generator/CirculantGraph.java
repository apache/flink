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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @see <a href="http://mathworld.wolfram.com/CirculantGraph.html">Circulant Graph at Wolfram MathWorld</a>
 */
public class CirculantGraph
extends GraphGeneratorBase<LongValue, NullValue, NullValue> {

	public static final int MINIMUM_VERTEX_COUNT = 2;

	public static final int MINIMUM_OFFSET = 1;

	// Required to create the DataSource
	private final ExecutionEnvironment env;

	// Required configuration
	private final long vertexCount;

	private List<OffsetRange> offsetRanges = new ArrayList<>();

	/**
	 * An oriented {@link Graph} with {@code n} vertices where each vertex
	 * v<sub>i</sub> is connected to vertex v<sub>(i+j)%n</sub> for each
	 * configured offset {@code j}.
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
	 * Required configuration for each range of offsets in the graph.
	 *
	 * @param offset first offset appointing the vertices' position
	 * @param length number of contiguous offsets in range
	 * @return this
	 */
	public CirculantGraph addRange(long offset, long length) {
		Preconditions.checkArgument(offset >= MINIMUM_OFFSET,
			"Range offset must be at least " + MINIMUM_OFFSET);
		Preconditions.checkArgument(length <= vertexCount - offset,
			"Range length must not be greater than the vertex count minus the range offset.");

		offsetRanges.add(new OffsetRange(offset, length));

		return this;
	}

	@Override
	public Graph<LongValue, NullValue, NullValue> generate() {
		// Vertices
		DataSet<Vertex<LongValue, NullValue>> vertices = GraphGeneratorUtils.vertexSequence(env, parallelism, vertexCount);

		// Edges
		LongValueSequenceIterator iterator = new LongValueSequenceIterator(0, this.vertexCount - 1);

		// Validate ranges
		Collections.sort(offsetRanges);
		Iterator<OffsetRange> iter = offsetRanges.iterator();
		OffsetRange lastRange = iter.next();

		while (iter.hasNext()) {
			OffsetRange nextRange = iter.next();

			if (lastRange.overlaps(nextRange)) {
				throw new IllegalArgumentException("Overlapping ranges " + lastRange + " and " + nextRange);
			}

			lastRange = nextRange;
		}

		DataSet<Edge<LongValue, NullValue>> edges = env
			.fromParallelCollection(iterator, LongValue.class)
				.setParallelism(parallelism)
				.name("Edge iterators")
			.flatMap(new LinkVertexToOffsets(vertexCount, offsetRanges))
				.setParallelism(parallelism)
				.name("Circulant graph edges");

		// Graph
		return Graph.fromDataSet(vertices, edges, env);
	}

	@FunctionAnnotation.ForwardedFields("*->f0")
	private static class LinkVertexToOffsets
	implements FlatMapFunction<LongValue, Edge<LongValue, NullValue>> {
		private final long vertexCount;

		private final List<OffsetRange> offsetRanges;

		private LongValue target = new LongValue();

		private Edge<LongValue, NullValue> edge = new Edge<>(null, target, NullValue.getInstance());

		public LinkVertexToOffsets(long vertexCount, List<OffsetRange> offsetRanges) {
			this.vertexCount = vertexCount;
			this.offsetRanges = offsetRanges;
		}

		@Override
		public void flatMap(LongValue source, Collector<Edge<LongValue, NullValue>> out)
				throws Exception {
			edge.f0 = source;
			long sourceID = source.getValue();

			for (OffsetRange offsetRange : offsetRanges) {
				long targetID = sourceID + offsetRange.getOffset();

				for (long i = offsetRange.getLength(); i > 0; i--) {
					// add positive offset
					target.setValue(targetID++ % vertexCount);
					out.collect(edge);
				}
			}
		}
	}

	/**
	 * Stores the start offset and length configuration for an offset range.
	 */
	public static class OffsetRange implements Serializable, Comparable<OffsetRange> {
		private long offset;

		private long length;

		/**
		 * Construct a range with the given offset and length.
		 *
		 * @param offset the range offset
		 * @param length the range length
		 */
		public OffsetRange(long offset, long length) {
			this.offset = offset;
			this.length = length;
		}

		/**
		 * Get the range offset.
		 *
		 * @return the offset
		 */
		public long getOffset() {
			return offset;
		}

		/**
		 * Get the range length.
		 *
		 * @return the length
		 */
		public long getLength() {
			return length;
		}

		/**
		 * Get the offset of the last index in the range.
		 *
		 * @return last offset
		 */
		public long getLastOffset() {
			return offset + length - 1;
		}

		/**
		 * Return true if and only if the other range and this range share a
		 * common offset ID.
		 *
		 * @param other other range
		 * @return whether ranges are overlapping
		 */
		public boolean overlaps(OffsetRange other) {
			boolean overlapping = false;

			long lastOffset = getLastOffset();
			long otherLastOffset = other.getLastOffset();

			// check whether this range contains other
			overlapping |= (offset <= other.offset && other.offset <= lastOffset);
			overlapping |= (offset <= otherLastOffset && otherLastOffset <= lastOffset);

			// check whether other contains this range
			overlapping |= (other.offset <= offset && offset <= otherLastOffset);
			overlapping |= (other.offset <= lastOffset && lastOffset <= otherLastOffset);

			return overlapping;
		}

		@Override
		public String toString() {
			return Long.toString(offset) + ":" + Long.toString(length);
		}

		@Override
		public int compareTo(OffsetRange o) {
			int cmp = Long.compare(offset, o.offset);
			if (cmp != 0) {
				return cmp;
			}

			return Long.compare(length, o.length);
		}
	}
}
