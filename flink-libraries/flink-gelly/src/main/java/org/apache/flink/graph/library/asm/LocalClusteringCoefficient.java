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

package org.apache.flink.graph.library.asm;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.degree.annotate.undirected.VertexDegree;
import org.apache.flink.graph.library.asm.LocalClusteringCoefficient.Result;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MathUtils;

/**
 * The local clustering coefficient measures the connectedness of each vertex's
 * neighborhood. Scores range from 0.0 (no edges between neighbors) to 1.0
 * (neighborhood is a clique).
 * <br/>
 * An edge between a vertex's neighbors is a triangle. Counting edges between
 * neighbors is equivalent to counting the number of triangles which include
 * the vertex.
 * <br/>
 * The algorithm takes a simple, undirected graph as input and outputs a `DataSet`
 * of tuples containing the vertex ID, vertex degree, and number of triangles
 * containing the vertex. The vertex ID must be `Comparable` and `Copyable`.
 * <br/>
 * The input graph must be a simple graph of undirected edges containing
 * no duplicates or self-loops.
 *
 * @param <K> graph ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class LocalClusteringCoefficient<K extends Comparable<K> & CopyableValue<K>, VV, EV>
implements GraphAlgorithm<K, VV, EV, DataSet<Result<K>>> {

	// Optional configuration
	private int littleParallelism = ExecutionConfig.PARALLELISM_UNKNOWN;

	/**
	 * Override the parallelism of operators processing small amounts of data.
	 *
	 * @param littleParallelism operator parallelism
	 * @return this
	 */
	public LocalClusteringCoefficient<K, VV, EV> setLittleParallelism(int littleParallelism) {
		this.littleParallelism = littleParallelism;

		return this;
	}

	@Override
	public DataSet<Result<K>> run(Graph<K, VV, EV> input)
			throws Exception {
		// u, v, w
		DataSet<Tuple3<K,K,K>> triangles = input
			.run(new TriangleListing<K,VV,EV>()
				.setSortTriangleVertices(false)
				.setLittleParallelism(littleParallelism));

		// u
		DataSet<Tuple2<K, LongValue>> triangleVertices = triangles
			.flatMap(new SplitTriangles<K>())
				.name("Split triangle vertices");

		// u, triangle count
		DataSet<Tuple2<K, LongValue>> vertexTriangleCount = triangleVertices
			.groupBy(0)
			.reduce(new CountVertices<K>())
				.name("Count triangles");

		// u, deg(u)
		DataSet<Vertex<K, LongValue>> vertexDegree = input
			.run(new VertexDegree<K, VV, EV>()
				.setParallelism(littleParallelism));

		// u, deg(u), neighbor edge count
		return vertexDegree
			.leftOuterJoin(vertexTriangleCount)
			.where(0)
			.equalTo(0)
			.with(new JoinVertexDegreeWithTriangleCount<K>())
				.setParallelism(littleParallelism)
				.name("Clustering coefficient");
	}

	/**
	 * Emits the three vertex IDs comprising each triangle along with an initial count.
	 *
	 * @param <T> ID type
	 */
	private class SplitTriangles<T>
	implements FlatMapFunction<Tuple3<T, T, T>, Tuple2<T, LongValue>> {
		private Tuple2<T, LongValue> output = new Tuple2<>(null, new LongValue(1));

		@Override
		public void flatMap(Tuple3<T, T, T> value, Collector<Tuple2<T, LongValue>> out)
				throws Exception {
			output.f0 = value.f0;
			out.collect(output);

			output.f0 = value.f1;
			out.collect(output);

			output.f0 = value.f2;
			out.collect(output);
		}
	}

	/**
	 * Combines the count of each vertex ID.
	 *
	 * @param <T> ID type
	 */
	@FunctionAnnotation.ForwardedFields("0")
	private class CountVertices<T>
	implements ReduceFunction<Tuple2<T, LongValue>> {
		@Override
		public Tuple2<T, LongValue> reduce(Tuple2<T, LongValue> left, Tuple2<T, LongValue> right)
				throws Exception {
			LongValue count = left.f1;
			count.setValue(count.getValue() + right.f1.getValue());
			return left;
		}
	}

	/**
	 * Joins the vertex and degree with the vertex's triangle count.
	 *
	 * @param <T> ID type
	 */
	@FunctionAnnotation.ForwardedFieldsFirst("0; 1->1.0")
	@FunctionAnnotation.ForwardedFieldsSecond("0; 1->1.1")
	private class JoinVertexDegreeWithTriangleCount<T>
	implements JoinFunction<Vertex<T, LongValue>, Tuple2<T, LongValue>, Result<T>> {
		private LongValue zero = new LongValue();

		private Result<T> output = new Result<>();

		@Override
		public Result<T> join(Vertex<T, LongValue> vertex_degree, Tuple2<T, LongValue> vertex_triangle_count)
				throws Exception {
			output.f0 = vertex_degree.f0;
			output.f1.f0 = vertex_degree.f1;
			output.f1.f1 = (vertex_triangle_count == null) ? zero : vertex_triangle_count.f1;

			return output;
		}
	}

	/**
	 * Wraps the vertex type to encapsulate results from the Clustering Coefficient algorithm.
	 *
	 * @param <T> ID type
	 */
	public static class Result<T>
	extends Vertex<T, Tuple2<LongValue, LongValue>> {
		public static final int HASH_SEED = 0xc23937c1;

		public Result() {
			f1 = new Tuple2<>();
		}

		/**
		 *
		 *
		 * @return
		 */
		public LongValue getDegree() {
			return f1.f0;
		}

		/**
		 *
		 *
		 * @return
		 */
		public LongValue getTriangleCount() {
			return f1.f1;
		}

		/**
		 *
		 *
		 * @return
		 */
		public double getLocalClusteringCoefficientScore() {
			long degree = getDegree().getValue();
			long neighborPairs = degree * (degree - 1) / 2;

			return getTriangleCount().getValue() / (double) neighborPairs;
		}

		@Override
		public int hashCode() {
			long d = f1.f0.getValue();
			long t = f1.f1.getValue();

			return MathUtils.murmurHash(HASH_SEED, f0.hashCode(), (int)(d >>> 32), (int)d, (int)(t >>> 32), (int)t);
		}
	}
}
