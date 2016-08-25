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

package org.apache.flink.graph.library.clustering.undirected;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.base.ReduceOperatorBase.CombineHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.degree.annotate.undirected.VertexDegree;
import org.apache.flink.graph.library.clustering.undirected.LocalClusteringCoefficient.Result;
import org.apache.flink.graph.utils.Murmur3_32;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmDelegatingDataSet;
import org.apache.flink.graph.utils.proxy.OptionalBoolean;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_DEFAULT;

/**
 * The local clustering coefficient measures the connectedness of each vertex's
 * neighborhood. Scores range from 0.0 (no edges between neighbors) to 1.0
 * (neighborhood is a clique).
 * <br/>
 * An edge between a vertex's neighbors is a triangle. Counting edges between
 * neighbors is equivalent to counting the number of triangles which include
 * the vertex.
 * <br/>
 * The input graph must be a simple, undirected graph containing no duplicate
 * edges or self-loops.
 *
 * @param <K> graph ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class LocalClusteringCoefficient<K extends Comparable<K> & CopyableValue<K>, VV, EV>
extends GraphAlgorithmDelegatingDataSet<K, VV, EV, Result<K>> {

	// Optional configuration
	private OptionalBoolean includeZeroDegreeVertices = new OptionalBoolean(true, true);

	private int littleParallelism = PARALLELISM_DEFAULT;

	/**
	 * By default the vertex set is checked for zero degree vertices. When this
	 * flag is disabled only clustering coefficient scores for vertices with
	 * a degree of a least one will be produced.
	 *
	 * @param includeZeroDegreeVertices whether to output scores for vertices
	 *                                  with a degree of zero
	 * @return this
	 */
	public LocalClusteringCoefficient<K, VV, EV> setIncludeZeroDegreeVertices(boolean includeZeroDegreeVertices) {
		this.includeZeroDegreeVertices.set(includeZeroDegreeVertices);

		return this;
	}

	/**
	 * Override the parallelism of operators processing small amounts of data.
	 *
	 * @param littleParallelism operator parallelism
	 * @return this
	 */
	public LocalClusteringCoefficient<K, VV, EV> setLittleParallelism(int littleParallelism) {
		Preconditions.checkArgument(littleParallelism > 0 || littleParallelism == PARALLELISM_DEFAULT,
			"The parallelism must be greater than zero.");

		this.littleParallelism = littleParallelism;

		return this;
	}

	@Override
	protected String getAlgorithmName() {
		return LocalClusteringCoefficient.class.getName();
	}

	@Override
	protected boolean mergeConfiguration(GraphAlgorithmDelegatingDataSet other) {
		Preconditions.checkNotNull(other);

		if (! LocalClusteringCoefficient.class.isAssignableFrom(other.getClass())) {
			return false;
		}

		LocalClusteringCoefficient rhs = (LocalClusteringCoefficient) other;

		// verify that configurations can be merged

		if (includeZeroDegreeVertices.conflictsWith(rhs.includeZeroDegreeVertices)) {
			return false;
		}

		// merge configurations

		includeZeroDegreeVertices.mergeWith(rhs.includeZeroDegreeVertices);
		littleParallelism = Math.min(littleParallelism, rhs.littleParallelism);

		return true;
	}

	/*
	 * Implementation notes:
	 *
	 * The requirement that "K extends CopyableValue<K>" can be removed when
	 *   removed from TriangleListing.
	 *
	 * CountVertices can be replaced by ".sum(1)" when Flink aggregators use
	 *   code generation.
	 */

	@Override
	public DataSet<Result<K>> runInternal(Graph<K, VV, EV> input)
			throws Exception {
		// u, v, w
		DataSet<Tuple3<K,K,K>> triangles = input
			.run(new TriangleListing<K,VV,EV>()
				.setLittleParallelism(littleParallelism));

		// u, 1
		DataSet<Tuple2<K, LongValue>> triangleVertices = triangles
			.flatMap(new SplitTriangles<K>())
				.name("Split triangle vertices");

		// u, triangle count
		DataSet<Tuple2<K, LongValue>> vertexTriangleCount = triangleVertices
			.groupBy(0)
			.reduce(new CountTriangles<K>())
				.setCombineHint(CombineHint.HASH)
				.name("Count triangles");

		// u, deg(u)
		DataSet<Vertex<K, LongValue>> vertexDegree = input
			.run(new VertexDegree<K, VV, EV>()
				.setIncludeZeroDegreeVertices(includeZeroDegreeVertices.get())
				.setParallelism(littleParallelism));

		// u, deg(u), triangle count
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
	private static class SplitTriangles<T>
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
	 * Sums the triangle count for each vertex ID.
	 *
	 * @param <T> ID type
	 */
	@FunctionAnnotation.ForwardedFields("0")
	private static class CountTriangles<T>
	implements ReduceFunction<Tuple2<T, LongValue>> {
		@Override
		public Tuple2<T, LongValue> reduce(Tuple2<T, LongValue> left, Tuple2<T, LongValue> right)
				throws Exception {
			left.f1.setValue(left.f1.getValue() + right.f1.getValue());
			return left;
		}
	}

	/**
	 * Joins the vertex and degree with the vertex's triangle count.
	 *
	 * @param <T> ID type
	 */
	@FunctionAnnotation.ForwardedFieldsFirst("0; 1->1.0")
	@FunctionAnnotation.ForwardedFieldsSecond("0")
	private static class JoinVertexDegreeWithTriangleCount<T>
	implements JoinFunction<Vertex<T, LongValue>, Tuple2<T, LongValue>, Result<T>> {
		private LongValue zero = new LongValue(0);

		private Result<T> output = new Result<>();

		@Override
		public Result<T> join(Vertex<T, LongValue> vertexAndDegree, Tuple2<T, LongValue> vertexAndTriangleCount)
				throws Exception {
			output.f0 = vertexAndDegree.f0;
			output.f1.f0 = vertexAndDegree.f1;
			output.f1.f1 = (vertexAndTriangleCount == null) ? zero : vertexAndTriangleCount.f1;

			return output;
		}
	}

	/**
	 * Wraps the vertex type to encapsulate results from the Local Clustering Coefficient algorithm.
	 *
	 * @param <T> ID type
	 */
	public static class Result<T>
	extends Vertex<T, Tuple2<LongValue, LongValue>> {
		private static final int HASH_SEED = 0xc23937c1;

		private Murmur3_32 hasher = new Murmur3_32(HASH_SEED);

		public Result() {
			f1 = new Tuple2<>();
		}

		/**
		 * Get the vertex degree.
		 *
		 * @return vertex degree
		 */
		public LongValue getDegree() {
			return f1.f0;
		}

		/**
		 * Get the number of triangles containing this vertex; equivalently,
		 * this is the number of edges between neighbors of this vertex.
		 *
		 * @return triangle count
		 */
		public LongValue getTriangleCount() {
			return f1.f1;
		}

		/**
		 * Get the local clustering coefficient score. This is computed as the
		 * number of edges between neighbors, equal to the triangle count,
		 * divided by the number of potential edges between neighbors.
		 *
		 * A score of {@code Double.NaN} is returned for a vertex with degree 1
		 * for which both the triangle count and number of neighbors are zero.
		 *
		 * @return local clustering coefficient score
		 */
		public double getLocalClusteringCoefficientScore() {
			long degree = getDegree().getValue();
			long neighborPairs = degree * (degree - 1) / 2;

			return (neighborPairs == 0) ? Double.NaN : getTriangleCount().getValue() / (double)neighborPairs;
		}

		/**
		 * Format values into a human-readable string.
		 *
		 * @return verbose string
		 */
		public String toVerboseString() {
			return "Vertex ID: " + f0
				+ ", vertex degree: " + getDegree()
				+ ", triangle count: " + getTriangleCount()
				+ ", local clustering coefficient: " + getLocalClusteringCoefficientScore();
		}

		@Override
		public int hashCode() {
			return hasher.reset()
				.hash(f0.hashCode())
				.hash(f1.f0.getValue())
				.hash(f1.f1.getValue())
				.hash();
		}
	}
}
