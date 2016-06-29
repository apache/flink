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

package org.apache.flink.graph.library.clustering.directed;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeOrder;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.asm.degree.annotate.directed.EdgeDegreesPair;
import org.apache.flink.graph.asm.degree.annotate.directed.VertexDegrees.Degrees;
import org.apache.flink.graph.library.clustering.directed.TriangleListing.Result;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmDelegatingDataSet;
import org.apache.flink.graph.utils.proxy.OptionalBoolean;
import org.apache.flink.types.ByteValue;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_DEFAULT;

/**
 * Generates a listing of distinct triangles from the input graph.
 * <br/>
 * A triangle is a 3-clique with vertices A, B, and C connected by edges
 * (A, B), (A, C), and (B, C).
 * <br/>
 * The input graph must not contain duplicate edges or self-loops.
 *
 * @param <K> graph ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class TriangleListing<K extends Comparable<K> & CopyableValue<K>, VV, EV>
extends GraphAlgorithmDelegatingDataSet<K, VV, EV, Result<K>> {

	// Optional configuration
	private OptionalBoolean sortTriangleVertices = new OptionalBoolean(false, false);

	private int littleParallelism = PARALLELISM_DEFAULT;

	/**
	 * Normalize the triangle listing such that for each result (K0, K1, K2)
	 * the vertex IDs are sorted K0 < K1 < K2.
	 *
	 * @param sortTriangleVertices whether to output each triangle's vertices in sorted order
	 * @return this
	 */
	public TriangleListing<K, VV, EV> setSortTriangleVertices(boolean sortTriangleVertices) {
		this.sortTriangleVertices.set(sortTriangleVertices);

		return this;
	}

	/**
	 * Override the parallelism of operators processing small amounts of data.
	 *
	 * @param littleParallelism operator parallelism
	 * @return this
	 */
	public TriangleListing<K, VV, EV> setLittleParallelism(int littleParallelism) {
		Preconditions.checkArgument(littleParallelism > 0 || littleParallelism == PARALLELISM_DEFAULT,
			"The parallelism must be greater than zero.");

		this.littleParallelism = littleParallelism;

		return this;
	}

	@Override
	protected String getAlgorithmName() {
		return TriangleListing.class.getName();
	}

	@Override
	protected boolean mergeConfiguration(GraphAlgorithmDelegatingDataSet other) {
		Preconditions.checkNotNull(other);

		if (! TriangleListing.class.isAssignableFrom(other.getClass())) {
			return false;
		}

		TriangleListing rhs = (TriangleListing) other;

		sortTriangleVertices.mergeWith(rhs.sortTriangleVertices);
		littleParallelism = Math.min(littleParallelism, rhs.littleParallelism);

		return true;
	}

	/*
	 * Implementation notes:
	 *
	 * The requirement that "K extends CopyableValue<K>" can be removed when
	 *   Flink has a self-join and GenerateTriplets is implemented as such.
	 *
	 * ProjectTriangles should eventually be replaced by ".projectFirst("*")"
	 *   when projections use code generation.
	 */

	@Override
	public DataSet<Result<K>> runInternal(Graph<K, VV, EV> input)
			throws Exception {
		// u, v, bitmask where u < v
		DataSet<Tuple3<K, K, ByteValue>> filteredByID = input
			.getEdges()
			.map(new OrderByID<K, EV>())
				.setParallelism(littleParallelism)
				.name("Order by ID")
			.groupBy(0, 1)
			.reduceGroup(new ReduceBitmask<K>())
				.setParallelism(littleParallelism)
				.name("Flatten by ID");

		// u, v, (deg(u), deg(v))
		DataSet<Edge<K, Tuple3<EV, Degrees, Degrees>>> pairDegrees = input
			.run(new EdgeDegreesPair<K, VV, EV>()
				.setParallelism(littleParallelism));

		// u, v, bitmask where deg(u) < deg(v) or (deg(u) == deg(v) and u < v)
		DataSet<Tuple3<K, K, ByteValue>> filteredByDegree = pairDegrees
			.map(new OrderByDegree<K, EV>())
				.setParallelism(littleParallelism)
				.name("Order by degree")
			.groupBy(0, 1)
			.reduceGroup(new ReduceBitmask<K>())
				.setParallelism(littleParallelism)
				.name("Flatten by degree");

		// u, v, w, bitmask where (u, v) and (u, w) are edges in graph
		DataSet<Tuple4<K, K, K, ByteValue>> triplets = filteredByDegree
			.groupBy(0)
			.sortGroup(1, Order.ASCENDING)
			.reduceGroup(new GenerateTriplets<K>())
				.setParallelism(littleParallelism)
				.name("Generate triplets");

		// u, v, w, bitmask where (u, v), (u, w), and (v, w) are edges in graph
		DataSet<Result<K>> triangles = triplets
			.join(filteredByID, JoinOperatorBase.JoinHint.REPARTITION_HASH_SECOND)
			.where(1, 2)
			.equalTo(0, 1)
			.with(new ProjectTriangles<K>())
				.setParallelism(littleParallelism)
				.name("Triangle listing");

		if (sortTriangleVertices.get()) {
			triangles = triangles
				.map(new SortTriangleVertices<K>())
					.name("Sort triangle vertices");
		}

		return triangles;
	}

	/**
	 * Removes edge values while emitting a Tuple3 where f0 and f1 are,
	 * respectively, the lesser and greater of the source and target IDs.
	 * The third field is a bitmask representing the vertex order.
	 *
	 * @param <T> ID type
	 * @param <ET> edge value type
	 */
	private static final class OrderByID<T extends Comparable<T>, ET>
	implements MapFunction<Edge<T, ET>, Tuple3<T, T, ByteValue>> {
		private ByteValue forward = new ByteValue(EdgeOrder.FORWARD.getBitmask());

		private ByteValue reverse = new ByteValue(EdgeOrder.REVERSE.getBitmask());

		private Tuple3<T, T, ByteValue> output = new Tuple3<>();

		@Override
		public Tuple3<T, T, ByteValue> map(Edge<T, ET> value)
				throws Exception {
			if (value.f0.compareTo(value.f1) < 0) {
				output.f0 = value.f0;
				output.f1 = value.f1;
				output.f2 = forward;
			} else {
				output.f0 = value.f1;
				output.f1 = value.f0;
				output.f2 = reverse;
			}

			return output;
		}
	}

	/**
	 * Reduce bitmasks to a single value using bitwise-or.
	 *
	 * @param <T> ID type
	 */
	@ForwardedFields("0; 1")
	private static final class ReduceBitmask<T>
	implements GroupReduceFunction<Tuple3<T, T, ByteValue>, Tuple3<T, T, ByteValue>> {
		@Override
		public void reduce(Iterable<Tuple3<T, T, ByteValue>> values, Collector<Tuple3<T, T, ByteValue>> out)
				throws Exception {
			Tuple3<T, T, ByteValue> output = null;

			byte bitmask = 0;

			for (Tuple3<T, T, ByteValue> value: values) {
				output = value;
				bitmask |= value.f2.getValue();
			}

			output.f2.setValue(bitmask);
			out.collect(output);
		}
	}

	/**
	 * Removes edge values while emitting a Tuple3 where f0 and f1 are,
	 * respectively, the lesser and greater of the source and target IDs
	 * by degree count. If the source and target vertex degrees are equal
	 * then the IDs are compared and emitted in order. The third field is
	 * a bitmask representing the vertex order.
	 *
	 * @param <T> ID type
	 * @param <ET> edge value type
	 */
	private static final class OrderByDegree<T extends Comparable<T>, ET>
	implements MapFunction<Edge<T, Tuple3<ET, Degrees, Degrees>>, Tuple3<T, T, ByteValue>> {
		private ByteValue forward = new ByteValue((byte)(EdgeOrder.FORWARD.getBitmask() << 2));

		private ByteValue reverse = new ByteValue((byte)(EdgeOrder.REVERSE.getBitmask() << 2));

		private Tuple3<T, T, ByteValue> output = new Tuple3<>();

		@Override
		public Tuple3<T, T, ByteValue> map(Edge<T, Tuple3<ET, Degrees, Degrees>> value)
				throws Exception {
			Tuple3<ET, Degrees, Degrees> degrees = value.f2;
			long sourceDegree = degrees.f1.getDegree().getValue();
			long targetDegree = degrees.f2.getDegree().getValue();

			if (sourceDegree < targetDegree ||
					(sourceDegree == targetDegree && value.f0.compareTo(value.f1) < 0)) {
				output.f0 = value.f0;
				output.f1 = value.f1;
				output.f2 = forward;
			} else {
				output.f0 = value.f1;
				output.f1 = value.f0;
				output.f2 = reverse;
			}

			return output;
		}
	}

	/**
	 * Generates the set of triplets by the pairwise enumeration of the open
	 * neighborhood for each vertex. The number of triplets is quadratic in
	 * the vertex degree; however, data skew is minimized by only generating
	 * triplets from the vertex with least degree.
	 *
	 * @param <T> ID type
	 */
	@ForwardedFields("0")
	private static final class GenerateTriplets<T extends CopyableValue<T>>
	implements GroupReduceFunction<Tuple3<T, T, ByteValue>, Tuple4<T, T, T, ByteValue>> {
		private Tuple4<T, T, T, ByteValue> output = new Tuple4<>(null, null, null, new ByteValue());

		private List<Tuple2<T, ByteValue>> visited = new ArrayList<>();

		@Override
		public void reduce(Iterable<Tuple3<T, T, ByteValue>> values, Collector<Tuple4<T, T, T, ByteValue>> out)
				throws Exception {
			int visitedCount = 0;

			Iterator<Tuple3<T, T, ByteValue>> iter = values.iterator();

			while (true) {
				Tuple3<T, T, ByteValue> edge = iter.next();
				byte bitmask = edge.f2.getValue();

				output.f0 = edge.f0;
				output.f2 = edge.f1;

				for (int i = 0; i < visitedCount; i++) {
					Tuple2<T, ByteValue> previous = visited.get(i);

					output.f1 = previous.f0;
					output.f3.setValue((byte)(previous.f1.getValue() | bitmask));

					// u, v, w, bitmask
					out.collect(output);
				}

				if (! iter.hasNext()) {
					break;
				}

				byte shiftedBitmask = (byte)(bitmask << 2);

				if (visitedCount == visited.size()) {
					visited.add(new Tuple2<>(edge.f1.copy(), new ByteValue(shiftedBitmask)));
				} else {
					Tuple2<T, ByteValue> update = visited.get(visitedCount);
					edge.f1.copyTo(update.f0);
					update.f1.setValue(shiftedBitmask);
				}

				visitedCount += 1;
			}
		}
	}

	/**
	 * Simply project the triplet as a triangle while collapsing triplet and edge bitmasks.
	 *
	 * @param <T> ID type
	 */
	@ForwardedFieldsFirst("0; 1; 2")
	@ForwardedFieldsSecond("0; 1")
	private static final class ProjectTriangles<T>
	implements JoinFunction<Tuple4<T, T, T, ByteValue>, Tuple3<T, T, ByteValue>, Result<T>> {
		private Result<T> output = new Result<>(null, null, null, new ByteValue());

		@Override
		public Result<T> join(Tuple4<T, T, T, ByteValue> triplet, Tuple3<T, T, ByteValue> edge)
				throws Exception {
			output.f0 = triplet.f0;
			output.f1 = triplet.f1;
			output.f2 = triplet.f2;
			output.f3.setValue((byte)(triplet.f3.getValue() | edge.f2.getValue()));
			return output;
		}
	}

	/**
	 * Reorders the vertices of each emitted triangle (K0, K1, K2, bitmask)
	 * into sorted order such that K0 < K1 < K2.
	 *
	 * @param <T> ID type
	 */
	private static final class SortTriangleVertices<T extends Comparable<T>>
	implements MapFunction<Result<T>, Result<T>> {
		@Override
		public Result<T> map(Result<T> value)
				throws Exception {
			// by the triangle listing algorithm we know f1 < f2
			if (value.f0.compareTo(value.f1) > 0) {
				byte bitmask = value.f3.getValue();

				T temp_val = value.f0;
				value.f0 = value.f1;

				if (temp_val.compareTo(value.f2) < 0) {
					value.f1 = temp_val;

					int f0f1 = ((bitmask & 0b100000) >>> 1) | ((bitmask & 0b010000) << 1);
					int f0f2 = (bitmask & 0b001100) >>> 2;
					int f1f2 = (bitmask & 0b000011) << 2;

					value.f3.setValue((byte)(f0f1 | f0f2 | f1f2));
				} else {
					value.f1 = value.f2;
					value.f2 = temp_val;

					int f0f1 = (bitmask & 0b000011) << 4;
					int f0f2 = ((bitmask & 0b100000) >>> 3) | ((bitmask & 0b010000) >>> 1);
					int f1f2 = ((bitmask & 0b001000) >>> 3) | ((bitmask & 0b000100) >>> 1);

					value.f3.setValue((byte)(f0f1 | f0f2 | f1f2));
				}
			}

			return value;
		}
	}

	/**
	 * Wraps the vertex type to encapsulate results from the Triangle Listing algorithm.
	 *
	 * @param <T> ID type
	 */
	public static class Result<T>
	extends Tuple4<T, T, T, ByteValue> {
		/**
		 * No-args constructor.
		 */
		public Result() {}

		/**
		 * Populates parent tuple with constructor parameters.
		 *
		 * @param value0 1st triangle vertex ID
		 * @param value1 2nd triangle vertex ID
		 * @param value2 3rd triangle vertex ID
		 * @param value3 bitmask indicating presence of six possible edges between triangle vertices
		 */
		public Result(T value0, T value1, T value2, ByteValue value3) {
			super(value0, value1, value2, value3);
		}

		/**
		 * Format values into a human-readable string.
		 *
		 * @return verbose string
		 */
		public String toVerboseString() {
			byte bitmask = f3.getValue();

			return "1st vertex ID: " + f0
				+ ", 2nd vertex ID: " + f1
				+ ", 3rd vertex ID: " + f2
				+ ", edge directions: " + f0 + maskToString(bitmask, 4) + f1
				+ ", " + f0 + maskToString(bitmask, 2) + f2
				+ ", " + f1 + maskToString(bitmask, 0) + f2;
		}

		private String maskToString(byte mask, int shift) {
			switch((mask >>> shift) & 0b000011) {
				case 0b01:
					// EdgeOrder.FORWARD
					return "->";
				case 0b10:
					// EdgeOrder.REVERSE
					return "<-";
				case 0b11:
					// EdgeOrder.MUTUAL
					return "<->";
				default:
					throw new IllegalArgumentException("Bitmask is missing an edge (mask = "
						+ mask + ", shift = " + shift);
			}
		}
	}
}
