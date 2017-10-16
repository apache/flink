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

package org.apache.flink.graph.library.metric.directed;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.accumulators.LongMaximum;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.base.ReduceOperatorBase.CombineHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.AnalyticHelper;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAnalyticBase;
import org.apache.flink.graph.asm.degree.annotate.directed.EdgeDegreesPair;
import org.apache.flink.graph.asm.degree.annotate.directed.VertexDegrees.Degrees;
import org.apache.flink.graph.asm.result.PrintableResult;
import org.apache.flink.graph.library.metric.directed.EdgeMetrics.Result;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.IOException;
import java.text.NumberFormat;

/**
 * Compute the following edge metrics in a directed graph.
 *  - number of triangle triplets
 *  - number of rectangle triplets
 *  - maximum number of triangle triplets
 *  - maximum number of rectangle triplets
 *
 * @param <K> graph ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class EdgeMetrics<K extends Comparable<K>, VV, EV>
extends GraphAnalyticBase<K, VV, EV, Result> {

	private static final String TRIANGLE_TRIPLET_COUNT = "triangleTripletCount";

	private static final String RECTANGLE_TRIPLET_COUNT = "rectangleTripletCount";

	private static final String MAXIMUM_TRIANGLE_TRIPLETS = "maximumTriangleTriplets";

	private static final String MAXIMUM_RECTANGLE_TRIPLETS = "maximumRectangleTriplets";

	private EdgeMetricsHelper<K> edgeMetricsHelper;

	/*
	 * Implementation notes:
	 *
	 * <p>Use aggregator to replace SumEdgeStats when aggregators are rewritten to use
	 * a hash-combineable hashable-reduce.
	 *
	 * <p>Use distinct to replace ReduceEdgeStats when the combiner can be disabled
	 * with a sorted-reduce forced.
	 */

	@Override
	public EdgeMetrics<K, VV, EV> run(Graph<K, VV, EV> input)
			throws Exception {
		super.run(input);

		// s, t, (d(s), d(t))
		DataSet<Edge<K, Tuple3<EV, Degrees, Degrees>>> edgeDegreesPair = input
			.run(new EdgeDegreesPair<K, VV, EV>()
				.setParallelism(parallelism));

		// s, d(s), count of (u, v) where deg(u) < deg(v) or (deg(u) == deg(v) and u < v)
		DataSet<Tuple3<K, Degrees, LongValue>> edgeStats = edgeDegreesPair
			.flatMap(new EdgeStats<>())
				.setParallelism(parallelism)
				.name("Edge stats")
			.groupBy(0, 1)
			.reduceGroup(new ReduceEdgeStats<>())
				.setParallelism(parallelism)
				.name("Reduce edge stats")
			.groupBy(0)
			.reduce(new SumEdgeStats<>())
			.setCombineHint(CombineHint.HASH)
				.setParallelism(parallelism)
				.name("Sum edge stats");

		edgeMetricsHelper = new EdgeMetricsHelper<>();

		edgeStats
			.output(edgeMetricsHelper)
				.setParallelism(parallelism)
				.name("Edge metrics");

		return this;
	}

	@Override
	public Result getResult() {
		long triangleTripletCount = edgeMetricsHelper.getAccumulator(env, TRIANGLE_TRIPLET_COUNT);
		long rectangleTripletCount = edgeMetricsHelper.getAccumulator(env, RECTANGLE_TRIPLET_COUNT);
		long maximumTriangleTriplets = edgeMetricsHelper.getAccumulator(env, MAXIMUM_TRIANGLE_TRIPLETS);
		long maximumRectangleTriplets = edgeMetricsHelper.getAccumulator(env, MAXIMUM_RECTANGLE_TRIPLETS);

		// each edge is counted twice, once from each vertex, so must be halved
		return new Result(triangleTripletCount, rectangleTripletCount,
			maximumTriangleTriplets, maximumRectangleTriplets);
	}

	/**
	 * Produces a pair of tuples. The first tuple contains the source vertex ID,
	 * the target vertex ID, the source degrees, and the low-order count. The
	 * second tuple is the same with the source and target roles reversed.
	 *
	 * <p>The low-order count is one if the source vertex degree is less than the
	 * target vertex degree or if the degrees are equal and the source vertex
	 * ID compares lower than the target vertex ID; otherwise the low-order
	 * count is zero.
	 *
	 * @param <T> ID type
	 * @param <ET> edge value type
	 */
	private static final class EdgeStats<T extends Comparable<T>, ET>
	implements FlatMapFunction<Edge<T, Tuple3<ET, Degrees, Degrees>>, Tuple4<T, T, Degrees, LongValue>> {
		private LongValue zero = new LongValue(0);

		private LongValue one = new LongValue(1);

		private Tuple4<T, T, Degrees, LongValue> output = new Tuple4<>();

		@Override
		public void flatMap(Edge<T, Tuple3<ET, Degrees, Degrees>> edge, Collector<Tuple4<T, T, Degrees, LongValue>> out)
				throws Exception {
			Tuple3<ET, Degrees, Degrees> degrees = edge.f2;
			long sourceDegree = degrees.f1.getDegree().getValue();
			long targetDegree = degrees.f2.getDegree().getValue();

			boolean ordered = (sourceDegree < targetDegree
				|| (sourceDegree == targetDegree && edge.f0.compareTo(edge.f1) < 0));

			output.f0 = edge.f0;
			output.f1 = edge.f1;
			output.f2 = edge.f2.f1;
			output.f3 = ordered ? one : zero;
			out.collect(output);

			output.f0 = edge.f1;
			output.f1 = edge.f0;
			output.f2 = edge.f2.f2;
			output.f3 = ordered ? zero : one;
			out.collect(output);
		}
	}

	/**
	 * Produces a distinct value for each edge.
	 *
	 * @param <T> ID type
	 */
	@ForwardedFields("0")
	private static final class ReduceEdgeStats<T>
	implements GroupReduceFunction<Tuple4<T, T, Degrees, LongValue>, Tuple3<T, Degrees, LongValue>> {
		Tuple3<T, Degrees, LongValue> output = new Tuple3<>();

		@Override
		public void reduce(Iterable<Tuple4<T, T, Degrees, LongValue>> values, Collector<Tuple3<T, Degrees, LongValue>> out)
				throws Exception {
			Tuple4<T, T, Degrees, LongValue> value = values.iterator().next();

			output.f0 = value.f0;
			output.f1 = value.f2;
			output.f2 = value.f3;

			out.collect(output);
		}
	}

	/**
	 * Sums the low-order counts.
	 *
	 * @param <T> ID type
	 */
	private static class SumEdgeStats<T>
	implements ReduceFunction<Tuple3<T, Degrees, LongValue>> {
		@Override
		public Tuple3<T, Degrees, LongValue> reduce(Tuple3<T, Degrees, LongValue> value1, Tuple3<T, Degrees, LongValue> value2)
				throws Exception {
			value1.f2.setValue(value1.f2.getValue() + value2.f2.getValue());
			return value1;
		}
	}

	/**
	 * Helper class to collect edge metrics.
	 *
	 * @param <T> ID type
	 */
	private static class EdgeMetricsHelper<T extends Comparable<T>>
	extends AnalyticHelper<Tuple3<T, Degrees, LongValue>> {
		private long triangleTripletCount;
		private long rectangleTripletCount;
		private long maximumTriangleTriplets;
		private long maximumRectangleTriplets;

		@Override
		public void configure(Configuration parameters) {}

		@Override
		public void open(int taskNumber, int numTasks) throws IOException {}

		@Override
		public void writeRecord(Tuple3<T, Degrees, LongValue> record) throws IOException {
			Degrees degrees = record.f1;
			long degree = degrees.getDegree().getValue();

			long lowDegree = record.f2.getValue();
			long highDegree = degree - lowDegree;

			long triangleTriplets = lowDegree * (lowDegree - 1) / 2;
			long rectangleTriplets = triangleTriplets + lowDegree * highDegree;

			triangleTripletCount += triangleTriplets;
			rectangleTripletCount += rectangleTriplets;

			maximumTriangleTriplets = Math.max(maximumTriangleTriplets, triangleTriplets);
			maximumRectangleTriplets = Math.max(maximumRectangleTriplets, rectangleTriplets);
		}

		@Override
		public void close() throws IOException {
			addAccumulator(TRIANGLE_TRIPLET_COUNT, new LongCounter(triangleTripletCount));
			addAccumulator(RECTANGLE_TRIPLET_COUNT, new LongCounter(rectangleTripletCount));
			addAccumulator(MAXIMUM_TRIANGLE_TRIPLETS, new LongMaximum(maximumTriangleTriplets));
			addAccumulator(MAXIMUM_RECTANGLE_TRIPLETS, new LongMaximum(maximumRectangleTriplets));
		}
	}

	/**
	 * Wraps edge metrics.
	 */
	public static class Result
	implements PrintableResult {
		private long triangleTripletCount;
		private long rectangleTripletCount;
		private long maximumTriangleTriplets;
		private long maximumRectangleTriplets;

		public Result(long triangleTripletCount, long rectangleTripletCount,
				long maximumTriangleTriplets, long maximumRectangleTriplets) {
			this.triangleTripletCount = triangleTripletCount;
			this.rectangleTripletCount = rectangleTripletCount;
			this.maximumTriangleTriplets = maximumTriangleTriplets;
			this.maximumRectangleTriplets = maximumRectangleTriplets;
		}

		/**
		 * Get the number of triangle triplets.
		 *
		 * @return number of triangle triplets
		 */
		public long getNumberOfTriangleTriplets() {
			return triangleTripletCount;
		}

		/**
		 * Get the number of rectangle triplets.
		 *
		 * @return number of rectangle triplets
		 */
		public long getNumberOfRectangleTriplets() {
			return rectangleTripletCount;
		}

		/**
		 * Get the maximum triangle triplets.
		 *
		 * @return maximum triangle triplets
		 */
		public long getMaximumTriangleTriplets() {
			return maximumTriangleTriplets;
		}

		/**
		 * Get the maximum rectangle triplets.
		 *
		 * @return maximum rectangle triplets
		 */
		public long getMaximumRectangleTriplets() {
			return maximumRectangleTriplets;
		}

		@Override
		public String toString() {
			return toPrintableString();
		}

		@Override
		public String toPrintableString() {
			NumberFormat nf = NumberFormat.getInstance();

			return "triangle triplet count: " + nf.format(triangleTripletCount)
				+ "; rectangle triplet count: " + nf.format(rectangleTripletCount)
				+ "; maximum triangle triplets: " + nf.format(maximumTriangleTriplets)
				+ "; maximum rectangle triplets: " + nf.format(maximumRectangleTriplets);
		}

		@Override
		public int hashCode() {
			return new HashCodeBuilder()
				.append(triangleTripletCount)
				.append(rectangleTripletCount)
				.append(maximumTriangleTriplets)
				.append(maximumRectangleTriplets)
				.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == null) {
				return false;
			}

			if (obj == this) {
				return true;
			}

			if (obj.getClass() != getClass()) {
				return false;
			}

			Result rhs = (Result) obj;

			return new EqualsBuilder()
				.append(triangleTripletCount, rhs.triangleTripletCount)
				.append(rectangleTripletCount, rhs.rectangleTripletCount)
				.append(maximumTriangleTriplets, rhs.maximumTriangleTriplets)
				.append(maximumRectangleTriplets, rhs.maximumRectangleTriplets)
				.isEquals();
		}
	}
}
