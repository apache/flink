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

package org.apache.flink.graph.library.metric.undirected;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.accumulators.LongMaximum;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.base.ReduceOperatorBase.CombineHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.AnalyticHelper;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAnalyticBase;
import org.apache.flink.graph.asm.degree.annotate.undirected.EdgeDegreePair;
import org.apache.flink.graph.asm.result.PrintableResult;
import org.apache.flink.graph.library.metric.undirected.EdgeMetrics.Result;
import org.apache.flink.types.LongValue;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.IOException;
import java.text.NumberFormat;

/**
 * Compute the following edge metrics in an undirected graph.
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

	// Optional configuration
	private boolean reduceOnTargetId = false;

	/**
	 * The degree can be counted from either the edge source or target IDs.
	 * By default the source IDs are counted. Reducing on target IDs may
	 * optimize the algorithm if the input edge list is sorted by target ID.
	 *
	 * @param reduceOnTargetId set to {@code true} if the input edge list
	 *                         is sorted by target ID
	 * @return this
	 */
	public EdgeMetrics<K, VV, EV> setReduceOnTargetId(boolean reduceOnTargetId) {
		this.reduceOnTargetId = reduceOnTargetId;

		return this;
	}

	/*
	 * Implementation notes:
	 *
	 * <p>Use aggregator to replace SumEdgeStats when aggregators are rewritten to use
	 * a hash-combineable hashed-reduce.
	 */

	@Override
	public EdgeMetrics<K, VV, EV> run(Graph<K, VV, EV> input)
			throws Exception {
		super.run(input);

		// s, t, (d(s), d(t))
		DataSet<Edge<K, Tuple3<EV, LongValue, LongValue>>> edgeDegreePair = input
			.run(new EdgeDegreePair<K, VV, EV>()
				.setReduceOnTargetId(reduceOnTargetId)
				.setParallelism(parallelism));

		// s, d(s), count of (u, v) where deg(u) < deg(v) or (deg(u) == deg(v) and u < v)
		DataSet<Tuple3<K, LongValue, LongValue>> edgeStats = edgeDegreePair
			.map(new EdgeStats<>())
				.setParallelism(parallelism)
				.name("Edge stats")
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

		return new Result(triangleTripletCount, rectangleTripletCount,
			maximumTriangleTriplets, maximumRectangleTriplets);
	}

	/**
	 * Evaluates each edge and emits a tuple containing the source vertex ID,
	 * the source vertex degree, and a value of zero or one indicating the
	 * low-order count. The low-order count is one if the source vertex degree
	 * is less than the target vertex degree or if the degrees are equal and
	 * the source vertex ID compares lower than the target vertex ID; otherwise
	 * the low-order count is zero.
	 *
	 * @param <T> ID type
	 * @param <ET> edge value type
	 */
	@FunctionAnnotation.ForwardedFields("0; 2.1->1")
	private static class EdgeStats<T extends Comparable<T>, ET>
	implements MapFunction<Edge<T, Tuple3<ET, LongValue, LongValue>>, Tuple3<T, LongValue, LongValue>> {
		private LongValue zero = new LongValue(0);

		private LongValue one = new LongValue(1);

		private Tuple3<T, LongValue, LongValue> output = new Tuple3<>();

		@Override
		public Tuple3<T, LongValue, LongValue> map(Edge<T, Tuple3<ET, LongValue, LongValue>> edge)
				throws Exception {
			Tuple3<ET, LongValue, LongValue> degrees = edge.f2;

			output.f0 = edge.f0;
			output.f1 = degrees.f1;

			long sourceDegree = degrees.f1.getValue();
			long targetDegree = degrees.f2.getValue();

			if (sourceDegree < targetDegree ||
					(sourceDegree == targetDegree && edge.f0.compareTo(edge.f1) < 0)) {
				output.f2 = one;
			} else {
				output.f2 = zero;
			}

			return output;
		}
	}

	/**
	 * Sums the low-order counts.
	 *
	 * @param <T> ID type
	 */
	private static class SumEdgeStats<T>
	implements ReduceFunction<Tuple3<T, LongValue, LongValue>> {
		@Override
		public Tuple3<T, LongValue, LongValue> reduce(Tuple3<T, LongValue, LongValue> value1, Tuple3<T, LongValue, LongValue> value2)
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
	extends AnalyticHelper<Tuple3<T, LongValue, LongValue>> {
		private long triangleTripletCount;
		private long rectangleTripletCount;
		private long maximumTriangleTriplets;
		private long maximumRectangleTriplets;

		@Override
		public void writeRecord(Tuple3<T, LongValue, LongValue> record) throws IOException {
			long degree = record.f1.getValue();

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
