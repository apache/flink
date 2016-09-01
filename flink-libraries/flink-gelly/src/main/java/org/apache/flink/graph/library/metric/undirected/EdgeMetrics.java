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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.accumulators.LongMaximum;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.operators.base.ReduceOperatorBase.CombineHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.AbstractGraphAnalytic;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.asm.degree.annotate.undirected.EdgeDegreePair;
import org.apache.flink.graph.library.metric.undirected.EdgeMetrics.Result;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.AbstractID;

import java.io.IOException;
import java.text.NumberFormat;

import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_DEFAULT;

/**
 * Compute the following edge metrics in an undirected graph:
 *  - number of vertices
 *  - number of edges
 *  - number of triangle triplets
 *  - number of rectangle triplets
 *  - number of triplets
 *  - maximum degree
 *  - maximum number of triangle triplets
 *  - maximum number of rectangle triplets
 *  - maximum number of triplets
 *
 * @param <K> graph ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class EdgeMetrics<K extends Comparable<K> & CopyableValue<K>, VV, EV>
extends AbstractGraphAnalytic<K, VV, EV, Result> {

	private String id = new AbstractID().toString();

	// Optional configuration
	private boolean reduceOnTargetId = false;

	private int parallelism = PARALLELISM_DEFAULT;

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

	/**
	 * Override the operator parallelism.
	 *
	 * @param parallelism operator parallelism
	 * @return this
	 */
	public EdgeMetrics<K, VV, EV> setParallelism(int parallelism) {
		this.parallelism = parallelism;

		return this;
	}

	/*
	 * Implementation notes:
	 *
	 * Use aggregator to replace SumEdgeStats when aggregators are rewritten to use
	 *   a hash-combineable hashed-reduce.
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
			.map(new EdgeStats<K, EV>())
				.setParallelism(parallelism)
				.name("Edge stats")
			.groupBy(0)
			.reduce(new SumEdgeStats<K>())
				.setCombineHint(CombineHint.HASH)
				.setParallelism(parallelism)
				.name("Sum edge stats");

		edgeStats
			.output(new EdgeMetricsHelper<K, EV>(id))
				.setParallelism(parallelism)
				.name("Edge metrics");

		return this;
	}

	@Override
	public Result getResult() {
		JobExecutionResult res = env.getLastJobExecutionResult();

		long vertexCount = res.getAccumulatorResult(id + "-0");
		long edgeCount = res.getAccumulatorResult(id + "-1");
		long triangleTripletCount = res.getAccumulatorResult(id + "-2");
		long rectangleTripletCount = res.getAccumulatorResult(id + "-3");
		long tripletCount = res.getAccumulatorResult(id + "-4");
		long maximumDegree = res.getAccumulatorResult(id + "-5");
		long maximumTriangleTriplets = res.getAccumulatorResult(id + "-6");
		long maximumRectangleTriplets = res.getAccumulatorResult(id + "-7");
		long maximumTriplets = res.getAccumulatorResult(id + "-8");

		return new Result(vertexCount, edgeCount / 2, triangleTripletCount, rectangleTripletCount, tripletCount,
			maximumDegree, maximumTriangleTriplets, maximumRectangleTriplets, maximumTriplets);
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
	private static class EdgeMetricsHelper<T extends Comparable<T>, ET>
	extends RichOutputFormat<Tuple3<T, LongValue, LongValue>> {
		private final String id;

		private long vertexCount;
		private long edgeCount;
		private long triangleTripletCount;
		private long rectangleTripletCount;
		private long tripletCount;
		private long maximumDegree;
		private long maximumTriangleTriplets;
		private long maximumRectangleTriplets;
		private long maximumTriplets;

		/**
		 * This helper class collects edge metrics by scanning over and
		 * discarding elements from the given DataSet.
		 *
		 * The unique id is required because Flink's accumulator namespace is
		 * among all operators.
		 *
		 * @param id unique string used for accumulator names
		 */
		public EdgeMetricsHelper(String id) {
			this.id = id;
		}

		@Override
		public void configure(Configuration parameters) {}

		@Override
		public void open(int taskNumber, int numTasks) throws IOException {}

		@Override
		public void writeRecord(Tuple3<T, LongValue, LongValue> record) throws IOException {
			long degree = record.f1.getValue();
			long lowDegree = record.f2.getValue();
			long highDegree = degree - lowDegree;

			long triangleTriplets = lowDegree * (lowDegree - 1) / 2;
			long rectangleTriplets = triangleTriplets + lowDegree * highDegree;
			long triplets = degree * (degree - 1) / 2;

			vertexCount++;
			edgeCount += degree;
			triangleTripletCount += triangleTriplets;
			rectangleTripletCount += rectangleTriplets;
			tripletCount += triplets;
			maximumDegree = Math.max(maximumDegree, degree);
			maximumTriangleTriplets = Math.max(maximumTriangleTriplets, triangleTriplets);
			maximumRectangleTriplets = Math.max(maximumRectangleTriplets, rectangleTriplets);
			maximumTriplets = Math.max(maximumTriplets, triplets);
		}

		@Override
		public void close() throws IOException {
			getRuntimeContext().addAccumulator(id + "-0", new LongCounter(vertexCount));
			getRuntimeContext().addAccumulator(id + "-1", new LongCounter(edgeCount));
			getRuntimeContext().addAccumulator(id + "-2", new LongCounter(triangleTripletCount));
			getRuntimeContext().addAccumulator(id + "-3", new LongCounter(rectangleTripletCount));
			getRuntimeContext().addAccumulator(id + "-4", new LongCounter(tripletCount));
			getRuntimeContext().addAccumulator(id + "-5", new LongMaximum(maximumDegree));
			getRuntimeContext().addAccumulator(id + "-6", new LongMaximum(maximumTriangleTriplets));
			getRuntimeContext().addAccumulator(id + "-7", new LongMaximum(maximumRectangleTriplets));
			getRuntimeContext().addAccumulator(id + "-8", new LongMaximum(maximumTriplets));
		}
	}

	/**
	 * Wraps edge metrics.
	 */
	public static class Result {
		private long vertexCount;
		private long edgeCount;
		private long triangleTripletCount;
		private long rectangleTripletCount;
		private long tripletCount;
		private long maximumDegree;
		private long maximumTriangleTriplets;
		private long maximumRectangleTriplets;
		private long maximumTriplets;

		public Result(long vertexCount, long edgeCount, long triangleTripletCount, long rectangleTripletCount, long tripletCount,
				long maximumDegree, long maximumTriangleTriplets, long maximumRectangleTriplets, long maximumTriplets) {
			this.vertexCount = vertexCount;
			this.edgeCount = edgeCount;
			this.triangleTripletCount = triangleTripletCount;
			this.rectangleTripletCount = rectangleTripletCount;
			this.tripletCount = tripletCount;
			this.maximumDegree = maximumDegree;
			this.maximumTriangleTriplets = maximumTriangleTriplets;
			this.maximumRectangleTriplets = maximumRectangleTriplets;
			this.maximumTriplets = maximumTriplets;
		}

		/**
		 * Get the number of vertices.
		 *
		 * @return number of vertices
		 */
		public long getNumberOfVertices() {
			return vertexCount;
		}

		/**
		 * Get the number of edges.
		 *
		 * @return number of edges
		 */
		public long getNumberOfEdges() {
			return edgeCount;
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
		 * Get the number of triplets.
		 *
		 * @return number of triplets
		 */
		public long getNumberOfTriplets() {
			return tripletCount;
		}

		/**
		 * Get the maximum degree.
		 *
		 * @return maximum degree
		 */
		public long getMaximumDegree() {
			return maximumDegree;
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

		/**
		 * Get the maximum triplets.
		 *
		 * @return maximum triplets
		 */
		public long getMaximumTriplets() {
			return maximumTriplets;
		}

		@Override
		public String toString() {
			NumberFormat nf = NumberFormat.getInstance();

			return "vertex count: " + nf.format(vertexCount)
				+ "; edge count: " + nf.format(edgeCount)
				+ "; triangle triplet count: " + nf.format(triangleTripletCount)
				+ "; rectangle triplet count: " + nf.format(rectangleTripletCount)
				+ "; triplet count: " + nf.format(tripletCount)
				+ "; maximum degree: " + nf.format(maximumDegree)
				+ "; maximum triangle triplets: " + nf.format(maximumTriangleTriplets)
				+ "; maximum rectangle triplets: " + nf.format(maximumRectangleTriplets)
				+ "; maximum triplets: " + nf.format(maximumTriplets);
		}

		@Override
		public int hashCode() {
			return new HashCodeBuilder()
				.append(vertexCount)
				.append(edgeCount)
				.append(triangleTripletCount)
				.append(rectangleTripletCount)
				.append(tripletCount)
				.append(maximumDegree)
				.append(maximumTriangleTriplets)
				.append(maximumRectangleTriplets)
				.append(maximumTriplets)
				.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == null) { return false; }
			if (obj == this) { return true; }
			if (obj.getClass() != getClass()) { return false; }

			Result rhs = (Result)obj;

			return new EqualsBuilder()
				.append(vertexCount, rhs.vertexCount)
				.append(edgeCount, rhs.edgeCount)
				.append(triangleTripletCount, rhs.triangleTripletCount)
				.append(rectangleTripletCount, rhs.rectangleTripletCount)
				.append(tripletCount, rhs.tripletCount)
				.append(maximumDegree, rhs.maximumDegree)
				.append(maximumTriangleTriplets, rhs.maximumTriangleTriplets)
				.append(maximumRectangleTriplets, rhs.maximumRectangleTriplets)
				.append(maximumTriplets, rhs.maximumTriplets)
				.isEquals();
		}
	}
}
