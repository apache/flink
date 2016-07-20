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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.accumulators.LongMaximum;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.operators.base.ReduceOperatorBase.CombineHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.AbstractGraphAnalytic;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.asm.degree.annotate.directed.EdgeDegreesPair;
import org.apache.flink.graph.asm.degree.annotate.directed.VertexDegrees.Degrees;
import org.apache.flink.graph.library.metric.directed.EdgeMetrics.Result;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.text.NumberFormat;

import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_DEFAULT;

/**
 * Compute the following edge metrics in a directed graph:
 *  - number of vertices
 *  - number of edges
 *  - number of triangle triplets
 *  - number of rectangle triplets
 *  - number of triplets
 *  - maximum degree
 *  - maximum out degree
 *  - maximum in degree
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

	private int parallelism = PARALLELISM_DEFAULT;

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
	 *   a hash-combineable hashable-reduce.
	 *
	 * Use distinct to replace ReduceEdgeStats when the combiner can be disabled
	 *   with a sorted-reduce forced.
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
			.flatMap(new EdgeStats<K, EV>())
				.setParallelism(parallelism)
				.name("Edge stats")
			.groupBy(0, 1)
			.reduceGroup(new ReduceEdgeStats<K>())
				.setParallelism(parallelism)
				.name("Reduce edge stats")
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
		long maximumOutDegree = res.getAccumulatorResult(id + "-6");
		long maximumInDegree = res.getAccumulatorResult(id + "-7");
		long maximumTriangleTriplets = res.getAccumulatorResult(id + "-8");
		long maximumRectangleTriplets = res.getAccumulatorResult(id + "-9");
		long maximumTriplets = res.getAccumulatorResult(id + "-a");

		return new Result(vertexCount, edgeCount, triangleTripletCount, rectangleTripletCount, tripletCount,
			maximumDegree, maximumOutDegree, maximumInDegree,
			maximumTriangleTriplets, maximumRectangleTriplets, maximumTriplets);
	}

	/**
	 * Produces a pair of tuples. The first tuple contains the source vertex ID,
	 * the target vertex ID, the source degrees, and the low-order count. The
	 * second tuple is the same with the source and target roles reversed.
	 *
	 * The low-order count is one if the source vertex degree is less than the
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
	private static class EdgeMetricsHelper<T extends Comparable<T>, ET>
	extends RichOutputFormat<Tuple3<T, Degrees, LongValue>> {
		private final String id;

		private long vertexCount;
		private long edgeCount;
		private long triangleTripletCount;
		private long rectangleTripletCount;
		private long tripletCount;
		private long maximumDegree;
		private long maximumOutDegree;
		private long maximumInDegree;
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
		public void writeRecord(Tuple3<T, Degrees, LongValue> record) throws IOException {
			Degrees degrees = record.f1;
			long degree = degrees.getDegree().getValue();
			long outDegree = degrees.getOutDegree().getValue();
			long inDegree = degrees.getInDegree().getValue();

			long lowDegree = record.f2.getValue();
			long highDegree = degree - lowDegree;

			long triangleTriplets = lowDegree * (lowDegree - 1) / 2;
			long rectangleTriplets = triangleTriplets + lowDegree * highDegree;
			long triplets = degree * (degree - 1) / 2;

			vertexCount++;
			edgeCount += outDegree;
			triangleTripletCount += triangleTriplets;
			rectangleTripletCount += rectangleTriplets;
			tripletCount += triplets;
			maximumDegree = Math.max(maximumDegree, degree);
			maximumOutDegree = Math.max(maximumOutDegree, outDegree);
			maximumInDegree = Math.max(maximumInDegree, inDegree);
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
			getRuntimeContext().addAccumulator(id + "-6", new LongMaximum(maximumOutDegree));
			getRuntimeContext().addAccumulator(id + "-7", new LongMaximum(maximumInDegree));
			getRuntimeContext().addAccumulator(id + "-8", new LongMaximum(maximumTriangleTriplets));
			getRuntimeContext().addAccumulator(id + "-9", new LongMaximum(maximumRectangleTriplets));
			getRuntimeContext().addAccumulator(id + "-a", new LongMaximum(maximumTriplets));
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
		private long maximumOutDegree;
		private long maximumInDegree;
		private long maximumTriangleTriplets;
		private long maximumRectangleTriplets;
		private long maximumTriplets;

		public Result(long vertexCount, long edgeCount, long triangleTripletCount, long rectangleTripletCount, long tripletCount,
				long maximumDegree, long maximumOutDegree, long maximumInDegree,
				long maximumTriangleTriplets, long maximumRectangleTriplets, long maximumTriplets) {
			this.vertexCount = vertexCount;
			this.edgeCount = edgeCount;
			this.triangleTripletCount = triangleTripletCount;
			this.rectangleTripletCount = rectangleTripletCount;
			this.tripletCount = tripletCount;
			this.maximumDegree = maximumDegree;
			this.maximumOutDegree = maximumOutDegree;
			this.maximumInDegree = maximumInDegree;
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
		 * Get the maximum out degree.
		 *
		 * @return maximum out degree
		 */
		public long getMaximumOutDegree() {
			return maximumOutDegree;
		}

		/**
		 * Get the maximum in degree.
		 *
		 * @return maximum in degree
		 */
		public long getMaximumInDegree() {
			return maximumInDegree;
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
				+ "; maximum out degree: " + nf.format(maximumOutDegree)
				+ "; maximum in degree: " + nf.format(maximumInDegree)
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
				.append(maximumOutDegree)
				.append(maximumInDegree)
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
				.append(maximumOutDegree, rhs.maximumOutDegree)
				.append(maximumInDegree, rhs.maximumInDegree)
				.append(maximumTriangleTriplets, rhs.maximumTriangleTriplets)
				.append(maximumRectangleTriplets, rhs.maximumRectangleTriplets)
				.append(maximumTriplets, rhs.maximumTriplets)
				.isEquals();
		}
	}
}
