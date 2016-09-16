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
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.AbstractGraphAnalytic;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.degree.annotate.directed.VertexDegrees;
import org.apache.flink.graph.asm.degree.annotate.directed.VertexDegrees.Degrees;
import org.apache.flink.graph.library.metric.directed.VertexMetrics.Result;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.util.AbstractID;

import java.io.IOException;
import java.text.NumberFormat;

import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_DEFAULT;

/**
 * Compute the following vertex metrics in a directed graph:
 *  - number of vertices
 *  - number of edges
 *  - number of triplets
 *  - maximum degree
 *  - maximum out degree
 *  - maximum in degree
 *  - maximum number of triplets
 *
 * @param <K> graph ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class VertexMetrics<K extends Comparable<K> & CopyableValue<K>, VV, EV>
extends AbstractGraphAnalytic<K, VV, EV, Result> {

	private String id = new AbstractID().toString();

	// Optional configuration
	private boolean includeZeroDegreeVertices = false;

	private int parallelism = PARALLELISM_DEFAULT;

	/**
	 * By default only the edge set is processed for the computation of degree.
	 * When this flag is set an additional join is performed against the vertex
	 * set in order to output vertices with a degree of zero.
	 *
	 * @param includeZeroDegreeVertices whether to output vertices with a
	 *                                  degree of zero
	 * @return this
	 */
	public VertexMetrics<K, VV, EV> setIncludeZeroDegreeVertices(boolean includeZeroDegreeVertices) {
		this.includeZeroDegreeVertices = includeZeroDegreeVertices;

		return this;
	}

	/**
	 * Override the operator parallelism.
	 *
	 * @param parallelism operator parallelism
	 * @return this
	 */
	public VertexMetrics<K, VV, EV> setParallelism(int parallelism) {
		this.parallelism = parallelism;

		return this;
	}

	@Override
	public VertexMetrics<K, VV, EV> run(Graph<K, VV, EV> input)
			throws Exception {
		super.run(input);

		DataSet<Vertex<K, Degrees>> vertexDegree = input
			.run(new VertexDegrees<K, VV, EV>()
				.setIncludeZeroDegreeVertices(includeZeroDegreeVertices)
				.setParallelism(parallelism));

		vertexDegree
			.output(new VertexMetricsHelper<K>(id))
				.name("Vertex metrics");

		return this;
	}

	@Override
	public Result getResult() {
		JobExecutionResult res = env.getLastJobExecutionResult();

		long vertexCount = res.getAccumulatorResult(id + "-0");
		long edgeCount = res.getAccumulatorResult(id + "-1");
		long tripletCount = res.getAccumulatorResult(id + "-2");
		long maximumDegree = res.getAccumulatorResult(id + "-3");
		long maximumOutDegree = res.getAccumulatorResult(id + "-4");
		long maximumInDegree = res.getAccumulatorResult(id + "-5");
		long maximumTriplets = res.getAccumulatorResult(id + "-6");

		return new Result(vertexCount, edgeCount, tripletCount, maximumDegree, maximumOutDegree, maximumInDegree, maximumTriplets);
	}

	/**
	 * Helper class to collect vertex metrics.
	 *
	 * @param <T> ID type
	 */
	private static class VertexMetricsHelper<T>
	extends RichOutputFormat<Vertex<T, Degrees>> {
		private final String id;

		private long vertexCount;
		private long edgeCount;
		private long tripletCount;
		private long maximumDegree;
		private long maximumOutDegree;
		private long maximumInDegree;
		private long maximumTriplets;

		/**
		 * This helper class collects vertex metrics by scanning over and
		 * discarding elements from the given DataSet.
		 *
		 * The unique id is required because Flink's accumulator namespace is
		 * shared among all operators.
		 *
		 * @param id unique string used for accumulator names
		 */
		public VertexMetricsHelper(String id) {
			this.id = id;
		}

		@Override
		public void configure(Configuration parameters) {}

		@Override
		public void open(int taskNumber, int numTasks) throws IOException {}

		@Override
		public void writeRecord(Vertex<T, Degrees> record) throws IOException {
			long degree = record.f1.getDegree().getValue();
			long outDegree = record.f1.getOutDegree().getValue();
			long inDegree = record.f1.getInDegree().getValue();
			long triplets = degree * (degree - 1) / 2;

			vertexCount++;
			edgeCount += outDegree;
			tripletCount += triplets;
			maximumDegree = Math.max(maximumDegree, degree);
			maximumOutDegree = Math.max(maximumOutDegree, outDegree);
			maximumInDegree = Math.max(maximumInDegree, inDegree);
			maximumTriplets = Math.max(maximumTriplets, triplets);
		}

		@Override
		public void close() throws IOException {
			getRuntimeContext().addAccumulator(id + "-0", new LongCounter(vertexCount));
			getRuntimeContext().addAccumulator(id + "-1", new LongCounter(edgeCount));
			getRuntimeContext().addAccumulator(id + "-2", new LongCounter(tripletCount));
			getRuntimeContext().addAccumulator(id + "-3", new LongMaximum(maximumDegree));
			getRuntimeContext().addAccumulator(id + "-4", new LongMaximum(maximumOutDegree));
			getRuntimeContext().addAccumulator(id + "-5", new LongMaximum(maximumInDegree));
			getRuntimeContext().addAccumulator(id + "-6", new LongMaximum(maximumTriplets));
		}
	}

	/**
	 * Wraps vertex metrics.
	 */
	public static class Result {
		private long vertexCount;
		private long edgeCount;
		private long tripletCount;
		private long maximumDegree;
		private long maximumOutDegree;
		private long maximumInDegree;
		private long maximumTriplets;

		public Result(long vertexCount, long edgeCount, long tripletCount, long maximumDegree, long maximumOutDegree, long maximumInDegree, long maximumTriplets) {
			this.vertexCount = vertexCount;
			this.edgeCount = edgeCount;
			this.tripletCount = tripletCount;
			this.maximumDegree = maximumDegree;
			this.maximumOutDegree = maximumOutDegree;
			this.maximumInDegree = maximumInDegree;
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
				+ "; triplet count: " + nf.format(tripletCount)
				+ "; maximum degree: " + nf.format(maximumDegree)
				+ "; maximum out degree: " + nf.format(maximumOutDegree)
				+ "; maximum in degree: " + nf.format(maximumInDegree)
				+ "; maximum triplets: " + nf.format(maximumTriplets);
		}

		@Override
		public int hashCode() {
			return new HashCodeBuilder()
				.append(vertexCount)
				.append(edgeCount)
				.append(tripletCount)
				.append(maximumDegree)
				.append(maximumOutDegree)
				.append(maximumInDegree)
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
				.append(tripletCount, rhs.tripletCount)
				.append(maximumDegree, rhs.maximumDegree)
				.append(maximumOutDegree, rhs.maximumOutDegree)
				.append(maximumInDegree, rhs.maximumInDegree)
				.append(maximumTriplets, rhs.maximumTriplets)
				.isEquals();
		}
	}
}
