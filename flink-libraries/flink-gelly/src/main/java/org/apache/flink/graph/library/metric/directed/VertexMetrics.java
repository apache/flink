/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.flink.graph.library.metric.directed;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
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

import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_DEFAULT;

/**
 * Compute the number of vertices, number of edges, and number of triplets in
 * a directed graph.
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

		return new Result(vertexCount, edgeCount / 2, tripletCount);
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

		/**
		 * This helper class collects vertex metrics by scanning over and
		 * discarding elements from the given DataSet.
		 *
		 * The unique id is required because Flink's accumulator namespace is
		 * among all operators.
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

			vertexCount++;
			edgeCount += outDegree;
			tripletCount += degree * (degree - 1) / 2;
		}

		@Override
		public void close() throws IOException {
			getRuntimeContext().addAccumulator(id + "-0", new LongCounter(vertexCount));
			getRuntimeContext().addAccumulator(id + "-1", new LongCounter(edgeCount));
			getRuntimeContext().addAccumulator(id + "-2", new LongCounter(tripletCount));
		}
	}

	/**
	 * Wraps vertex metrics.
	 */
	public static class Result {
		private long vertexCount;
		private long edgeCount;
		private long tripletCount;

		public Result(long vertexCount, long edgeCount, long tripletCount) {
			this.vertexCount = vertexCount;
			this.edgeCount = edgeCount;
			this.tripletCount = tripletCount;
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

		@Override
		public String toString() {
			return "vertex count: " + vertexCount
				+ ", edge count:" + edgeCount
				+ ", triplet count: " + tripletCount;
		}

		@Override
		public int hashCode() {
			return new HashCodeBuilder()
				.append(vertexCount)
				.append(edgeCount)
				.append(tripletCount)
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
				.isEquals();
		}
	}
}
