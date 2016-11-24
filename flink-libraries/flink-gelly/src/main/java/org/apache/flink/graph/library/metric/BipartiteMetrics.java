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

package org.apache.flink.graph.library.metric;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongMaximum;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.bipartite.BipartiteEdge;
import org.apache.flink.graph.bipartite.BipartiteGraph;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.text.NumberFormat;

import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_DEFAULT;

/**
 * Compute the following edge metrics in a bipartite graph:
 *  - number of top vertices
 *  - number of bottom vertices
 *  - number of edges
 *  - maximum top degree
 *  - maximum bottom degree
 *  - number of vertices
 */
public class BipartiteMetrics {

	private String id = new AbstractID().toString();
	private ExecutionEnvironment env;

	private int parallelism = PARALLELISM_DEFAULT;

	private long numberOfTopVertices;
	private long numberOfBottomVertices;
	private long numberOfEdges;

	/**
	 * Override the operator parallelism.
	 *
	 * @param parallelism operator parallelism
	 * @return this
	 */
	public BipartiteMetrics setParallelism(int parallelism) {
		this.parallelism = parallelism;

		return this;
	}

	/**
	 * Run metrics calculation.
	 *
	 * @param <KT> the key type of top vertices
	 * @param <KB> the key type of bottom vertices
	 * @param <VVT> the vertex value type of top vertices
	 * @param <VVB> the vertex value type of bottom vertices
	 * @param <EV> the edge value type
	 * @param bipartiteGraph input graph
	 *
	 * @return this
	 * @throws Exception
	 */
	public <KT, KB, VVT, VVB, EV> BipartiteMetrics run(BipartiteGraph<KT, KB, VVT, VVB, EV> bipartiteGraph) throws Exception {
		env = bipartiteGraph.getContext();
		numberOfBottomVertices = bipartiteGraph.getBottomVertices().count();
		numberOfTopVertices = bipartiteGraph.getTopVertices().count();
		numberOfEdges = bipartiteGraph.getEdges().count();

		DataSet<Tuple2<KT, Long>> topVertices = bipartiteGraph.getEdges()
			.groupBy(0)
			.reduceGroup(new TopVerticesReduce<KT, KB, EV>())
			.setParallelism(parallelism);

		DataSet<Tuple2<KB, Long>> bottomVertices = bipartiteGraph.getEdges()
			.groupBy(1)
			.reduceGroup(new BottomVerticesReduce<KT, KB, EV>())
			.setParallelism(parallelism);

		topVertices.output(new BipartiteGraphMetricsHelper<KT>(id, "-0"));
		bottomVertices.output(new BipartiteGraphMetricsHelper<KB>(id, "-1"));

		return this;
	}

	/**
	 * Execute the metrics calculation and return the result.
	 *
	 * @return the result
	 * @throws Exception
	 */
	public Result execute() throws Exception {
		Preconditions.checkNotNull(env);

		env.execute();
		return getResult();
	}

	/**
	 * This method must be called after the program has executed:
	 *  1) "run" analytics and algorithms
	 *  2) call ExecutionEnvironment.execute()
	 *  3) get analytic results
	 *
	 * @return the result
	 */
	public Result getResult() {
		JobExecutionResult res = env.getLastJobExecutionResult();

		long maxTopDegree = res.getAccumulatorResult(id + "-0");
		long maxBottomDegree = res.getAccumulatorResult(id + "-1");

		return new Result(
			numberOfTopVertices, numberOfBottomVertices,
			numberOfEdges,
			maxTopDegree, maxBottomDegree);
	}

	private static class TopVerticesReduce<KT, KB, EV> implements GroupReduceFunction<BipartiteEdge<KT,KB,EV>, Tuple2<KT, Long>> {

		private Tuple2<KT, Long> result = new Tuple2<>();

		@Override
		public void reduce(Iterable<BipartiteEdge<KT, KB, EV>> values, Collector<Tuple2<KT, Long>> out) throws Exception {
			KT id = null;
			long count = 0;
			for (BipartiteEdge<KT, KB, EV> edge : values) {
				id = edge.getTopId();
				count++;
			}

			result.f0 = id;
			result.f1 = count;
			out.collect(result);
		}
	}

	private static class BottomVerticesReduce<KT, KB, EV> implements GroupReduceFunction<BipartiteEdge<KT,KB,EV>, Tuple2<KB, Long>> {

		private Tuple2<KB, Long> result = new Tuple2<>();

		@Override
		public void reduce(Iterable<BipartiteEdge<KT, KB, EV>> values, Collector<Tuple2<KB, Long>> out) throws Exception {
			KB id = null;
			long count = 0;
			for (BipartiteEdge<KT, KB, EV> edge : values) {
				id = edge.getBottomId();
				count++;
			}

			result.f0 = id;
			result.f1 = count;
			out.collect(result);
		}
	}

	private static class BipartiteGraphMetricsHelper<KT> extends RichOutputFormat<Tuple2<KT,Long>> {

		private final String id;
		private final String vertexCountSuffix;
		private long maxDegree;
		
		public BipartiteGraphMetricsHelper(String id, String vertexCountSuffix) {
			this.id = id;
			this.vertexCountSuffix = vertexCountSuffix;
		}

		@Override
		public void configure(Configuration parameters) { }

		@Override
		public void open(int taskNumber, int numTasks) throws IOException { }

		@Override
		public void writeRecord(Tuple2<KT, Long> record) throws IOException {
			long degree = record.f1;
			maxDegree = Math.max(maxDegree, degree);
		}

		@Override
		public void close() throws IOException {
			getRuntimeContext().addAccumulator(id + vertexCountSuffix, new LongMaximum(maxDegree));
		}
	}

	/**
	 * Wraps bipartite metrics.
	 */
	public static class Result {
		private long numberOfTopVertices;
		private long numberOfBottomVertices;
		private long numberOfEdges;
		private long maximumTopDegree;
		private long maximumBottomDegree;

		public Result(long numberOfTopVertices,
					long numberOfBottomVertices,
					long numberOfEdges,
					long maximumTopDegree,
					long maximumBottomDegree) {
			this.numberOfTopVertices = numberOfTopVertices;
			this.numberOfBottomVertices = numberOfBottomVertices;
			this.numberOfEdges = numberOfEdges;
			this.maximumTopDegree = maximumTopDegree;
			this.maximumBottomDegree = maximumBottomDegree;
		}

		/**
		 * Get the number of top vertices.
		 *
		 * @return number of top vertices
		 */
		public long getNumberOfTopVertices() {
			return numberOfTopVertices;
		}

		/**
		 * Get the number of bottom vertices.
		 *
		 * @return number of bottom vertices
		 */
		public long getNumberOfBottomVertices() {
			return numberOfBottomVertices;
		}

		/**
		 * Get the number of edges.
		 *
		 * @return number of edges
		 */
		public long getNumberOfEdges() {
			return numberOfEdges;
		}

		/**
		 * Get the maximum top degree.
		 *
		 * @return maximum top degree
		 */
		public long getMaximumTopDegree() {
			return maximumTopDegree;
		}

		/**
		 * Get the maximum bottom degree.
		 *
		 * @return maximum bottom degree
		 */
		public long getMaximumBottomDegree() {

			return maximumBottomDegree;
		}
		@Override
		public String toString() {
			NumberFormat nf = NumberFormat.getInstance();

			return "number of top vertices: " + nf.format(numberOfTopVertices)
				+ "; number of bottom vertices: " + nf.format(numberOfBottomVertices)
				+ "; number of edges: " + nf.format(numberOfEdges)
				+ "; maximum top degree: " + nf.format(maximumTopDegree)
				+ "; maximum bottom degree: " + nf.format(maximumBottomDegree);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}

			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			Result result = (Result) o;

			return new EqualsBuilder()
				.append(numberOfTopVertices, result.numberOfTopVertices)
				.append(numberOfBottomVertices, result.numberOfBottomVertices)
				.append(numberOfEdges, result.numberOfEdges)
				.append(maximumTopDegree, result.maximumTopDegree)
				.append(maximumBottomDegree, result.maximumBottomDegree)
				.isEquals();
		}

		@Override
		public int hashCode() {
			return new HashCodeBuilder(17, 37)
				.append(numberOfTopVertices)
				.append(numberOfBottomVertices)
				.append(numberOfEdges)
				.append(maximumTopDegree)
				.append(maximumBottomDegree)
				.toHashCode();
		}
	}
}
