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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.AbstractGraphAnalytic;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.clustering.undirected.AverageClusteringCoefficient.Result;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.util.AbstractID;

import java.io.IOException;

import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_DEFAULT;

/**
 * The average clustering coefficient measures the mean connectedness of a
 * graph. Scores range from 0.0 (no triangles) to 1.0 (complete graph).
 *
 * @param <K> graph ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class AverageClusteringCoefficient<K extends Comparable<K> & CopyableValue<K>, VV, EV>
extends AbstractGraphAnalytic<K, VV, EV, Result> {

	private String id = new AbstractID().toString();

	// Optional configuration
	private int littleParallelism = PARALLELISM_DEFAULT;

	/**
	 * Override the parallelism of operators processing small amounts of data.
	 *
	 * @param littleParallelism operator parallelism
	 * @return this
	 */
	public AverageClusteringCoefficient<K, VV, EV> setLittleParallelism(int littleParallelism) {
		this.littleParallelism = littleParallelism;

		return this;
	}

	/*
	 * Implementation notes:
	 *
	 * The requirement that "K extends CopyableValue<K>" can be removed when
	 *   removed from LocalClusteringCoefficient.
	 */

	@Override
	public AverageClusteringCoefficient<K, VV, EV> run(Graph<K, VV, EV> input)
			throws Exception {
		super.run(input);

		DataSet<LocalClusteringCoefficient.Result<K>> localClusteringCoefficient = input
			.run(new LocalClusteringCoefficient<K, VV, EV>()
				.setLittleParallelism(littleParallelism));

		localClusteringCoefficient
			.output(new AverageClusteringCoefficientHelper<K>(id))
				.name("Average clustering coefficient");

		return this;
	}

	@Override
	public Result getResult() {
		JobExecutionResult res = env.getLastJobExecutionResult();

		long vertexCount = res.getAccumulatorResult(id + "-0");
		double sumOfLocalClusteringCoefficient = res.getAccumulatorResult(id + "-1");

		return new Result(vertexCount, sumOfLocalClusteringCoefficient);
	}

	/**
	 * Helper class to collect the average clustering coefficient.
	 *
	 * @param <T> ID type
	 */
	private static class AverageClusteringCoefficientHelper<T>
	extends RichOutputFormat<LocalClusteringCoefficient.Result<T>> {
		private final String id;

		private long vertexCount;
		private double sumOfLocalClusteringCoefficient;

		/**
		 * The unique id is required because Flink's accumulator namespace is
		 * shared among all operators.
		 *
		 * @param id unique string used for accumulator names
		 */
		public AverageClusteringCoefficientHelper(String id) {
			this.id = id;
		}

		@Override
		public void configure(Configuration parameters) {}

		@Override
		public void open(int taskNumber, int numTasks) throws IOException {}

		@Override
		public void writeRecord(LocalClusteringCoefficient.Result<T> record) throws IOException {
			vertexCount++;

			// local clustering coefficient is only defined on vertices with
			// at least two neighbors yielding at least one pair of neighbors
			if (record.getDegree().getValue() > 1) {
				sumOfLocalClusteringCoefficient += record.getLocalClusteringCoefficientScore();
			}
		}

		@Override
		public void close() throws IOException {
			getRuntimeContext().addAccumulator(id + "-0", new LongCounter(vertexCount));
			getRuntimeContext().addAccumulator(id + "-1", new DoubleCounter(sumOfLocalClusteringCoefficient));
		}
	}

	/**
	 * Wraps global clustering coefficient metrics.
	 */
	public static class Result {
		private long vertexCount;

		private double averageLocalClusteringCoefficient;

		/**
		 * Instantiate an immutable result.
		 *
		 * @param vertexCount vertex count
		 * @param sumOfLocalClusteringCoefficient sum over the vertices' local
		 *                                        clustering coefficients
		 */
		public Result(long vertexCount, double sumOfLocalClusteringCoefficient) {
			this.vertexCount = vertexCount;
			this.averageLocalClusteringCoefficient = sumOfLocalClusteringCoefficient / vertexCount;
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
		 * Get the average clustering coefficient.
		 *
		 * @return number of triangles
		 */
		public double getAverageClusteringCoefficient() {
			return averageLocalClusteringCoefficient;
		}

		@Override
		public String toString() {
			return "vertex count: " + vertexCount
				+ ", average clustering coefficient: " + averageLocalClusteringCoefficient;
		}

		@Override
		public int hashCode() {
			return new HashCodeBuilder()
				.append(vertexCount)
				.append(averageLocalClusteringCoefficient)
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
				.append(averageLocalClusteringCoefficient, rhs.averageLocalClusteringCoefficient)
				.isEquals();
		}
	}
}
