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

import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.AnalyticHelper;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAnalyticBase;
import org.apache.flink.graph.asm.result.PrintableResult;
import org.apache.flink.graph.library.clustering.undirected.AverageClusteringCoefficient.Result;
import org.apache.flink.types.CopyableValue;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.IOException;

/**
 * The average clustering coefficient measures the mean connectedness of a
 * graph. Scores range from 0.0 (no triangles) to 1.0 (complete graph).
 *
 * @param <K> graph ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class AverageClusteringCoefficient<K extends Comparable<K> & CopyableValue<K>, VV, EV>
extends GraphAnalyticBase<K, VV, EV, Result> {

	private static final String VERTEX_COUNT = "vertexCount";

	private static final String SUM_OF_LOCAL_CLUSTERING_COEFFICIENT = "sumOfLocalClusteringCoefficient";

	private AverageClusteringCoefficientHelper<K> averageClusteringCoefficientHelper;

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
				.setParallelism(parallelism));

		averageClusteringCoefficientHelper = new AverageClusteringCoefficientHelper<>();

		localClusteringCoefficient
			.output(averageClusteringCoefficientHelper)
				.name("Average clustering coefficient");

		return this;
	}

	@Override
	public Result getResult() {
		long vertexCount = averageClusteringCoefficientHelper.getAccumulator(env, VERTEX_COUNT);
		double sumOfLocalClusteringCoefficient = averageClusteringCoefficientHelper.getAccumulator(env, SUM_OF_LOCAL_CLUSTERING_COEFFICIENT);

		return new Result(vertexCount, sumOfLocalClusteringCoefficient);
	}

	/**
	 * Helper class to collect the average clustering coefficient.
	 *
	 * @param <T> ID type
	 */
	private static class AverageClusteringCoefficientHelper<T>
	extends AnalyticHelper<LocalClusteringCoefficient.Result<T>> {
		private long vertexCount;
		private double sumOfLocalClusteringCoefficient;

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
			addAccumulator(VERTEX_COUNT, new LongCounter(vertexCount));
			addAccumulator(SUM_OF_LOCAL_CLUSTERING_COEFFICIENT, new DoubleCounter(sumOfLocalClusteringCoefficient));
		}
	}

	/**
	 * Wraps global clustering coefficient metrics.
	 */
	public static class Result
	implements PrintableResult {
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
			return toPrintableString();
		}

		@Override
		public String toPrintableString() {
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
				.append(vertexCount, rhs.vertexCount)
				.append(averageLocalClusteringCoefficient, rhs.averageLocalClusteringCoefficient)
				.isEquals();
		}
	}
}
