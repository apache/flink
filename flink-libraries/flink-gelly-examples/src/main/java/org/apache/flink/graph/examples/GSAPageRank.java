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

package org.apache.flink.graph.examples;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.EdgeJoinFunction;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.gsa.ApplyFunction;
import org.apache.flink.graph.gsa.GSAConfiguration;
import org.apache.flink.graph.gsa.GatherFunction;
import org.apache.flink.graph.gsa.Neighbor;
import org.apache.flink.graph.gsa.SumFunction;
import org.apache.flink.types.LongValue;

/**
 * This is an implementation of a simple PageRank algorithm, using a gather-sum-apply iteration.
 * The user can define the damping factor and the maximum number of iterations.
 *
 * <p>The implementation assumes that each page has at least one incoming and one outgoing link.
 */
public class GSAPageRank<K> implements GraphAlgorithm<K, Double, Double, DataSet<Vertex<K, Double>>> {

	private double beta;
	private int maxIterations;

	/**
	 * Creates an instance of the GSA PageRank algorithm.
	 *
	 * <p>The implementation assumes that each page has at least one incoming and one outgoing link.
	 *
	 * @param beta the damping factor
	 * @param maxIterations the maximum number of iterations
	 */
	public GSAPageRank(double beta, int maxIterations) {
		this.beta = beta;
		this.maxIterations = maxIterations;
	}

	@Override
	public DataSet<Vertex<K, Double>> run(Graph<K, Double, Double> network) throws Exception {
		DataSet<Tuple2<K, LongValue>> vertexOutDegrees = network.outDegrees();

		Graph<K, Double, Double> networkWithWeights = network
				.joinWithEdgesOnSource(vertexOutDegrees, new InitWeights());

		GSAConfiguration parameters = new GSAConfiguration();
		parameters.setOptNumVertices(true);

		return networkWithWeights.runGatherSumApplyIteration(new GatherRanks(), new SumRanks(),
			new UpdateRanks<>(beta), maxIterations, parameters)
				.getVertices();
	}

	// --------------------------------------------------------------------------------------------
	//  Page Rank UDFs
	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("serial")
	private static final class GatherRanks extends GatherFunction<Double, Double, Double> {

		@Override
		public Double gather(Neighbor<Double, Double> neighbor) {
			double neighborRank = neighbor.getNeighborValue();

			if (getSuperstepNumber() == 1) {
				neighborRank = 1.0 / this.getNumberOfVertices();
			}

			return neighborRank * neighbor.getEdgeValue();
		}
	}

	@SuppressWarnings("serial")
	private static final class SumRanks extends SumFunction<Double, Double, Double> {

		@Override
		public Double sum(Double newValue, Double currentValue) {
			return newValue + currentValue;
		}
	}

	@SuppressWarnings("serial")
	private static final class UpdateRanks<K> extends ApplyFunction<K, Double, Double> {

		private final double beta;

		public UpdateRanks(double beta) {
			this.beta = beta;
		}

		@Override
		public void apply(Double rankSum, Double currentValue) {
			setResult((1 - beta) / this.getNumberOfVertices() + beta * rankSum);
		}
	}

	@SuppressWarnings("serial")
	private static final class InitWeights implements EdgeJoinFunction<Double, LongValue> {

		public Double edgeJoin(Double edgeValue, LongValue inputValue) {
			return edgeValue / (double) inputValue.getValue();
		}
	}
}
