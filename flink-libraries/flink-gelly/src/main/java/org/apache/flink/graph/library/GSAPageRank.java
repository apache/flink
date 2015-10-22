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

package org.apache.flink.graph.library;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.EdgeJoinFunction;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.gsa.ApplyFunction;
import org.apache.flink.graph.gsa.GatherFunction;
import org.apache.flink.graph.gsa.Neighbor;
import org.apache.flink.graph.gsa.SumFunction;

/**
 * This is an implementation of a simple PageRank algorithm, using a gather-sum-apply iteration.
 * The user can define the damping factor and the maximum number of iterations.
 * If the number of vertices of the input graph is known, it should be provided as a parameter
 * to speed up computation. Otherwise, the algorithm will first execute a job to count the vertices.
 * 
 * The implementation assumes that each page has at least one incoming and one outgoing link.
 */
public class GSAPageRank<K> implements GraphAlgorithm<K, Double, Double, DataSet<Vertex<K, Double>>> {

	private double beta;
	private int maxIterations;
	private long numberOfVertices;

	/**
	 * Creates an instance of the GSA PageRank algorithm.
	 * If the number of vertices of the input graph is known,
	 * use the {@link GSAPageRank#GSAPageRank(double, long, int)} constructor instead.
	 * 
	 * The implementation assumes that each page has at least one incoming and one outgoing link.
	 * 
	 * @param beta the damping factor
	 * @param maxIterations the maximum number of iterations
	 */
	public GSAPageRank(double beta, int maxIterations) {
		this.beta = beta;
		this.maxIterations = maxIterations;
	}

	/**
	 * Creates an instance of the GSA PageRank algorithm.
	 * If the number of vertices of the input graph is known,
	 * use the {@link GSAPageRank#GSAPageRank(double, long)} constructor instead.
	 * 
	 * The implementation assumes that each page has at least one incoming and one outgoing link.
	 * 
	 * @param beta the damping factor
	 * @param maxIterations the maximum number of iterations
	 * @param numVertices the number of vertices in the input
	 */
	public GSAPageRank(double beta, long numVertices, int maxIterations) {
		this.beta = beta;
		this.numberOfVertices = numVertices;
		this.maxIterations = maxIterations;
	}

	@Override
	public DataSet<Vertex<K, Double>> run(Graph<K, Double, Double> network) throws Exception {

		if (numberOfVertices == 0) {
			numberOfVertices = network.numberOfVertices();
		}

		DataSet<Tuple2<K, Long>> vertexOutDegrees = network.outDegrees();

		Graph<K, Double, Double> networkWithWeights = network
				.joinWithEdgesOnSource(vertexOutDegrees, new InitWeights());

		return networkWithWeights.runGatherSumApplyIteration(new GatherRanks(numberOfVertices), new SumRanks(),
				new UpdateRanks<K>(beta, numberOfVertices), maxIterations)
				.getVertices();
	}

	// --------------------------------------------------------------------------------------------
	//  Page Rank UDFs
	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("serial")
	private static final class GatherRanks extends GatherFunction<Double, Double, Double> {

		long numberOfVertices;

		public GatherRanks(long numberOfVertices) {
			this.numberOfVertices = numberOfVertices;
		}

		@Override
		public Double gather(Neighbor<Double, Double> neighbor) {
			double neighborRank = neighbor.getNeighborValue();

			if(getSuperstepNumber() == 1) {
				neighborRank = 1.0 / numberOfVertices;
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
		private final long numVertices;

		public UpdateRanks(double beta, long numberOfVertices) {
			this.beta = beta;
			this.numVertices = numberOfVertices;
		}

		@Override
		public void apply(Double rankSum, Double currentValue) {
			setResult((1-beta)/numVertices + beta * rankSum);
		}
	}

	@SuppressWarnings("serial")
	private static final class InitWeights implements EdgeJoinFunction<Double, Long> {

		public Double edgeJoin(Double edgeValue, Long inputValue) {
			return edgeValue / (double) inputValue;
		}
	}
}
