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

import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.gsa.ApplyFunction;
import org.apache.flink.graph.gsa.GatherFunction;
import org.apache.flink.graph.gsa.Neighbor;
import org.apache.flink.graph.gsa.SumFunction;

/**
 * This is an implementation of a simple PageRank algorithm, using a gather-sum-apply iteration.
 */
public class GSAPageRank<K> implements GraphAlgorithm<K, Double, Double> {

	private double beta;
	private int maxIterations;

	public GSAPageRank(double beta, int maxIterations) {
		this.beta = beta;
		this.maxIterations = maxIterations;
	}

	@Override
	public Graph<K, Double, Double> run(Graph<K, Double, Double> network) throws Exception {

		final long numberOfVertices = network.numberOfVertices();

		return network.runGatherSumApplyIteration(new GatherRanks(numberOfVertices), new SumRanks(),
				new UpdateRanks<K>(beta, numberOfVertices), maxIterations);
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
}
