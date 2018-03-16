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
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeJoinFunction;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.graph.spargel.ScatterGatherConfiguration;
import org.apache.flink.types.LongValue;

/**
 * This is an implementation of a simple PageRank algorithm, using a scatter-gather iteration.
 * The user can define the damping factor and the maximum number of iterations.
 *
 * <p>The implementation assumes that each page has at least one incoming and one outgoing link.
 */
public class PageRank<K> implements GraphAlgorithm<K, Double, Double, DataSet<Vertex<K, Double>>> {

	private double beta;
	private int maxIterations;

	/**
	 * Creates an instance of the PageRank algorithm.
	 *
	 * <p>The implementation assumes that each page has at least one incoming and one outgoing link.
	 *
	 * @param beta the damping factor
	 * @param maxIterations the maximum number of iterations
	 */
	public PageRank(double beta, int maxIterations) {
		this.beta = beta;
		this.maxIterations = maxIterations;
	}

	@Override
	public DataSet<Vertex<K, Double>> run(Graph<K, Double, Double> network) throws Exception {
		DataSet<Tuple2<K, LongValue>> vertexOutDegrees = network.outDegrees();

		Graph<K, Double, Double> networkWithWeights = network
				.joinWithEdgesOnSource(vertexOutDegrees, new InitWeights());

		ScatterGatherConfiguration parameters = new ScatterGatherConfiguration();
		parameters.setOptNumVertices(true);

		return networkWithWeights.runScatterGatherIteration(new RankMessenger<>(),
			new VertexRankUpdater<>(beta), maxIterations, parameters)
				.getVertices();
	}

	/**
	 * Distributes the rank of a vertex among all target vertices according to
	 * the transition probability, which is associated with an edge as the edge
	 * value.
	 */
	@SuppressWarnings("serial")
	public static final class RankMessenger<K> extends ScatterFunction<K, Double, Double, Double> {
		@Override
		public void sendMessages(Vertex<K, Double> vertex) {
			if (getSuperstepNumber() == 1) {
				// initialize vertex ranks
				vertex.setValue(1.0 / this.getNumberOfVertices());
			}

			for (Edge<K, Double> edge : getEdges()) {
				sendMessageTo(edge.getTarget(), vertex.getValue() * edge.getValue());
			}
		}
	}

	/**
	 * Function that updates the rank of a vertex by summing up the partial
	 * ranks from all incoming messages and then applying the dampening formula.
	 */
	@SuppressWarnings("serial")
	public static final class VertexRankUpdater<K> extends GatherFunction<K, Double, Double> {
		private final double beta;

		public VertexRankUpdater(double beta) {
			this.beta = beta;
		}

		@Override
		public void updateVertex(Vertex<K, Double> vertex, MessageIterator<Double> inMessages) {
			double rankSum = 0.0;
			for (double msg : inMessages) {
				rankSum += msg;
			}

			// apply the dampening factor / random jump
			double newRank = (beta * rankSum) + (1 - beta) / this.getNumberOfVertices();
			setNewVertexValue(newRank);
		}
	}

	@SuppressWarnings("serial")
	private static final class InitWeights implements EdgeJoinFunction<Double, LongValue> {
		public Double edgeJoin(Double edgeValue, LongValue inputValue) {
			return edgeValue / (double) inputValue.getValue();
		}
	}

}
