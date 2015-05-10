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

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;

public class PageRank<K> implements	GraphAlgorithm<K, Double, Double> {

	private double beta;
	private int maxIterations;

	public PageRank(double beta, int maxIterations) {
		this.beta = beta;
		this.maxIterations = maxIterations;
	}

	@Override
	public Graph<K, Double, Double> run(Graph<K, Double, Double> network) throws Exception {

		final long numberOfVertices = network.numberOfVertices();
		return network.runVertexCentricIteration(new VertexRankUpdater<K>(beta, numberOfVertices), new RankMessenger<K>(numberOfVertices),
				maxIterations);
	}

	/**
	 * Function that updates the rank of a vertex by summing up the partial
	 * ranks from all incoming messages and then applying the dampening formula.
	 */
	@SuppressWarnings("serial")
	public static final class VertexRankUpdater<K> extends VertexUpdateFunction<K, Double, Double> {

		private final double beta;
		private final long numVertices;
		
		public VertexRankUpdater(double beta, long numberOfVertices) {
			this.beta = beta;
			this.numVertices = numberOfVertices;
		}

		@Override
		public void updateVertex(K vertexKey, Double vertexValue,
				MessageIterator<Double> inMessages) {
			double rankSum = 0.0;
			for (double msg : inMessages) {
				rankSum += msg;
			}

			// apply the dampening factor / random jump
			double newRank = (beta * rankSum) + (1 - beta) / numVertices;
			setNewVertexValue(newRank);
		}
	}

	/**
	 * Distributes the rank of a vertex among all target vertices according to
	 * the transition probability, which is associated with an edge as the edge
	 * value.
	 */
	@SuppressWarnings("serial")
	public static final class RankMessenger<K> extends MessagingFunction<K, Double, Double, Double> {

		private final long numVertices;

		public RankMessenger(long numberOfVertices) {
			this.numVertices = numberOfVertices;
		}

		@Override
		public void sendMessages(K vertexId, Double newRank) {
			if (getSuperstepNumber() == 1) {
				// initialize vertex ranks
				newRank = 1.0 / numVertices;
			}
			for (Edge<K, Double> edge : getOutgoingEdges()) {
				sendMessageTo(edge.getTarget(), newRank * edge.getValue());
			}
		}
	}
}
