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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;

/**
 * This is an implementation of a simple PageRank algorithm, using a vertex-centric iteration.
 * The user can define the damping factor and the maximum number of iterations.
 * If the number of vertices of the input graph is known, it should be provided as a parameter
 * to speed up computation. Otherwise, the algorithm will first execute a job to count the vertices.
 * 
 * The implementation assumes that each page has at least one incoming and one outgoing link.
 */
public class PageRank<K> implements GraphAlgorithm<K, Double, Double, DataSet<Vertex<K, Double>>> {

	private double beta;
	private int maxIterations;
	private long numberOfVertices;

	/**
	 * Creates an instance of the PageRank algorithm.
	 * If the number of vertices of the input graph is known,
	 * use the {@link PageRank#PageRank(double, long, int)} constructor instead.
	 * 
	 * The implementation assumes that each page has at least one incoming and one outgoing link.
	 * 
	 * @param beta the damping factor
	 * @param maxIterations the maximum number of iterations
	 */
	public PageRank(double beta, int maxIterations) {
		this.beta = beta;
		this.maxIterations = maxIterations;
		this.numberOfVertices = 0;
	}

	/**
	 * Creates an instance of the PageRank algorithm.
	 * If the number of vertices of the input graph is known,
	 * use the {@link PageRank#PageRank(double, long)} constructor instead.
	 * 
	 * The implementation assumes that each page has at least one incoming and one outgoing link.
	 * 
	 * @param beta the damping factor
	 * @param maxIterations the maximum number of iterations
	 * @param numVertices the number of vertices in the input
	 */
	public PageRank(double beta, long numVertices, int maxIterations) {
		this.beta = beta;
		this.maxIterations = maxIterations;
		this.numberOfVertices = numVertices;
	}

	@Override
	public DataSet<Vertex<K, Double>> run(Graph<K, Double, Double> network) throws Exception {

		if (numberOfVertices == 0) {
			numberOfVertices = network.numberOfVertices();
		}

		DataSet<Tuple2<K, Long>> vertexOutDegrees = network.outDegrees();

		Graph<K, Double, Double> networkWithWeights = network
				.joinWithEdgesOnSource(vertexOutDegrees, new InitWeightsMapper());

		return networkWithWeights.runVertexCentricIteration(new VertexRankUpdater<K>(beta, numberOfVertices),
				new RankMessenger<K>(numberOfVertices), maxIterations)
				.getVertices();
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
		public void updateVertex(Vertex<K, Double> vertex, MessageIterator<Double> inMessages) {
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
		public void sendMessages(Vertex<K, Double> vertex) {
			if (getSuperstepNumber() == 1) {
				// initialize vertex ranks
				vertex.setValue(new Double(1.0 / numVertices));
			}

			for (Edge<K, Double> edge : getEdges()) {
				sendMessageTo(edge.getTarget(), vertex.getValue() * edge.getValue());
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class InitWeightsMapper implements MapFunction<Tuple2<Double, Long>, Double> {
		public Double map(Tuple2<Double, Long> value) {
			return value.f0 / value.f1;
		}
	}

}
