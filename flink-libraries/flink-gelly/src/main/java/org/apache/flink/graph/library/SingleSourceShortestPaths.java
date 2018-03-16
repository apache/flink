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
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;

/**
 * This is an implementation of the Single-Source-Shortest Paths algorithm, using a scatter-gather iteration.
 */
@SuppressWarnings("serial")
public class SingleSourceShortestPaths<K, VV> implements GraphAlgorithm<K, VV, Double, DataSet<Vertex<K, Double>>> {

	private final K srcVertexId;
	private final Integer maxIterations;

	/**
	 * Creates an instance of the SingleSourceShortestPaths algorithm.
	 *
	 * @param srcVertexId The ID of the source vertex.
	 * @param maxIterations The maximum number of iterations to run.
	 */
	public SingleSourceShortestPaths(K srcVertexId, Integer maxIterations) {
		this.srcVertexId = srcVertexId;
		this.maxIterations = maxIterations;
	}

	@Override
	public DataSet<Vertex<K, Double>> run(Graph<K, VV, Double> input) {

		return input.mapVertices(new InitVerticesMapper<>(srcVertexId))
				.runScatterGatherIteration(new MinDistanceMessenger<>(), new VertexDistanceUpdater<>(),
				maxIterations).getVertices();
	}

	private static final class InitVerticesMapper<K, VV> implements MapFunction<Vertex<K, VV>, Double> {

		private K srcVertexId;

		public InitVerticesMapper(K srcId) {
			this.srcVertexId = srcId;
		}

		public Double map(Vertex<K, VV> value) {
			if (value.f0.equals(srcVertexId)) {
				return 0.0;
			} else {
				return Double.MAX_VALUE;
			}
		}
	}

	/**
	 * Distributes the minimum distance associated with a given vertex among all
	 * the target vertices summed up with the edge's value.
	 *
	 * @param <K>
	 */
	public static final class MinDistanceMessenger<K> extends ScatterFunction<K, Double, Double, Double> {

		@Override
		public void sendMessages(Vertex<K, Double> vertex) {
			if (vertex.getValue() < Double.POSITIVE_INFINITY) {
				for (Edge<K, Double> edge : getEdges()) {
					sendMessageTo(edge.getTarget(), vertex.getValue() + edge.getValue());
				}
			}
		}
	}

	/**
	 * Function that updates the value of a vertex by picking the minimum
	 * distance from all incoming messages.
	 *
	 * @param <K>
	 */
	public static final class VertexDistanceUpdater<K> extends GatherFunction<K, Double, Double> {

		@Override
		public void updateVertex(Vertex<K, Double> vertex,
				MessageIterator<Double> inMessages) {

			Double minDistance = Double.MAX_VALUE;

			for (double msg : inMessages) {
				if (msg < minDistance) {
					minDistance = msg;
				}
			}

			if (vertex.getValue() > minDistance) {
				setNewVertexValue(minDistance);
			}
		}
	}
}
