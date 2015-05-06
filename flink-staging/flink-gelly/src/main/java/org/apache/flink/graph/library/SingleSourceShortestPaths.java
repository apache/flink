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
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;

@SuppressWarnings("serial")
public class SingleSourceShortestPaths<K> implements GraphAlgorithm<K, Double, Double> {

	private final K srcVertexId;
	private final Integer maxIterations;

	public SingleSourceShortestPaths(K srcVertexId, Integer maxIterations) {
		this.srcVertexId = srcVertexId;
		this.maxIterations = maxIterations;
	}

	@Override
	public Graph<K, Double, Double> run(Graph<K, Double, Double> input) {

		return input.mapVertices(new InitVerticesMapper<K>(srcVertexId))
				.runVertexCentricIteration(new VertexDistanceUpdater<K>(), new MinDistanceMessenger<K>(),
				maxIterations);
	}

	public static final class InitVerticesMapper<K>	implements MapFunction<Vertex<K, Double>, Double> {

		private K srcVertexId;

		public InitVerticesMapper(K srcId) {
			this.srcVertexId = srcId;
		}

		public Double map(Vertex<K, Double> value) {
			if (value.f0.equals(srcVertexId)) {
				return 0.0;
			} else {
				return Double.MAX_VALUE;
			}
		}
	}

	/**
	 * Function that updates the value of a vertex by picking the minimum
	 * distance from all incoming messages.
	 * 
	 * @param <K>
	 */
	public static final class VertexDistanceUpdater<K> extends VertexUpdateFunction<K, Double, Double> {

		@Override
		public void updateVertex(K vertexKey, Double vertexValue,
				MessageIterator<Double> inMessages) {

			Double minDistance = Double.MAX_VALUE;

			for (double msg : inMessages) {
				if (msg < minDistance) {
					minDistance = msg;
				}
			}

			if (vertexValue > minDistance) {
				setNewVertexValue(minDistance);
			}
		}
	}

	/**
	 * Distributes the minimum distance associated with a given vertex among all
	 * the target vertices summed up with the edge's value.
	 * 
	 * @param <K>
	 */
	public static final class MinDistanceMessenger<K> extends MessagingFunction<K, Double, Double, Double> {

		@Override
		public void sendMessages(K vertexKey, Double newDistance)
				throws Exception {
			for (Edge<K, Double> edge : getOutgoingEdges()) {
				sendMessageTo(edge.getTarget(), newDistance + edge.getValue());
			}
		}
	}
}