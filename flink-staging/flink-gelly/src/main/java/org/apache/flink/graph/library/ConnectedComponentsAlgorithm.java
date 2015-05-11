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
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.types.NullValue;

/**
 * Connected components algorithm.
 *
 * Initially, each vertex will have its own ID as a value(is its own component). The vertices propagate their
 * current component ID in iterations, each time adopting a new value from the received neighbor IDs,
 * provided that the value is less than the current minimum.
 *
 * The algorithm converges when vertices no longer update their value or when the maximum number of iterations
 * is reached.
 */
@SuppressWarnings("serial")
public class ConnectedComponentsAlgorithm implements GraphAlgorithm<Long, Long, NullValue>{

	private Integer maxIterations;

	public ConnectedComponentsAlgorithm(Integer maxIterations) {
		this.maxIterations = maxIterations;
	}

	@Override
	public Graph<Long, Long, NullValue> run(Graph<Long, Long, NullValue> graph) throws Exception {

		Graph<Long, Long, NullValue> undirectedGraph = graph.getUndirected();

		// initialize vertex values and run the Vertex Centric Iteration
		return undirectedGraph.runVertexCentricIteration(new CCUpdater(),
				new CCMessenger(), maxIterations);
	}

	/**
	 * Updates the value of a vertex by picking the minimum neighbor ID out of all the incoming messages.
	 */
	public static final class CCUpdater extends VertexUpdateFunction<Long, Long, Long> {

		@Override
		public void updateVertex(Vertex<Long, Long> vertex, MessageIterator<Long> messages) throws Exception {
			long min = Long.MAX_VALUE;

			for (long msg : messages) {
				min = Math.min(min, msg);
			}

			// update vertex value, if new minimum
			if (min < vertex.getValue()) {
				setNewVertexValue(min);
			}
		}
	}

	/**
	 * Distributes the minimum ID associated with a given vertex among all the target vertices.
	 */
	public static final class CCMessenger extends MessagingFunction<Long, Long, Long, NullValue> {

		@Override
		public void sendMessages(Vertex<Long, Long> vertex) throws Exception {
			// send current minimum to neighbors
			sendMessageToAllNeighbors(vertex.getValue());
		}
	}
}
