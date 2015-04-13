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
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.types.NullValue;

/**
 * Minimum Vertex ID propagation algorithm.
 *
 * Initially, each vertex will have its own ID as a value. The vertices propagate their
 * current minimum ID in iterations, each time adopting a new value from the received neighbor IDs,
 * provided that the value is less than the current minimum.
 *
 * The algorithm converges when vertices no longer update their value or when the maximum number of iterations
 * is reached.
 */
@SuppressWarnings("serial")
public class MinVertexIdPropagation implements GraphAlgorithm<Integer, Integer, NullValue>{

	private Integer maxIterations;

	public MinVertexIdPropagation(Integer maxIterations) {
		this.maxIterations = maxIterations;
	}

	@Override
	public Graph<Integer, Integer, NullValue> run(Graph<Integer, Integer, NullValue> graph) throws Exception {

		Graph<Integer, Integer, NullValue> undirectedGraph = graph.getUndirected();

		// initialize vertex values and run the Vertex Centric Iteration
		return undirectedGraph.runVertexCentricIteration(new MinNeighborUpdater(),
				new MinMessenger(), maxIterations);
	}

	/**
	 * Updates the value of a vertex by picking the minimum neighbor ID out of all the incoming messages.
	 */
	public static final class MinNeighborUpdater extends VertexUpdateFunction<Integer, Integer, Integer> {

		@Override
		public void updateVertex(Integer id, Integer currentMin, MessageIterator<Integer> messages) throws Exception {
			int min = Integer.MAX_VALUE;

			// iterate over all received messages
			while (messages.hasNext()) {
				int next = messages.next();
				min = next < min ? next : min;
			}

			// update vertex value, if new minimum
			if (min < currentMin) {
				setNewVertexValue(min);
			}
		}
	}

	/**
	 * Distributes the minimum ID associated with a given vertex among all the target vertices.
	 */
	public static final class MinMessenger extends MessagingFunction<Integer, Integer, Integer, NullValue> {

		@Override
		public void sendMessages(Integer id, Integer currentMin) throws Exception {
			// send current minimum to neighbors
			sendMessageToAllNeighbors(currentMin);
		}
	}
}
