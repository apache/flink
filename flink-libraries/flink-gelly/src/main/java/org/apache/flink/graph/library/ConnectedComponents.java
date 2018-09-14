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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.graph.utils.GraphUtils.MapTo;
import org.apache.flink.types.NullValue;

/**
 * A scatter-gather implementation of the Weakly Connected Components algorithm.
 *
 * <p>This implementation uses a comparable vertex value as initial component
 * identifier (ID). Vertices propagate their current value in each iteration.
 * Upon receiving component IDs from its neighbors, a vertex adopts a new
 * component ID if its value is lower than its current component ID.
 *
 * <p>The algorithm converges when vertices no longer update their component ID
 * value or when the maximum number of iterations has been reached.
 *
 * <p>The result is a DataSet of vertices, where the vertex value corresponds to
 * the assigned component ID.
 *
 * @see GSAConnectedComponents
 */
@SuppressWarnings("serial")
public class ConnectedComponents<K, VV extends Comparable<VV>, EV>
	implements GraphAlgorithm<K, VV, EV, DataSet<Vertex<K, VV>>> {

	private Integer maxIterations;

	/**
	 * Creates an instance of the Connected Components algorithm.
	 * The algorithm computes weakly connected components
	 * and converges when no vertex updates its component ID
	 * or when the maximum number of iterations has been reached.
	 *
	 * @param maxIterations The maximum number of iterations to run.
	 */
	public ConnectedComponents(Integer maxIterations) {
		this.maxIterations = maxIterations;
	}

	@Override
	public DataSet<Vertex<K, VV>> run(Graph<K, VV, EV> graph) throws Exception {

		// get type information for vertex value
		TypeInformation<VV> valueTypeInfo = ((TupleTypeInfo<?>) graph.getVertices().getType()).getTypeAt(1);

		Graph<K, VV, NullValue> undirectedGraph = graph
			.mapEdges(new MapTo<>(NullValue.getInstance()))
			.getUndirected();

		return undirectedGraph.runScatterGatherIteration(
			new CCMessenger<>(valueTypeInfo),
			new CCUpdater<>(),
			maxIterations).getVertices();
	}

	/**
	 * Sends the current vertex value to all adjacent vertices.
	 */
	public static final class CCMessenger<K, VV extends Comparable<VV>>
		extends ScatterFunction<K, VV, VV, NullValue>
		implements ResultTypeQueryable<VV> {

		private final TypeInformation<VV> typeInformation;

		public CCMessenger(TypeInformation<VV> typeInformation) {
			this.typeInformation = typeInformation;
		}

		@Override
		public void sendMessages(Vertex<K, VV> vertex) throws Exception {
			// send current minimum to neighbors
			sendMessageToAllNeighbors(vertex.getValue());
		}

		@Override
		public TypeInformation<VV> getProducedType() {
			return typeInformation;
		}
	}

	/**
	 * Updates the value of a vertex by picking the minimum neighbor value out of all the incoming messages.
	 */
	public static final class CCUpdater<K, VV extends Comparable<VV>>
		extends GatherFunction<K, VV, VV> {

		@Override
		public void updateVertex(Vertex<K, VV> vertex, MessageIterator<VV> messages) throws Exception {
			VV current = vertex.getValue();
			VV min = current;

			for (VV msg : messages) {
				if (msg.compareTo(min) < 0) {
					min = msg;
				}
			}

			if (!min.equals(current)) {
				setNewVertexValue(min);
			}
		}
	}
}
