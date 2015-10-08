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
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.ReduceNeighborsFunction;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;

import java.util.TreeMap;

/**
 * Triangle Count Algorithm.
 *
 * This algorithm operates in three phases. First, vertices select neighbors with id greater than theirs
 * and send messages to them. Each received message is then propagated to neighbors with higher id.
 * Finally, if a node encounters the target id in the list of received messages, it increments the number
 * of triangles found.
 *
 * This implementation is non - iterative.
 *
 * The algorithm takes an undirected, unweighted graph as input and outputs a DataSet
 * which contains a single integer representing the number of triangles.
 */
public class GSATriangleCount<K extends Comparable<K>, VV, EV> implements
		GraphAlgorithm<K, VV, EV, DataSet<Integer>> {

	@SuppressWarnings("serial")
	@Override
	public DataSet<Integer> run(Graph<K, VV, EV> input) throws Exception {

		ExecutionEnvironment env = input.getContext();

		// order the edges so that src is always higher than trg
		DataSet<Edge<K, NullValue>> edges = input.getEdges().map(new OrderEdges<K, EV>()).distinct();

		Graph<K, TreeMap<K, Integer>, NullValue> graph = Graph.fromDataSet(edges,
				new VertexInitializer<K>(), env);

		// select neighbors with ids higher than the current vertex id
		// Gather: a no-op in this case
		// Sum: create the set of neighbors
		DataSet<Tuple2<K, TreeMap<K, Integer>>> higherIdNeighbors =
				graph.reduceOnNeighbors(new GatherHigherIdNeighbors<K>(), EdgeDirection.IN);

		Graph<K, TreeMap<K, Integer>, NullValue> graphWithReinitializedVertexValues =
				graph.mapVertices(new VertexInitializerEmptyTreeMap<K>());

		// Apply: attach the computed values to the vertices
		// joinWithVertices to update the node values
		DataSet<Vertex<K, TreeMap<K, Integer>>> verticesWithHigherIdNeighbors =
				graphWithReinitializedVertexValues.joinWithVertices(higherIdNeighbors, new AttachValues<K>()).getVertices();

		Graph<K, TreeMap<K,Integer>, NullValue> graphWithNeighbors = Graph.fromDataSet(verticesWithHigherIdNeighbors,
				edges, env);

		// propagate each received value to neighbors with higher id
		// Gather: a no-op in this case
		// Sum: propagate values
		DataSet<Tuple2<K, TreeMap<K, Integer>>> propagatedValues = graphWithNeighbors
				.reduceOnNeighbors(new GatherHigherIdNeighbors<K>(), EdgeDirection.IN);

		// Apply: attach propagated values to vertices
		DataSet<Vertex<K, TreeMap<K, Integer>>> verticesWithPropagatedValues =
				graphWithReinitializedVertexValues.joinWithVertices(propagatedValues, new AttachValues<K>()).getVertices();

		Graph<K, TreeMap<K, Integer>, NullValue> graphWithPropagatedNeighbors =
				Graph.fromDataSet(verticesWithPropagatedValues, graphWithNeighbors.getEdges(), env);

		// Scatter: compute the number of triangles
		DataSet<Integer> numberOfTriangles = graphWithPropagatedNeighbors.getTriplets()
				.map(new ComputeTriangles<K>()).reduce(new ReduceFunction<Integer>() {

					@Override
					public Integer reduce(Integer first, Integer second) throws Exception {
						return first + second;
					}
				});

		return numberOfTriangles;
	}

	@SuppressWarnings("serial")
	private static final class OrderEdges<K extends Comparable<K>, EV> implements
		MapFunction<Edge<K, EV>, Edge<K, NullValue>> {

		@Override
		public Edge<K, NullValue> map(Edge<K, EV> edge) throws Exception {
			if (edge.getSource().compareTo(edge.getTarget()) < 0) {
				return new Edge<K, NullValue>(edge.getTarget(), edge.getSource(), NullValue.getInstance());
			} else {
				return new Edge<K, NullValue>(edge.getSource(), edge.getTarget(), NullValue.getInstance());
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class VertexInitializer<K> implements MapFunction<K, TreeMap<K, Integer>> {

		@Override
		public TreeMap<K, Integer> map(K value) throws Exception {
			TreeMap<K, Integer> neighbors = new TreeMap<K, Integer>();
			neighbors.put(value, 1);

			return neighbors;
		}
	}

	@SuppressWarnings("serial")
	private static final class VertexInitializerEmptyTreeMap<K> implements
			MapFunction<Vertex<K, TreeMap<K, Integer>>, TreeMap<K, Integer>> {

		@Override
		public TreeMap<K, Integer> map(Vertex<K, TreeMap<K, Integer>> vertex) throws Exception {
			return new TreeMap<K, Integer>();
		}
	}

	@SuppressWarnings("serial")
	private static final class AttachValues<K> implements MapFunction<Tuple2<TreeMap<K, Integer>,
			TreeMap<K, Integer>>, TreeMap<K, Integer>> {

		@Override
		public TreeMap<K, Integer> map(Tuple2<TreeMap<K, Integer>, TreeMap<K, Integer>> tuple2) throws Exception {
			return tuple2.f1;
		}
	}

	@SuppressWarnings("serial")
	private static final class GatherHigherIdNeighbors<K> implements
		ReduceNeighborsFunction<TreeMap<K,Integer>> {

		@Override
		public TreeMap<K, Integer> reduceNeighbors(TreeMap<K,Integer> first, TreeMap<K,Integer> second) {
			for (K key : second.keySet()) {
				Integer value = first.get(key);
				if (value != null) {
					first.put(key, value + second.get(key));
				} else {
					first.put(key, second.get(key));
				}
			}
			return first;
		}
	}

	@SuppressWarnings("serial")
	private static final class ComputeTriangles<K> implements MapFunction<Triplet<K, TreeMap<K, Integer>, NullValue>,
			Integer> {

		@Override
		public Integer map(Triplet<K, TreeMap<K, Integer>, NullValue> triplet) throws Exception {

			Vertex<K, TreeMap<K, Integer>> srcVertex = triplet.getSrcVertex();
			Vertex<K, TreeMap<K, Integer>> trgVertex = triplet.getTrgVertex();
			int triangles = 0;

			if(trgVertex.getValue().get(srcVertex.getId()) != null) {
				triangles = trgVertex.getValue().get(srcVertex.getId());
			}
			return triangles;
		}
	}
}
