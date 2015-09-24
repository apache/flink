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
import org.apache.flink.api.java.tuple.Tuple1;
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
 * The algorithm takes an undirected, unweighted graph as input and outputs a DataSet of
 * Tuple1 which contains a single integer representing the number of triangles.
 */
public class GSATriangleCount implements
		GraphAlgorithm<Long, NullValue, NullValue, DataSet<Tuple1<Integer>>> {

	@Override
	public DataSet<Tuple1<Integer>> run(Graph<Long, NullValue, NullValue> input) throws Exception {

		ExecutionEnvironment env = input.getContext();

		// order the edges so that src is always higher than trg
		DataSet<Edge<Long, NullValue>> edges = input.getEdges()
				.map(new OrderEdges()).distinct();

		Graph<Long, TreeMap<Long, Integer>, NullValue> graph = Graph.fromDataSet(edges,
				new VertexInitializer(), env);

		// select neighbors with ids higher than the current vertex id
		// Gather: a no-op in this case
		// Sum: create the set of neighbors
		DataSet<Tuple2<Long, TreeMap<Long, Integer>>> higherIdNeighbors =
				graph.reduceOnNeighbors(new GatherHigherIdNeighbors(), EdgeDirection.IN);

		Graph<Long, TreeMap<Long, Integer>, NullValue> graphWithReinitializedVertexValues =
				graph.mapVertices(new VertexInitializerEmptyTreeMap());

		// Apply: attach the computed values to the vertices
		// joinWithVertices to update the node values
		DataSet<Vertex<Long, TreeMap<Long, Integer>>> verticesWithHigherIdNeighbors =
				graphWithReinitializedVertexValues.joinWithVertices(higherIdNeighbors, new AttachValues()).getVertices();

		Graph<Long, TreeMap<Long,Integer>, NullValue> graphWithNeighbors = Graph.fromDataSet(verticesWithHigherIdNeighbors,
				edges, env);

		// propagate each received value to neighbors with higher id
		// Gather: a no-op in this case
		// Sum: propagate values
		DataSet<Tuple2<Long, TreeMap<Long, Integer>>> propagatedValues = graphWithNeighbors
				.reduceOnNeighbors(new GatherHigherIdNeighbors(), EdgeDirection.IN);

		// Apply: attach propagated values to vertices
		DataSet<Vertex<Long, TreeMap<Long, Integer>>> verticesWithPropagatedValues =
				graphWithReinitializedVertexValues.joinWithVertices(propagatedValues, new AttachValues()).getVertices();

		Graph<Long, TreeMap<Long, Integer>, NullValue> graphWithPropagatedNeighbors =
				Graph.fromDataSet(verticesWithPropagatedValues, graphWithNeighbors.getEdges(), env);

		// Scatter: compute the number of triangles
		DataSet<Tuple1<Integer>> numberOfTriangles = graphWithPropagatedNeighbors.getTriplets()
				.map(new ComputeTriangles()).reduce(new ReduceFunction<Tuple1<Integer>>() {

					@Override
					public Tuple1<Integer> reduce(Tuple1<Integer> firstTuple, Tuple1<Integer> secondTuple) throws Exception {
						return new Tuple1<Integer>(firstTuple.f0 + secondTuple.f0);
					}
				});

		return numberOfTriangles;
	}

	@SuppressWarnings("serial")
	private static final class OrderEdges implements MapFunction<Edge<Long, NullValue>, Edge<Long, NullValue>> {

		@Override
		public Edge<Long, NullValue> map(Edge<Long, NullValue> edge) throws Exception {
			if (edge.getSource() < edge.getTarget()) {
				return new Edge<Long, NullValue>(edge.getTarget(), edge.getSource(), NullValue.getInstance());
			} else {
				return edge;
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class VertexInitializer implements MapFunction<Long, TreeMap<Long, Integer>> {

		@Override
		public TreeMap<Long, Integer> map(Long value) throws Exception {
			TreeMap<Long, Integer> neighbors = new TreeMap<Long, Integer>();
			neighbors.put(value, 1);

			return neighbors;
		}
	}

	@SuppressWarnings("serial")
	private static final class VertexInitializerEmptyTreeMap implements
			MapFunction<Vertex<Long, TreeMap<Long, Integer>>, TreeMap<Long, Integer>> {

		@Override
		public TreeMap<Long, Integer> map(Vertex<Long, TreeMap<Long, Integer>> vertex) throws Exception {
			return new TreeMap<Long, Integer>();
		}
	}

	@SuppressWarnings("serial")
	private static final class AttachValues implements MapFunction<Tuple2<TreeMap<Long, Integer>,
			TreeMap<Long, Integer>>, TreeMap<Long, Integer>> {

		@Override
		public TreeMap<Long, Integer> map(Tuple2<TreeMap<Long, Integer>, TreeMap<Long, Integer>> tuple2) throws Exception {
			return tuple2.f1;
		}
	}

	@SuppressWarnings("serial")
	private static final class GatherHigherIdNeighbors implements ReduceNeighborsFunction<TreeMap<Long,Integer>> {

		@Override
		public TreeMap<Long,Integer> reduceNeighbors(TreeMap<Long,Integer> first, TreeMap<Long,Integer> second) {
			for (Long key : second.keySet()) {
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
	private static final class ComputeTriangles implements MapFunction<Triplet<Long, TreeMap<Long, Integer>, NullValue>,
			Tuple1<Integer>> {

		@Override
		public Tuple1<Integer> map(Triplet<Long, TreeMap<Long, Integer>, NullValue> triplet) throws Exception {

			Vertex<Long, TreeMap<Long, Integer>> srcVertex = triplet.getSrcVertex();
			Vertex<Long, TreeMap<Long, Integer>> trgVertex = triplet.getTrgVertex();
			int triangles = 0;

			if(trgVertex.getValue().get(srcVertex.getId()) != null) {
				triangles=trgVertex.getValue().get(srcVertex.getId());
			}
			return new Tuple1<Integer>(triangles);
		}
	}
}
