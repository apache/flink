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

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.NeighborsFunctionWithVertexValue;
import org.apache.flink.graph.utils.VertexToTuple2Map;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.TreeMap;

/**
 * Triangle Count Algorithm.
 *
 * This algorithm operates in three phases. First, vertices select neighbors with id greater than theirs
 * and send messages to them. Each received message is then propagated to neighbors with higher id.
 * Finally, if a node encounters the target id in the list of received messages, it increments the number
 * of triangles found.
 *
 * For skewed graphs, we recommend calling the GSATriangleCount library method as it uses the more restrictive
 * `reduceOnNeighbors` function which internally makes use of combiners to speed up computation.
 *
 * This implementation is non - iterative.
 *
 * The algorithm takes an undirected, unweighted graph as input and outputs a DataSet of
 * Tuple1 which contains a single integer representing the number of triangles.
 */
public class TriangleCount implements
		GraphAlgorithm<Long, NullValue, NullValue, DataSet<Tuple1<Integer>>> {

	@Override
	public DataSet<Tuple1<Integer>> run(Graph<Long, NullValue, NullValue> graph) throws Exception {

		// simulate the first superstep
		// select the neighbors with id greater than the current vertex's id
		DataSet<Vertex<Long, Long>> verticesWithHigherNeighbors =
				graph.groupReduceOnNeighbors(new GatherHigherIdNeighbors(), EdgeDirection.IN);

		// then group them by id to attach the resulting sets to the vertices
		DataSet<Vertex<Long, TreeMap<Long, Integer>>> verticesWithNeighborTreeMaps =
				verticesWithHigherNeighbors.groupBy(0).reduceGroup(new AttachNeighborIdsAsVertexValues());

		// assign a value to the vertices with no neighbors as well
		Graph<Long, TreeMap<Long, Integer>, NullValue> graphWithInitializedVertexNeighbors =
				graph.mapVertices(new InitializeTreeMaps());

		Graph<Long, TreeMap<Long, Integer>, NullValue> graphWithVertexNeighbors = graphWithInitializedVertexNeighbors.
				joinWithVertices(verticesWithNeighborTreeMaps.map(new VertexToTuple2Map<Long, TreeMap<Long, Integer>>()),
						new RetrieveValueMapper());

		// simulate the second superstep
		// propagate each received "message" to neighbors with higher id
		DataSet<Vertex<Long, Long>> verticesWithPropagatedValues =
				graphWithVertexNeighbors.groupReduceOnNeighbors(new PropagateNeighborValues(), EdgeDirection.IN);

		DataSet<Vertex<Long, TreeMap<Long, Integer>>> verticesWithPropagatedTreeMaps =
				verticesWithPropagatedValues.groupBy(0).reduceGroup(new AttachNeighborIdsAsVertexValues());

		DataSet<Tuple1<Integer>> numberOfTriangles = verticesWithPropagatedTreeMaps
				.join(graph.getEdges())
				.where(0).equalTo(0).with(new CountTriangles()).reduce(new ReduceFunction<Tuple1<Integer>>() {

					@Override
					public Tuple1<Integer> reduce(Tuple1<Integer> firstTuple, Tuple1<Integer> secondTuple) throws Exception {
						return new Tuple1<Integer>(firstTuple.f0 + secondTuple.f0);
					}
				});

		return numberOfTriangles;
	}

	@SuppressWarnings("serial")
	private static final class GatherHigherIdNeighbors implements
			NeighborsFunctionWithVertexValue<Long, NullValue, NullValue, Vertex<Long, Long>> {

		@Override
		public void iterateNeighbors(Vertex<Long, NullValue> vertex,
							Iterable<Tuple2<Edge<Long, NullValue>, Vertex<Long, NullValue>>> neighbors,
							Collector<Vertex<Long, Long>> collector) throws Exception {

			Tuple2<Edge<Long, NullValue>, Vertex<Long, NullValue>> next = null;
			Iterator<Tuple2<Edge<Long, NullValue>, Vertex<Long, NullValue>>> neighborsIterator =
					neighbors.iterator();

			while (neighborsIterator.hasNext()) {
				next = neighborsIterator.next();
				if(next.f1.getId() > vertex.getId()) {
					collector.collect(new Vertex<Long, Long>(next.f1.getId(), vertex.getId()));
				}
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class AttachNeighborIdsAsVertexValues implements GroupReduceFunction<Vertex<Long, Long>,
			Vertex<Long, TreeMap<Long, Integer>>> {

		@Override
		public void reduce(Iterable<Vertex<Long, Long>> vertices,
					Collector<Vertex<Long, TreeMap<Long, Integer>>> collector) throws Exception {

			Iterator<Vertex<Long, Long>> vertexIertator = vertices.iterator();
			Vertex<Long, Long> next = null;
			TreeMap<Long, Integer> neighbors = new TreeMap<Long, Integer>();
			Long id = null;

			while (vertexIertator.hasNext()) {
				next = vertexIertator.next();
				id = next.getId();

				Integer value = neighbors.get(next.getValue());
				if (value != null) {
					neighbors.put(next.getValue(), value + 1);
				} else {
					neighbors.put(next.getValue(), 1);
				}
			}

			collector.collect(new Vertex<Long, TreeMap<Long, Integer>>(id, neighbors));
		}
	}

	@SuppressWarnings("serial")
	private static final class InitializeTreeMaps implements MapFunction<Vertex<Long, NullValue>, TreeMap<Long, Integer>> {

		@Override
		public TreeMap<Long, Integer> map(Vertex<Long, NullValue> vertex) throws Exception {
			return new TreeMap<Long, Integer>();
		}
	}

	@SuppressWarnings("serial")
	private static final class RetrieveValueMapper implements MapFunction<Tuple2<TreeMap<Long, Integer>,
			TreeMap<Long, Integer>>, TreeMap<Long, Integer>> {

		@Override
		public TreeMap<Long, Integer> map(Tuple2<TreeMap<Long, Integer>, TreeMap<Long, Integer>> value) throws Exception {
			return value.f1;
		}
	}

	@SuppressWarnings("serial")
	private static final class PropagateNeighborValues implements
			NeighborsFunctionWithVertexValue<Long, TreeMap<Long, Integer>, NullValue, Vertex<Long, Long>> {

		@Override
		public void iterateNeighbors(Vertex<Long, TreeMap<Long, Integer>> vertex,
						Iterable<Tuple2<Edge<Long, NullValue>, Vertex<Long, TreeMap<Long, Integer>>>> neighbors,
						Collector<Vertex<Long, Long>> collector) throws Exception {

			Tuple2<Edge<Long, NullValue>, Vertex<Long, TreeMap<Long, Integer>>> next = null;
			Iterator<Tuple2<Edge<Long, NullValue>, Vertex<Long, TreeMap<Long, Integer>>>> neighborsIterator = neighbors.iterator();
			TreeMap<Long, Integer> vertexSet = vertex.getValue();

			while (neighborsIterator.hasNext()) {
				next = neighborsIterator.next();
				if(next.f1.getId() > vertex.getId()) {
					for(Long key: vertexSet.keySet()) {
						collector.collect(new Vertex<Long, Long>(next.f1.getId(), key));
					}
				}
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class CountTriangles implements
			FlatJoinFunction<Vertex<Long, TreeMap<Long, Integer>>, Edge<Long, NullValue>, Tuple1<Integer>> {

		@Override
		public void join(Vertex<Long, TreeMap<Long, Integer>> vertex,
					Edge<Long, NullValue> edge, Collector<Tuple1<Integer>> collector) throws Exception {

			if (vertex.getValue().get(edge.getTarget()) != null) {
				collector.collect(new Tuple1<Integer>(vertex.getValue().get(edge.getTarget())));
			}
		}
	}
}
