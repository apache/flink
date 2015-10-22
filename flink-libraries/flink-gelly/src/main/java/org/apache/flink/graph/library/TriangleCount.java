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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.NeighborsFunctionWithVertexValue;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.VertexJoinFunction;
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
 *
 * @param <K> the Vertex ID type
 */
public class TriangleCount<K extends Comparable<K>, VV, EV> implements
		GraphAlgorithm<K, VV, EV, DataSet<Integer>> {

	@SuppressWarnings("serial")
	@Override
	public DataSet<Integer> run(Graph<K, VV, EV> input) throws Exception {

		DataSet<Vertex<K, VV>> vertices = input.getVertices();

		TypeInformation<K> keyType = ((TupleTypeInfo<?>) vertices.getType()).getTypeAt(0);

		@SuppressWarnings({ "unchecked", "rawtypes" })
		TypeInformation<Vertex<K, K>> returnType = (TypeInformation<Vertex<K, K>>) new TupleTypeInfo(
				Vertex.class, keyType, keyType);

		// simulate the first superstep
		// select the neighbors with id greater than the current vertex's id
		DataSet<Vertex<K, K>> verticesWithHigherNeighbors =
				input.groupReduceOnNeighbors(new GatherHigherIdNeighbors(), EdgeDirection.IN, returnType);

		// then group them by id to attach the resulting sets to the vertices
		DataSet<Vertex<K, TreeMap<K, Integer>>> verticesWithNeighborTreeMaps =
				verticesWithHigherNeighbors.groupBy(0).reduceGroup(new AttachNeighborIdsAsVertexValues());

		// assign a value to the vertices with no neighbors as well
		Graph<K, TreeMap<K, Integer>, NullValue> graphWithInitializedVertexNeighbors =
				input.mapVertices(new InitializeTreeMaps());

		Graph<K, TreeMap<K, Integer>, NullValue> graphWithVertexNeighbors = graphWithInitializedVertexNeighbors.
				joinWithVertices(verticesWithNeighborTreeMaps.map(new VertexToTuple2Map<K, TreeMap<K, Integer>>()),
						new RetrieveValueMapper());

		// simulate the second superstep
		// propagate each received "message" to neighbors with higher id
		DataSet<Vertex<K, K>> verticesWithPropagatedValues =
				graphWithVertexNeighbors.groupReduceOnNeighbors(new PropagateNeighborValues(), EdgeDirection.IN, returnType);

		DataSet<Vertex<K, TreeMap<K, Integer>>> verticesWithPropagatedTreeMaps =
				verticesWithPropagatedValues.groupBy(0).reduceGroup(new AttachNeighborIdsAsVertexValues());

		DataSet<Integer> numberOfTriangles = verticesWithPropagatedTreeMaps
				.join(input.getEdges())
				.where(0).equalTo(0).with(new CountTriangles()).reduce(new ReduceFunction<Integer>() {

					@Override
					public Integer reduce(Integer first, Integer second) throws Exception {
						return new Integer(first + second);
					}
				});

		return numberOfTriangles;
	}

	@SuppressWarnings("serial")
	private static final class GatherHigherIdNeighbors<K extends Comparable<K>> implements
			NeighborsFunctionWithVertexValue<K, NullValue, NullValue, Vertex<K, K>> {

		@Override
		public void iterateNeighbors(Vertex<K, NullValue> vertex,
									Iterable<Tuple2<Edge<K, NullValue>, Vertex<K, NullValue>>> neighbors,
									Collector<Vertex<K, K>> collector) throws Exception {

			Tuple2<Edge<K, NullValue>, Vertex<K, NullValue>> next = null;
			Iterator<Tuple2<Edge<K, NullValue>, Vertex<K, NullValue>>> neighborsIterator =
					neighbors.iterator();

			while (neighborsIterator.hasNext()) {
				next = neighborsIterator.next();
				if(next.f1.getId().compareTo(vertex.getId()) > 0) {
					collector.collect(new Vertex<K, K>(next.f1.getId(), vertex.getId()));
				}
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class AttachNeighborIdsAsVertexValues<K> implements GroupReduceFunction<Vertex<K, K>,
			Vertex<K, TreeMap<K, Integer>>> {

		@Override
		public void reduce(Iterable<Vertex<K, K>> vertices,
						Collector<Vertex<K, TreeMap<K, Integer>>> collector) throws Exception {

			Iterator<Vertex<K, K>> vertexIertator = vertices.iterator();
			Vertex<K, K> next = null;
			TreeMap<K, Integer> neighbors = new TreeMap<K, Integer>();
			K id = null;

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

			collector.collect(new Vertex<K, TreeMap<K, Integer>>(id, neighbors));
		}
	}

	@SuppressWarnings("serial")
	private static final class InitializeTreeMaps<K> implements MapFunction<Vertex<K, NullValue>,
			TreeMap<K, Integer>> {

		@Override
		public TreeMap<K, Integer> map(Vertex<K, NullValue> vertex) throws Exception {
			return new TreeMap<K, Integer>();
		}
	}

	@SuppressWarnings("serial")
	private static final class RetrieveValueMapper<K> implements VertexJoinFunction<TreeMap<K, Integer>,
			TreeMap<K, Integer>> {


		@Override
		public TreeMap<K, Integer> vertexJoin(TreeMap<K, Integer> vertexValue, TreeMap<K, Integer> inputValue) throws Exception {
			return inputValue;
		}
	}

	@SuppressWarnings("serial")
	private static final class PropagateNeighborValues<K extends Comparable<K>> implements
			NeighborsFunctionWithVertexValue<K, TreeMap<K, Integer>, NullValue, Vertex<K, K>> {

		@Override
		public void iterateNeighbors(Vertex<K, TreeMap<K, Integer>> vertex,
									Iterable<Tuple2<Edge<K, NullValue>, Vertex<K, TreeMap<K, Integer>>>> neighbors,
									Collector<Vertex<K, K>> collector) throws Exception {

			Tuple2<Edge<K, NullValue>, Vertex<K, TreeMap<K, Integer>>> next = null;
			Iterator<Tuple2<Edge<K, NullValue>, Vertex<K, TreeMap<K, Integer>>>> neighborsIterator = neighbors.iterator();
			TreeMap<K, Integer> vertexSet = vertex.getValue();

			while (neighborsIterator.hasNext()) {
				next = neighborsIterator.next();
				if(next.f1.getId().compareTo(vertex.getId()) > 0) {
					for(K key: vertexSet.keySet()) {
						collector.collect(new Vertex<K, K>(next.f1.getId(), key));
					}
				}
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class CountTriangles<K> implements
			FlatJoinFunction<Vertex<K, TreeMap<K, Integer>>, Edge<K, NullValue>, Integer> {

		@Override
		public void join(Vertex<K, TreeMap<K, Integer>> vertex,
						Edge<K, NullValue> edge, Collector<Integer> collector) throws Exception {

			if (vertex.getValue().get(edge.getTarget()) != null) {
				collector.collect(vertex.getValue().get(edge.getTarget()));
			}
		}
	}
}
