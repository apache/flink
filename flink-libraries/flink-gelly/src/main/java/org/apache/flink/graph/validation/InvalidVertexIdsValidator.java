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

package org.apache.flink.graph.validation;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;

/**
 * Validate that the edge set vertex IDs exist in the vertex set.
 *
 * @param <K> graph ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
@SuppressWarnings("serial")
public class InvalidVertexIdsValidator<K, VV, EV> extends GraphValidator<K, VV, EV> {

	/**
	 * Checks that the edge set input contains valid vertex Ids, i.e. that they
	 * also exist in the vertex input set.
	 *
	 * @return a boolean stating whether a graph is valid
	 *         with respect to its vertex ids.
	 */
	@Override
	public boolean validate(Graph<K, VV, EV> graph) throws Exception {
		DataSet<Tuple1<K>> edgeIds = graph.getEdges()
				.flatMap(new MapEdgeIds<>()).distinct();
		DataSet<K> invalidIds = graph.getVertices().coGroup(edgeIds).where(0)
				.equalTo(0).with(new GroupInvalidIds<>()).first(1);

		return invalidIds.map(new KToTupleMap<>()).count() == 0;
	}

	private static final class MapEdgeIds<K, EV> implements FlatMapFunction<Edge<K, EV>, Tuple1<K>> {
		public void flatMap(Edge<K, EV> edge, Collector<Tuple1<K>> out) {
			out.collect(new Tuple1<>(edge.f0));
			out.collect(new Tuple1<>(edge.f1));
		}
	}

	private static final class GroupInvalidIds<K, VV> implements CoGroupFunction<Vertex<K, VV>, Tuple1<K>, K> {
		public void coGroup(Iterable<Vertex<K, VV>> vertexId,
				Iterable<Tuple1<K>> edgeId, Collector<K> out) {
			if (!(vertexId.iterator().hasNext())) {
				// found an id that doesn't exist in the vertex set
				out.collect(edgeId.iterator().next().f0);
			}
		}
	}

	private static final class KToTupleMap<K> implements MapFunction<K, Tuple1<K>> {
		public Tuple1<K> map(K key) throws Exception {
			return new Tuple1<>(key);
		}
	}

}
