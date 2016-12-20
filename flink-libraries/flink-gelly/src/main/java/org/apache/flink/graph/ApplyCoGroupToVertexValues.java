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

package org.apache.flink.graph;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * Function that applies an instance of {@link VertexJoinFunction} on a result
 * of a co-group between a dataset of vertices and a dataset of Tuple2.
 *
 * Used in {@link Graph#joinWithVertices(DataSet, VertexJoinFunction)},
 * {@link org.apache.flink.graph.bipartite.BipartiteGraph#joinWithTopVertices(DataSet, VertexJoinFunction)},
 * and {@link org.apache.flink.graph.bipartite.BipartiteGraph#joinWithBottomVertices(DataSet, VertexJoinFunction)}
 *
 * @param <K> the key type of vertices
 * @param <VV> the value type of vertices
 * @param <T> type of a second field in the Tuple2 dataset
 */
public class ApplyCoGroupToVertexValues<K, VV, T>
	implements CoGroupFunction<Vertex<K, VV>, Tuple2<K, T>, Vertex<K, VV>> {

	private VertexJoinFunction<VV, T> vertexJoinFunction;

	public ApplyCoGroupToVertexValues(VertexJoinFunction<VV, T> mapper) {
		this.vertexJoinFunction = mapper;
	}

	@Override
	public void coGroup(Iterable<Vertex<K, VV>> vertices,
						Iterable<Tuple2<K, T>> input, Collector<Vertex<K, VV>> collector) throws Exception {

		final Iterator<Vertex<K, VV>> vertexIterator = vertices.iterator();
		final Iterator<Tuple2<K, T>> inputIterator = input.iterator();

		if (vertexIterator.hasNext()) {
			if (inputIterator.hasNext()) {
				final Tuple2<K, T> inputNext = inputIterator.next();

				collector.collect(new Vertex<>(inputNext.f0, vertexJoinFunction
					.vertexJoin(vertexIterator.next().f1, inputNext.f1)));
			} else {
				collector.collect(vertexIterator.next());
			}
		}
	}
}
