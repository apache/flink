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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.bipartite.BipartiteEdge;
import org.apache.flink.graph.bipartite.BipartiteGraph;
import org.apache.flink.util.Collector;
import static org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;

/**
 * Checks that the edge set input contains valid vertex Ids, i.e. that they
 * also exist in top and bottom vertex sets.
 */
public class InvalidBipartiteVertexIdsValidator<KT, KB, VVT, VVB, EV> extends BipartiteGraphValidator<KT, KB, VVT, VVB, EV> {

	@Override
	public boolean validate(BipartiteGraph<KT, KB, VVT, VVB, EV> bipartiteGraph) throws Exception {
		DataSet<Tuple1<KT>> edgesTopIds = bipartiteGraph.getEdges().map(new GetTopIdsMap<KT, KB, EV>());
		DataSet<Tuple1<KB>> edgesBottomIds = bipartiteGraph.getEdges().map(new GetBottomIdsMap<KT, KB, EV>());

		DataSet<KT> invalidTopIds = invalidIds(bipartiteGraph.getTopVertices(), edgesTopIds);
		DataSet<KB> invalidBottomIds = invalidIds(bipartiteGraph.getBottomVertices(), edgesBottomIds);

		return invalidTopIds.count() == 0 && invalidBottomIds.count() == 0;
	}

	private <K, V> DataSet<K> invalidIds(DataSet<Vertex<K, V>> topVertices, DataSet<Tuple1<K>> edgesIds) {
		return topVertices.coGroup(edgesIds)
			.where(0)
			.equalTo(0)
			.with(new CoGroupFunction<Vertex<K,V>, Tuple1<K>, K>() {
				@Override
				public void coGroup(Iterable<Vertex<K, V>> vertices, Iterable<Tuple1<K>> edgeIds, Collector<K> out) throws Exception {
					if (!vertices.iterator().hasNext()) {
						out.collect(edgeIds.iterator().next().f0);
					}
				}
			});
	}

	@ForwardedFields("f0")
	private class GetTopIdsMap<KT, KB, EV> implements MapFunction<BipartiteEdge<KT,KB,EV>, Tuple1<KT>> {

		private Tuple1<KT> result = new Tuple1<>();

		@Override
		public Tuple1<KT> map(BipartiteEdge<KT, KB, EV> value) throws Exception {
			result.f0 = value.getTopId();
			return result;
		}
	}

	@ForwardedFields("f1->f0")
	private class GetBottomIdsMap<KT, KB, EV> implements MapFunction<BipartiteEdge<KT,KB,EV>, Tuple1<KB>> {

		private Tuple1<KB> result = new Tuple1<>();

		@Override
		public Tuple1<KB> map(BipartiteEdge<KT, KB, EV> value) throws Exception {
			result.f0 = value.getBottomId();
			return result;
		}
	}
}
