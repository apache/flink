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

package org.apache.flink.graph.bipartite.validation;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
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

		DataSet<BipartiteEdge<KT, KB, EV>> invalidTopVertexId = bipartiteGraph.getEdges().leftOuterJoin(bipartiteGraph.getTopVertices()).where(0).equalTo(0)
			.with(new InvalidEdgeJoin<KT, KB, EV, KT, VVT>());

		DataSet<BipartiteEdge<KT, KB, EV>> invalidBottomVertexId = bipartiteGraph.getEdges()
			.leftOuterJoin(bipartiteGraph.getBottomVertices())
			.where(1)
			.equalTo(0)
			.with(new InvalidEdgeJoin<KT, KB, EV, KB, VVB>());

		return invalidBottomVertexId.union(invalidTopVertexId).count() == 0;
	}

	private static class InvalidEdgeJoin<KT, KB, EV, KV, VVT>
		implements FlatJoinFunction<BipartiteEdge<KT, KB, EV>, Vertex<KV, VVT>, BipartiteEdge<KT, KB, EV>> {
		@Override
		public void join(BipartiteEdge<KT, KB, EV> edge, Vertex<KV, VVT> vertex, Collector<BipartiteEdge<KT, KB, EV>> out) throws Exception {
			if (vertex == null) {
				out.collect(edge);
			}
		}
	}
}
