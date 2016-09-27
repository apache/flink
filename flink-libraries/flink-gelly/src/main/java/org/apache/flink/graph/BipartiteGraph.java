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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 *
 * Bipartite graph is a graph that contains two sets of vertices: top vertices and bottom vertices. Edges can only exist
 * between a pair of vertices from different vertices sets. E.g. there can be no vertices between a pair
 * of top vertices.
 *
 * <p>Bipartite graph is useful to represent graphs with two sets of objects, like researchers and their publications,
 * where an edge represents that a particular publication was authored by a particular author.
 *
 * <p>Bipartite interface is different from {@link Graph} interface, so to apply algorithms that work on a regular graph
 * a bipartite graph should be first converted into a {@link Graph} instance. This can be achieved by using
 * {@link BipartiteGraph#topProjection(GroupReduceFunction)} or
 * {@link BipartiteGraph#bottomProjection(GroupReduceFunction)} methods.
 *
 * @param <KT> the key type of the top vertices
 * @param <KB> the key type of the bottom vertices
 * @param <VT> the top vertices value type
 * @param <VB> the bottom vertices value type
 * @param <EV> the edge value type
 */
public class BipartiteGraph<KT, KB, VT, VB, EV> {
	private final ExecutionEnvironment context;
	private final DataSet<Vertex<KT, VT>> topVertices;
	private final DataSet<Vertex<KB, VB>> bottomVertices;
	private final DataSet<BipartiteEdge<KT, KB, EV>> edges;

	private BipartiteGraph(
			DataSet<Vertex<KT, VT>> topVertices,
			DataSet<Vertex<KB, VB>> bottomVertices,
			DataSet<BipartiteEdge<KT, KB, EV>> edges,
			ExecutionEnvironment context) {
		this.topVertices = topVertices;
		this.bottomVertices = bottomVertices;
		this.edges = edges;
		this.context = context;
	}

	/**
	 * Create bipartite graph from datasets.
	 *
	 * @param topVertices  dataset of top vertices in the graph
	 * @param bottomVertices dataset of bottom vertices in the graph
	 * @param edges dataset of edges between vertices
	 * @param context Flink execution context
	 * @param <KT> the key type of the top vertices
	 * @param <KB> the key type of the bottom vertices
	 * @param <VT> the top vertices value type
	 * @param <VB> the bottom vertices value type
	 * @param <EV> the edge value type
	 * @return new bipartite graph created from provided datasets
	 */
	public static <KT, KB, VT, VB, EV> BipartiteGraph<KT, KB, VT, VB, EV> fromDataSet(
			DataSet<Vertex<KT, VT>> topVertices,
			DataSet<Vertex<KB, VB>> bottomVertices,
			DataSet<BipartiteEdge<KT, KB, EV>> edges,
			ExecutionEnvironment context) {
		return new BipartiteGraph<>(topVertices, bottomVertices, edges, context);
	}

	/**
	 * Get dataset with top vertices.
	 *
	 * @return dataset with top vertices
	 */
	public DataSet<Vertex<KT, VT>> getTopVertices() {
		return topVertices;
	}

	/**
	 * Get dataset with bottom vertices.
	 *
	 * @return dataset with bottom vertices
	 */
	public DataSet<Vertex<KB, VB>> getBottomVertices() {
		return bottomVertices;
	}

	/**
	 * Get dataset with graph edges.
	 *
	 * @return dataset with graph edges
	 */
	public DataSet<BipartiteEdge<KT, KB, EV>> getEdges() {
		return edges;
	}

	/**
	 * Convert a bipartite into a graph that contains only top vertices. An edge between two vertices in the new
	 * graph will exist only if the original bipartite graph contains a bottom vertex they both connected to.
	 *
	 * <p>Caller should provide a function that will create an edge between two top vertices. This function will receive
	 * a collection of all connections between each pair of top vertices. Note that this function will be called twice for
	 * each pair of connected vertices, so it's up to a caller if one or two edges should be created.
	 *
	 * @param edgeFactory function that will be used to create edges in the new graph
	 * @param <NEV> the edge value type in the new graph
	 * @return top projection of the bipartite graph
	 */
	public <NEV> Graph<KT, VT, NEV> topProjection(
		final GroupReduceFunction<Tuple2<
									Tuple2<BipartiteEdge<KT, KB, EV>, BipartiteEdge<KT, KB, EV>>,
									Vertex<KB, VB>>,
								Edge<KT, NEV>> edgeFactory) {

		DataSet<Edge<KT, NEV>> newEdges = edges.join(edges)
			.where(new TopProjectionKeySelector<KT, KB, EV>())
			.equalTo(new TopProjectionKeySelector<KT, KB, EV>())
			.filter(new FilterFunction<Tuple2<BipartiteEdge<KT, KB, EV>, BipartiteEdge<KT, KB, EV>>>() {
				@Override
				public boolean filter(Tuple2<BipartiteEdge<KT, KB, EV>, BipartiteEdge<KT, KB, EV>> value) throws Exception {
					BipartiteEdge<KT, KB, EV> edge1 = value.f0;
					BipartiteEdge<KT, KB, EV> edge2 = value.f1;
					return !edge1.getTopId().equals(edge2.getTopId());
				}
			})
			.join(bottomVertices)
			.where(new KeySelector<Tuple2<BipartiteEdge<KT, KB, EV>,BipartiteEdge<KT, KB, EV>>, KB>() {
				@Override
				public KB getKey(Tuple2<BipartiteEdge<KT, KB, EV>, BipartiteEdge<KT, KB, EV>> value) throws Exception {
					return value.f0.getBottomId();
				}
			})
			.equalTo(new KeySelector<Vertex<KB, VB>, KB>() {
				@Override
				public KB getKey(Vertex<KB, VB> value) throws Exception {
					return value.getId();
				}
			})
			.groupBy(new KeySelector<Tuple2<Tuple2<BipartiteEdge<KT, KB, EV>, BipartiteEdge<KT, KB, EV>>, Vertex<KB, VB>>, Tuple2<KT, KT>>() {
				@Override
				public Tuple2<KT, KT> getKey(Tuple2<Tuple2<BipartiteEdge<KT, KB, EV>, BipartiteEdge<KT, KB, EV>>, Vertex<KB, VB>> value) throws Exception {
					return new Tuple2<>(value.f0.f0.getTopId(), value.f0.f1.getTopId());
				}
			})
			.reduceGroup(edgeFactory);
		return Graph.fromDataSet(topVertices, newEdges, context);
	}

	/**
	 * Convert a bipartite into a graph that contains only bottom vertices. An edge between two vertices in the new
	 * graph will exist only if the original bipartite graph contains a top vertex they both connected to.
	 *
	 * <p>Caller should provide a function that will create an edge between two bottom vertices. This function will receive
	 * a collection of all connections between each pair of bottom vertices. Note that this function will be called twice for
	 * each pair of connected vertices, so it's up to a caller if one or two edges should be created.
	 *
	 * @param edgeFactory function that will be used to create edges in the new graph
	 * @param <NEV> type of data associated with edges of the bottom projection
	 * @return bottom projection of the bipartite graph
	 */
	public <NEV> Graph<KB, VB, NEV> bottomProjection(
		final GroupReduceFunction<Tuple2<
			Tuple2<BipartiteEdge<KT, KB, EV>, BipartiteEdge<KT, KB, EV>>,
			Vertex<KT, VT>>,
			Edge<KB, NEV>> edgeFactory) {

		DataSet<Edge<KB, NEV>> newEdges  =  edges.join(edges)
			.where(new BottomProjectionKeySelector<KT, KB, EV>())
			.equalTo(new BottomProjectionKeySelector<KT, KB, EV>())
			.filter(new FilterFunction<Tuple2<BipartiteEdge<KT, KB, EV>, BipartiteEdge<KT, KB, EV>>>() {
				@Override
				public boolean filter(Tuple2<BipartiteEdge<KT, KB, EV>, BipartiteEdge<KT, KB, EV>> value) throws Exception {
					BipartiteEdge<KT, KB, EV> edge1 = value.f0;
					BipartiteEdge<KT, KB, EV> edge2 = value.f1;
					return !edge1.getBottomId().equals(edge2.getBottomId());
				}
			})
			.join(topVertices)
			.where(new KeySelector<Tuple2<BipartiteEdge<KT, KB, EV>,BipartiteEdge<KT, KB, EV>>, KT>() {
				@Override
				public KT getKey(Tuple2<BipartiteEdge<KT, KB, EV>, BipartiteEdge<KT, KB, EV>> value) throws Exception {
										return value.f0.getTopId();
																	}
																	})
			.equalTo(new KeySelector<Vertex<KT, VT>, KT>() {
				@Override
				public KT getKey(Vertex<KT, VT> value) throws Exception {
										return value.getId();
																}
																})
			.groupBy(new KeySelector<Tuple2<Tuple2<BipartiteEdge<KT, KB, EV>, BipartiteEdge<KT, KB, EV>>, Vertex<KT, VT>>, Tuple2<KB, KB>>() {
				@Override
				public Tuple2<KB, KB> getKey(Tuple2<Tuple2<BipartiteEdge<KT, KB, EV>, BipartiteEdge<KT, KB, EV>>, Vertex<KT, VT>> value) throws Exception {
					return new Tuple2<>(value.f0.f0.getBottomId(), value.f0.f1.getBottomId());
				}
			})
			.reduceGroup(edgeFactory);
			return Graph.fromDataSet(bottomVertices, newEdges, context);
	}

	private class TopProjectionKeySelector<KT, KB, EV> implements KeySelector<BipartiteEdge<KT, KB, EV>, KB> {
		@Override
		public KB getKey(BipartiteEdge<KT, KB, EV> value) throws Exception {
			return value.getBottomId();
		}
	}

	private class BottomProjectionKeySelector<KT, KB, EV> implements KeySelector<BipartiteEdge<KT, KB, EV>, KT> {
		@Override
		public KT getKey(BipartiteEdge<KT, KB, EV> value) throws Exception {
			return value.getTopId();
		}
	}
}
