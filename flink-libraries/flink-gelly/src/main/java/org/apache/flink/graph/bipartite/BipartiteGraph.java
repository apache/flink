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

package org.apache.flink.graph.bipartite;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;

/**
 * The vertices of a bipartite graph are divided into two disjoint sets, referenced by the names "top" and "bottom".
 * Top and bottom vertices with the same key value represent distinct entities and must be specially handled
 * when projecting to a simple {@link Graph}. Edges can only exist between a pair of vertices from different vertices
 * sets. E.g. there can be no vertices between a pair of top vertices.
 *
 * <p>Bipartite graphs are useful to represent graphs with two sets of objects, like researchers and their publications,
 * where an edge represents that a particular publication was authored by a particular author.
 *
 * <p>Bipartite interface is different from {@link Graph} interface, so to apply algorithms that work on a regular graph
 * a bipartite graph should be first converted into a {@link Graph} instance. This can be achieved by using
 * the projection methods.
 *
 * @param <KT> the key type of the top vertices
 * @param <KB> the key type of the bottom vertices
 * @param <VVT> the vertex value type for top vertices
 * @param <VVB> the vertex value type for bottom vertices
 * @param <EV> the edge value type
 */
public class BipartiteGraph<KT, KB, VVT, VVB, EV> {
	private final ExecutionEnvironment context;
	private final DataSet<Vertex<KT, VVT>> topVertices;
	private final DataSet<Vertex<KB, VVB>> bottomVertices;
	private final DataSet<BipartiteEdge<KT, KB, EV>> edges;

	private BipartiteGraph(
			DataSet<Vertex<KT, VVT>> topVertices,
			DataSet<Vertex<KB, VVB>> bottomVertices,
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
	public DataSet<Vertex<KT, VVT>> getTopVertices() {
		return topVertices;
	}

	/**
	 * Get dataset with bottom vertices.
	 *
	 * @return dataset with bottom vertices
	 */
	public DataSet<Vertex<KB, VVB>> getBottomVertices() {
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
	 * Convert a bipartite graph into an undirected graph that contains only top vertices. An edge between two vertices
	 * in the new graph will exist only if the original bipartite graph contains a bottom vertex they are both
	 * connected to.
	 *
	 * @return top projection of the bipartite graph where every edge contains a tuple with values of two edges that
	 * connect top vertices in the original graph
	 */
	public Graph<KT, VVT, Tuple2<EV, EV>> projectionTopSimple() {

		DataSet<Edge<KT, Tuple2<EV, EV>>> newEdges =  edges.join(edges)
			.where(1)
			.equalTo(1)
			.flatMap(new TopSimpleProjectionFunction<KT, KB, EV>());

		return Graph.fromDataSet(topVertices, newEdges, context);
	}

	/**
	 * Convert a bipartite into a graph that contains only bottom vertices. An edge between two vertices in the new
	 * graph will exist only if the original bipartite graph contains a top vertex they both connected to.
	 *
	 * @return bottom projection of the bipartite graph where every edge contains a tuple with values of two edges that
	 * connect bottom vertices in the original graph
	 */
	public Graph<KB, VVB, Tuple2<EV, EV>> projectionBottomSimple() {

		DataSet<Edge<KB, Tuple2<EV, EV>>> newEdges =  edges.join(edges)
			.where(0)
			.equalTo(0)
			.flatMap(new BottomSimpleProjectionFunction<KT, KB, EV>());

		return Graph.fromDataSet(bottomVertices, newEdges, context);
	}

	/**
	 * Convert a bipartite into a graph that contains only top vertices. An edge between two vertices in the new
	 * graph will exist only if the original bipartite graph contains a bottom vertex they both connected to.
	 *
	 * @return top projection of the bipartite graph where every edge contains a data about projected connection
	 * between vertices in the original graph
	 */
	public Graph<KT, VVT, Projection<KB, VVB, EV, VVT>> projectionTopFull() {
		DataSet<Tuple2<Tuple2<BipartiteEdge<KT, KB, EV>, Vertex<KT, VVT>>, Vertex<KB, VVB>>> edgesWithVertices
			= joinEdgesWithVertices();

		DataSet<Edge<KT, Projection<KB, VVB, EV, VVT>>> newEdges = edgesWithVertices.join(edgesWithVertices)
			.where("f1.f0")
			.equalTo("f1.f0")
			.flatMap(new TopFullProjectionFunction<KT, KB, EV, VVT, VVB>());

		return Graph.fromDataSet(topVertices, newEdges, context);
	}

	private DataSet<Tuple2<Tuple2<BipartiteEdge<KT, KB, EV>, Vertex<KT, VVT>>, Vertex<KB, VVB>>> joinEdgesWithVertices() {
		return edges.join(topVertices, JoinOperatorBase.JoinHint.REPARTITION_HASH_SECOND)
		.where(0)
		.equalTo(0)
		.join(bottomVertices, JoinOperatorBase.JoinHint.REPARTITION_HASH_SECOND)
		.where("f0.f1")
		.equalTo(0);
	}

	/**
	 * Convert a bipartite into a graph that contains only bottom vertices. An edge between two vertices in the new
	 * graph will exist only if the original bipartite graph contains a top vertex they both connected to.
	 *
	 * @return bottom projection of the bipartite graph where every edge contains a data about projected connection
	 * between vertices in the original graph
	 */
	public Graph<KB, VVB, Projection<KT, VVT, EV, VVB>> projectionBottomFull() {

		DataSet<Tuple2<Tuple2<BipartiteEdge<KT, KB, EV>, Vertex<KT, VVT>>, Vertex<KB, VVB>>> edgesWithVertices
			= joinEdgesWithVertices();

		DataSet<Edge<KB, Projection<KT, VVT, EV, VVB>>> newEdges = edgesWithVertices.join(edgesWithVertices)
			.where("f0.f1.f0")
			.equalTo("f0.f1.f0")
			.flatMap(new BottomFullProjectionFunction<KT, KB, EV, VVT, VVB>());

		return Graph.fromDataSet(bottomVertices, newEdges, context);
	}

	static private class BottomFullProjectionFunction<KT, KB, EV, VVT, VVB> implements FlatMapFunction<Tuple2<Tuple2<Tuple2<BipartiteEdge<KT, KB, EV>, Vertex<KT, VVT>>, Vertex<KB, VVB>>, Tuple2<Tuple2<BipartiteEdge<KT, KB, EV>, Vertex<KT, VVT>>, Vertex<KB, VVB>>>, Edge<KB, Projection<KT, VVT, EV, VVB>>> {

		private Edge<KB, Projection<KT, VVT, EV, VVB>> edge = new Edge<>();

		@Override
		public void flatMap(Tuple2<Tuple2<Tuple2<BipartiteEdge<KT, KB, EV>, Vertex<KT, VVT>>, Vertex<KB, VVB>>, Tuple2<Tuple2<BipartiteEdge<KT, KB, EV>, Vertex<KT, VVT>>, Vertex<KB, VVB>>> value, Collector<Edge<KB, Projection<KT, VVT, EV, VVB>>> out) throws Exception {
			Tuple2<Tuple2<BipartiteEdge<KT, KB, EV>, Vertex<KT, VVT>>, Vertex<KB, VVB>> sourceSide = value.f0;
			Tuple2<Tuple2<BipartiteEdge<KT, KB, EV>, Vertex<KT, VVT>>, Vertex<KB, VVB>> targetSide = value.f1;

			Vertex<KB, VVB> targetVertex = targetSide.f1;
			Vertex<KB, VVB> sourceVertex = sourceSide.f1;

			if (!targetVertex.getId().equals(sourceVertex.getId())) {
				BipartiteEdge<KT, KB, EV> targetEdge = targetSide.f0.f0;
				BipartiteEdge<KT, KB, EV> sourceEdge = sourceSide.f0.f0;

				Vertex<KT, VVT> intermediateVertex = targetSide.f0.f1;

				Projection<KT, VVT, EV, VVB> projection = new Projection<>(intermediateVertex, sourceEdge.getValue(),
					targetEdge.getValue(), sourceVertex.getValue(), targetVertex.getValue());
				edge.setSource(sourceVertex.getId());
				edge.setTarget(targetVertex.getId());
				edge.setValue(projection);

				out.collect(edge);
			}
		}
	}

	static private class TopFullProjectionFunction<KT, KB, EV, VVT, VVB> implements FlatMapFunction<Tuple2<Tuple2<Tuple2<BipartiteEdge<KT,KB,EV>,Vertex<KT,VVT>>,Vertex<KB,VVB>>,Tuple2<Tuple2<BipartiteEdge<KT,KB,EV>,Vertex<KT,VVT>>,Vertex<KB,VVB>>>, Edge<KT, Projection<KB, VVB, EV, VVT>>> {

		private Edge<KT, Projection<KB, VVB, EV, VVT>> edge = new Edge<>();

		@Override
		public void flatMap(Tuple2<Tuple2<Tuple2<BipartiteEdge<KT, KB, EV>, Vertex<KT, VVT>>, Vertex<KB, VVB>>, Tuple2<Tuple2<BipartiteEdge<KT, KB, EV>, Vertex<KT, VVT>>, Vertex<KB, VVB>>> value, Collector<Edge<KT, Projection<KB, VVB, EV, VVT>>> out) throws Exception {
			Tuple2<Tuple2<BipartiteEdge<KT, KB, EV>, Vertex<KT, VVT>>, Vertex<KB, VVB>> sourceSide = value.f0;
			Tuple2<Tuple2<BipartiteEdge<KT, KB, EV>, Vertex<KT, VVT>>, Vertex<KB, VVB>> targetSide = value.f1;

			Vertex<KT, VVT> targetVertex = targetSide.f0.f1;
			Vertex<KT, VVT> sourceVertex = sourceSide.f0.f1;

			if (!targetVertex.getId().equals(sourceVertex.getId())) {
				BipartiteEdge<KT, KB, EV> targetEdge = targetSide.f0.f0;
				BipartiteEdge<KT, KB, EV> sourceEdge = sourceSide.f0.f0;

				Vertex<KB, VVB> intermediateVertex = targetSide.f1;

				Projection<KB, VVB, EV, VVT> projection = new Projection<>(intermediateVertex, sourceEdge.getValue(),
					targetEdge.getValue(), sourceVertex.getValue(), targetVertex.getValue());
				edge.setSource(sourceVertex.getId());
				edge.setTarget(targetVertex.getId());
				edge.setValue(projection);

				out.collect(edge);
			}
		}
	}

	static private class BottomSimpleProjectionFunction<KT, KB, EV> implements FlatMapFunction<Tuple2<BipartiteEdge<KT, KB, EV>, BipartiteEdge<KT, KB, EV>>, Edge<KB, Tuple2<EV, EV>>> {

		private Edge<KB, Tuple2<EV, EV>> edge = new Edge<>();

		@Override
		public void flatMap(Tuple2<BipartiteEdge<KT, KB, EV>, BipartiteEdge<KT, KB, EV>> value, Collector<Edge<KB, Tuple2<EV, EV>>> out) throws Exception {
			BipartiteEdge<KT, KB, EV> edge1 = value.f0;
			BipartiteEdge<KT, KB, EV> edge2 = value.f1;
			if (!edge1.getBottomId().equals(edge2.getBottomId())) {
				edge.setSource(value.f0.getBottomId());
				edge.setTarget(value.f1.getBottomId());
				edge.setValue(new Tuple2<>(
						value.f0.getValue(),
						value.f1.getValue()));
				out.collect(edge);
			}
		}
	}

	static private class TopSimpleProjectionFunction<KT, KB, EV> implements FlatMapFunction<Tuple2<BipartiteEdge<KT, KB, EV>, BipartiteEdge<KT, KB, EV>>, Edge<KT, Tuple2<EV, EV>>> {

		private Edge<KT, Tuple2<EV, EV>> edge = new Edge<>();

		@Override
		public void flatMap(Tuple2<BipartiteEdge<KT, KB, EV>, BipartiteEdge<KT, KB, EV>> value, Collector<Edge<KT, Tuple2<EV, EV>>> out) throws Exception {
			BipartiteEdge<KT, KB, EV> edge1 = value.f0;
			BipartiteEdge<KT, KB, EV> edge2 = value.f1;
			if (!edge1.getTopId().equals(edge2.getTopId())) {
				edge.setSource(value.f0.getTopId());
				edge.setTarget(value.f1.getTopId());
				edge.setValue(new Tuple2<>(
					value.f0.getValue(),
					value.f1.getValue()));
				out.collect(edge);
			}
		}
	}
}
