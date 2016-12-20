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

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.ApplyCoGroupToVertexValues;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.VertexJoinFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;

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
 * @param <KT> the key type of top vertices
 * @param <KB> the key type of bottom vertices
 * @param <VVT> the vertex value type of top vertices
 * @param <VVB> the vertex value type of bottom vertices
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
	 * @param topVertices dataset of top vertices in the graph
	 * @param bottomVertices dataset of bottom vertices in the graph
	 * @param edges dataset of edges between vertices
	 * @param context Flink execution context
	 * @return new bipartite graph created from provided datasets
	 */
	public static <KT, KB, VVT, VVB, EV> BipartiteGraph<KT, KB, VVT, VVB, EV> fromDataSet(
			DataSet<Vertex<KT, VVT>> topVertices,
			DataSet<Vertex<KB, VVB>> bottomVertices,
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
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public DataSet<BipartiteEdge<KT, KB, EV>> getEdges() {
		return edges;
	}

	/**
	 * Apply a function to the attribute of each top vertex in the graph.
	 *
	 * @param mapper the map function to apply.
	 * @return a new graph
	 */
	public <NVT> BipartiteGraph<KT, KB, NVT, VVB, EV> mapTopVertices(MapFunction<Vertex<KT, VVT>, NVT> mapper) {
		TypeInformation<KT> keyType = ((TupleTypeInfo<?>) topVertices.getType()).getTypeAt(0);

		TypeInformation<NVT> valueType = TypeExtractor.createTypeInfo(MapFunction.class, mapper.getClass(), 1, topVertices.getType(), null);

		TypeInformation<Vertex<KT, NVT>> returnType = (TypeInformation<Vertex<KT, NVT>>) new TupleTypeInfo(
			Vertex.class, keyType, valueType);

		return mapTopVertices(mapper, returnType);
	}

	/**
	 * Apply a function to the attribute of each top vertex in the graph.
	 *
	 * @param mapper the map function to apply.
	 * @param returnType the explicit return type.
	 * @return a new graph
	 */
	public <NVT> BipartiteGraph<KT, KB, NVT, VVB, EV> mapTopVertices(final MapFunction<Vertex<KT, VVT>, NVT> mapper, TypeInformation<Vertex<KT,NVT>> returnType) {
		DataSet<Vertex<KT, NVT>> mappedVertices = topVertices.map(
			new MapVertices<>(mapper))
			.returns(returnType)
			.name("Map vertices");

		return new BipartiteGraph<>(mappedVertices, bottomVertices, edges, context);
	}

	@ForwardedFields({"f0", "f1"})
	private static class MapVertices<KT, VVT, NVT> implements MapFunction<Vertex<KT, VVT>, Vertex<KT, NVT>> {
		private final MapFunction<Vertex<KT, VVT>, NVT> mapper;
		private Vertex<KT, NVT> output;

		public MapVertices(MapFunction<Vertex<KT, VVT>, NVT> mapper) {
			this.mapper = mapper;
			output = new Vertex<>();
		}

		public Vertex<KT, NVT> map(Vertex<KT, VVT> value) throws Exception {
			output.f0 = value.f0;
			output.f1 = mapper.map(value);
			return output;
		}
	}

	/**
	 * Apply a function to the attribute of each bottom vertex in the graph.
	 *
	 * @param mapper the map function to apply.
	 * @return a new graph
	 */
	public <NVB> BipartiteGraph<KT, KB, VVT, NVB, EV> mapBottomVertices(MapFunction<Vertex<KB, VVB>, NVB> mapper) {
		TypeInformation<KB> keyType = ((TupleTypeInfo<?>) bottomVertices.getType()).getTypeAt(0);

		TypeInformation<NVB> valueType = TypeExtractor.createTypeInfo(MapFunction.class, mapper.getClass(), 1, topVertices.getType(), null);

		TypeInformation<Vertex<KB, NVB>> returnType = (TypeInformation<Vertex<KB, NVB>>) new TupleTypeInfo(
			Vertex.class, keyType, valueType);

		return mapBottomVertices(mapper, returnType);
	}

	/**
	 * Apply a function to the attribute of each top vertex in the graph.
	 *
	 * @param mapper the map function to apply.
	 * @param returnType the explicit return type.
	 * @return a new graph
	 */
	public <NVB> BipartiteGraph<KT, KB, VVT, NVB, EV> mapBottomVertices(final MapFunction<Vertex<KB, VVB>, NVB> mapper, TypeInformation<Vertex<KB, NVB>> returnType) {
		DataSet<Vertex<KB, NVB>> mappedVertices = bottomVertices.map(
			new MapVertices<>(mapper))
			.returns(returnType)
			.name("Map vertices");

		return new BipartiteGraph<>(topVertices, mappedVertices, edges, context);
	}

	/**
	 * Apply a function to the attribute of each edge in the graph.
	 *
	 * @param mapper the map function to apply.
	 * @return a new graph
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public <NV> BipartiteGraph<KT, KB, VVT, VVB, NV> mapEdges(final MapFunction<BipartiteEdge<KT, KB, EV>, NV> mapper) {

		TypeInformation<KT> topKeyType = ((TupleTypeInfo<?>) edges.getType()).getTypeAt(0);
		TypeInformation<KB> bottomKeyType = ((TupleTypeInfo<?>) edges.getType()).getTypeAt(1);

		TypeInformation<NV> valueType = TypeExtractor.createTypeInfo(MapFunction.class, mapper.getClass(), 1, edges.getType(), null);

		TypeInformation<BipartiteEdge<KT, KB, NV>> returnType = (TypeInformation<BipartiteEdge<KT, KB, NV>>) new TupleTypeInfo(
			Edge.class, topKeyType, bottomKeyType, valueType);

		return mapEdges(mapper, returnType);
	}

	/**
	 * Apply a function to the attribute of each edge in the graph.
	 *
	 * @param mapper the map function to apply.
	 * @param returnType the explicit return type.
	 * @return a new graph
	 */
	public <NV> BipartiteGraph<KT, KB, VVT, VVB, NV> mapEdges(final MapFunction<BipartiteEdge<KT, KB, EV>, NV> mapper, TypeInformation<BipartiteEdge<KT, KB, NV>> returnType) {
		DataSet<BipartiteEdge<KT, KB, NV>> mappedEdges = edges.map(
			new MapEdges<>(mapper))
			.returns(returnType)
			.name("Map edges");

		return new BipartiteGraph<>(topVertices, bottomVertices, mappedEdges, context);
	}

	@ForwardedFields({"f0", "f1"})
	private static class MapEdges<KT, KB, EV, NV> implements MapFunction<BipartiteEdge<KT, KB, EV>, BipartiteEdge<KT, KB, NV>> {
		private final MapFunction<BipartiteEdge<KT, KB, EV>, NV> mapper;
		private BipartiteEdge<KT, KB, NV> output;

		public MapEdges(MapFunction<BipartiteEdge<KT, KB, EV>, NV> mapper) {
			this.mapper = mapper;
			output = new BipartiteEdge<>();
		}

		public BipartiteEdge<KT, KB, NV> map(BipartiteEdge<KT, KB, EV> value) throws Exception {
			output.f0 = value.f0;
			output.f1 = value.f1;
			output.f2 = mapper.map(value);
			return output;
		}
	}

	/**
	 * Joins the top vertex DataSet of this graph with an input Tuple2 DataSet and applies
	 * a user-defined transformation on the values of the matched records.
	 * The vertex ID and the first field of the Tuple2 DataSet are used as the join keys.
	 *
	 * @param inputDataSet the Tuple2 DataSet to join with.
	 * The first field of the Tuple2 is used as the join key and the second field is passed
	 * as a parameter to the transformation function.
	 * @param vertexJoinFunction the transformation function to apply.
	 * The first parameter is the current vertex value and the second parameter is the value
	 * of the matched Tuple2 from the input DataSet.
	 * @return a new BipartiteGraph, where the top vertex values have been updated according to the
	 * result of the vertexJoinFunction.
	 *
	 * @param <T> the type of the second field of the input Tuple2 DataSet.
	 */
	public <T> BipartiteGraph<KT, KB, VVT, VVB, EV> joinWithTopVertices(
				DataSet<Tuple2<KT, T>> inputDataSet,
				final VertexJoinFunction<VVT, T> vertexJoinFunction) {

		DataSet<Vertex<KT, VVT>> resultedVertices = topVertices
			.coGroup(inputDataSet)
			.where(0)
			.equalTo(0)
			.with(new ApplyCoGroupToVertexValues<KT, VVT, T>(vertexJoinFunction))
			.name("Join with top vertices");
		return new BipartiteGraph<>(resultedVertices, bottomVertices, edges, context);
	}

	/**
	 * Joins the bottom vertex DataSet of this graph with an input Tuple2 DataSet and applies
	 * a user-defined transformation on the values of the matched records.
	 * The vertex ID and the first field of the Tuple2 DataSet are used as the join keys.
	 *
	 * @param inputDataSet the Tuple2 DataSet to join with.
	 * The first field of the Tuple2 is used as the join key and the second field is passed
	 * as a parameter to the transformation function.
	 * @param vertexJoinFunction the transformation function to apply.
	 * The first parameter is the current vertex value and the second parameter is the value
	 * of the matched Tuple2 from the input DataSet.
	 * @return a new BipartiteGraph, where the bottom vertex values have been updated according to the
	 * result of the vertexJoinFunction.
	 *
	 * @param <T> the type of the second field of the input Tuple2 DataSet.
	 */
	public <T> BipartiteGraph<KT, KB, VVT, VVB, EV> joinWithBottomVertices(
				DataSet<Tuple2<KB, T>> inputDataSet,
				final VertexJoinFunction<VVB, T> vertexJoinFunction) {

		DataSet<Vertex<KB, VVB>> resultedVertices = bottomVertices
			.coGroup(inputDataSet)
			.where(0)
			.equalTo(0)
			.with(new ApplyCoGroupToVertexValues<KB, VVB, T>(vertexJoinFunction))
			.name("Join with bottom vertices");
		return new BipartiteGraph<>(topVertices, resultedVertices, edges, context);
	}

	/**
	 * Joins the edge DataSet with an input DataSet on the composite key of both
	 * top and bottom IDs and applies a user-defined transformation on the values
	 * of the matched records. The first two fields of the input DataSet are used as join keys.
	 *
	 * @param inputDataSet the DataSet to join with.
	 * The first two fields of the Tuple3 are used as the composite join key
	 * and the third field is passed as a parameter to the transformation function.
	 * @param edgeJoinFunction the transformation function to apply.
	 * The first parameter is the current edge value and the second parameter is the value
	 * of the matched Tuple3 from the input DataSet.
	 * @param <T> the type of the third field of the input Tuple3 DataSet.
	 * @return a new BipartiteGraph, where the edge values have been updated according to the
	 * result of the edgeJoinFunction.
	 */
	public <T> BipartiteGraph<KT, KB, VVT, VVB, EV> joinWithEdges(
				DataSet<Tuple3<KT, KB, T>> inputDataSet,
				final BipartiteEdgeJoinFunction<EV, T> edgeJoinFunction) {

		DataSet<BipartiteEdge<KT, KB, EV>> resultedEdges = this.getEdges()
			.coGroup(inputDataSet).where(0, 1).equalTo(0, 1)
			.with(new ApplyCoGroupToEdgeValues<KT, KB, EV, T>(edgeJoinFunction))
			.name("Join with edges");
		return new BipartiteGraph<>(topVertices, bottomVertices, resultedEdges, context);
	}

	private static final class ApplyCoGroupToEdgeValues<KT, KB, EV, T>
		implements CoGroupFunction<BipartiteEdge<KT, KB, EV>, Tuple3<KT, KB, T>, BipartiteEdge<KT, KB, EV>> {
		private BipartiteEdge<KT, KB, EV> output = new BipartiteEdge<>();

		private BipartiteEdgeJoinFunction<EV, T> edgeJoinFunction;

		public ApplyCoGroupToEdgeValues(BipartiteEdgeJoinFunction<EV, T> mapper) {
			this.edgeJoinFunction = mapper;
		}

		@Override
		public void coGroup(Iterable<BipartiteEdge<KT, KB, EV>> edges, Iterable<Tuple3<KT, KB, T>> input,
							Collector<BipartiteEdge<KT, KB, EV>> collector) throws Exception {

			final Iterator<BipartiteEdge<KT, KB, EV>> edgesIterator = edges.iterator();
			final Iterator<Tuple3<KT, KB, T>> inputIterator = input.iterator();

			if (edgesIterator.hasNext()) {
				if (inputIterator.hasNext()) {
					final Tuple3<KT, KB, T> inputNext = inputIterator.next();

					output.f0 = inputNext.f0;
					output.f1 = inputNext.f1;
					output.f2 = edgeJoinFunction.edgeJoin(edgesIterator.next().f2, inputNext.f2);
					collector.collect(output);
				} else {
					collector.collect(edgesIterator.next());
				}
			}
		}
	}

	/**
	 * Apply filtering functions to the graph and return a sub-graph that
	 * satisfies the predicates for both vertices and edges.
	 *
	 * @param topVertexFilter the filter function for top vertices.
	 * @param bottomVertexFilter the filter function for bottom vertices.
	 * @param edgeFilter the filter function for edges.
	 * @return the resulting sub-graph.
	 */
	public BipartiteGraph<KT, KB, VVT, VVB, EV> subgraph(
			FilterFunction<Vertex<KT, VVT>> topVertexFilter,
			FilterFunction<Vertex<KB, VVB>> bottomVertexFilter,
			FilterFunction<BipartiteEdge<KT, KB, EV>> edgeFilter) {

		DataSet<Vertex<KT, VVT>> filteredTopVertices = topVertices.filter(topVertexFilter);
		DataSet<Vertex<KB, VVB>> filteredBottomVertices = bottomVertices.filter(bottomVertexFilter);

		DataSet<BipartiteEdge<KT, KB, EV>> remainingEdges = edges
			.filter(edgeFilter)
			.join(filteredTopVertices)
			.where(0)
			.equalTo(0)
			.with(new JoinFunction<BipartiteEdge<KT, KB, EV>, Vertex<KT, VVT>, BipartiteEdge<KT, KB, EV>>() {
				@Override
				public BipartiteEdge<KT, KB, EV> join(BipartiteEdge<KT, KB, EV> first, Vertex<KT, VVT> second) throws Exception {
					return first;
				}
			})
			.join(filteredBottomVertices)
			.where(1)
			.equalTo(0)
			.with(new JoinFunction<BipartiteEdge<KT, KB, EV>, Vertex<KB, VVB>, BipartiteEdge<KT, KB, EV>>() {
				@Override
				public BipartiteEdge<KT, KB, EV> join(BipartiteEdge<KT, KB, EV> first, Vertex<KB, VVB> second) throws Exception {
					return first;
				}
			});

		return new BipartiteGraph<>(filteredTopVertices, filteredBottomVertices, remainingEdges, context);
	}

	/**
	 * Apply a filtering function to the graph and return a sub-graph that
	 * satisfies the predicates only for the top vertices.
	 *
	 * @param vertexFilter the filter function for top vertices.
	 * @return the resulting sub-graph.
	 */
	public BipartiteGraph<KT, KB, VVT, VVB, EV> filterOnTopVertices(FilterFunction<Vertex<KT, VVT>> vertexFilter) {

		DataSet<Vertex<KT, VVT>> filteredVertices = topVertices.filter(vertexFilter);

		DataSet<BipartiteEdge<KT, KB, EV>> remainingEdges = edges.join(filteredVertices)
			.where(0)
			.equalTo(0)
			.with(new JoinFunction<BipartiteEdge<KT, KB, EV>, Vertex<KT, VVT>, BipartiteEdge<KT, KB, EV>>() {
				@Override
				public BipartiteEdge<KT, KB, EV> join(BipartiteEdge<KT, KB, EV> first, Vertex<KT, VVT> second) throws Exception {
					return first;
				}
			})
			.name("Filter on top vertices");

		return new BipartiteGraph<>(filteredVertices, bottomVertices, remainingEdges, context);
	}

	/**
	 * Apply a filtering function to the graph and return a sub-graph that
	 * satisfies the predicates only for the bottom vertices.
	 *
	 * @param vertexFilter the filter function for bottom vertices.
	 * @return the resulting sub-graph.
	 */
	public BipartiteGraph<KT, KB, VVT, VVB, EV> filterOnBottomVertices(FilterFunction<Vertex<KB, VVB>> vertexFilter) {

		DataSet<Vertex<KB, VVB>> filteredVertices = bottomVertices.filter(vertexFilter);

		DataSet<BipartiteEdge<KT, KB, EV>> remainingEdges = edges.join(filteredVertices)
			.where(1)
			.equalTo(0)
			.with(new JoinFunction<BipartiteEdge<KT, KB, EV>, Vertex<KB, VVB>, BipartiteEdge<KT, KB, EV>>() {
				@Override
				public BipartiteEdge<KT, KB, EV> join(BipartiteEdge<KT, KB, EV> first, Vertex<KB, VVB> second) throws Exception {
					return first;
				}
			})
			.name("Filter on bottom vertices");

		return new BipartiteGraph<>(topVertices, filteredVertices, remainingEdges, context);
	}

	/**
	 * Apply a filtering function to the graph and return a sub-graph that
	 * satisfies the predicates only for the edges.
	 *
	 * @param edgeFilter the filter function for edges.
	 * @return the resulting sub-graph.
	 */
	public BipartiteGraph<KT, KB, VVT, VVB, EV> filterOnEdges(FilterFunction<BipartiteEdge<KT, KB, EV>> edgeFilter) {
		DataSet<BipartiteEdge<KT, KB, EV>> filteredEdges = edges.filter(edgeFilter).name("Filter on edges");

		return new BipartiteGraph<>(topVertices, bottomVertices, filteredEdges, context);
	}

	/**
	 * Convert a bipartite graph into an undirected graph that contains only top vertices. An edge between two vertices
	 * in the new graph will exist only if the original bipartite graph contains a bottom vertex they are both
	 * connected to.
	 *
	 * The simple projection performs a single join and returns edges containing the bipartite edge values.
	 *
	 * Note: KT must override .equals(). This requirement may be removed in a future release.
	 *
	 * @return simple top projection of the bipartite graph
	 */
	public Graph<KT, VVT, Tuple2<EV, EV>> projectionTopSimple() {
		DataSet<Edge<KT, Tuple2<EV, EV>>> newEdges = edges.join(edges)
			.where(1)
			.equalTo(1)
			.with(new ProjectionTopSimple<KT, KB, EV>())
				.name("Simple top projection");

		return Graph.fromDataSet(topVertices, newEdges, context);
	}

	@ForwardedFieldsFirst("0; 2->2.0")
	@ForwardedFieldsSecond("0->1; 2->2.1")
	private static class ProjectionTopSimple<KT, KB, EV>
	implements FlatJoinFunction<BipartiteEdge<KT, KB, EV>, BipartiteEdge<KT, KB, EV>, Edge<KT, Tuple2<EV, EV>>> {
		private Tuple2<EV, EV> edgeValues = new Tuple2<>();

		private Edge<KT, Tuple2<EV, EV>> edge = new Edge<>(null, null, edgeValues);

		@Override
		public void join(BipartiteEdge<KT, KB, EV> first, BipartiteEdge<KT, KB, EV> second, Collector<Edge<KT, Tuple2<EV, EV>>> out)
				throws Exception {
			if (!first.f0.equals(second.f0)) {
				edge.f0 = first.f0;
				edge.f1 = second.f0;

				edgeValues.f0 = first.f2;
				edgeValues.f1 = second.f2;

				out.collect(edge);
			}
		}
	}

	/**
	 * Convert a bipartite graph into an undirected graph that contains only bottom vertices. An edge between two
	 * vertices in the new graph will exist only if the original bipartite graph contains a top vertex they are both
	 * connected to.
	 *
	 * The simple projection performs a single join and returns edges containing the bipartite edge values.
	 *
	 * Note: KB must override .equals(). This requirement may be removed in a future release.
	 *
	 * @return simple bottom projection of the bipartite graph
	 */
	public Graph<KB, VVB, Tuple2<EV, EV>> projectionBottomSimple() {
		DataSet<Edge<KB, Tuple2<EV, EV>>> newEdges =  edges.join(edges)
			.where(0)
			.equalTo(0)
			.with(new ProjectionBottomSimple<KT, KB, EV>())
			.name("Simple bottom projection");

		return Graph.fromDataSet(bottomVertices, newEdges, context);
	}

	@ForwardedFieldsFirst("1->0; 2->2.0")
	@ForwardedFieldsSecond("1; 2->2.1")
	private static class ProjectionBottomSimple<KT, KB, EV>
	implements FlatJoinFunction<BipartiteEdge<KT, KB, EV>, BipartiteEdge<KT, KB, EV>, Edge<KB, Tuple2<EV, EV>>> {
		private Tuple2<EV, EV> edgeValues = new Tuple2<>();

		private Edge<KB, Tuple2<EV, EV>> edge = new Edge<>(null, null, edgeValues);

		@Override
		public void join(BipartiteEdge<KT, KB, EV> first, BipartiteEdge<KT, KB, EV> second, Collector<Edge<KB, Tuple2<EV, EV>>> out)
				throws Exception {
			if (!first.f1.equals(second.f1)) {
				edge.f0 = first.f1;
				edge.f1 = second.f1;

				edgeValues.f0 = first.f2;
				edgeValues.f1 = second.f2;

				out.collect(edge);
			}
		}
	}

	/**
	 * Convert a bipartite graph into a graph that contains only top vertices. An edge between two vertices in the new
	 * graph will exist only if the original bipartite graph contains at least one bottom vertex they both connect to.
	 *
	 * The full projection performs three joins and returns edges containing the the connecting vertex ID and value,
	 * both top vertex values, and both bipartite edge values.
	 *
	 * Note: KT must override .equals(). This requirement may be removed in a future release.
	 *
	 * @return full top projection of the bipartite graph
	 */
	public Graph<KT, VVT, Projection<KB, VVB, VVT, EV>> projectionTopFull() {
		DataSet<Tuple5<KT, KB, EV, VVT, VVB>> edgesWithVertices	= joinEdgeWithVertices();

		DataSet<Edge<KT, Projection<KB, VVB, VVT, EV>>> newEdges = edgesWithVertices.join(edgesWithVertices)
			.where(1)
			.equalTo(1)
			.with(new ProjectionTopFull<KT, KB, EV, VVT, VVB>())
				.name("Full top projection");

		return Graph.fromDataSet(topVertices, newEdges, context);
	}

	private DataSet<Tuple5<KT, KB, EV, VVT, VVB>> joinEdgeWithVertices() {
		return edges
			.join(topVertices, JoinHint.REPARTITION_HASH_SECOND)
			.where(0)
			.equalTo(0)
			.projectFirst(0, 1, 2)
			.<Tuple4<KT, KB, EV, VVT>>projectSecond(1)
				.name("Edge with top vertex")
			.join(bottomVertices, JoinHint.REPARTITION_HASH_SECOND)
			.where(1)
			.equalTo(0)
			.projectFirst(0, 1, 2, 3)
			.<Tuple5<KT, KB, EV, VVT, VVB>>projectSecond(1)
				.name("Edge with bottom vertices");
	}

	@ForwardedFieldsFirst("0; 1->2.0; 2->2.4; 3->2.2; 4->2.1")
	@ForwardedFieldsSecond("0->1; 2->2.5; 3->2.3")
	private static class ProjectionTopFull<KT, KB, EV, VVT, VVB>
	implements FlatJoinFunction<Tuple5<KT, KB, EV, VVT, VVB>, Tuple5<KT, KB, EV, VVT, VVB>, Edge<KT, Projection<KB, VVB, VVT, EV>>> {
		private Projection<KB, VVB, VVT, EV> projection = new Projection<>();

		private Edge<KT, Projection<KB, VVB, VVT, EV>> edge = new Edge<>(null, null, projection);

		@Override
		public void join(Tuple5<KT, KB, EV, VVT, VVB> first, Tuple5<KT, KB, EV, VVT, VVB> second, Collector<Edge<KT, Projection<KB, VVB, VVT, EV>>> out)
				throws Exception {
			if (!first.f0.equals(second.f0)) {
				edge.f0 = first.f0;
				edge.f1 = second.f0;

				projection.f0 = first.f1;
				projection.f1 = first.f4;
				projection.f2 = first.f3;
				projection.f3 = second.f3;
				projection.f4 = first.f2;
				projection.f5 = second.f2;

				out.collect(edge);
			}
		}
	}

	/**
	 * Convert a bipartite graph into a graph that contains only bottom vertices. An edge between two vertices in the
	 * new graph will exist only if the original bipartite graph contains at least one top vertex they both connect to.
	 *
	 * The full projection performs three joins and returns edges containing the the connecting vertex ID and value,
	 * both bottom vertex values, and both bipartite edge values.
	 *
	 * Note: KB must override .equals(). This requirement may be removed in a future release.
	 *
	 * @return full bottom projection of the bipartite graph
	 */
	public Graph<KB, VVB, Projection<KT, VVT, VVB, EV>> projectionBottomFull() {
		DataSet<Tuple5<KT, KB, EV, VVT, VVB>> edgesWithVertices	= joinEdgeWithVertices();

		DataSet<Edge<KB, Projection<KT, VVT, VVB, EV>>> newEdges = edgesWithVertices.join(edgesWithVertices)
			.where(0)
			.equalTo(0)
			.with(new ProjectionBottomFull<KT, KB, EV, VVT, VVB>())
				.name("Full bottom projection");

		return Graph.fromDataSet(bottomVertices, newEdges, context);
	}

	@ForwardedFieldsFirst("1->0; 2->2.4; 3->2.1; 4->2.2")
	@ForwardedFieldsSecond("1; 2->2.5; 4->2.3")
	private static class ProjectionBottomFull<KT, KB, EV, VVT, VVB>
	implements FlatJoinFunction<Tuple5<KT, KB, EV, VVT, VVB>, Tuple5<KT, KB, EV, VVT, VVB>, Edge<KB, Projection<KT, VVT, VVB, EV>>> {
		private Projection<KT, VVT, VVB, EV> projection = new Projection<>();

		private Edge<KB, Projection<KT, VVT, VVB, EV>> edge = new Edge<>(null, null, projection);

		@Override
		public void join(Tuple5<KT, KB, EV, VVT, VVB> first, Tuple5<KT, KB, EV, VVT, VVB> second, Collector<Edge<KB, Projection<KT, VVT, VVB, EV>>> out)
			throws Exception {
			if (!first.f1.equals(second.f1)) {
				edge.f0 = first.f1;
				edge.f1 = second.f1;

				projection.f0 = first.f0;
				projection.f1 = first.f3;
				projection.f2 = first.f4;
				projection.f3 = second.f4;
				projection.f4 = first.f2;
				projection.f5 = second.f2;

				out.collect(edge);
			}
		}
	}
}
