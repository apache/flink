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

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.utils.Tuple2ToTuple3Map;
import org.apache.flink.graph.utils.Tuple2ToVertexMap;
import org.apache.flink.graph.utils.Tuple3ToBipartiteEdgeMap;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import static org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;

import java.util.Collection;

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

	/**
	 * Creates a bipartite graph from three DataSets: top vertices, bottom vertices, and edges
	 *
	 * @param topVertices a DataSet of top vertices.
	 * @param bottomVertices a DataSet of bottom vertices.
	 * @param edges a DataSet of edges.
	 * @param context the Flink execution environment.
	 */
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
	 * Creates a graph from two Collection of top and bottom vertices and a Collection of edges.
	 *
	 * @param topVertices a Collection of top vertices.
	 * @param bottomVertices a Collection of bottom vertices.
	 * @param edges a Collection of edges.
	 * @param context the Flink execution environment.
	 * @return the newly created bipartite graph.
	 */
	public static <KT, KB, VVT, VVB, EV> BipartiteGraph<KT, KB, VVT, VVB, EV> fromCollection(
															Collection<Vertex<KT, VVT>> topVertices,
															Collection<Vertex<KB, VVB>> bottomVertices,
															Collection<BipartiteEdge<KT, KB, EV>> edges,
															ExecutionEnvironment context) {

		return fromDataSet(
			context.fromCollection(topVertices),
			context.fromCollection(bottomVertices),
			context.fromCollection(edges), context);
	}

	/**
	 * Creates a graph from a Collection of edges.
	 * Vertices are created automatically and their values are set to
	 * NullValue.
	 *
	 * @param edges a Collection of edges.
	 * @param context the Flink execution environment.
	 * @return the newly created bipartite graph.
	 */
	public static <KT, KB, EV> BipartiteGraph<KT, KB, NullValue, NullValue, EV> fromCollection(
																Collection<BipartiteEdge<KT, KB, EV>> edges,
																ExecutionEnvironment context) {

		return fromDataSet(context.fromCollection(edges), context);
	}

	/**
	 * Creates a graph from a Collection of edges.
	 * Vertices are created automatically and their values are set
	 * by applying the provided map function to the vertex IDs.
	 *
	 * @param edges a Collection of edges.
	 * @param topVertexValueInitializer a map function that initializes the top vertex values.
	 * It allows to apply a map transformation on the vertex ID to produce an initial vertex value.
	 * @param bottomVertexValueInitializer a map function that initializes the top vertex values.
	 * It allows to apply a map transformation on the vertex ID to produce an initial vertex value.
	 * @param context the Flink execution environment.
	 * @return the newly created bipartite graph.
	 */
	public static <KT, KB, VVT, VVB, EV> BipartiteGraph<KT, KB, VVT, VVB, EV> fromCollection(
															Collection<BipartiteEdge<KT, KB, EV>> edges,
															final MapFunction<KT, VVT> topVertexValueInitializer,
															final MapFunction<KB, VVB> bottomVertexValueInitializer,
															ExecutionEnvironment context) {

		return fromDataSet(
			context.fromCollection(edges),
			topVertexValueInitializer,
			bottomVertexValueInitializer,
			context);
	}

	/**
	 * Creates a graph from a DataSet of Tuple3 objects for edges.
	 * <p>
	 * Each Tuple3 will become one Edge, where the source ID will be the first field of the Tuple2,
	 * the target ID will be the second field of the Tuple2
	 * and the Edge value will be the third field of the Tuple3.
	 * <p>
	 * Vertices are created automatically and their values are initialized
	 * by applying provided topVertexValueInitializer and bottomVertexValueInitializer map functions to the vertex IDs.
	 *
	 * @param edges a DataSet of Tuple3.
	 * @param topVertexValueInitializer the mapper function that initializes the top vertex values.
	 * @param bottomVertexValueInitializer the mapper function that initializes the bottom vertex values.
	 * @param context the Flink execution environment.
	 * @param <KT> the key type of the top vertices
	 * @param <KB> the key type of the bottom vertices
	 * @param <VVT> the top vertices value type
	 * @param <VVB> the bottom vertices value type
	 * @param <EV> the edge value type
	 * @return the newly created bipartite graph.
	 */
	public static <KT, KB, VVT, VVB, EV> BipartiteGraph<KT, KB, VVT, VVB, EV> fromTupleDataSet(
		DataSet<Tuple3<KT, KB, EV>> edges, MapFunction<KT, VVT> topVertexValueInitializer,
		MapFunction<KB, VVB> bottomVertexValueInitializer, ExecutionEnvironment context) {
		return BipartiteGraph.fromDataSet(
			edges.map(new Tuple3ToBipartiteEdgeMap<KT, KB, EV>()),
			topVertexValueInitializer,
			bottomVertexValueInitializer,
			context
		);
	}

	/**
	 * Creates a graph from a DataSet of Tuple2 objects for edges.
	 * <p>
	 * The first field of the Tuple2 object will become the source ID,
	 * and the second field will become the target ID
	 * <p>
	 * Vertices are created automatically and their values are set to NullValue.
	 *
	 * @param edges a DataSet of Tuple2 representing the edges.
	 * @param context the Flink execution environment.
	 * @param <KT> the key type of the top vertices
	 * @param <KB> the key type of the bottom vertices
	 * @return the newly created graph.
	 */
	public static <KT, KB> BipartiteGraph<KT, KB, NullValue, NullValue, NullValue> fromTuple2DataSet(
		DataSet<Tuple2<KT, KB>> edges, ExecutionEnvironment context) {

		return BipartiteGraph.fromTupleDataSet(
			edges.map(new Tuple2ToTuple3Map<KT, KB>()),
			context
		);
	}

	/**
	 * Creates a graph from a DataSet of Tuple3 objects for edges.
	 * <p>
	 * The first field of the Tuple3 object will become the top vertex ID,
	 * the second field will become the bottom vertex ID, and the third field will become
	 * the edge value.
	 * <p>
	 * Vertices are created automatically and their values are set to NullValue.
	 *
	 * @param edges a DataSet of Tuple3 representing the edges.
	 * @param context the Flink execution environment.
	 * @param <KT> the key type of the top vertices
	 * @param <KB> the key type of the bottom vertices
	 * @param <EV> the edge value type
	 * @return the newly created graph.
	 */
	public static <KT, KB, EV> BipartiteGraph<KT, KB, NullValue, NullValue, EV> fromTupleDataSet(
		DataSet<Tuple3<KT, KB, EV>> edges, ExecutionEnvironment context) {

		return BipartiteGraph.fromDataSet(
			edges.map(new Tuple3ToBipartiteEdgeMap<KT, KB, EV>()),
			context
		);
	}

	/**
	 * Creates a graph from a DataSet of edges.
	 * Vertices are created automatically and their values are set to
	 * NullValue.
	 *
	 * @param edges a DataSet of edges.
	 * @param context the Flink execution environment.
	 * @param <KT> the key type of the top vertices
	 * @param <KB> the key type of the bottom vertices
	 * @param <EV> the edge value type
	 * @return the newly created graph.
	 */
	public static <KT, KB, EV> BipartiteGraph<KT, KB, NullValue, NullValue, EV> fromDataSet(
		DataSet<BipartiteEdge<KT, KB, EV>> edges, ExecutionEnvironment context) {

		DataSet<Vertex<KT, NullValue>> topVertices = edges.map(new EmitTopIds<KT, KB, EV>()).distinct();
		DataSet<Vertex<KB, NullValue>> bottomVertices = edges.map(new EmitBottomIds<KT, KB, EV>()).distinct();

		return new BipartiteGraph<>(topVertices, bottomVertices, edges, context);
	}

	@ForwardedFields("f0")
	private static class EmitTopIds<KT, KB, EV> implements MapFunction<BipartiteEdge<KT, KB, EV>, Vertex<KT, NullValue>> {

		private Vertex<KT, NullValue> result = new Vertex<>();

		@Override
		public Vertex<KT, NullValue> map(BipartiteEdge<KT, KB, EV> value) throws Exception {
			result.setId(value.getTopId());
			return result;
		}
	}

	@ForwardedFields("f1->f0")
	private static class EmitBottomIds<KT, KB, EV> implements MapFunction<BipartiteEdge<KT, KB, EV>, Vertex<KB, NullValue>> {

		private Vertex<KB, NullValue> result = new Vertex<>();

		@Override
		public Vertex<KB, NullValue> map(BipartiteEdge<KT, KB, EV> value) throws Exception {
			result.setId(value.getBottomId());
			return result;
		}
	}

	/**
	 * Creates a graph from a DataSet of edges.
	 * Vertices are created automatically and their values are set
	 * by applying the provided map functions to the vertex IDs.
	 *
	 * @param edges a DataSet of edges.
	 * @param topVertexValueInitializer the mapper function that initializes the top vertex values.
	 * @param bottomVertexValueInitializer the mapper function that initializes the bottom vertex values.
	 * @param context the Flink execution environment.
	 * @param <KT> the key type of the top vertices
	 * @param <KB> the key type of the bottom vertices
	 * @param <VVT> the top vertices value type
	 * @param <VVB> the bottom vertices value type
	 * @param <EV> the edge value type
	 * @return the newly created bipartite graph.
	 */
	public static <KT, KB, VVT, VVB, EV> BipartiteGraph<KT, KB, VVT, VVB, EV> fromDataSet(
		DataSet<BipartiteEdge<KT, KB, EV>> edges,
		final MapFunction<KT, VVT> topVertexValueInitializer,
		final MapFunction<KB, VVB> bottomVertexValueInitializer,
		ExecutionEnvironment context) {

		TypeInformation<KT> topKeyType = ((TupleTypeInfo<?>) edges.getType()).getTypeAt(0);
		TypeInformation<KB> bootomKeyType = ((TupleTypeInfo<?>) edges.getType()).getTypeAt(1);

		TypeInformation<VVT> topValueType = TypeExtractor.createTypeInfo(
			MapFunction.class, topVertexValueInitializer.getClass(), 1, null, null);
		TypeInformation<VVB> bottomValueType = TypeExtractor.createTypeInfo(
			MapFunction.class, bottomVertexValueInitializer.getClass(), 1, null, null);

		@SuppressWarnings({ "unchecked", "rawtypes" })
		TypeInformation<Vertex<KT, VVT>> topReturnType = (TypeInformation<Vertex<KT, VVT>>) new TupleTypeInfo(
			Vertex.class, topKeyType, topValueType);

		@SuppressWarnings({ "unchecked", "rawtypes" })
		TypeInformation<Vertex<KB, VVB>> bottomReturnType = (TypeInformation<Vertex<KB, VVB>>) new TupleTypeInfo(
			Vertex.class, bootomKeyType, bottomValueType);

		DataSet<Vertex<KT, VVT>> topVerties = edges
			.map(new EmitTopAsTuple1<KT, KB, EV>()).distinct()
			.map(new MapFunction<Tuple1<KT>, Vertex<KT, VVT>>() {
				public Vertex<KT, VVT> map(Tuple1<KT> value) throws Exception {
					return new Vertex<>(value.f0, topVertexValueInitializer.map(value.f0));
				}
			}).returns(topReturnType).withForwardedFields("f0");

		DataSet<Vertex<KB, VVB>> bottomVerties = edges
			.map(new EmitBottomAsTuple1<KT, KB, EV>()).distinct()
			.map(new MapFunction<Tuple1<KB>, Vertex<KB, VVB>>() {
				public Vertex<KB, VVB> map(Tuple1<KB> value) throws Exception {
					return new Vertex<>(value.f0, bottomVertexValueInitializer.map(value.f0));
				}
			}).returns(bottomReturnType).withForwardedFields("f0");

		return BipartiteGraph.fromDataSet(topVerties, bottomVerties, edges, context);
	}

	@ForwardedFields("f0")
	private static final class EmitTopAsTuple1<KT, KB, EV> implements MapFunction<BipartiteEdge<KT, KB, EV>, Tuple1<KT>> {

		private Tuple1<KT> result  = new Tuple1<>();

		@Override
		public Tuple1<KT> map(BipartiteEdge<KT, KB, EV> value) throws Exception {
			result.f0 = value.getTopId();
			return result;
		}
	}

	@ForwardedFields("f1->f0")
	private static final class EmitBottomAsTuple1<KT, KB, EV> implements MapFunction<BipartiteEdge<KT, KB, EV>, Tuple1<KB>> {

		private Tuple1<KB> result  = new Tuple1<>();

		@Override
		public Tuple1<KB> map(BipartiteEdge<KT, KB, EV> value) throws Exception {
			result.f0 = value.getBottomId();
			return result;
		}
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
	 * @param <VVT> the top vertices value type
	 * @param <VVB> the bottom vertices value type
	 * @param <EV> the edge value type
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
	 * Creates a BipartiteGraph from CSV files of vertices and a CSV file of edges.
	 *
	 * @param topVerticesPath path to a CSV file with the top Vertex data.
	 * @param bottomVerticesPath path to a CSV file with the bottom Vertex data.
	 * @param edgesPath path to a CSV file with the Edge data
	 * @param context the Flink execution environment.
	 * @return An instance of {@link org.apache.flink.graph.bipartite.BipartiteGraphCsvReader},
	 * on which calling methods to specify types of the Vertex ID, Vertex value and Edge value returns a Graph.
	 *
	 * @see org.apache.flink.graph.bipartite.BipartiteGraphCsvReader#types(Class, Class, Class, Class, Class)
	 * @see org.apache.flink.graph.bipartite.BipartiteGraphCsvReader#vertexTypes(Class, Class, Class, Class)
	 * @see org.apache.flink.graph.bipartite.BipartiteGraphCsvReader#edgeTypes(Class, Class, Class)
	 * @see org.apache.flink.graph.bipartite.BipartiteGraphCsvReader#keyType(Class, Class)
	 */
	public static BipartiteGraphCsvReader fromCsvReader(String topVerticesPath, String bottomVerticesPath, String edgesPath, ExecutionEnvironment context) {
		return new BipartiteGraphCsvReader(topVerticesPath, bottomVerticesPath, edgesPath, context);
	}

	/**
	 * Creates a graph from a CSV file of edges. Vertices will be created automatically.
	 *
	 * @param edgesPath a path to a CSV file with the Edges data
	 * @param context the execution environment.
	 * @return An instance of {@link org.apache.flink.graph.bipartite.BipartiteGraphCsvReader},
	 * on which calling methods to specify types of the Vertex ID, Vertex value and Edge value returns a Graph.
	 *
	 * @see org.apache.flink.graph.bipartite.BipartiteGraphCsvReader#types(Class, Class, Class, Class, Class)
	 * @see org.apache.flink.graph.bipartite.BipartiteGraphCsvReader#vertexTypes(Class, Class, Class, Class)
	 * @see org.apache.flink.graph.bipartite.BipartiteGraphCsvReader#edgeTypes(Class, Class, Class)
	 * @see org.apache.flink.graph.bipartite.BipartiteGraphCsvReader#keyType(Class, Class)
	 */
	public static BipartiteGraphCsvReader fromCsvReader(String edgesPath, ExecutionEnvironment context) {
		return new BipartiteGraphCsvReader(edgesPath, context);
	}

	/**
	 * Creates a graph from a CSV file of edges. Vertices will be created automatically and
	 * Vertex values can be initialized using a user-defined mapper.
	 *
	 * @param edgesPath a path to a CSV file with the Edge data
	 * @param topVertexValueInitializer the mapper function that initializes the top vertex values.
	 * It allows to apply a map transformation on the vertex ID to produce an initial vertex value.
	 * @param bottomVertexValueInitializer the mapper function that initializes the bottom vertex values.
	 * It allows to apply a map transformation on the vertex ID to produce an initial vertex value.
	 * @param context the execution environment.
	 * @return An instance of {@link org.apache.flink.graph.bipartite.BipartiteGraphCsvReader},
	 * on which calling methods to specify types of the Vertex ID, Vertex Value and Edge value returns a Graph.
	 *
	 * @see org.apache.flink.graph.bipartite.BipartiteGraphCsvReader#types(Class, Class, Class, Class, Class)
	 * @see org.apache.flink.graph.bipartite.BipartiteGraphCsvReader#vertexTypes(Class, Class, Class, Class)
	 * @see org.apache.flink.graph.bipartite.BipartiteGraphCsvReader#edgeTypes(Class, Class, Class)
	 * @see org.apache.flink.graph.bipartite.BipartiteGraphCsvReader#keyType(Class, Class)
	 */
	public static <KT, KB, VVT, VVB> BipartiteGraphCsvReader fromCsvReader(String edgesPath,
													final MapFunction<KT, VVT> topVertexValueInitializer,
													final MapFunction<KB, VVB> bottomVertexValueInitializer,
													ExecutionEnvironment context) {
		return new BipartiteGraphCsvReader(edgesPath, topVertexValueInitializer, bottomVertexValueInitializer, context);
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
				.name("Edge with vertex")
			.join(bottomVertices, JoinHint.REPARTITION_HASH_SECOND)
			.where(1)
			.equalTo(0)
			.projectFirst(0, 1, 2, 3)
			.<Tuple5<KT, KB, EV, VVT, VVB>>projectSecond(1)
				.name("Edge with vertices");
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

	/**
	 * Creates a graph from a DataSet of Tuple2 objects for top vertices,
	 * a DataSet of Tuple2 objects for bottom vertices, and DataSet
	 * Tuple3 objects for edges.
	 * <p>
	 * The first field of the Tuple2 vertex object will become the vertex ID
	 * and the second field will become the vertex value.
	 * The first field of the Tuple3 object for edges will become the top vertex ID,
	 * the second field will become the bottom vertex ID, and the third field will become
	 * the edge value.
	 *
	 * @param <KT> the key type of top vertices
	 * @param <KB> the key type of bottom vertices
	 * @param <VVT> the vertex value type of top vertices
	 * @param <VVB> the vertex value type of bottom vertices
	 * @param <EV> the edge value type
	 *
	 * @param topVertices a DataSet of Tuple2 representing the top vertices.
	 * @param bottomVertices a DataSet of Tuple2 representing the bottom vertices.
	 * @param edges a DataSet of Tuple3 representing the edges.
	 * @param executionContext the Flink execution environment.
	 * @return the newly created bipartite graph.
	 */
	public static <KT, KB, VVT, VVB, EV> BipartiteGraph<KT, KB, VVT, VVB, EV> fromTupleDataSet(
		DataSet<Tuple2<KT, VVT>> topVertices, DataSet<Tuple2<KB, VVB>> bottomVertices,
		DataSet<Tuple3<KT, KB, EV>> edges, ExecutionEnvironment executionContext) {

		return BipartiteGraph.fromDataSet(
			topVertices.map(new Tuple2ToVertexMap<KT, VVT>()),
			bottomVertices.map(new Tuple2ToVertexMap<KB, VVB>()),
			edges.map(new Tuple3ToBipartiteEdgeMap<KT, KB, EV>()),
			executionContext
		);
	}
}
