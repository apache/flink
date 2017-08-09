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
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.asm.translate.TranslateEdgeValues;
import org.apache.flink.graph.asm.translate.TranslateFunction;
import org.apache.flink.graph.asm.translate.TranslateGraphIds;
import org.apache.flink.graph.asm.translate.TranslateVertexValues;
import org.apache.flink.graph.gsa.ApplyFunction;
import org.apache.flink.graph.gsa.GSAConfiguration;
import org.apache.flink.graph.gsa.GatherSumApplyIteration;
import org.apache.flink.graph.gsa.SumFunction;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageCombiner;
import org.apache.flink.graph.pregel.VertexCentricConfiguration;
import org.apache.flink.graph.pregel.VertexCentricIteration;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.graph.spargel.ScatterGatherConfiguration;
import org.apache.flink.graph.spargel.ScatterGatherIteration;
import org.apache.flink.graph.utils.EdgeToTuple3Map;
import org.apache.flink.graph.utils.Tuple2ToEdgeMap;
import org.apache.flink.graph.utils.Tuple2ToVertexMap;
import org.apache.flink.graph.utils.Tuple3ToEdgeMap;
import org.apache.flink.graph.utils.VertexToTuple2Map;
import org.apache.flink.graph.validation.GraphValidator;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Represents a Graph consisting of {@link Edge edges} and {@link Vertex
 * vertices}.
 *
 * @see org.apache.flink.graph.Edge
 * @see org.apache.flink.graph.Vertex
 *
 * @param <K> the key type for edge and vertex identifiers
 * @param <VV> the value type for vertices
 * @param <EV> the value type for edges
 */
@SuppressWarnings("serial")
public class Graph<K, VV, EV> {

	private final ExecutionEnvironment context;
	private final DataSet<Vertex<K, VV>> vertices;
	private final DataSet<Edge<K, EV>> edges;

	/**
	 * Creates a graph from two DataSets: vertices and edges.
	 *
	 * @param vertices a DataSet of vertices.
	 * @param edges a DataSet of edges.
	 * @param context the flink execution environment.
	 */
	protected Graph(DataSet<Vertex<K, VV>> vertices, DataSet<Edge<K, EV>> edges, ExecutionEnvironment context) {
		this.vertices = vertices;
		this.edges = edges;
		this.context = context;
	}

	/**
	 * Creates a graph from a Collection of vertices and a Collection of edges.
	 *
	 * @param vertices a Collection of vertices.
	 * @param edges a Collection of edges.
	 * @param context the flink execution environment.
	 * @return the newly created graph.
	 */
	public static <K, VV, EV> Graph<K, VV, EV> fromCollection(Collection<Vertex<K, VV>> vertices,
			Collection<Edge<K, EV>> edges, ExecutionEnvironment context) {

		return fromDataSet(context.fromCollection(vertices),
				context.fromCollection(edges), context);
	}

	/**
	 * Creates a graph from a Collection of edges.
	 * Vertices are created automatically and their values are set to
	 * NullValue.
	 *
	 * @param edges a Collection of edges.
	 * @param context the flink execution environment.
	 * @return the newly created graph.
	 */
	public static <K, EV> Graph<K, NullValue, EV> fromCollection(Collection<Edge<K, EV>> edges,
			ExecutionEnvironment context) {

		return fromDataSet(context.fromCollection(edges), context);
	}

	/**
	 * Creates a graph from a Collection of edges.
	 * Vertices are created automatically and their values are set
	 * by applying the provided map function to the vertex IDs.
	 *
	 * @param edges a Collection of edges.
	 * @param vertexValueInitializer a map function that initializes the vertex values.
	 * It allows to apply a map transformation on the vertex ID to produce an initial vertex value.
	 * @param context the flink execution environment.
	 * @return the newly created graph.
	 */
	public static <K, VV, EV> Graph<K, VV, EV> fromCollection(Collection<Edge<K, EV>> edges,
			final MapFunction<K, VV> vertexValueInitializer, ExecutionEnvironment context) {

		return fromDataSet(context.fromCollection(edges), vertexValueInitializer, context);
	}

	/**
	 * Creates a graph from a DataSet of vertices and a DataSet of edges.
	 *
	 * @param vertices a DataSet of vertices.
	 * @param edges a DataSet of edges.
	 * @param context the flink execution environment.
	 * @return the newly created graph.
	 */
	public static <K, VV, EV> Graph<K, VV, EV> fromDataSet(DataSet<Vertex<K, VV>> vertices,
			DataSet<Edge<K, EV>> edges, ExecutionEnvironment context) {

		return new Graph<>(vertices, edges, context);
	}

	/**
	 * Creates a graph from a DataSet of edges.
	 * Vertices are created automatically and their values are set to
	 * NullValue.
	 *
	 * @param edges a DataSet of edges.
	 * @param context the flink execution environment.
	 * @return the newly created graph.
	 */
	public static <K, EV> Graph<K, NullValue, EV> fromDataSet(
			DataSet<Edge<K, EV>> edges, ExecutionEnvironment context) {

		DataSet<Vertex<K, NullValue>> vertices = edges
			.flatMap(new EmitSrcAndTarget<>())
				.name("Source and target IDs")
			.distinct()
				.name("IDs");

		return new Graph<>(vertices, edges, context);
	}

	private static final class EmitSrcAndTarget<K, EV>
	implements FlatMapFunction<Edge<K, EV>, Vertex<K, NullValue>> {
		private Vertex<K, NullValue> output = new Vertex<>(null, NullValue.getInstance());

		public void flatMap(Edge<K, EV> edge, Collector<Vertex<K, NullValue>> out) {
			output.f0 = edge.f0;
			out.collect(output);
			output.f0 = edge.f1;
			out.collect(output);
		}
	}

	/**
	 * Creates a graph from a DataSet of edges.
	 * Vertices are created automatically and their values are set
	 * by applying the provided map function to the vertex IDs.
	 *
	 * @param edges a DataSet of edges.
	 * @param vertexValueInitializer the mapper function that initializes the vertex values.
	 * It allows to apply a map transformation on the vertex ID to produce an initial vertex value.
	 * @param context the flink execution environment.
	 * @return the newly created graph.
	 */
	public static <K, VV, EV> Graph<K, VV, EV> fromDataSet(DataSet<Edge<K, EV>> edges,
			final MapFunction<K, VV> vertexValueInitializer, ExecutionEnvironment context) {

		TypeInformation<K> keyType = ((TupleTypeInfo<?>) edges.getType()).getTypeAt(0);

		TypeInformation<VV> valueType = TypeExtractor.createTypeInfo(
				MapFunction.class, vertexValueInitializer.getClass(), 1, keyType, null);

		@SuppressWarnings({ "unchecked", "rawtypes" })
		TypeInformation<Vertex<K, VV>> returnType = (TypeInformation<Vertex<K, VV>>) new TupleTypeInfo(
				Vertex.class, keyType, valueType);

		DataSet<Vertex<K, VV>> vertices = edges
			.flatMap(new EmitSrcAndTargetAsTuple1<>())
				.name("Source and target IDs")
			.distinct()
				.name("IDs")
			.map(new MapFunction<Tuple1<K>, Vertex<K, VV>>() {
				private Vertex<K, VV> output = new Vertex<>();

				public Vertex<K, VV> map(Tuple1<K> value) throws Exception {
					output.f0 = value.f0;
					output.f1 = vertexValueInitializer.map(value.f0);
					return output;
				}
			}).returns(returnType).withForwardedFields("f0").name("Initialize vertex values");

		return new Graph<>(vertices, edges, context);
	}

	private static final class EmitSrcAndTargetAsTuple1<K, EV>
	implements FlatMapFunction<Edge<K, EV>, Tuple1<K>> {
		private Tuple1<K> output = new Tuple1<>();

		public void flatMap(Edge<K, EV> edge, Collector<Tuple1<K>> out) {
			output.f0 = edge.f0;
			out.collect(output);
			output.f0 = edge.f1;
			out.collect(output);
		}
	}

	/**
	 * Creates a graph from a DataSet of Tuple2 objects for vertices and
	 * Tuple3 objects for edges.
	 *
	 * <p>The first field of the Tuple2 vertex object will become the vertex ID
	 * and the second field will become the vertex value.
	 * The first field of the Tuple3 object for edges will become the source ID,
	 * the second field will become the target ID, and the third field will become
	 * the edge value.
	 *
	 * @param vertices a DataSet of Tuple2 representing the vertices.
	 * @param edges a DataSet of Tuple3 representing the edges.
	 * @param context the flink execution environment.
	 * @return the newly created graph.
	 */
	public static <K, VV, EV> Graph<K, VV, EV> fromTupleDataSet(DataSet<Tuple2<K, VV>> vertices,
			DataSet<Tuple3<K, K, EV>> edges, ExecutionEnvironment context) {

		DataSet<Vertex<K, VV>> vertexDataSet = vertices
			.map(new Tuple2ToVertexMap<>())
				.name("Type conversion");

		DataSet<Edge<K, EV>> edgeDataSet = edges
			.map(new Tuple3ToEdgeMap<>())
				.name("Type conversion");

		return fromDataSet(vertexDataSet, edgeDataSet, context);
	}

	/**
	 * Creates a graph from a DataSet of Tuple3 objects for edges.
	 *
	 * <p>The first field of the Tuple3 object will become the source ID,
	 * the second field will become the target ID, and the third field will become
	 * the edge value.
	 *
	 * <p>Vertices are created automatically and their values are set to NullValue.
	 *
	 * @param edges a DataSet of Tuple3 representing the edges.
	 * @param context the flink execution environment.
	 * @return the newly created graph.
	 */
	public static <K, EV> Graph<K, NullValue, EV> fromTupleDataSet(DataSet<Tuple3<K, K, EV>> edges,
			ExecutionEnvironment context) {

		DataSet<Edge<K, EV>> edgeDataSet = edges
			.map(new Tuple3ToEdgeMap<>())
				.name("Type conversion");

		return fromDataSet(edgeDataSet, context);
	}

	/**
	 * Creates a graph from a DataSet of Tuple3 objects for edges.
	 *
	 * <p>Each Tuple3 will become one Edge, where the source ID will be the first field of the Tuple2,
	 * the target ID will be the second field of the Tuple2
	 * and the Edge value will be the third field of the Tuple3.
	 *
	 * <p>Vertices are created automatically and their values are initialized
	 * by applying the provided vertexValueInitializer map function to the vertex IDs.
	 *
	 * @param edges a DataSet of Tuple3.
	 * @param vertexValueInitializer the mapper function that initializes the vertex values.
	 * It allows to apply a map transformation on the vertex ID to produce an initial vertex value.
	 * @param context the flink execution environment.
	 * @return the newly created graph.
	 */
	public static <K, VV, EV> Graph<K, VV, EV> fromTupleDataSet(DataSet<Tuple3<K, K, EV>> edges,
			final MapFunction<K, VV> vertexValueInitializer, ExecutionEnvironment context) {

		DataSet<Edge<K, EV>> edgeDataSet = edges
			.map(new Tuple3ToEdgeMap<>())
				.name("Type conversion");

		return fromDataSet(edgeDataSet, vertexValueInitializer, context);
	}

	/**
	 * Creates a graph from a DataSet of Tuple2 objects for edges.
	 * Each Tuple2 will become one Edge, where the source ID will be the first field of the Tuple2
	 * and the target ID will be the second field of the Tuple2.
	 *
	 * <p>Edge value types and Vertex values types will be set to NullValue.
	 *
	 * @param edges a DataSet of Tuple2.
	 * @param context the flink execution environment.
	 * @return the newly created graph.
	 */
	public static <K> Graph<K, NullValue, NullValue> fromTuple2DataSet(DataSet<Tuple2<K, K>> edges,
			ExecutionEnvironment context) {

		DataSet<Edge<K, NullValue>> edgeDataSet = edges
			.map(new Tuple2ToEdgeMap<>())
				.name("To Edge");

		return fromDataSet(edgeDataSet, context);
	}

	/**
	 * Creates a graph from a DataSet of Tuple2 objects for edges.
	 * Each Tuple2 will become one Edge, where the source ID will be the first field of the Tuple2
	 * and the target ID will be the second field of the Tuple2.
	 *
	 * <p>Edge value types will be set to NullValue.
	 * Vertex values can be initialized by applying a user-defined map function on the vertex IDs.
	 *
	 * @param edges a DataSet of Tuple2, where the first field corresponds to the source ID
	 * and the second field corresponds to the target ID.
	 * @param vertexValueInitializer the mapper function that initializes the vertex values.
	 * It allows to apply a map transformation on the vertex ID to produce an initial vertex value.
	 * @param context the flink execution environment.
	 * @return the newly created graph.
	 */
	public static <K, VV> Graph<K, VV, NullValue> fromTuple2DataSet(DataSet<Tuple2<K, K>> edges,
			final MapFunction<K, VV> vertexValueInitializer, ExecutionEnvironment context) {

		DataSet<Edge<K, NullValue>> edgeDataSet = edges
			.map(new Tuple2ToEdgeMap<>())
				.name("To Edge");

		return fromDataSet(edgeDataSet, vertexValueInitializer, context);
	}

	/**
	* Creates a Graph from a CSV file of vertices and a CSV file of edges.
	*
	* @param verticesPath path to a CSV file with the Vertex data.
	* @param edgesPath path to a CSV file with the Edge data
	* @param context the Flink execution environment.
	* @return An instance of {@link org.apache.flink.graph.GraphCsvReader},
	* on which calling methods to specify types of the Vertex ID, Vertex value and Edge value returns a Graph.
	*
	* @see org.apache.flink.graph.GraphCsvReader#types(Class, Class, Class)
	* @see org.apache.flink.graph.GraphCsvReader#vertexTypes(Class, Class)
	* @see org.apache.flink.graph.GraphCsvReader#edgeTypes(Class, Class)
	* @see org.apache.flink.graph.GraphCsvReader#keyType(Class)
	*/
	public static GraphCsvReader fromCsvReader(String verticesPath, String edgesPath, ExecutionEnvironment context) {
		return new GraphCsvReader(verticesPath, edgesPath, context);
	}

	/**
	* Creates a graph from a CSV file of edges. Vertices will be created automatically.
	*
	* @param edgesPath a path to a CSV file with the Edges data
	* @param context the execution environment.
	* @return An instance of {@link org.apache.flink.graph.GraphCsvReader},
	* on which calling methods to specify types of the Vertex ID, Vertex value and Edge value returns a Graph.
	*
	* @see org.apache.flink.graph.GraphCsvReader#types(Class, Class, Class)
	* @see org.apache.flink.graph.GraphCsvReader#vertexTypes(Class, Class)
	* @see org.apache.flink.graph.GraphCsvReader#edgeTypes(Class, Class)
	* @see org.apache.flink.graph.GraphCsvReader#keyType(Class)
	*/
	public static GraphCsvReader fromCsvReader(String edgesPath, ExecutionEnvironment context) {
		return new GraphCsvReader(edgesPath, context);
	}

	/**
	 * Creates a graph from a CSV file of edges. Vertices will be created automatically and
	 * Vertex values can be initialized using a user-defined mapper.
	 *
	 * @param edgesPath a path to a CSV file with the Edge data
	 * @param vertexValueInitializer the mapper function that initializes the vertex values.
	 * It allows to apply a map transformation on the vertex ID to produce an initial vertex value.
	 * @param context the execution environment.
	 * @return An instance of {@link org.apache.flink.graph.GraphCsvReader},
	 * on which calling methods to specify types of the Vertex ID, Vertex Value and Edge value returns a Graph.
	 *
	 * @see org.apache.flink.graph.GraphCsvReader#types(Class, Class, Class)
	 * @see org.apache.flink.graph.GraphCsvReader#vertexTypes(Class, Class)
	 * @see org.apache.flink.graph.GraphCsvReader#edgeTypes(Class, Class)
	 * @see org.apache.flink.graph.GraphCsvReader#keyType(Class)
	 */
	public static <K, VV> GraphCsvReader fromCsvReader(String edgesPath,
			final MapFunction<K, VV> vertexValueInitializer, ExecutionEnvironment context) {
		return new GraphCsvReader(edgesPath, vertexValueInitializer, context);
	}

	/**
	 * @return the flink execution environment.
	 */
	public ExecutionEnvironment getContext() {
		return this.context;
	}

	/**
	 * Function that checks whether a Graph is a valid Graph,
	 * as defined by the given {@link GraphValidator}.
	 *
	 * @return true if the Graph is valid.
	 */
	public Boolean validate(GraphValidator<K, VV, EV> validator) throws Exception {
		return validator.validate(this);
	}

	/**
	 * @return the vertex DataSet.
	 */
	public DataSet<Vertex<K, VV>> getVertices() {
		return vertices;
	}

	/**
	 * @return the edge DataSet.
	 */
	public DataSet<Edge<K, EV>> getEdges() {
		return edges;
	}

	/**
	 * @return the vertex DataSet as Tuple2.
	 */
	public DataSet<Tuple2<K, VV>> getVerticesAsTuple2() {
		return vertices.map(new VertexToTuple2Map<>());
	}

	/**
	 * @return the edge DataSet as Tuple3.
	 */
	public DataSet<Tuple3<K, K, EV>> getEdgesAsTuple3() {
		return edges.map(new EdgeToTuple3Map<>());
	}

	/**
	 * This method allows access to the graph's edge values along with its source and target vertex values.
	 *
	 * @return a triplet DataSet consisting of (srcVertexId, trgVertexId, srcVertexValue, trgVertexValue, edgeValue)
	 */
	public DataSet<Triplet<K, VV, EV>> getTriplets() {
		return this.getVertices()
			.join(this.getEdges()).where(0).equalTo(0)
			.with(new ProjectEdgeWithSrcValue<>())
				.name("Project edge with source value")
			.join(this.getVertices()).where(1).equalTo(0)
			.with(new ProjectEdgeWithVertexValues<>())
				.name("Project edge with vertex values");
	}

	@ForwardedFieldsFirst("f1->f2")
	@ForwardedFieldsSecond("f0; f1; f2->f3")
	private static final class ProjectEdgeWithSrcValue<K, VV, EV> implements
			FlatJoinFunction<Vertex<K, VV>, Edge<K, EV>, Tuple4<K, K, VV, EV>> {

		@Override
		public void join(Vertex<K, VV> vertex, Edge<K, EV> edge, Collector<Tuple4<K, K, VV, EV>> collector)
				throws Exception {

			collector.collect(new Tuple4<>(edge.getSource(), edge.getTarget(), vertex.getValue(),
					edge.getValue()));
		}
	}

	@ForwardedFieldsFirst("f0; f1; f2; f3->f4")
	@ForwardedFieldsSecond("f1->f3")
	private static final class ProjectEdgeWithVertexValues<K, VV, EV> implements
			FlatJoinFunction<Tuple4<K, K, VV, EV>, Vertex<K, VV>, Triplet<K, VV, EV>> {

		@Override
		public void join(Tuple4<K, K, VV, EV> tripletWithSrcValSet,
						Vertex<K, VV> vertex, Collector<Triplet<K, VV, EV>> collector) throws Exception {

			collector.collect(new Triplet<>(tripletWithSrcValSet.f0, tripletWithSrcValSet.f1,
					tripletWithSrcValSet.f2, vertex.getValue(), tripletWithSrcValSet.f3));
		}
	}

	/**
	 * Apply a function to the attribute of each vertex in the graph.
	 *
	 * @param mapper the map function to apply.
	 * @return a new graph
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public <NV> Graph<K, NV, EV> mapVertices(final MapFunction<Vertex<K, VV>, NV> mapper) {

		TypeInformation<K> keyType = ((TupleTypeInfo<?>) vertices.getType()).getTypeAt(0);

		TypeInformation<NV> valueType;

		if (mapper instanceof ResultTypeQueryable) {
			valueType = ((ResultTypeQueryable) mapper).getProducedType();
		} else {
			valueType = TypeExtractor.createTypeInfo(MapFunction.class, mapper.getClass(), 1, vertices.getType(), null);
		}

		TypeInformation<Vertex<K, NV>> returnType = (TypeInformation<Vertex<K, NV>>) new TupleTypeInfo(
				Vertex.class, keyType, valueType);

		return mapVertices(mapper, returnType);
	}

	/**
	 * Apply a function to the attribute of each vertex in the graph.
	 *
	 * @param mapper the map function to apply.
	 * @param returnType the explicit return type.
	 * @return a new graph
	 */
	public <NV> Graph<K, NV, EV> mapVertices(final MapFunction<Vertex<K, VV>, NV> mapper, TypeInformation<Vertex<K, NV>> returnType) {
		DataSet<Vertex<K, NV>> mappedVertices = vertices.map(
				new MapFunction<Vertex<K, VV>, Vertex<K, NV>>() {
					private Vertex<K, NV> output = new Vertex<>();

					public Vertex<K, NV> map(Vertex<K, VV> value) throws Exception {
						output.f0 = value.f0;
						output.f1 = mapper.map(value);
						return output;
					}
				})
				.returns(returnType)
				.withForwardedFields("f0")
					.name("Map vertices");

		return new Graph<>(mappedVertices, this.edges, this.context);
	}

	/**
	 * Apply a function to the attribute of each edge in the graph.
	 *
	 * @param mapper the map function to apply.
	 * @return a new graph
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public <NV> Graph<K, VV, NV> mapEdges(final MapFunction<Edge<K, EV>, NV> mapper) {

		TypeInformation<K> keyType = ((TupleTypeInfo<?>) edges.getType()).getTypeAt(0);

		TypeInformation<NV> valueType;

		if (mapper instanceof ResultTypeQueryable) {
			valueType = ((ResultTypeQueryable) mapper).getProducedType();
		} else {
			valueType = TypeExtractor.createTypeInfo(MapFunction.class, mapper.getClass(), 1, edges.getType(), null);
		}

		TypeInformation<Edge<K, NV>> returnType = (TypeInformation<Edge<K, NV>>) new TupleTypeInfo(
				Edge.class, keyType, keyType, valueType);

		return mapEdges(mapper, returnType);
	}

	/**
	 * Apply a function to the attribute of each edge in the graph.
	 *
	 * @param mapper the map function to apply.
	 * @param returnType the explicit return type.
	 * @return a new graph
	 */
	public <NV> Graph<K, VV, NV> mapEdges(final MapFunction<Edge<K, EV>, NV> mapper, TypeInformation<Edge<K, NV>> returnType) {
		DataSet<Edge<K, NV>> mappedEdges = edges.map(
			new MapFunction<Edge<K, EV>, Edge<K, NV>>() {
				private Edge<K, NV> output = new Edge<>();

				public Edge<K, NV> map(Edge<K, EV> value) throws Exception {
					output.f0 = value.f0;
					output.f1 = value.f1;
					output.f2 = mapper.map(value);
					return output;
				}
			})
			.returns(returnType)
			.withForwardedFields("f0; f1")
				.name("Map edges");

		return new Graph<>(this.vertices, mappedEdges, this.context);
	}

	/**
	 * Translate {@link Vertex} and {@link Edge} IDs using the given {@link MapFunction}.
	 *
	 * @param translator implements conversion from {@code K} to {@code NEW}
	 * @param <NEW> new ID type
	 * @return graph with translated vertex and edge IDs
	 * @throws Exception
	 */
	public <NEW> Graph<NEW, VV, EV> translateGraphIds(TranslateFunction<K, NEW> translator) throws Exception {
		return run(new TranslateGraphIds<>(translator));
	}

	/**
	 * Translate {@link Vertex} values using the given {@link MapFunction}.
	 *
	 * @param translator implements conversion from {@code VV} to {@code NEW}
	 * @param <NEW> new vertex value type
	 * @return graph with translated vertex values
	 * @throws Exception
	 */
	public <NEW> Graph<K, NEW, EV> translateVertexValues(TranslateFunction<VV, NEW> translator) throws Exception {
		return run(new TranslateVertexValues<>(translator));
	}

	/**
	 * Translate {@link Edge} values using the given {@link MapFunction}.
	 *
	 * @param translator implements conversion from {@code EV} to {@code NEW}
	 * @param <NEW> new edge value type
	 * @return graph with translated edge values
	 * @throws Exception
	 */
	public <NEW> Graph<K, VV, NEW> translateEdgeValues(TranslateFunction<EV, NEW> translator) throws Exception {
		return run(new TranslateEdgeValues<>(translator));
	}

	/**
	 * Joins the vertex DataSet of this graph with an input Tuple2 DataSet and applies
	 * a user-defined transformation on the values of the matched records.
	 * The vertex ID and the first field of the Tuple2 DataSet are used as the join keys.
	 *
	 * @param inputDataSet the Tuple2 DataSet to join with.
	 * The first field of the Tuple2 is used as the join key and the second field is passed
	 * as a parameter to the transformation function.
	 * @param vertexJoinFunction the transformation function to apply.
	 * The first parameter is the current vertex value and the second parameter is the value
	 * of the matched Tuple2 from the input DataSet.
	 * @return a new Graph, where the vertex values have been updated according to the
	 * result of the vertexJoinFunction.
	 *
	 * @param <T> the type of the second field of the input Tuple2 DataSet.
	*/
	public <T> Graph<K, VV, EV> joinWithVertices(DataSet<Tuple2<K, T>> inputDataSet,
			final VertexJoinFunction<VV, T> vertexJoinFunction) {

		DataSet<Vertex<K, VV>> resultedVertices = this.getVertices()
				.coGroup(inputDataSet).where(0).equalTo(0)
				.with(new ApplyCoGroupToVertexValues<>(vertexJoinFunction))
					.name("Join with vertices");
		return new Graph<>(resultedVertices, this.edges, this.context);
	}

	private static final class ApplyCoGroupToVertexValues<K, VV, T>
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

	/**
	 * Joins the edge DataSet with an input DataSet on the composite key of both
	 * source and target IDs and applies a user-defined transformation on the values
	 * of the matched records. The first two fields of the input DataSet are used as join keys.
	 *
	 * @param inputDataSet the DataSet to join with.
	 * The first two fields of the Tuple3 are used as the composite join key
	 * and the third field is passed as a parameter to the transformation function.
	 * @param edgeJoinFunction the transformation function to apply.
	 * The first parameter is the current edge value and the second parameter is the value
	 * of the matched Tuple3 from the input DataSet.
	 * @param <T> the type of the third field of the input Tuple3 DataSet.
	 * @return a new Graph, where the edge values have been updated according to the
	 * result of the edgeJoinFunction.
	*/
	public <T> Graph<K, VV, EV> joinWithEdges(DataSet<Tuple3<K, K, T>> inputDataSet,
			final EdgeJoinFunction<EV, T> edgeJoinFunction) {

		DataSet<Edge<K, EV>> resultedEdges = this.getEdges()
				.coGroup(inputDataSet).where(0, 1).equalTo(0, 1)
				.with(new ApplyCoGroupToEdgeValues<>(edgeJoinFunction))
					.name("Join with edges");
		return new Graph<>(this.vertices, resultedEdges, this.context);
	}

	private static final class ApplyCoGroupToEdgeValues<K, EV, T>
	implements CoGroupFunction<Edge<K, EV>, Tuple3<K, K, T>, Edge<K, EV>> {
		private Edge<K, EV> output = new Edge<>();

		private EdgeJoinFunction<EV, T> edgeJoinFunction;

		public ApplyCoGroupToEdgeValues(EdgeJoinFunction<EV, T> mapper) {
			this.edgeJoinFunction = mapper;
		}

		@Override
		public void coGroup(Iterable<Edge<K, EV>> edges, Iterable<Tuple3<K, K, T>> input,
				Collector<Edge<K, EV>> collector) throws Exception {

			final Iterator<Edge<K, EV>> edgesIterator = edges.iterator();
			final Iterator<Tuple3<K, K, T>> inputIterator = input.iterator();

			if (edgesIterator.hasNext()) {
				if (inputIterator.hasNext()) {
					final Tuple3<K, K, T> inputNext = inputIterator.next();

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
	 * Joins the edge DataSet with an input Tuple2 DataSet and applies a user-defined transformation
	 * on the values of the matched records.
	 * The source ID of the edges input and the first field of the input DataSet are used as join keys.
	 *
	 * @param inputDataSet the DataSet to join with.
	 * The first field of the Tuple2 is used as the join key
	 * and the second field is passed as a parameter to the transformation function.
	 * @param edgeJoinFunction the transformation function to apply.
	 * The first parameter is the current edge value and the second parameter is the value
	 * of the matched Tuple2 from the input DataSet.
	 * @param <T> the type of the second field of the input Tuple2 DataSet.
	 * @return a new Graph, where the edge values have been updated according to the
	 * result of the edgeJoinFunction.
	*/
	public <T> Graph<K, VV, EV> joinWithEdgesOnSource(DataSet<Tuple2<K, T>> inputDataSet,
			final EdgeJoinFunction<EV, T> edgeJoinFunction) {

		DataSet<Edge<K, EV>> resultedEdges = this.getEdges()
				.coGroup(inputDataSet).where(0).equalTo(0)
				.with(new ApplyCoGroupToEdgeValuesOnEitherSourceOrTarget<>(edgeJoinFunction))
					.name("Join with edges on source");

		return new Graph<>(this.vertices, resultedEdges, this.context);
	}

	private static final class ApplyCoGroupToEdgeValuesOnEitherSourceOrTarget<K, EV, T>
	implements CoGroupFunction<Edge<K, EV>, Tuple2<K, T>, Edge<K, EV>> {
		private Edge<K, EV> output = new Edge<>();

		private EdgeJoinFunction<EV, T> edgeJoinFunction;

		public ApplyCoGroupToEdgeValuesOnEitherSourceOrTarget(EdgeJoinFunction<EV, T> mapper) {
			this.edgeJoinFunction = mapper;
		}

		@Override
		public void coGroup(Iterable<Edge<K, EV>> edges, Iterable<Tuple2<K, T>> input,
				Collector<Edge<K, EV>> collector) throws Exception {

			final Iterator<Edge<K, EV>> edgesIterator = edges.iterator();
			final Iterator<Tuple2<K, T>> inputIterator = input.iterator();

			if (inputIterator.hasNext()) {
				final Tuple2<K, T> inputNext = inputIterator.next();

				while (edgesIterator.hasNext()) {
					Edge<K, EV> edgesNext = edgesIterator.next();

					output.f0 = edgesNext.f0;
					output.f1 = edgesNext.f1;
					output.f2 = edgeJoinFunction.edgeJoin(edgesNext.f2, inputNext.f1);
					collector.collect(output);
				}

			} else {
				while (edgesIterator.hasNext()) {
					collector.collect(edgesIterator.next());
				}
			}
		}
	}

	/**
	 * Joins the edge DataSet with an input Tuple2 DataSet and applies a user-defined transformation
	 * on the values of the matched records.
	 * The target ID of the edges input and the first field of the input DataSet are used as join keys.
	 *
	 * @param inputDataSet the DataSet to join with.
	 * The first field of the Tuple2 is used as the join key
	 * and the second field is passed as a parameter to the transformation function.
	 * @param edgeJoinFunction the transformation function to apply.
	 * The first parameter is the current edge value and the second parameter is the value
	 * of the matched Tuple2 from the input DataSet.
	 * @param <T> the type of the second field of the input Tuple2 DataSet.
	 * @return a new Graph, where the edge values have been updated according to the
	 * result of the edgeJoinFunction.
	*/
	public <T> Graph<K, VV, EV> joinWithEdgesOnTarget(DataSet<Tuple2<K, T>> inputDataSet,
			final EdgeJoinFunction<EV, T> edgeJoinFunction) {

		DataSet<Edge<K, EV>> resultedEdges = this.getEdges()
				.coGroup(inputDataSet).where(1).equalTo(0)
				.with(new ApplyCoGroupToEdgeValuesOnEitherSourceOrTarget<>(edgeJoinFunction))
					.name("Join with edges on target");

		return new Graph<>(this.vertices, resultedEdges, this.context);
	}

	/**
	 * Apply filtering functions to the graph and return a sub-graph that
	 * satisfies the predicates for both vertices and edges.
	 *
	 * @param vertexFilter the filter function for vertices.
	 * @param edgeFilter the filter function for edges.
	 * @return the resulting sub-graph.
	 */
	public Graph<K, VV, EV> subgraph(FilterFunction<Vertex<K, VV>> vertexFilter, FilterFunction<Edge<K, EV>> edgeFilter) {

		DataSet<Vertex<K, VV>> filteredVertices = this.vertices.filter(vertexFilter);

		DataSet<Edge<K, EV>> remainingEdges = this.edges.join(filteredVertices)
				.where(0).equalTo(0).with(new ProjectEdge<>())
				.join(filteredVertices).where(1).equalTo(0)
				.with(new ProjectEdge<>()).name("Subgraph");

		DataSet<Edge<K, EV>> filteredEdges = remainingEdges.filter(edgeFilter);

		return new Graph<>(filteredVertices, filteredEdges, this.context);
	}

	/**
	 * Apply a filtering function to the graph and return a sub-graph that
	 * satisfies the predicates only for the vertices.
	 *
	 * @param vertexFilter the filter function for vertices.
	 * @return the resulting sub-graph.
	 */
	public Graph<K, VV, EV> filterOnVertices(FilterFunction<Vertex<K, VV>> vertexFilter) {

		DataSet<Vertex<K, VV>> filteredVertices = this.vertices.filter(vertexFilter);

		DataSet<Edge<K, EV>> remainingEdges = this.edges.join(filteredVertices)
				.where(0).equalTo(0).with(new ProjectEdge<>())
				.join(filteredVertices).where(1).equalTo(0)
				.with(new ProjectEdge<>()).name("Filter on vertices");

		return new Graph<>(filteredVertices, remainingEdges, this.context);
	}

	/**
	 * Apply a filtering function to the graph and return a sub-graph that
	 * satisfies the predicates only for the edges.
	 *
	 * @param edgeFilter the filter function for edges.
	 * @return the resulting sub-graph.
	 */
	public Graph<K, VV, EV> filterOnEdges(FilterFunction<Edge<K, EV>> edgeFilter) {
		DataSet<Edge<K, EV>> filteredEdges = this.edges.filter(edgeFilter).name("Filter on edges");

		return new Graph<>(this.vertices, filteredEdges, this.context);
	}

	@ForwardedFieldsFirst("f0; f1; f2")
	private static final class ProjectEdge<K, VV, EV> implements FlatJoinFunction<
		Edge<K, EV>, Vertex<K, VV>, Edge<K, EV>> {
		public void join(Edge<K, EV> first, Vertex<K, VV> second, Collector<Edge<K, EV>> out) {
			out.collect(first);
		}
	}

	/**
	 * Return the out-degree of all vertices in the graph.
	 *
	 * @return A DataSet of {@code Tuple2<vertexId, outDegree>}
	 */
	public DataSet<Tuple2<K, LongValue>> outDegrees() {

		return vertices.coGroup(edges).where(0).equalTo(0).with(new CountNeighborsCoGroup<>())
			.name("Out-degree");
	}

	private static final class CountNeighborsCoGroup<K, VV, EV>
			implements CoGroupFunction<Vertex<K, VV>, Edge<K, EV>, Tuple2<K, LongValue>> {
		private LongValue degree = new LongValue();

		private Tuple2<K, LongValue> vertexDegree = new Tuple2<>(null, degree);

		@SuppressWarnings("unused")
		public void coGroup(Iterable<Vertex<K, VV>> vertex, Iterable<Edge<K, EV>> outEdges,
				Collector<Tuple2<K, LongValue>> out) {
			long count = 0;
			for (Edge<K, EV> edge : outEdges) {
				count++;
			}
			degree.setValue(count);

			Iterator<Vertex<K, VV>> vertexIterator = vertex.iterator();

			if (vertexIterator.hasNext()) {
				vertexDegree.f0 = vertexIterator.next().f0;
				out.collect(vertexDegree);
			} else {
				throw new NoSuchElementException("The edge src/trg id could not be found within the vertexIds");
			}
		}
	}

	/**
	 * Return the in-degree of all vertices in the graph.
	 *
	 * @return A DataSet of {@code Tuple2<vertexId, inDegree>}
	 */
	public DataSet<Tuple2<K, LongValue>> inDegrees() {

		return vertices.coGroup(edges).where(0).equalTo(1).with(new CountNeighborsCoGroup<>())
			.name("In-degree");
	}

	/**
	 * Return the degree of all vertices in the graph.
	 *
	 * @return A DataSet of {@code Tuple2<vertexId, degree>}
	 */
	public DataSet<Tuple2<K, LongValue>> getDegrees() {
		return outDegrees()
			.union(inDegrees()).name("In- and out-degree")
			.groupBy(0).sum(1).name("Sum");
	}

	/**
	 * This operation adds all inverse-direction edges to the graph.
	 *
	 * @return the undirected graph.
	 */
	public Graph<K, VV, EV> getUndirected() {

		DataSet<Edge<K, EV>> undirectedEdges = edges.
			flatMap(new RegularAndReversedEdgesMap<>()).name("To undirected graph");
		return new Graph<>(vertices, undirectedEdges, this.context);
	}

	/**
	 * Groups by vertex and computes a GroupReduce transformation over the edge values of each vertex.
	 * The edgesFunction applied on the edges has access to both the id and the value
	 * of the grouping vertex.
	 *
	 * <p>For each vertex, the edgesFunction can iterate over all edges of this vertex
	 * with the specified direction, and emit any number of output elements, including none.
	 *
	 * @param edgesFunction the group reduce function to apply to the neighboring edges of each vertex.
	 * @param direction the edge direction (in-, out-, all-).
	 * @param <T> the output type
	 * @return a DataSet containing elements of type T
	 * @throws IllegalArgumentException
	*/
	public <T> DataSet<T> groupReduceOnEdges(EdgesFunctionWithVertexValue<K, VV, EV, T> edgesFunction,
			EdgeDirection direction) throws IllegalArgumentException {

		switch (direction) {
		case IN:
			return vertices.coGroup(edges).where(0).equalTo(1)
					.with(new ApplyCoGroupFunction<>(edgesFunction)).name("GroupReduce on in-edges");
		case OUT:
			return vertices.coGroup(edges).where(0).equalTo(0)
					.with(new ApplyCoGroupFunction<>(edgesFunction)).name("GroupReduce on out-edges");
		case ALL:
			return vertices.coGroup(edges.flatMap(new EmitOneEdgePerNode<>())
						.name("Emit edge"))
					.where(0).equalTo(0).with(new ApplyCoGroupFunctionOnAllEdges<>(edgesFunction))
						.name("GroupReduce on in- and out-edges");
		default:
			throw new IllegalArgumentException("Illegal edge direction");
		}
	}

	/**
	 * Groups by vertex and computes a GroupReduce transformation over the edge values of each vertex.
	 * The edgesFunction applied on the edges has access to both the id and the value
	 * of the grouping vertex.
	 *
	 * <p>For each vertex, the edgesFunction can iterate over all edges of this vertex
	 * with the specified direction, and emit any number of output elements, including none.
	 *
	 * @param edgesFunction the group reduce function to apply to the neighboring edges of each vertex.
	 * @param direction the edge direction (in-, out-, all-).
	 * @param <T> the output type
	 * @param typeInfo the explicit return type.
	 * @return a DataSet containing elements of type T
	 * @throws IllegalArgumentException
	*/
	public <T> DataSet<T> groupReduceOnEdges(EdgesFunctionWithVertexValue<K, VV, EV, T> edgesFunction,
			EdgeDirection direction, TypeInformation<T> typeInfo) throws IllegalArgumentException {

		switch (direction) {
			case IN:
				return vertices.coGroup(edges).where(0).equalTo(1)
						.with(new ApplyCoGroupFunction<>(edgesFunction))
							.name("GroupReduce on in-edges").returns(typeInfo);
			case OUT:
				return vertices.coGroup(edges).where(0).equalTo(0)
						.with(new ApplyCoGroupFunction<>(edgesFunction))
							.name("GroupReduce on out-edges").returns(typeInfo);
			case ALL:
				return vertices.coGroup(edges.flatMap(new EmitOneEdgePerNode<>())
							.name("Emit edge"))
						.where(0).equalTo(0).with(new ApplyCoGroupFunctionOnAllEdges<>(edgesFunction))
							.name("GroupReduce on in- and out-edges").returns(typeInfo);
			default:
				throw new IllegalArgumentException("Illegal edge direction");
		}
	}

	/**
	 * Groups by vertex and computes a GroupReduce transformation over the edge values of each vertex.
	 * The edgesFunction applied on the edges only has access to the vertex id (not the vertex value)
	 * of the grouping vertex.
	 *
	 * <p>For each vertex, the edgesFunction can iterate over all edges of this vertex
	 * with the specified direction, and emit any number of output elements, including none.
	 *
	 * @param edgesFunction the group reduce function to apply to the neighboring edges of each vertex.
	 * @param direction the edge direction (in-, out-, all-).
	 * @param <T> the output type
	 * @return a DataSet containing elements of type T
	 * @throws IllegalArgumentException
	*/
	public <T> DataSet<T> groupReduceOnEdges(EdgesFunction<K, EV, T> edgesFunction,
			EdgeDirection direction) throws IllegalArgumentException {

		TypeInformation<K> keyType = ((TupleTypeInfo<?>) vertices.getType()).getTypeAt(0);
		TypeInformation<EV> edgeValueType = ((TupleTypeInfo<?>) edges.getType()).getTypeAt(2);
		TypeInformation<T> returnType = TypeExtractor.createTypeInfo(EdgesFunction.class, edgesFunction.getClass(), 2,
			keyType, edgeValueType);

		return groupReduceOnEdges(edgesFunction, direction, returnType);
	}

	/**
	 * Groups by vertex and computes a GroupReduce transformation over the edge values of each vertex.
	 * The edgesFunction applied on the edges only has access to the vertex id (not the vertex value)
	 * of the grouping vertex.
	 *
	 * <p>For each vertex, the edgesFunction can iterate over all edges of this vertex
	 * with the specified direction, and emit any number of output elements, including none.
	 *
	 * @param edgesFunction the group reduce function to apply to the neighboring edges of each vertex.
	 * @param direction the edge direction (in-, out-, all-).
	 * @param <T> the output type
	 * @param typeInfo the explicit return type.
	 * @return a DataSet containing elements of type T
	 * @throws IllegalArgumentException
	*/
	public <T> DataSet<T> groupReduceOnEdges(EdgesFunction<K, EV, T> edgesFunction,
			EdgeDirection direction, TypeInformation<T> typeInfo) throws IllegalArgumentException {

		switch (direction) {
			case IN:
				return edges.map(new ProjectVertexIdMap<>(1)).name("Vertex ID")
						.withForwardedFields("f1->f0")
						.groupBy(0).reduceGroup(new ApplyGroupReduceFunction<>(edgesFunction))
							.name("GroupReduce on in-edges").returns(typeInfo);
			case OUT:
				return edges.map(new ProjectVertexIdMap<>(0)).name("Vertex ID")
						.withForwardedFields("f0")
						.groupBy(0).reduceGroup(new ApplyGroupReduceFunction<>(edgesFunction))
							.name("GroupReduce on out-edges").returns(typeInfo);
			case ALL:
				return edges.flatMap(new EmitOneEdgePerNode<>()).name("Emit edge")
						.groupBy(0).reduceGroup(new ApplyGroupReduceFunction<>(edgesFunction))
							.name("GroupReduce on in- and out-edges").returns(typeInfo);
			default:
				throw new IllegalArgumentException("Illegal edge direction");
		}
	}

	private static final class ProjectVertexIdMap<K, EV> implements MapFunction<
		Edge<K, EV>, Tuple2<K, Edge<K, EV>>> {

		private int fieldPosition;

		public ProjectVertexIdMap(int position) {
			this.fieldPosition = position;
		}

		@SuppressWarnings("unchecked")
		public Tuple2<K, Edge<K, EV>> map(Edge<K, EV> edge) {
			return new Tuple2<>((K) edge.getField(fieldPosition), edge);
		}
	}

	private static final class ProjectVertexWithEdgeValueMap<K, EV> implements MapFunction<
		Edge<K, EV>, Tuple2<K, EV>> {

		private int fieldPosition;

		public ProjectVertexWithEdgeValueMap(int position) {
			this.fieldPosition = position;
		}

		@SuppressWarnings("unchecked")
		public Tuple2<K, EV> map(Edge<K, EV> edge) {
			return new Tuple2<>((K) edge.getField(fieldPosition), edge.getValue());
		}
	}

	private static final class ApplyGroupReduceFunction<K, EV, T> implements GroupReduceFunction<Tuple2<K, Edge<K, EV>>, T> {

		private EdgesFunction<K, EV, T> function;

		public ApplyGroupReduceFunction(EdgesFunction<K, EV, T> fun) {
			this.function = fun;
		}

		public void reduce(Iterable<Tuple2<K, Edge<K, EV>>> edges, Collector<T> out) throws Exception {
			function.iterateEdges(edges, out);
		}
	}

	private static final class EmitOneEdgePerNode<K, EV> implements FlatMapFunction<
		Edge<K, EV>, Tuple2<K, Edge<K, EV>>> {

		public void flatMap(Edge<K, EV> edge, Collector<Tuple2<K, Edge<K, EV>>> out) {
			out.collect(new Tuple2<>(edge.getSource(), edge));
			out.collect(new Tuple2<>(edge.getTarget(), edge));
		}
	}

	private static final class EmitOneVertexWithEdgeValuePerNode<K, EV>	implements FlatMapFunction<
		Edge<K, EV>, Tuple2<K, EV>> {

		public void flatMap(Edge<K, EV> edge, Collector<Tuple2<K, EV>> out) {
			out.collect(new Tuple2<>(edge.getSource(), edge.getValue()));
			out.collect(new Tuple2<>(edge.getTarget(), edge.getValue()));
		}
	}

	private static final class EmitOneEdgeWithNeighborPerNode<K, EV> implements FlatMapFunction<
		Edge<K, EV>, Tuple3<K, K, Edge<K, EV>>> {

		public void flatMap(Edge<K, EV> edge, Collector<Tuple3<K, K, Edge<K, EV>>> out) {
			out.collect(new Tuple3<>(edge.getSource(), edge.getTarget(), edge));
			out.collect(new Tuple3<>(edge.getTarget(), edge.getSource(), edge));
		}
	}

	private static final class ApplyCoGroupFunction<K, VV, EV, T> implements CoGroupFunction<
		Vertex<K, VV>, Edge<K, EV>, T>, ResultTypeQueryable<T> {

		private EdgesFunctionWithVertexValue<K, VV, EV, T> function;

		public ApplyCoGroupFunction(EdgesFunctionWithVertexValue<K, VV, EV, T> fun) {
			this.function = fun;
		}

		public void coGroup(Iterable<Vertex<K, VV>> vertex,
				Iterable<Edge<K, EV>> edges, Collector<T> out) throws Exception {

			Iterator<Vertex<K, VV>> vertexIterator = vertex.iterator();

			if (vertexIterator.hasNext()) {
				function.iterateEdges(vertexIterator.next(), edges, out);
			} else {
				throw new NoSuchElementException("The edge src/trg id could not be found within the vertexIds");
			}
		}

		@Override
		public TypeInformation<T> getProducedType() {
			return TypeExtractor.createTypeInfo(EdgesFunctionWithVertexValue.class, function.getClass(), 3,
					null, null);
		}
	}

	private static final class ApplyCoGroupFunctionOnAllEdges<K, VV, EV, T>
			implements	CoGroupFunction<Vertex<K, VV>, Tuple2<K, Edge<K, EV>>, T>, ResultTypeQueryable<T> {

		private EdgesFunctionWithVertexValue<K, VV, EV, T> function;

		public ApplyCoGroupFunctionOnAllEdges(EdgesFunctionWithVertexValue<K, VV, EV, T> fun) {
			this.function = fun;
		}

		public void coGroup(Iterable<Vertex<K, VV>> vertex, final Iterable<Tuple2<K, Edge<K, EV>>> keysWithEdges,
				Collector<T> out) throws Exception {

			final Iterator<Edge<K, EV>> edgesIterator = new Iterator<Edge<K, EV>>() {

				final Iterator<Tuple2<K, Edge<K, EV>>> keysWithEdgesIterator = keysWithEdges.iterator();

				@Override
				public boolean hasNext() {
					return keysWithEdgesIterator.hasNext();
				}

				@Override
				public Edge<K, EV> next() {
					return keysWithEdgesIterator.next().f1;
				}

				@Override
				public void remove() {
					keysWithEdgesIterator.remove();
				}
			};

			Iterable<Edge<K, EV>> edgesIterable = new Iterable<Edge<K, EV>>() {
				public Iterator<Edge<K, EV>> iterator() {
					return edgesIterator;
				}
			};

			Iterator<Vertex<K, VV>> vertexIterator = vertex.iterator();

			if (vertexIterator.hasNext()) {
				function.iterateEdges(vertexIterator.next(), edgesIterable, out);
			} else {
				throw new NoSuchElementException("The edge src/trg id could not be found within the vertexIds");
			}
		}

		@Override
		public TypeInformation<T> getProducedType() {
			return TypeExtractor.createTypeInfo(EdgesFunctionWithVertexValue.class, function.getClass(), 3,
					null, null);
		}
	}

	@ForwardedFields("f0->f1; f1->f0; f2")
	private static final class ReverseEdgesMap<K, EV>
	implements MapFunction<Edge<K, EV>, Edge<K, EV>> {
		public Edge<K, EV> output = new Edge<>();

		public Edge<K, EV> map(Edge<K, EV> edge) {
			output.setFields(edge.f1, edge.f0, edge.f2);
			return output;
		}
	}

	private static final class RegularAndReversedEdgesMap<K, EV>
	implements FlatMapFunction<Edge<K, EV>, Edge<K, EV>> {
		public Edge<K, EV> output = new Edge<>();

		@Override
		public void flatMap(Edge<K, EV> edge, Collector<Edge<K, EV>> out) throws Exception {
			out.collect(edge);

			output.setFields(edge.f1, edge.f0, edge.f2);
			out.collect(output);
		}
	}

	/**
	 * Reverse the direction of the edges in the graph.
	 *
	 * @return a new graph with all edges reversed
	 * @throws UnsupportedOperationException
	 */
	public Graph<K, VV, EV> reverse() throws UnsupportedOperationException {
		DataSet<Edge<K, EV>> reversedEdges = edges.map(new ReverseEdgesMap<>()).name("Reverse edges");
		return new Graph<>(vertices, reversedEdges, this.context);
	}

	/**
	 * @return a long integer representing the number of vertices
	 */
	public long numberOfVertices() throws Exception {
		return vertices.count();
	}

	/**
	 * @return a long integer representing the number of edges
	 */
	public long numberOfEdges() throws Exception {
		return edges.count();
	}

	/**
	 * @return The IDs of the vertices as DataSet
	 */
	public DataSet<K> getVertexIds() {
		return vertices.map(new ExtractVertexIDMapper<>()).name("Vertex IDs");
	}

	private static final class ExtractVertexIDMapper<K, VV>
			implements MapFunction<Vertex<K, VV>, K> {
		@Override
		public K map(Vertex<K, VV> vertex) {
			return vertex.f0;
		}
	}

	/**
	 * @return The IDs of the edges as DataSet
	 */
	public DataSet<Tuple2<K, K>> getEdgeIds() {
		return edges.map(new ExtractEdgeIDsMapper<>()).name("Edge IDs");
	}

	@ForwardedFields("f0; f1")
	private static final class ExtractEdgeIDsMapper<K, EV>
			implements MapFunction<Edge<K, EV>, Tuple2<K, K>> {
		@Override
		public Tuple2<K, K> map(Edge<K, EV> edge) throws Exception {
			return new Tuple2<>(edge.f0, edge.f1);
		}
	}

	/**
	 * Adds the input vertex to the graph. If the vertex already
	 * exists in the graph, it will not be added again.
	 *
	 * @param vertex the vertex to be added
	 * @return the new graph containing the existing vertices as well as the one just added
	 */
	public Graph<K, VV, EV> addVertex(final Vertex<K, VV> vertex) {
		List<Vertex<K, VV>> newVertex = new ArrayList<>();
		newVertex.add(vertex);

		return addVertices(newVertex);
	}

	/**
	 * Adds the list of vertices, passed as input, to the graph.
	 * If the vertices already exist in the graph, they will not be added once more.
	 *
	 * @param verticesToAdd the list of vertices to add
	 * @return the new graph containing the existing and newly added vertices
	 */
	public Graph<K, VV, EV> addVertices(List<Vertex<K, VV>> verticesToAdd) {
		// Add the vertices
		DataSet<Vertex<K, VV>> newVertices = this.vertices.coGroup(this.context.fromCollection(verticesToAdd))
				.where(0).equalTo(0).with(new VerticesUnionCoGroup<>()).name("Add vertices");

		return new Graph<>(newVertices, this.edges, this.context);
	}

	private static final class VerticesUnionCoGroup<K, VV> implements CoGroupFunction<Vertex<K, VV>, Vertex<K, VV>, Vertex<K, VV>> {

		@Override
		public void coGroup(Iterable<Vertex<K, VV>> oldVertices, Iterable<Vertex<K, VV>> newVertices,
							Collector<Vertex<K, VV>> out) throws Exception {

			final Iterator<Vertex<K, VV>> oldVerticesIterator = oldVertices.iterator();
			final Iterator<Vertex<K, VV>> newVerticesIterator = newVertices.iterator();

			// if there is both an old vertex and a new vertex then only the old vertex is emitted
			if (oldVerticesIterator.hasNext()) {
				out.collect(oldVerticesIterator.next());
			} else {
				out.collect(newVerticesIterator.next());
			}
		}
	}

	/**
	 * Adds the given edge to the graph. If the source and target vertices do
	 * not exist in the graph, they will also be added.
	 *
	 * @param source the source vertex of the edge
	 * @param target the target vertex of the edge
	 * @param edgeValue the edge value
	 * @return the new graph containing the existing vertices and edges plus the
	 *         newly added edge
	 */
	public Graph<K, VV, EV> addEdge(Vertex<K, VV> source, Vertex<K, VV> target, EV edgeValue) {
		Graph<K, VV, EV> partialGraph = fromCollection(Arrays.asList(source, target),
				Collections.singletonList(new Edge<>(source.f0, target.f0, edgeValue)),
				this.context);
		return this.union(partialGraph);
	}

	/**
	 * Adds the given list edges to the graph.
	 *
	 * <p>When adding an edge for a non-existing set of vertices, the edge is considered invalid and ignored.
	 *
	 * @param newEdges the data set of edges to be added
	 * @return a new graph containing the existing edges plus the newly added edges.
	 */
	public Graph<K, VV, EV> addEdges(List<Edge<K, EV>> newEdges) {

		DataSet<Edge<K, EV>> newEdgesDataSet = this.context.fromCollection(newEdges);

		DataSet<Edge<K, EV>> validNewEdges = this.getVertices().join(newEdgesDataSet)
				.where(0).equalTo(0)
				.with(new JoinVerticesWithEdgesOnSrc<>()).name("Join with source")
				.join(this.getVertices()).where(1).equalTo(0)
				.with(new JoinWithVerticesOnTrg<>()).name("Join with target");

		return Graph.fromDataSet(this.vertices, this.edges.union(validNewEdges), this.context);
	}

	@ForwardedFieldsSecond("f0; f1; f2")
	private static final class JoinVerticesWithEdgesOnSrc<K, VV, EV> implements
			JoinFunction<Vertex<K, VV>, Edge<K, EV>, Edge<K, EV>> {

		@Override
		public Edge<K, EV> join(Vertex<K, VV> vertex, Edge<K, EV> edge) throws Exception {
			return edge;
		}
	}

	@ForwardedFieldsFirst("f0; f1; f2")
	private static final class JoinWithVerticesOnTrg<K, VV, EV> implements
			JoinFunction<Edge<K, EV>, Vertex<K, VV>, Edge<K, EV>> {

		@Override
		public Edge<K, EV> join(Edge<K, EV> edge, Vertex<K, VV> vertex) throws Exception {
			return edge;
		}
	}

	/**
	 * Removes the given vertex and its edges from the graph.
	 *
	 * @param vertex the vertex to remove
	 * @return the new graph containing the existing vertices and edges without
	 *         the removed vertex and its edges
	 */
	public Graph<K, VV, EV> removeVertex(Vertex<K, VV> vertex) {

		List<Vertex<K, VV>> vertexToBeRemoved = new ArrayList<>();
		vertexToBeRemoved.add(vertex);

		return removeVertices(vertexToBeRemoved);
	}
	/**
	 * Removes the given list of vertices and its edges from the graph.
	 *
	 * @param verticesToBeRemoved the list of vertices to be removed
	 * @return the resulted graph containing the initial vertices and edges minus the vertices
	 * 		   and edges removed.
	 */

	public Graph<K, VV, EV> removeVertices(List<Vertex<K, VV>> verticesToBeRemoved) {
		return removeVertices(this.context.fromCollection(verticesToBeRemoved));
	}

	/**
	 * Removes the given list of vertices and its edges from the graph.
	 *
	 * @param verticesToBeRemoved the DataSet of vertices to be removed
	 * @return the resulted graph containing the initial vertices and edges minus the vertices
	 * 		   and edges removed.
	 */
	private Graph<K, VV, EV> removeVertices(DataSet<Vertex<K, VV>> verticesToBeRemoved) {

		DataSet<Vertex<K, VV>> newVertices = getVertices().coGroup(verticesToBeRemoved).where(0).equalTo(0)
				.with(new VerticesRemovalCoGroup<>()).name("Remove vertices");

		DataSet <Edge< K, EV>> newEdges = newVertices.join(getEdges()).where(0).equalTo(0)
				// if the edge source was removed, the edge will also be removed
				.with(new ProjectEdgeToBeRemoved<>()).name("Edges to be removed")
				// if the edge target was removed, the edge will also be removed
				.join(newVertices).where(1).equalTo(0)
				.with(new ProjectEdge<>()).name("Remove edges");

		return new Graph<>(newVertices, newEdges, context);
	}

	private static final class VerticesRemovalCoGroup<K, VV> implements CoGroupFunction<Vertex<K, VV>, Vertex<K, VV>, Vertex<K, VV>> {

		@Override
		public void coGroup(Iterable<Vertex<K, VV>> vertex, Iterable<Vertex<K, VV>> vertexToBeRemoved,
							Collector<Vertex<K, VV>> out) throws Exception {

			final Iterator<Vertex<K, VV>> vertexIterator = vertex.iterator();
			final Iterator<Vertex<K, VV>> vertexToBeRemovedIterator = vertexToBeRemoved.iterator();
			Vertex<K, VV> next;

			if (vertexIterator.hasNext()) {
				if (!vertexToBeRemovedIterator.hasNext()) {
					next = vertexIterator.next();
					out.collect(next);
				}
			}
		}
	}

	@ForwardedFieldsSecond("f0; f1; f2")
	private static final class ProjectEdgeToBeRemoved<K, VV, EV> implements JoinFunction<Vertex<K, VV>, Edge<K, EV>, Edge<K, EV>> {
		@Override
		public Edge<K, EV> join(Vertex<K, VV> vertex, Edge<K, EV> edge) throws Exception {
			return edge;
		}
	}

	 /**
	 * Removes all edges that match the given edge from the graph.
	 *
	 * @param edge the edge to remove
	 * @return the new graph containing the existing vertices and edges without
	 *         the removed edges
	 */
	public Graph<K, VV, EV> removeEdge(Edge<K, EV> edge) {
		DataSet<Edge<K, EV>> newEdges = getEdges().filter(new EdgeRemovalEdgeFilter<>(edge)).name("Remove edge");
		return new Graph<>(this.vertices, newEdges, this.context);
	}

	private static final class EdgeRemovalEdgeFilter<K, EV>
			implements FilterFunction<Edge<K, EV>> {
		private Edge<K, EV> edgeToRemove;

		public EdgeRemovalEdgeFilter(Edge<K, EV> edge) {
			edgeToRemove = edge;
		}

		@Override
		public boolean filter(Edge<K, EV> edge) {
			return (!(edge.f0.equals(edgeToRemove.f0) && edge.f1
					.equals(edgeToRemove.f1)));
		}
	}

	/**
	 * Removes all the edges that match the edges in the given data set from the graph.
	 *
	 * @param edgesToBeRemoved the list of edges to be removed
	 * @return a new graph where the edges have been removed and in which the vertices remained intact
	 */
	public Graph<K, VV, EV> removeEdges(List<Edge<K, EV>> edgesToBeRemoved) {

		DataSet<Edge<K, EV>> newEdges = getEdges().coGroup(this.context.fromCollection(edgesToBeRemoved))
				.where(0, 1).equalTo(0, 1).with(new EdgeRemovalCoGroup<>()).name("Remove edges");

		return new Graph<>(this.vertices, newEdges, context);
	}

	private static final class EdgeRemovalCoGroup<K, EV> implements CoGroupFunction<Edge<K, EV>, Edge<K, EV>, Edge<K, EV>> {

		@Override
		public void coGroup(Iterable<Edge<K, EV>> edge, Iterable<Edge<K, EV>> edgeToBeRemoved,
							Collector<Edge<K, EV>> out) throws Exception {
			if (!edgeToBeRemoved.iterator().hasNext()) {
				for (Edge<K, EV> next : edge) {
					out.collect(next);
				}
			}
		}
	}

	/**
	 * Performs union on the vertices and edges sets of the input graphs
	 * removing duplicate vertices but maintaining duplicate edges.
	 *
	 * @param graph the graph to perform union with
	 * @return a new graph
	 */
	public Graph<K, VV, EV> union(Graph<K, VV, EV> graph) {
		DataSet<Vertex<K, VV>> unionedVertices = graph
			.getVertices()
			.union(this.getVertices())
				.name("Vertices")
			.distinct()
				.name("Vertices");

		DataSet<Edge<K, EV>> unionedEdges = graph
			.getEdges()
			.union(this.getEdges())
				.name("Edges");

		return new Graph<>(unionedVertices, unionedEdges, this.context);
	}

	/**
	 * Performs Difference on the vertex and edge sets of the input graphs
	 * removes common vertices and edges. If a source/target vertex is removed,
	 * its corresponding edge will also be removed
	 *
	 * @param graph the graph to perform difference with
	 * @return a new graph where the common vertices and edges have been removed
	 */
	public Graph<K, VV, EV> difference(Graph<K, VV, EV> graph) {
		DataSet<Vertex<K, VV>> removeVerticesData = graph.getVertices();
		return this.removeVertices(removeVerticesData);
	}

	/**
	 * Performs intersect on the edge sets of the input graphs. Edges are considered equal, if they
	 * have the same source identifier, target identifier and edge value.
	 *
	 * <p>The method computes pairs of equal edges from the input graphs. If the same edge occurs
	 * multiple times in the input graphs, there will be multiple edge pairs to be considered. Each
	 * edge instance can only be part of one pair. If the given parameter {@code distinctEdges} is set
	 * to {@code true}, there will be exactly one edge in the output graph representing all pairs of
	 * equal edges. If the parameter is set to {@code false}, both edges of each pair will be in the
	 * output.
	 *
	 * <p>Vertices in the output graph will have no vertex values.
	 *
	 * @param graph the graph to perform intersect with
	 * @param distinctEdges if set to {@code true}, there will be exactly one edge in the output graph
	 *                      representing all pairs of equal edges, otherwise, for each pair, both
	 *                      edges will be in the output graph
	 * @return a new graph which contains only common vertices and edges from the input graphs
	 */
	public Graph<K, NullValue, EV> intersect(Graph<K, VV, EV> graph, boolean distinctEdges) {
		DataSet<Edge<K, EV>> intersectEdges;
		if (distinctEdges) {
			intersectEdges = getDistinctEdgeIntersection(graph.getEdges());
		} else {
			intersectEdges = getPairwiseEdgeIntersection(graph.getEdges());
		}

		return Graph.fromDataSet(intersectEdges, getContext());
	}

	/**
	 * Computes the intersection between the edge set and the given edge set. For all matching pairs,
	 * only one edge will be in the resulting data set.
	 *
	 * @param edges edges to compute intersection with
	 * @return edge set containing one edge for all matching pairs of the same edge
	 */
	private DataSet<Edge<K, EV>> getDistinctEdgeIntersection(DataSet<Edge<K, EV>> edges) {
		return this.getEdges()
				.join(edges)
				.where(0, 1, 2)
				.equalTo(0, 1, 2)
				.with(new JoinFunction<Edge<K, EV>, Edge<K, EV>, Edge<K, EV>>() {
					@Override
					public Edge<K, EV> join(Edge<K, EV> first, Edge<K, EV> second) throws Exception {
						return first;
					}
				}).withForwardedFieldsFirst("*").name("Intersect edges")
				.distinct()
					.name("Edges");
	}

	/**
	 * Computes the intersection between the edge set and the given edge set. For all matching pairs, both edges will be
	 * in the resulting data set.
	 *
	 * @param edges edges to compute intersection with
	 * @return edge set containing both edges from all matching pairs of the same edge
	 */
	private DataSet<Edge<K, EV>> getPairwiseEdgeIntersection(DataSet<Edge<K, EV>> edges) {
		return this.getEdges()
				.coGroup(edges)
				.where(0, 1, 2)
				.equalTo(0, 1, 2)
				.with(new MatchingEdgeReducer<>())
					.name("Intersect edges");
	}

	/**
	 * As long as both input iterables have more edges, the reducer outputs each edge of a pair.
	 *
	 * @param <K> 	vertex identifier type
	 * @param <EV> 	edge value type
	 */
	private static final class MatchingEdgeReducer<K, EV>
			implements CoGroupFunction<Edge<K, EV>, Edge<K, EV>, Edge<K, EV>> {

		@Override
		public void coGroup(Iterable<Edge<K, EV>> edgesLeft, Iterable<Edge<K, EV>> edgesRight, Collector<Edge<K, EV>> out)
				throws Exception {
			Iterator<Edge<K, EV>> leftIt = edgesLeft.iterator();
			Iterator<Edge<K, EV>> rightIt = edgesRight.iterator();

			// collect pairs once
			while (leftIt.hasNext() && rightIt.hasNext()) {
				out.collect(leftIt.next());
				out.collect(rightIt.next());
			}
		}
	}

	/**
	 * Runs a ScatterGather iteration on the graph.
	 * No configuration options are provided.
	 *
	 * @param scatterFunction the scatter function
	 * @param gatherFunction the gather function
	 * @param maximumNumberOfIterations maximum number of iterations to perform
	 *
	 * @return the updated Graph after the scatter-gather iteration has converged or
	 * after maximumNumberOfIterations.
	 */
	public <M> Graph<K, VV, EV> runScatterGatherIteration(
			ScatterFunction<K, VV, M, EV> scatterFunction,
			org.apache.flink.graph.spargel.GatherFunction<K, VV, M> gatherFunction,
			int maximumNumberOfIterations) {

		return this.runScatterGatherIteration(scatterFunction, gatherFunction,
				maximumNumberOfIterations, null);
	}

	/**
	 * Runs a ScatterGather iteration on the graph with configuration options.
	 *
	 * @param scatterFunction the scatter function
	 * @param gatherFunction the gather function
	 * @param maximumNumberOfIterations maximum number of iterations to perform
	 * @param parameters the iteration configuration parameters
	 *
	 * @return the updated Graph after the scatter-gather iteration has converged or
	 * after maximumNumberOfIterations.
	 */
	public <M> Graph<K, VV, EV> runScatterGatherIteration(
			ScatterFunction<K, VV, M, EV> scatterFunction,
			org.apache.flink.graph.spargel.GatherFunction<K, VV, M> gatherFunction,
			int maximumNumberOfIterations, ScatterGatherConfiguration parameters) {

		ScatterGatherIteration<K, VV, M, EV> iteration = ScatterGatherIteration.withEdges(
				edges, scatterFunction, gatherFunction, maximumNumberOfIterations);

		iteration.configure(parameters);

		DataSet<Vertex<K, VV>> newVertices = this.getVertices().runOperation(iteration);

		return new Graph<>(newVertices, this.edges, this.context);
	}

	/**
	 * Runs a Gather-Sum-Apply iteration on the graph.
	 * No configuration options are provided.
	 *
	 * @param gatherFunction the gather function collects information about adjacent vertices and edges
	 * @param sumFunction the sum function aggregates the gathered information
	 * @param applyFunction the apply function updates the vertex values with the aggregates
	 * @param maximumNumberOfIterations maximum number of iterations to perform
	 * @param <M> the intermediate type used between gather, sum and apply
	 *
	 * @return the updated Graph after the gather-sum-apply iteration has converged or
	 * after maximumNumberOfIterations.
	 */
	public <M> Graph<K, VV, EV> runGatherSumApplyIteration(
			org.apache.flink.graph.gsa.GatherFunction<VV, EV, M> gatherFunction, SumFunction<VV, EV, M> sumFunction,
			ApplyFunction<K, VV, M> applyFunction, int maximumNumberOfIterations) {

		return this.runGatherSumApplyIteration(gatherFunction, sumFunction, applyFunction,
				maximumNumberOfIterations, null);
	}

	/**
	 * Runs a Gather-Sum-Apply iteration on the graph with configuration options.
	 *
	 * @param gatherFunction the gather function collects information about adjacent vertices and edges
	 * @param sumFunction the sum function aggregates the gathered information
	 * @param applyFunction the apply function updates the vertex values with the aggregates
	 * @param maximumNumberOfIterations maximum number of iterations to perform
	 * @param parameters the iteration configuration parameters
	 * @param <M> the intermediate type used between gather, sum and apply
	 *
	 * @return the updated Graph after the gather-sum-apply iteration has converged or
	 * after maximumNumberOfIterations.
	 */
	public <M> Graph<K, VV, EV> runGatherSumApplyIteration(
			org.apache.flink.graph.gsa.GatherFunction<VV, EV, M> gatherFunction, SumFunction<VV, EV, M> sumFunction,
			ApplyFunction<K, VV, M> applyFunction, int maximumNumberOfIterations,
			GSAConfiguration parameters) {

		GatherSumApplyIteration<K, VV, EV, M> iteration = GatherSumApplyIteration.withEdges(
				edges, gatherFunction, sumFunction, applyFunction, maximumNumberOfIterations);

		iteration.configure(parameters);

		DataSet<Vertex<K, VV>> newVertices = vertices.runOperation(iteration);

		return new Graph<>(newVertices, this.edges, this.context);
	}

	/**
	 * Runs a {@link VertexCentricIteration} on the graph.
	 * No configuration options are provided.
	 *
	 * @param computeFunction the vertex compute function
	 * @param combiner an optional message combiner
	 * @param maximumNumberOfIterations maximum number of iterations to perform
	 *
	 * @return the updated Graph after the vertex-centric iteration has converged or
	 * after maximumNumberOfIterations.
	 */
	public <M> Graph<K, VV, EV> runVertexCentricIteration(
			ComputeFunction<K, VV, EV, M> computeFunction,
			MessageCombiner<K, M> combiner, int maximumNumberOfIterations) {

		return this.runVertexCentricIteration(computeFunction, combiner, maximumNumberOfIterations, null);
	}

	/**
	 * Runs a {@link VertexCentricIteration} on the graph with configuration options.
	 *
	 * @param computeFunction the vertex compute function
	 * @param combiner an optional message combiner
	 * @param maximumNumberOfIterations maximum number of iterations to perform
	 * @param parameters the {@link VertexCentricConfiguration} parameters
	 *
	 * @return the updated Graph after the vertex-centric iteration has converged or
	 * after maximumNumberOfIterations.
	 */
	public <M> Graph<K, VV, EV> runVertexCentricIteration(
			ComputeFunction<K, VV, EV, M> computeFunction,
			MessageCombiner<K, M> combiner, int maximumNumberOfIterations,
			VertexCentricConfiguration parameters) {

		VertexCentricIteration<K, VV, EV, M> iteration = VertexCentricIteration.withEdges(
				edges, computeFunction, combiner, maximumNumberOfIterations);
		iteration.configure(parameters);
		DataSet<Vertex<K, VV>> newVertices = this.getVertices().runOperation(iteration);
		return new Graph<>(newVertices, this.edges, this.context);
	}

	/**
	 * @param algorithm the algorithm to run on the Graph
	 * @param <T> the return type
	 * @return the result of the graph algorithm
	 * @throws Exception
	 */
	public <T> T run(GraphAlgorithm<K, VV, EV, T> algorithm) throws Exception {
		return algorithm.run(this);
	}

	/**
	 * A {@code GraphAnalytic} is similar to a {@link GraphAlgorithm} but is terminal
	 * and results are retrieved via accumulators.  A Flink program has a single
	 * point of execution. A {@code GraphAnalytic} defers execution to the user to
	 * allow composing multiple analytics and algorithms into a single program.
	 *
	 * @param analytic the analytic to run on the Graph
	 * @param <T> the result type
	 * @throws Exception
	 */
	public <T> GraphAnalytic<K, VV, EV, T> run(GraphAnalytic<K, VV, EV, T> analytic) throws Exception {
		analytic.run(this);
		return analytic;
	}

	/**
	 * Groups by vertex and computes a GroupReduce transformation over the neighbors (both edges and vertices)
	 * of each vertex. The neighborsFunction applied on the neighbors only has access to both the vertex id
	 * and the vertex value of the grouping vertex.
	 *
	 * <p>For each vertex, the neighborsFunction can iterate over all neighbors of this vertex
	 * with the specified direction, and emit any number of output elements, including none.
	 *
	 * @param neighborsFunction the group reduce function to apply to the neighboring edges and vertices
	 * of each vertex.
	 * @param direction the edge direction (in-, out-, all-).
	 * @param <T> the output type
	 * @return a DataSet containing elements of type T
	 * @throws IllegalArgumentException
	*/
	public <T> DataSet<T> groupReduceOnNeighbors(NeighborsFunctionWithVertexValue<K, VV, EV, T> neighborsFunction,
			EdgeDirection direction) throws IllegalArgumentException {
		switch (direction) {
		case IN:
			// create <edge-sourceVertex> pairs
			DataSet<Tuple2<Edge<K, EV>, Vertex<K, VV>>> edgesWithSources = edges
					.join(this.vertices).where(0).equalTo(0).name("Edge with source vertex");
			return vertices.coGroup(edgesWithSources)
					.where(0).equalTo("f0.f1")
					.with(new ApplyNeighborCoGroupFunction<>(neighborsFunction)).name("Neighbors function");
		case OUT:
			// create <edge-targetVertex> pairs
			DataSet<Tuple2<Edge<K, EV>, Vertex<K, VV>>> edgesWithTargets = edges
					.join(this.vertices).where(1).equalTo(0).name("Edge with target vertex");
			return vertices.coGroup(edgesWithTargets)
					.where(0).equalTo("f0.f0")
					.with(new ApplyNeighborCoGroupFunction<>(neighborsFunction)).name("Neighbors function");
		case ALL:
			// create <edge-sourceOrTargetVertex> pairs
			DataSet<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> edgesWithNeighbors = edges
					.flatMap(new EmitOneEdgeWithNeighborPerNode<>()).name("Forward and reverse edges")
					.join(this.vertices).where(1).equalTo(0)
					.with(new ProjectEdgeWithNeighbor<>()).name("Edge with vertex");

			return vertices.coGroup(edgesWithNeighbors)
					.where(0).equalTo(0)
					.with(new ApplyCoGroupFunctionOnAllNeighbors<>(neighborsFunction)).name("Neighbors function");
		default:
			throw new IllegalArgumentException("Illegal edge direction");
		}
	}

	/**
	 * Groups by vertex and computes a GroupReduce transformation over the neighbors (both edges and vertices)
	 * of each vertex. The neighborsFunction applied on the neighbors only has access to both the vertex id
	 * and the vertex value of the grouping vertex.
	 *
	 * <p>For each vertex, the neighborsFunction can iterate over all neighbors of this vertex
	 * with the specified direction, and emit any number of output elements, including none.
	 *
	 * @param neighborsFunction the group reduce function to apply to the neighboring edges and vertices
	 * of each vertex.
	 * @param direction the edge direction (in-, out-, all-).
	 * @param <T> the output type
	 * @param typeInfo the explicit return type
	 * @return a DataSet containing elements of type T
	 * @throws IllegalArgumentException
	*/
	public <T> DataSet<T> groupReduceOnNeighbors(NeighborsFunctionWithVertexValue<K, VV, EV, T> neighborsFunction,
			EdgeDirection direction, TypeInformation<T> typeInfo) throws IllegalArgumentException {
		switch (direction) {
			case IN:
				// create <edge-sourceVertex> pairs
				DataSet<Tuple2<Edge<K, EV>, Vertex<K, VV>>> edgesWithSources = edges
						.join(this.vertices).where(0).equalTo(0).name("Edge with source vertex");
				return vertices.coGroup(edgesWithSources)
						.where(0).equalTo("f0.f1")
						.with(new ApplyNeighborCoGroupFunction<>(neighborsFunction))
							.name("Neighbors function").returns(typeInfo);
			case OUT:
				// create <edge-targetVertex> pairs
				DataSet<Tuple2<Edge<K, EV>, Vertex<K, VV>>> edgesWithTargets = edges
						.join(this.vertices).where(1).equalTo(0).name("Edge with target vertex");
				return vertices.coGroup(edgesWithTargets)
						.where(0).equalTo("f0.f0")
						.with(new ApplyNeighborCoGroupFunction<>(neighborsFunction))
							.name("Neighbors function").returns(typeInfo);
			case ALL:
				// create <edge-sourceOrTargetVertex> pairs
				DataSet<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> edgesWithNeighbors = edges
						.flatMap(new EmitOneEdgeWithNeighborPerNode<>()).name("Forward and reverse edges")
						.join(this.vertices).where(1).equalTo(0)
						.with(new ProjectEdgeWithNeighbor<>()).name("Edge with vertex");

				return vertices.coGroup(edgesWithNeighbors)
						.where(0).equalTo(0)
						.with(new ApplyCoGroupFunctionOnAllNeighbors<>(neighborsFunction))
							.name("Neighbors function").returns(typeInfo);
			default:
				throw new IllegalArgumentException("Illegal edge direction");
		}
	}

	/**
	 * Groups by vertex and computes a GroupReduce transformation over the neighbors (both edges and vertices)
	 * of each vertex. The neighborsFunction applied on the neighbors only has access to the vertex id
	 * (not the vertex value) of the grouping vertex.
	 *
	 * <p>For each vertex, the neighborsFunction can iterate over all neighbors of this vertex
	 * with the specified direction, and emit any number of output elements, including none.
	 *
	 * @param neighborsFunction the group reduce function to apply to the neighboring edges and vertices
	 * of each vertex.
	 * @param direction the edge direction (in-, out-, all-).
	 * @param <T> the output type
	 * @return a DataSet containing elements of type T
	 * @throws IllegalArgumentException
	*/
	public <T> DataSet<T> groupReduceOnNeighbors(NeighborsFunction<K, VV, EV, T> neighborsFunction,
			EdgeDirection direction) throws IllegalArgumentException {
		switch (direction) {
		case IN:
			// create <edge-sourceVertex> pairs
			DataSet<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> edgesWithSources = edges
					.join(this.vertices).where(0).equalTo(0)
					.with(new ProjectVertexIdJoin<>(1))
					.withForwardedFieldsFirst("f1->f0").name("Edge with source vertex ID");
			return edgesWithSources.groupBy(0).reduceGroup(
				new ApplyNeighborGroupReduceFunction<>(neighborsFunction)).name("Neighbors function");
		case OUT:
			// create <edge-targetVertex> pairs
			DataSet<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> edgesWithTargets = edges
					.join(this.vertices).where(1).equalTo(0)
					.with(new ProjectVertexIdJoin<>(0))
					.withForwardedFieldsFirst("f0").name("Edge with target vertex ID");
			return edgesWithTargets.groupBy(0).reduceGroup(
				new ApplyNeighborGroupReduceFunction<>(neighborsFunction)).name("Neighbors function");
		case ALL:
			// create <edge-sourceOrTargetVertex> pairs
			DataSet<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> edgesWithNeighbors = edges
					.flatMap(new EmitOneEdgeWithNeighborPerNode<>()).name("Forward and reverse edges")
					.join(this.vertices).where(1).equalTo(0)
					.with(new ProjectEdgeWithNeighbor<>()).name("Edge with vertex ID");

			return edgesWithNeighbors.groupBy(0).reduceGroup(
				new ApplyNeighborGroupReduceFunction<>(neighborsFunction)).name("Neighbors function");
		default:
			throw new IllegalArgumentException("Illegal edge direction");
		}
	}

	/**
	 * Groups by vertex and computes a GroupReduce transformation over the neighbors (both edges and vertices)
	 * of each vertex. The neighborsFunction applied on the neighbors only has access to the vertex id
	 * (not the vertex value) of the grouping vertex.
	 *
	 * <p>For each vertex, the neighborsFunction can iterate over all neighbors of this vertex
	 * with the specified direction, and emit any number of output elements, including none.
	 *
	 * @param neighborsFunction the group reduce function to apply to the neighboring edges and vertices
	 * of each vertex.
	 * @param direction the edge direction (in-, out-, all-).
	 * @param <T> the output type
	 * @param typeInfo the explicit return type
	 * @return a DataSet containing elements of type T
	 * @throws IllegalArgumentException
	*/
	public <T> DataSet<T> groupReduceOnNeighbors(NeighborsFunction<K, VV, EV, T> neighborsFunction,
			EdgeDirection direction, TypeInformation<T> typeInfo) throws IllegalArgumentException {
		switch (direction) {
			case IN:
				// create <edge-sourceVertex> pairs
				DataSet<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> edgesWithSources = edges
						.join(this.vertices).where(0).equalTo(0)
						.with(new ProjectVertexIdJoin<>(1))
						.withForwardedFieldsFirst("f1->f0").name("Edge with source vertex ID");
				return edgesWithSources.groupBy(0).reduceGroup(
					new ApplyNeighborGroupReduceFunction<>(neighborsFunction))
						.name("Neighbors function").returns(typeInfo);
			case OUT:
				// create <edge-targetVertex> pairs
				DataSet<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> edgesWithTargets = edges
						.join(this.vertices).where(1).equalTo(0)
						.with(new ProjectVertexIdJoin<>(0))
						.withForwardedFieldsFirst("f0").name("Edge with target vertex ID");
				return edgesWithTargets.groupBy(0).reduceGroup(
					new ApplyNeighborGroupReduceFunction<>(neighborsFunction))
						.name("Neighbors function").returns(typeInfo);
			case ALL:
				// create <edge-sourceOrTargetVertex> pairs
				DataSet<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> edgesWithNeighbors = edges
						.flatMap(new EmitOneEdgeWithNeighborPerNode<>())
						.join(this.vertices).where(1).equalTo(0)
						.with(new ProjectEdgeWithNeighbor<>()).name("Edge with vertex ID");

				return edgesWithNeighbors.groupBy(0).reduceGroup(
					new ApplyNeighborGroupReduceFunction<>(neighborsFunction))
						.name("Neighbors function").returns(typeInfo);
			default:
				throw new IllegalArgumentException("Illegal edge direction");
		}
	}

	private static final class ApplyNeighborGroupReduceFunction<K, VV, EV, T>
			implements GroupReduceFunction<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>, T>, ResultTypeQueryable<T> {

		private NeighborsFunction<K, VV, EV, T> function;

		public ApplyNeighborGroupReduceFunction(NeighborsFunction<K, VV, EV, T> fun) {
			this.function = fun;
		}

		public void reduce(Iterable<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> edges, Collector<T> out) throws Exception {
			function.iterateNeighbors(edges, out);
		}

		@Override
		public TypeInformation<T> getProducedType() {
			return TypeExtractor.createTypeInfo(NeighborsFunction.class, function.getClass(), 3, null, null);
		}
	}

	@ForwardedFieldsSecond("f1")
	private static final class ProjectVertexWithNeighborValueJoin<K, VV, EV>
			implements FlatJoinFunction<Edge<K, EV>, Vertex<K, VV>, Tuple2<K, VV>> {

		private int fieldPosition;

		public ProjectVertexWithNeighborValueJoin(int position) {
			this.fieldPosition = position;
		}

		@SuppressWarnings("unchecked")
		public void join(Edge<K, EV> edge, Vertex<K, VV> otherVertex,
				Collector<Tuple2<K, VV>> out) {
			out.collect(new Tuple2<>((K) edge.getField(fieldPosition), otherVertex.getValue()));
		}
	}

	private static final class ProjectVertexIdJoin<K, VV, EV> implements FlatJoinFunction<
		Edge<K, EV>, Vertex<K, VV>, Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> {

		private int fieldPosition;

		public ProjectVertexIdJoin(int position) {
			this.fieldPosition = position;
		}

		@SuppressWarnings("unchecked")
		public void join(Edge<K, EV> edge, Vertex<K, VV> otherVertex,
						Collector<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> out) {
			out.collect(new Tuple3<>((K) edge.getField(fieldPosition), edge, otherVertex));
		}
	}

	@ForwardedFieldsFirst("f0")
	@ForwardedFieldsSecond("f1")
	private static final class ProjectNeighborValue<K, VV, EV> implements FlatJoinFunction<
		Tuple3<K, K, Edge<K, EV>>, Vertex<K, VV>, Tuple2<K, VV>> {

		public void join(Tuple3<K, K, Edge<K, EV>> keysWithEdge, Vertex<K, VV> neighbor,
				Collector<Tuple2<K, VV>> out) {

			out.collect(new Tuple2<>(keysWithEdge.f0, neighbor.getValue()));
		}
	}

	@ForwardedFieldsFirst("f0; f2->f1")
	@ForwardedFieldsSecond("*->f2")
	private static final class ProjectEdgeWithNeighbor<K, VV, EV> implements FlatJoinFunction<
		Tuple3<K, K, Edge<K, EV>>, Vertex<K, VV>, Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> {

		public void join(Tuple3<K, K, Edge<K, EV>> keysWithEdge, Vertex<K, VV> neighbor,
						Collector<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> out) {
			out.collect(new Tuple3<>(keysWithEdge.f0, keysWithEdge.f2, neighbor));
		}
	}

	private static final class ApplyNeighborCoGroupFunction<K, VV, EV, T> implements CoGroupFunction<
		Vertex<K, VV>, Tuple2<Edge<K, EV>, Vertex<K, VV>>, T>, ResultTypeQueryable<T> {

		private NeighborsFunctionWithVertexValue<K, VV, EV, T> function;

		public ApplyNeighborCoGroupFunction(NeighborsFunctionWithVertexValue<K, VV, EV, T> fun) {
			this.function = fun;
		}

		public void coGroup(Iterable<Vertex<K, VV>> vertex, Iterable<Tuple2<Edge<K, EV>, Vertex<K, VV>>> neighbors,
				Collector<T> out) throws Exception {
			function.iterateNeighbors(vertex.iterator().next(), neighbors, out);
		}

		@Override
		public TypeInformation<T> getProducedType() {
			return TypeExtractor.createTypeInfo(NeighborsFunctionWithVertexValue.class, function.getClass(), 3, null, null);
		}
	}

	private static final class ApplyCoGroupFunctionOnAllNeighbors<K, VV, EV, T>
			implements CoGroupFunction<Vertex<K, VV>, Tuple3<K, Edge<K, EV>, Vertex<K, VV>>, T>, ResultTypeQueryable<T> {

		private NeighborsFunctionWithVertexValue<K, VV, EV, T> function;

		public ApplyCoGroupFunctionOnAllNeighbors(NeighborsFunctionWithVertexValue<K, VV, EV, T> fun) {
			this.function = fun;
		}

		public void coGroup(Iterable<Vertex<K, VV>> vertex,
				final Iterable<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> keysWithNeighbors,
				Collector<T> out) throws Exception {

			final Iterator<Tuple2<Edge<K, EV>, Vertex<K, VV>>> neighborsIterator = new Iterator<Tuple2<Edge<K, EV>, Vertex<K, VV>>>() {

				final Iterator<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> keysWithEdgesIterator = keysWithNeighbors.iterator();

				@Override
				public boolean hasNext() {
					return keysWithEdgesIterator.hasNext();
				}

				@Override
				public Tuple2<Edge<K, EV>, Vertex<K, VV>> next() {
					Tuple3<K, Edge<K, EV>, Vertex<K, VV>> next = keysWithEdgesIterator.next();
					return new Tuple2<>(next.f1, next.f2);
				}

				@Override
				public void remove() {
					keysWithEdgesIterator.remove();
				}
			};

			Iterable<Tuple2<Edge<K, EV>, Vertex<K, VV>>> neighborsIterable = new Iterable<Tuple2<Edge<K, EV>, Vertex<K, VV>>>() {
				public Iterator<Tuple2<Edge<K, EV>, Vertex<K, VV>>> iterator() {
					return neighborsIterator;
				}
			};

			Iterator<Vertex<K, VV>> vertexIterator = vertex.iterator();

			if (vertexIterator.hasNext()) {
				function.iterateNeighbors(vertexIterator.next(), neighborsIterable, out);
			} else {
				throw new NoSuchElementException("The edge src/trg id could not be found within the vertexIds");
			}
		}

		@Override
		public TypeInformation<T> getProducedType() {
			return TypeExtractor.createTypeInfo(NeighborsFunctionWithVertexValue.class, function.getClass(), 3, null, null);
		}
	}

	/**
	 * Compute a reduce transformation over the neighbors' vertex values of each vertex.
	 * For each vertex, the transformation consecutively calls a
	 * {@link ReduceNeighborsFunction} until only a single value for each vertex remains.
	 * The {@link ReduceNeighborsFunction} combines a pair of neighbor vertex values
	 * into one new value of the same type.
	 *
	 * @param reduceNeighborsFunction the reduce function to apply to the neighbors of each vertex.
	 * @param direction the edge direction (in-, out-, all-)
	 * @return a Dataset of Tuple2, with one tuple per vertex.
	 * The first field of the Tuple2 is the vertex ID and the second field
	 * is the aggregate value computed by the provided {@link ReduceNeighborsFunction}.
	 * @throws IllegalArgumentException
	*/
	public DataSet<Tuple2<K, VV>> reduceOnNeighbors(ReduceNeighborsFunction<VV> reduceNeighborsFunction,
			EdgeDirection direction) throws IllegalArgumentException {
		switch (direction) {
			case IN:
				// create <vertex-source value> pairs
				final DataSet<Tuple2<K, VV>> verticesWithSourceNeighborValues = edges
						.join(this.vertices).where(0).equalTo(0)
						.with(new ProjectVertexWithNeighborValueJoin<>(1))
						.withForwardedFieldsFirst("f1->f0").name("Vertex with in-neighbor value");
				return verticesWithSourceNeighborValues.groupBy(0).reduce(new ApplyNeighborReduceFunction<>(
					reduceNeighborsFunction)).name("Neighbors function");
			case OUT:
				// create <vertex-target value> pairs
				DataSet<Tuple2<K, VV>> verticesWithTargetNeighborValues = edges
						.join(this.vertices).where(1).equalTo(0)
						.with(new ProjectVertexWithNeighborValueJoin<>(0))
						.withForwardedFieldsFirst("f0").name("Vertex with out-neighbor value");
				return verticesWithTargetNeighborValues.groupBy(0).reduce(new ApplyNeighborReduceFunction<>(
					reduceNeighborsFunction)).name("Neighbors function");
			case ALL:
				// create <vertex-neighbor value> pairs
				DataSet<Tuple2<K, VV>> verticesWithNeighborValues = edges
						.flatMap(new EmitOneEdgeWithNeighborPerNode<>())
						.join(this.vertices).where(1).equalTo(0)
						.with(new ProjectNeighborValue<>()).name("Vertex with neighbor value");

				return verticesWithNeighborValues.groupBy(0).reduce(new ApplyNeighborReduceFunction<>(
					reduceNeighborsFunction)).name("Neighbors function");
			default:
				throw new IllegalArgumentException("Illegal edge direction");
		}
	}

	@ForwardedFields("f0")
	private static final class ApplyNeighborReduceFunction<K, VV> implements ReduceFunction<Tuple2<K, VV>> {

		private ReduceNeighborsFunction<VV> function;

		public ApplyNeighborReduceFunction(ReduceNeighborsFunction<VV> fun) {
			this.function = fun;
		}

		@Override
		public Tuple2<K, VV> reduce(Tuple2<K, VV> first, Tuple2<K, VV> second) throws Exception {
			first.f1 = function.reduceNeighbors(first.f1, second.f1);
			return first;
		}
	}

	/**
	 * Compute a reduce transformation over the edge values of each vertex.
	 * For each vertex, the transformation consecutively calls a
	 * {@link ReduceEdgesFunction} until only a single value for each edge remains.
	 * The {@link ReduceEdgesFunction} combines two edge values into one new value of the same type.
	 *
	 * @param reduceEdgesFunction the reduce function to apply to the neighbors of each vertex.
	 * @param direction the edge direction (in-, out-, all-)
	 * @return a Dataset of Tuple2, with one tuple per vertex.
	 * The first field of the Tuple2 is the vertex ID and the second field
	 * is the aggregate value computed by the provided {@link ReduceEdgesFunction}.
	 * @throws IllegalArgumentException
	*/
	public DataSet<Tuple2<K, EV>> reduceOnEdges(ReduceEdgesFunction<EV> reduceEdgesFunction,
			EdgeDirection direction) throws IllegalArgumentException {

		switch (direction) {
			case IN:
				return edges.map(new ProjectVertexWithEdgeValueMap<>(1))
						.withForwardedFields("f1->f0")
							.name("Vertex with in-edges")
						.groupBy(0).reduce(new ApplyReduceFunction<>(reduceEdgesFunction))
							.name("Reduce on edges");
			case OUT:
				return edges.map(new ProjectVertexWithEdgeValueMap<>(0))
						.withForwardedFields("f0->f0")
							.name("Vertex with out-edges")
						.groupBy(0).reduce(new ApplyReduceFunction<>(reduceEdgesFunction))
							.name("Reduce on edges");
			case ALL:
				return edges.flatMap(new EmitOneVertexWithEdgeValuePerNode<>())
						.withForwardedFields("f2->f1")
							.name("Vertex with all edges")
						.groupBy(0).reduce(new ApplyReduceFunction<>(reduceEdgesFunction))
							.name("Reduce on edges");
			default:
				throw new IllegalArgumentException("Illegal edge direction");
		}
	}

	@ForwardedFields("f0")
	private static final class ApplyReduceFunction<K, EV> implements ReduceFunction<Tuple2<K, EV>> {

		private ReduceEdgesFunction<EV> function;

		public ApplyReduceFunction(ReduceEdgesFunction<EV> fun) {
			this.function = fun;
		}

		@Override
		public Tuple2<K, EV> reduce(Tuple2<K, EV> first, Tuple2<K, EV> second) throws Exception {
			first.f1 = function.reduceEdges(first.f1, second.f1);
			return first;
		}
	}
}
