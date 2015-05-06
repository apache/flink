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

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.List;
import java.util.Arrays;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.gsa.ApplyFunction;
import org.apache.flink.graph.gsa.GatherFunction;
import org.apache.flink.graph.gsa.GatherSumApplyIteration;
import org.apache.flink.graph.gsa.SumFunction;
import org.apache.flink.graph.spargel.IterationConfiguration;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexCentricIteration;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.graph.utils.EdgeToTuple3Map;
import org.apache.flink.graph.utils.Tuple2ToVertexMap;
import org.apache.flink.graph.utils.Tuple3ToEdgeMap;
import org.apache.flink.graph.utils.VertexToTuple2Map;
import org.apache.flink.graph.validation.GraphValidator;
import org.apache.flink.util.Collector;
import org.apache.flink.types.NullValue;

/**
 * Represents a Graph consisting of {@link Edge edges} and {@link Vertex
 * vertices}.
 * 
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
	 * Creates a graph from two DataSets: vertices and edges
	 * 
	 * @param vertices a DataSet of vertices.
	 * @param edges a DataSet of edges.
	 * @param context the flink execution environment.
	 */
	private Graph(DataSet<Vertex<K, VV>> vertices, DataSet<Edge<K, EV>> edges, ExecutionEnvironment context) {
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
	 * Creates a graph from a Collection of edges, vertices are induced from the
	 * edges. Vertices are created automatically and their values are set to
	 * NullValue.
	 * 
	 * @param edges a Collection of vertices.
	 * @param context the flink execution environment.
	 * @return the newly created graph.
	 */
	public static <K, EV> Graph<K, NullValue, EV> fromCollection(Collection<Edge<K, EV>> edges,
			ExecutionEnvironment context) {

		return fromDataSet(context.fromCollection(edges), context);
	}

	/**
	 * Creates a graph from a Collection of edges, vertices are induced from the
	 * edges and vertex values are calculated by a mapper function. Vertices are
	 * created automatically and their values are set by applying the provided
	 * map function to the vertex ids.
	 * 
	 * @param edges a Collection of edges.
	 * @param mapper the mapper function.
	 * @param context the flink execution environment.
	 * @return the newly created graph.
	 */
	public static <K, VV, EV> Graph<K, VV, EV> fromCollection(Collection<Edge<K, EV>> edges,
			final MapFunction<K, VV> mapper,ExecutionEnvironment context) {

		return fromDataSet(context.fromCollection(edges), mapper, context);
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

		return new Graph<K, VV, EV>(vertices, edges, context);
	}

	/**
	 * Creates a graph from a DataSet of edges, vertices are induced from the
	 * edges. Vertices are created automatically and their values are set to
	 * NullValue.
	 * 
	 * @param edges a DataSet of edges.
	 * @param context the flink execution environment.
	 * @return the newly created graph.
	 */
	public static <K, EV> Graph<K, NullValue, EV> fromDataSet(
			DataSet<Edge<K, EV>> edges, ExecutionEnvironment context) {

		DataSet<Vertex<K, NullValue>> vertices = edges.flatMap(new EmitSrcAndTarget<K, EV>()).distinct();

		return new Graph<K, NullValue, EV>(vertices, edges, context);
	}

	private static final class EmitSrcAndTarget<K, EV> implements FlatMapFunction<
		Edge<K, EV>, Vertex<K, NullValue>> {

		public void flatMap(Edge<K, EV> edge, Collector<Vertex<K, NullValue>> out) {
			out.collect(new Vertex<K, NullValue>(edge.f0, NullValue.getInstance()));
			out.collect(new Vertex<K, NullValue>(edge.f1, NullValue.getInstance()));
		}
	}

	/**
	 * Creates a graph from a DataSet of edges, vertices are induced from the
	 * edges and vertex values are calculated by a mapper function. Vertices are
	 * created automatically and their values are set by applying the provided
	 * map function to the vertex ids.
	 * 
	 * @param edges a DataSet of edges.
	 * @param mapper the mapper function.
	 * @param context the flink execution environment.
	 * @return the newly created graph.
	 */
	public static <K, VV, EV> Graph<K, VV, EV> fromDataSet(DataSet<Edge<K, EV>> edges,
			final MapFunction<K, VV> mapper, ExecutionEnvironment context) {

		TypeInformation<K> keyType = ((TupleTypeInfo<?>) edges.getType()).getTypeAt(0);

		TypeInformation<VV> valueType = TypeExtractor.createTypeInfo(
				MapFunction.class, mapper.getClass(), 1, null, null);

		@SuppressWarnings({ "unchecked", "rawtypes" })
		TypeInformation<Vertex<K, VV>> returnType = (TypeInformation<Vertex<K, VV>>) new TupleTypeInfo(
				Vertex.class, keyType, valueType);

		DataSet<Vertex<K, VV>> vertices = edges
				.flatMap(new EmitSrcAndTargetAsTuple1<K, EV>()).distinct()
				.map(new MapFunction<Tuple1<K>, Vertex<K, VV>>() {
					public Vertex<K, VV> map(Tuple1<K> value) throws Exception {
						return new Vertex<K, VV>(value.f0, mapper.map(value.f0));
					}
				}).returns(returnType).withForwardedFields("f0");

		return new Graph<K, VV, EV>(vertices, edges, context);
	}

	private static final class EmitSrcAndTargetAsTuple1<K, EV> implements FlatMapFunction<
		Edge<K, EV>, Tuple1<K>> {

		public void flatMap(Edge<K, EV> edge, Collector<Tuple1<K>> out) {
			out.collect(new Tuple1<K>(edge.f0));
			out.collect(new Tuple1<K>(edge.f1));
		}
	}

	/**
	 * Creates a graph from a DataSet of Tuple objects for vertices and edges.
	 * 
	 * Vertices with value are created from Tuple2, Edges with value are created
	 * from Tuple3.
	 * 
	 * @param vertices a DataSet of Tuple2.
	 * @param edges a DataSet of Tuple3.
	 * @param context the flink execution environment.
	 * @return the newly created graph.
	 */
	public static <K, VV, EV> Graph<K, VV, EV> fromTupleDataSet(DataSet<Tuple2<K, VV>> vertices,
			DataSet<Tuple3<K, K, EV>> edges, ExecutionEnvironment context) {

		DataSet<Vertex<K, VV>> vertexDataSet = vertices.map(new Tuple2ToVertexMap<K, VV>());
		DataSet<Edge<K, EV>> edgeDataSet = edges.map(new Tuple3ToEdgeMap<K, EV>());
		return fromDataSet(vertexDataSet, edgeDataSet, context);
	}

	/**
	 * Creates a graph from a DataSet of Tuple objects for edges, vertices are
	 * induced from the edges.
	 * 
	 * Edges with value are created from Tuple3. Vertices are created
	 * automatically and their values are set to NullValue.
	 * 
	 * @param edges a DataSet of Tuple3.
	 * @param context the flink execution environment.
	 * @return the newly created graph.
	 */
	public static <K, EV> Graph<K, NullValue, EV> fromTupleDataSet(DataSet<Tuple3<K, K, EV>> edges,
			ExecutionEnvironment context) {

		DataSet<Edge<K, EV>> edgeDataSet = edges.map(new Tuple3ToEdgeMap<K, EV>());
		return fromDataSet(edgeDataSet, context);
	}

	/**
	 * Creates a graph from a DataSet of Tuple objects for edges, vertices are
	 * induced from the edges and vertex values are calculated by a mapper
	 * function. Edges with value are created from Tuple3. Vertices are created
	 * automatically and their values are set by applying the provided map
	 * function to the vertex ids.
	 * 
	 * @param edges a DataSet of Tuple3.
	 * @param mapper the mapper function.
	 * @param context the flink execution environment.
	 * @return the newly created graph.
	 */
	public static <K, VV, EV> Graph<K, VV, EV> fromTupleDataSet(DataSet<Tuple3<K, K, EV>> edges,
			final MapFunction<K, VV> mapper, ExecutionEnvironment context) {

		DataSet<Edge<K, EV>> edgeDataSet = edges.map(new Tuple3ToEdgeMap<K, EV>());
		return fromDataSet(edgeDataSet, mapper, context);
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
		return vertices.map(new VertexToTuple2Map<K, VV>());
	}

	/**
	 * @return the edge DataSet as Tuple3.
	 */
	public DataSet<Tuple3<K, K, EV>> getEdgesAsTuple3() {
		return edges.map(new EdgeToTuple3Map<K, EV>());
	}

	/**
	 * This method allows access to the graph's edge values along with its source and target vertex values.
	 *
	 * @return a triplet DataSet consisting of (srcVertexId, trgVertexId, srcVertexValue, trgVertexValue, edgeValue)
	 */
	public DataSet<Triplet<K, VV, EV>> getTriplets() {
		return this.getVertices().join(this.getEdges()).where(0).equalTo(0)
				.with(new FlatJoinFunction<Vertex<K, VV>, Edge<K, EV>, Tuple4<K, K, VV, EV>>() {

					@Override
					public void join(Vertex<K, VV> vertex, Edge<K, EV> edge, Collector<Tuple4<K, K, VV, EV>> collector)
							throws Exception {

						collector.collect(new Tuple4<K, K, VV, EV>(edge.getSource(), edge.getTarget(), vertex.getValue(),
								edge.getValue()));
					}
				})
				.join(this.getVertices()).where(1).equalTo(0)
				.with(new FlatJoinFunction<Tuple4<K, K, VV, EV>, Vertex<K, VV>, Triplet<K, VV, EV>>() {

					@Override
					public void join(Tuple4<K, K, VV, EV> tripletWithSrcValSet,
							Vertex<K, VV> vertex, Collector<Triplet<K, VV, EV>> collector) throws Exception {

						collector.collect(new Triplet<K, VV, EV>(tripletWithSrcValSet.f0, tripletWithSrcValSet.f1,
								tripletWithSrcValSet.f2, vertex.getValue(), tripletWithSrcValSet.f3));
					}
				});
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

		TypeInformation<NV> valueType = TypeExtractor.createTypeInfo(MapFunction.class, mapper.getClass(), 1, null, null);

		TypeInformation<Vertex<K, NV>> returnType = (TypeInformation<Vertex<K, NV>>) new TupleTypeInfo(
				Vertex.class, keyType, valueType);

		DataSet<Vertex<K, NV>> mappedVertices = vertices.map(
				new MapFunction<Vertex<K, VV>, Vertex<K, NV>>() {
					public Vertex<K, NV> map(Vertex<K, VV> value) throws Exception {
						return new Vertex<K, NV>(value.f0, mapper.map(value));
					}
				}).returns(returnType);

		return new Graph<K, NV, EV>(mappedVertices, this.edges, this.context);
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

		TypeInformation<NV> valueType = TypeExtractor.createTypeInfo(MapFunction.class, mapper.getClass(), 1, null, null);

		TypeInformation<Edge<K, NV>> returnType = (TypeInformation<Edge<K, NV>>) new TupleTypeInfo(
				Edge.class, keyType, keyType, valueType);

		DataSet<Edge<K, NV>> mappedEdges = edges.map(
				new MapFunction<Edge<K, EV>, Edge<K, NV>>() {
					public Edge<K, NV> map(Edge<K, EV> value) throws Exception {
						return new Edge<K, NV>(value.f0, value.f1, mapper
								.map(value));
					}
				}).returns(returnType);

		return new Graph<K, VV, NV>(this.vertices, mappedEdges, this.context);
	}

	/**
	 * Joins the vertex DataSet of this graph with an input DataSet and applies
	 * a UDF on the resulted values.
	 * 
	 * @param inputDataSet the DataSet to join with.
	 * @param mapper the UDF map function to apply.
	 * @return a new graph where the vertex values have been updated.
	 */
	public <T> Graph<K, VV, EV> joinWithVertices(DataSet<Tuple2<K, T>> inputDataSet, 
			final MapFunction<Tuple2<VV, T>, VV> mapper) {

		DataSet<Vertex<K, VV>> resultedVertices = this.getVertices()
				.coGroup(inputDataSet).where(0).equalTo(0)
				.with(new ApplyCoGroupToVertexValues<K, VV, T>(mapper));
		return new Graph<K, VV, EV>(resultedVertices, this.edges, this.context);
	}

	private static final class ApplyCoGroupToVertexValues<K, VV, T>
			implements CoGroupFunction<Vertex<K, VV>, Tuple2<K, T>, Vertex<K, VV>> {

		private MapFunction<Tuple2<VV, T>, VV> mapper;

		public ApplyCoGroupToVertexValues(MapFunction<Tuple2<VV, T>, VV> mapper) {
			this.mapper = mapper;
		}

		@Override
		public void coGroup(Iterable<Vertex<K, VV>> vertices,
				Iterable<Tuple2<K, T>> input, Collector<Vertex<K, VV>> collector) throws Exception {

			final Iterator<Vertex<K, VV>> vertexIterator = vertices.iterator();
			final Iterator<Tuple2<K, T>> inputIterator = input.iterator();

			if (vertexIterator.hasNext()) {
				if (inputIterator.hasNext()) {
					final Tuple2<K, T> inputNext = inputIterator.next();

					collector.collect(new Vertex<K, VV>(inputNext.f0, mapper
							.map(new Tuple2<VV, T>(vertexIterator.next().f1,
									inputNext.f1))));
				} else {
					collector.collect(vertexIterator.next());
				}

			}
		}
	}

	/**
	 * Joins the edge DataSet with an input DataSet on a composite key of both
	 * source and target and applies a UDF on the resulted values.
	 * 
	 * @param inputDataSet the DataSet to join with.
	 * @param mapper the UDF map function to apply.
	 * @param <T> the return type
	 * @return a new graph where the edge values have been updated.
	 */
	public <T> Graph<K, VV, EV> joinWithEdges(DataSet<Tuple3<K, K, T>> inputDataSet,
			final MapFunction<Tuple2<EV, T>, EV> mapper) {

		DataSet<Edge<K, EV>> resultedEdges = this.getEdges()
				.coGroup(inputDataSet).where(0, 1).equalTo(0, 1)
				.with(new ApplyCoGroupToEdgeValues<K, EV, T>(mapper));
		return new Graph<K, VV, EV>(this.vertices, resultedEdges, this.context);
	}

	private static final class ApplyCoGroupToEdgeValues<K, EV, T>
			implements CoGroupFunction<Edge<K, EV>, Tuple3<K, K, T>, Edge<K, EV>> {

		private MapFunction<Tuple2<EV, T>, EV> mapper;

		public ApplyCoGroupToEdgeValues(MapFunction<Tuple2<EV, T>, EV> mapper) {
			this.mapper = mapper;
		}

		@Override
		public void coGroup(Iterable<Edge<K, EV>> edges, Iterable<Tuple3<K, K, T>> input,
				Collector<Edge<K, EV>> collector) throws Exception {

			final Iterator<Edge<K, EV>> edgesIterator = edges.iterator();
			final Iterator<Tuple3<K, K, T>> inputIterator = input.iterator();

			if (edgesIterator.hasNext()) {
				if (inputIterator.hasNext()) {
					final Tuple3<K, K, T> inputNext = inputIterator.next();

					collector.collect(new Edge<K, EV>(inputNext.f0,
							inputNext.f1, mapper.map(new Tuple2<EV, T>(
									edgesIterator.next().f2, inputNext.f2))));
				} else {
					collector.collect(edgesIterator.next());
				}
			}
		}
	}

	/**
	 * Joins the edge DataSet with an input DataSet on the source key of the
	 * edges and the first attribute of the input DataSet and applies a UDF on
	 * the resulted values. In case the inputDataSet contains the same key more
	 * than once, only the first value will be considered.
	 * 
	 * @param inputDataSet the DataSet to join with.
	 * @param mapper the UDF map function to apply.
	 * @param <T> the return type
	 * @return a new graph where the edge values have been updated.
	 */
	public <T> Graph<K, VV, EV> joinWithEdgesOnSource(DataSet<Tuple2<K, T>> inputDataSet,
			final MapFunction<Tuple2<EV, T>, EV> mapper) {

		DataSet<Edge<K, EV>> resultedEdges = this.getEdges()
				.coGroup(inputDataSet).where(0).equalTo(0)
				.with(new ApplyCoGroupToEdgeValuesOnEitherSourceOrTarget<K, EV, T>(mapper));

		return new Graph<K, VV, EV>(this.vertices, resultedEdges, this.context);
	}

	private static final class ApplyCoGroupToEdgeValuesOnEitherSourceOrTarget<K, EV, T>
			implements CoGroupFunction<Edge<K, EV>, Tuple2<K, T>, Edge<K, EV>> {

		private MapFunction<Tuple2<EV, T>, EV> mapper;

		public ApplyCoGroupToEdgeValuesOnEitherSourceOrTarget(
				MapFunction<Tuple2<EV, T>, EV> mapper) {
			this.mapper = mapper;
		}

		@Override
		public void coGroup(Iterable<Edge<K, EV>> edges,
				Iterable<Tuple2<K, T>> input, Collector<Edge<K, EV>> collector) throws Exception {

			final Iterator<Edge<K, EV>> edgesIterator = edges.iterator();
			final Iterator<Tuple2<K, T>> inputIterator = input.iterator();

			if (inputIterator.hasNext()) {
				final Tuple2<K, T> inputNext = inputIterator.next();

				while (edgesIterator.hasNext()) {
					Edge<K, EV> edgesNext = edgesIterator.next();

					collector.collect(new Edge<K, EV>(edgesNext.f0,
							edgesNext.f1, mapper.map(new Tuple2<EV, T>(
									edgesNext.f2, inputNext.f1))));
				}

			} else {
				while (edgesIterator.hasNext()) {
					collector.collect(edgesIterator.next());
				}
			}
		}
	}

	/**
	 * Joins the edge DataSet with an input DataSet on the target key of the
	 * edges and the first attribute of the input DataSet and applies a UDF on
	 * the resulted values. Should the inputDataSet contain the same key more
	 * than once, only the first value will be considered.
	 * 
	 * @param inputDataSet the DataSet to join with.
	 * @param mapper the UDF map function to apply.
	 * @param <T> the return type
	 * @return a new graph where the edge values have been updated.
	 */
	public <T> Graph<K, VV, EV> joinWithEdgesOnTarget(DataSet<Tuple2<K, T>> inputDataSet,
			final MapFunction<Tuple2<EV, T>, EV> mapper) {

		DataSet<Edge<K, EV>> resultedEdges = this.getEdges()
				.coGroup(inputDataSet).where(1).equalTo(0)
				.with(new ApplyCoGroupToEdgeValuesOnEitherSourceOrTarget<K, EV, T>(mapper));

		return new Graph<K, VV, EV>(this.vertices, resultedEdges, this.context);
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
				.where(0).equalTo(0).with(new ProjectEdge<K, VV, EV>())
				.join(filteredVertices).where(1).equalTo(0)
				.with(new ProjectEdge<K, VV, EV>());

		DataSet<Edge<K, EV>> filteredEdges = remainingEdges.filter(edgeFilter);

		return new Graph<K, VV, EV>(filteredVertices, filteredEdges,
				this.context);
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
				.where(0).equalTo(0).with(new ProjectEdge<K, VV, EV>())
				.join(filteredVertices).where(1).equalTo(0)
				.with(new ProjectEdge<K, VV, EV>());

		return new Graph<K, VV, EV>(filteredVertices, remainingEdges, this.context);
	}

	/**
	 * Apply a filtering function to the graph and return a sub-graph that
	 * satisfies the predicates only for the edges.
	 * 
	 * @param edgeFilter the filter function for edges.
	 * @return the resulting sub-graph.
	 */
	public Graph<K, VV, EV> filterOnEdges(FilterFunction<Edge<K, EV>> edgeFilter) {
		DataSet<Edge<K, EV>> filteredEdges = this.edges.filter(edgeFilter);

		return new Graph<K, VV, EV>(this.vertices, filteredEdges, this.context);
	}

	@ForwardedFieldsFirst("0->0;1->1;2->2")
	private static final class ProjectEdge<K, VV, EV> implements FlatJoinFunction<
		Edge<K, EV>, Vertex<K, VV>, Edge<K, EV>> {
		public void join(Edge<K, EV> first, Vertex<K, VV> second, Collector<Edge<K, EV>> out) {
			out.collect(first);
		}
	}

	/**
	 * Return the out-degree of all vertices in the graph
	 * 
	 * @return A DataSet of Tuple2<vertexId, outDegree>
	 */
	public DataSet<Tuple2<K, Long>> outDegrees() {

		return vertices.coGroup(edges).where(0).equalTo(0).with(new CountNeighborsCoGroup<K, VV, EV>());
	}

	private static final class CountNeighborsCoGroup<K, VV, EV>
			implements CoGroupFunction<Vertex<K, VV>, Edge<K, EV>, Tuple2<K, Long>> {
		@SuppressWarnings("unused")
		public void coGroup(Iterable<Vertex<K, VV>> vertex,	Iterable<Edge<K, EV>> outEdges,
				Collector<Tuple2<K, Long>> out) {
			long count = 0;
			for (Edge<K, EV> edge : outEdges) {
				count++;
			}

			Iterator<Vertex<K, VV>> vertexIterator = vertex.iterator();

			if(vertexIterator.hasNext()) {
				out.collect(new Tuple2<K, Long>(vertexIterator.next().f0, count));
			} else {
				throw new NoSuchElementException("The edge src/trg id could not be found within the vertexIds");
			}
		}
	}

	/**
	 * Return the in-degree of all vertices in the graph
	 * 
	 * @return A DataSet of Tuple2<vertexId, inDegree>
	 */
	public DataSet<Tuple2<K, Long>> inDegrees() {

		return vertices.coGroup(edges).where(0).equalTo(1).with(new CountNeighborsCoGroup<K, VV, EV>());
	}

	/**
	 * Return the degree of all vertices in the graph
	 * 
	 * @return A DataSet of Tuple2<vertexId, degree>
	 */
	public DataSet<Tuple2<K, Long>> getDegrees() {
		return outDegrees().union(inDegrees()).groupBy(0).sum(1);
	}

	/**
	 * This operation adds all inverse-direction edges to the graph.
	 * 
	 * @return the undirected graph.
	 */
	public Graph<K, VV, EV> getUndirected() {

		DataSet<Edge<K, EV>> undirectedEdges = edges.union(edges.map(new ReverseEdgesMap<K, EV>()));
		return new Graph<K, VV, EV>(vertices, undirectedEdges, this.context);
	}

	/**
	 * Compute an aggregate over the edges of each vertex. The function applied
	 * on the edges has access to the vertex value.
	 * 
	 * @param edgesFunction
	 *            the function to apply to the neighborhood
	 * @param direction
	 *            the edge direction (in-, out-, all-)
	 * @param <T>
	 *            the output type
	 * @return a dataset of a T
	 * @throws IllegalArgumentException
	 */
	public <T> DataSet<T> groupReduceOnEdges(EdgesFunctionWithVertexValue<K, VV, EV, T> edgesFunction,
											EdgeDirection direction) throws IllegalArgumentException {

		switch (direction) {
		case IN:
			return vertices.coGroup(edges).where(0).equalTo(1)
					.with(new ApplyCoGroupFunction<K, VV, EV, T>(edgesFunction));
		case OUT:
			return vertices.coGroup(edges).where(0).equalTo(0)
					.with(new ApplyCoGroupFunction<K, VV, EV, T>(edgesFunction));
		case ALL:
			return vertices.coGroup(edges.flatMap(new EmitOneEdgePerNode<K, VV, EV>()))
					.where(0).equalTo(0).with(new ApplyCoGroupFunctionOnAllEdges<K, VV, EV, T>(edgesFunction));
		default:
			throw new IllegalArgumentException("Illegal edge direction");
		}
	}

	/**
	 * Compute an aggregate over the edges of each vertex. The function applied
	 * on the edges only has access to the vertex id (not the vertex value).
	 * 
	 * @param edgesFunction
	 *            the function to apply to the neighborhood
	 * @param direction
	 *            the edge direction (in-, out-, all-)
	 * @param <T>
	 *            the output type
	 * @return a dataset of T
	 * @throws IllegalArgumentException
	 */
	public <T> DataSet<T> groupReduceOnEdges(EdgesFunction<K, EV, T> edgesFunction,
											EdgeDirection direction) throws IllegalArgumentException {

		switch (direction) {
		case IN:
			return edges.map(new ProjectVertexIdMap<K, EV>(1))
					.groupBy(0).reduceGroup(new ApplyGroupReduceFunction<K, EV, T>(edgesFunction));
		case OUT:
			return edges.map(new ProjectVertexIdMap<K, EV>(0))
					.groupBy(0).reduceGroup(new ApplyGroupReduceFunction<K, EV, T>(edgesFunction));
		case ALL:
			return edges.flatMap(new EmitOneEdgePerNode<K, VV, EV>())
					.groupBy(0).reduceGroup(new ApplyGroupReduceFunction<K, EV, T>(edgesFunction));
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
			return new Tuple2<K, Edge<K, EV>>((K) edge.getField(fieldPosition),	edge);
		}
	}

	private static final class ProjectVertexWithEdgeValueMap<K, EV>	implements MapFunction<
		Edge<K, EV>, Tuple2<K, EV>> {

		private int fieldPosition;

		public ProjectVertexWithEdgeValueMap(int position) {
			this.fieldPosition = position;
		}

		@SuppressWarnings("unchecked")
		public Tuple2<K, EV> map(Edge<K, EV> edge) {
			return new Tuple2<K, EV>((K) edge.getField(fieldPosition),	edge.getValue());
		}
	}

	private static final class ApplyGroupReduceFunction<K, EV, T> implements GroupReduceFunction<
		Tuple2<K, Edge<K, EV>>, T>,	ResultTypeQueryable<T> {

		private EdgesFunction<K, EV, T> function;

		public ApplyGroupReduceFunction(EdgesFunction<K, EV, T> fun) {
			this.function = fun;
		}

		public void reduce(Iterable<Tuple2<K, Edge<K, EV>>> edges, Collector<T> out) throws Exception {
			function.iterateEdges(edges, out);
		}

		@Override
		public TypeInformation<T> getProducedType() {
			return TypeExtractor.createTypeInfo(EdgesFunction.class, function.getClass(), 2, null, null);
		}
	}

	private static final class EmitOneEdgePerNode<K, VV, EV> implements FlatMapFunction<
		Edge<K, EV>, Tuple2<K, Edge<K, EV>>> {

		public void flatMap(Edge<K, EV> edge, Collector<Tuple2<K, Edge<K, EV>>> out) {
			out.collect(new Tuple2<K, Edge<K, EV>>(edge.getSource(), edge));
			out.collect(new Tuple2<K, Edge<K, EV>>(edge.getTarget(), edge));
		}
	}

	private static final class EmitOneVertexWithEdgeValuePerNode<K, VV, EV>	implements FlatMapFunction<
		Edge<K, EV>, Tuple2<K, EV>> {

		public void flatMap(Edge<K, EV> edge, Collector<Tuple2<K, EV>> out) {
			out.collect(new Tuple2<K, EV>(edge.getSource(), edge.getValue()));
			out.collect(new Tuple2<K, EV>(edge.getTarget(), edge.getValue()));
		}
	}

	private static final class EmitOneEdgeWithNeighborPerNode<K, VV, EV> implements FlatMapFunction<
		Edge<K, EV>, Tuple3<K, K, Edge<K, EV>>> {

		public void flatMap(Edge<K, EV> edge, Collector<Tuple3<K, K, Edge<K, EV>>> out) {
			out.collect(new Tuple3<K, K, Edge<K, EV>>(edge.getSource(), edge.getTarget(), edge));
			out.collect(new Tuple3<K, K, Edge<K, EV>>(edge.getTarget(), edge.getSource(), edge));
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
			function.iterateEdges(vertex.iterator().next(), edges, out);
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

		public void coGroup(Iterable<Vertex<K, VV>> vertex,	final Iterable<Tuple2<K, Edge<K, EV>>> keysWithEdges,
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

			function.iterateEdges(vertex.iterator().next(),	edgesIterable, out);
		}

		@Override
		public TypeInformation<T> getProducedType() {
			return TypeExtractor.createTypeInfo(EdgesFunctionWithVertexValue.class, function.getClass(), 3,
					null, null);
		}
	}

	@ForwardedFields("0->1;1->0;2->2")
	private static final class ReverseEdgesMap<K, EV>
			implements MapFunction<Edge<K, EV>, Edge<K, EV>> {

		public Edge<K, EV> map(Edge<K, EV> value) {
			return new Edge<K, EV>(value.f1, value.f0, value.f2);
		}
	}

	/**
	 * Reverse the direction of the edges in the graph
	 * 
	 * @return a new graph with all edges reversed
	 * @throws UnsupportedOperationException
	 */
	public Graph<K, VV, EV> reverse() throws UnsupportedOperationException {
		DataSet<Edge<K, EV>> reversedEdges = edges.map(new ReverseEdgesMap<K, EV>());
		return new Graph<K, VV, EV>(vertices, reversedEdges, this.context);
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
		return vertices.map(new ExtractVertexIDMapper<K, VV>());
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
		return edges.map(new ExtractEdgeIDsMapper<K, EV>());
	}

	private static final class ExtractEdgeIDsMapper<K, EV>
			implements MapFunction<Edge<K, EV>, Tuple2<K, K>> {
		@Override
		public Tuple2<K, K> map(Edge<K, EV> edge) throws Exception {
			return new Tuple2<K, K>(edge.f0, edge.f1);
		}
	}

	/**
	 * Adds the input vertex and edges to the graph. If the vertex already
	 * exists in the graph, it will not be added again, but the given edges
	 * will.
	 * 
	 * @param vertex the vertex to add to the graph
	 * @param edges a list of edges to add to the grap
	 * @return the new graph containing the existing and newly added vertices
	 *         and edges
	 */
	@SuppressWarnings("unchecked")
	public Graph<K, VV, EV> addVertex(final Vertex<K, VV> vertex, List<Edge<K, EV>> edges) {
		DataSet<Vertex<K, VV>> newVertex = this.context.fromElements(vertex);

		// Take care of empty edge set
		if (edges.isEmpty()) {
			return new Graph<K, VV, EV>(this.vertices.union(newVertex)
					.distinct(), this.edges, this.context);
		}

		// Add the vertex and its edges
		DataSet<Vertex<K, VV>> newVertices = this.vertices.union(newVertex).distinct();
		DataSet<Edge<K, EV>> newEdges = this.edges.union(context.fromCollection(edges));

		return new Graph<K, VV, EV>(newVertices, newEdges, this.context);
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
	@SuppressWarnings("unchecked")
	public Graph<K, VV, EV> addEdge(Vertex<K, VV> source, Vertex<K, VV> target, EV edgeValue) {
		Graph<K, VV, EV> partialGraph = fromCollection(Arrays.asList(source, target),
				Arrays.asList(new Edge<K, EV>(source.f0, target.f0, edgeValue)),
				this.context);
		return this.union(partialGraph);
	}

	/**
	 * Removes the given vertex and its edges from the graph.
	 * 
	 * @param vertex the vertex to remove
	 * @return the new graph containing the existing vertices and edges without
	 *         the removed vertex and its edges
	 */
	public Graph<K, VV, EV> removeVertex(Vertex<K, VV> vertex) {

		DataSet<Vertex<K, VV>> newVertices = getVertices().filter(new RemoveVertexFilter<K, VV>(vertex));
		DataSet<Edge<K, EV>> newEdges = getEdges().filter(new VertexRemovalEdgeFilter<K, VV, EV>(vertex));
		return new Graph<K, VV, EV>(newVertices, newEdges, this.context);
	}

	private static final class RemoveVertexFilter<K, VV>
			implements FilterFunction<Vertex<K, VV>> {

		private Vertex<K, VV> vertexToRemove;

		public RemoveVertexFilter(Vertex<K, VV> vertex) {
			vertexToRemove = vertex;
		}

		@Override
		public boolean filter(Vertex<K, VV> vertex) throws Exception {
			return !vertex.f0.equals(vertexToRemove.f0);
		}
	}

	private static final class VertexRemovalEdgeFilter<K, VV, EV>
			implements FilterFunction<Edge<K, EV>> {

		private Vertex<K, VV> vertexToRemove;

		public VertexRemovalEdgeFilter(Vertex<K, VV> vertex) {
			vertexToRemove = vertex;
		}

		@Override
		public boolean filter(Edge<K, EV> edge) throws Exception {

			if (edge.f0.equals(vertexToRemove.f0)) {
				return false;
			}
			if (edge.f1.equals(vertexToRemove.f0)) {
				return false;
			}
			return true;
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
		DataSet<Edge<K, EV>> newEdges = getEdges().filter(new EdgeRemovalEdgeFilter<K, EV>(edge));
		return new Graph<K, VV, EV>(this.vertices, newEdges, this.context);
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
	 * Performs union on the vertices and edges sets of the input graphs
	 * removing duplicate vertices but maintaining duplicate edges.
	 * 
	 * @param graph the graph to perform union with
	 * @return a new graph
	 */
	public Graph<K, VV, EV> union(Graph<K, VV, EV> graph) {

		DataSet<Vertex<K, VV>> unionedVertices = graph.getVertices().union(this.getVertices()).distinct();
		DataSet<Edge<K, EV>> unionedEdges = graph.getEdges().union(this.getEdges());
		return new Graph<K, VV, EV>(unionedVertices, unionedEdges, this.context);
	}

	/**
	 * Runs a Vertex-Centric iteration on the graph.
	 * No configuration options are provided.

	 * @param vertexUpdateFunction the vertex update function
	 * @param messagingFunction the messaging function
	 * @param maximumNumberOfIterations maximum number of iterations to perform
	 * 
	 * @return the updated Graph after the vertex-centric iteration has converged or
	 * after maximumNumberOfIterations.
	 */
	public <M> Graph<K, VV, EV> runVertexCentricIteration(
			VertexUpdateFunction<K, VV, M> vertexUpdateFunction,
			MessagingFunction<K, VV, M, EV> messagingFunction,
			int maximumNumberOfIterations) {

		return this.runVertexCentricIteration(vertexUpdateFunction, messagingFunction,
				maximumNumberOfIterations, null);
	}

	/**
	 * Runs a Vertex-Centric iteration on the graph with configuration options.
	 * 
	 * @param vertexUpdateFunction the vertex update function
	 * @param messagingFunction the messaging function
	 * @param maximumNumberOfIterations maximum number of iterations to perform
	 * @param parameters the iteration configuration parameters
	 * 
	 * @return the updated Graph after the vertex-centric iteration has converged or
	 * after maximumNumberOfIterations.
	 */
	public <M> Graph<K, VV, EV> runVertexCentricIteration(
			VertexUpdateFunction<K, VV, M> vertexUpdateFunction,
			MessagingFunction<K, VV, M, EV> messagingFunction,
			int maximumNumberOfIterations, IterationConfiguration parameters) {

		VertexCentricIteration<K, VV, M, EV> iteration = VertexCentricIteration.withEdges(
				edges, vertexUpdateFunction, messagingFunction, maximumNumberOfIterations);

		iteration.configure(parameters);

		DataSet<Vertex<K, VV>> newVertices = vertices.runOperation(iteration);

		return new Graph<K, VV, EV>(newVertices, this.edges, this.context);
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
	 * @return the updated Graph after the vertex-centric iteration has converged or
	 * after maximumNumberOfIterations.
	 */
	public <M> Graph<K, VV, EV> runGatherSumApplyIteration(
			GatherFunction<VV, EV, M> gatherFunction, SumFunction<VV, EV, M> sumFunction,
			ApplyFunction<K, VV, M> applyFunction, int maximumNumberOfIterations) {

		GatherSumApplyIteration<K, VV, EV, M> iteration = GatherSumApplyIteration.withEdges(
				edges, gatherFunction, sumFunction, applyFunction, maximumNumberOfIterations);

		DataSet<Vertex<K, VV>> newVertices = vertices.runOperation(iteration);

		return new Graph<K, VV, EV>(newVertices, this.edges, this.context);
	}

	public Graph<K, VV, EV> run(GraphAlgorithm<K, VV, EV> algorithm) throws Exception {
		return algorithm.run(this);
	}

	/**
	 * Compute an aggregate over the neighbors (edges and vertices) of each
	 * vertex. The function applied on the neighbors has access to the vertex
	 * value.
	 * 
	 * @param neighborsFunction the function to apply to the neighborhood
	 * @param direction the edge direction (in-, out-, all-)
	 * @param <T> the output type
	 * @return a dataset of a T
	 * @throws IllegalArgumentException
	 */
	public <T> DataSet<T> groupReduceOnNeighbors(NeighborsFunctionWithVertexValue<K, VV, EV, T> neighborsFunction,
												EdgeDirection direction) throws IllegalArgumentException {
		switch (direction) {
		case IN:
			// create <edge-sourceVertex> pairs
			DataSet<Tuple2<Edge<K, EV>, Vertex<K, VV>>> edgesWithSources = edges
					.join(this.vertices).where(0).equalTo(0);
			return vertices.coGroup(edgesWithSources)
					.where(0).equalTo("f0.f1")
					.with(new ApplyNeighborCoGroupFunction<K, VV, EV, T>(neighborsFunction));
		case OUT:
			// create <edge-targetVertex> pairs
			DataSet<Tuple2<Edge<K, EV>, Vertex<K, VV>>> edgesWithTargets = edges
					.join(this.vertices).where(1).equalTo(0);
			return vertices.coGroup(edgesWithTargets)
					.where(0).equalTo("f0.f0")
					.with(new ApplyNeighborCoGroupFunction<K, VV, EV, T>(neighborsFunction));
		case ALL:
			// create <edge-sourceOrTargetVertex> pairs
			DataSet<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> edgesWithNeighbors = edges
					.flatMap(new EmitOneEdgeWithNeighborPerNode<K, VV, EV>())
					.join(this.vertices).where(1).equalTo(0)
					.with(new ProjectEdgeWithNeighbor<K, VV, EV>());

			return vertices.coGroup(edgesWithNeighbors)
					.where(0).equalTo(0)
					.with(new ApplyCoGroupFunctionOnAllNeighbors<K, VV, EV, T>(neighborsFunction));
		default:
			throw new IllegalArgumentException("Illegal edge direction");
		}
	}

	/**
	 * Compute an aggregate over the neighbors (edges and vertices) of each
	 * vertex. The function applied on the neighbors only has access to the
	 * vertex id (not the vertex value).
	 * 
	 * @param neighborsFunction the function to apply to the neighborhood
	 * @param direction the edge direction (in-, out-, all-)
	 * @param <T> the output type
	 * @return a dataset of a T
	 * @throws IllegalArgumentException
	 */
	public <T> DataSet<T> groupReduceOnNeighbors(NeighborsFunction<K, VV, EV, T> neighborsFunction,
												EdgeDirection direction) throws IllegalArgumentException {
		switch (direction) {
		case IN:
			// create <edge-sourceVertex> pairs
			DataSet<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> edgesWithSources = edges
					.join(this.vertices).where(0).equalTo(0)
					.with(new ProjectVertexIdJoin<K, VV, EV>(1));
			return edgesWithSources.groupBy(0).reduceGroup(
					new ApplyNeighborGroupReduceFunction<K, VV, EV, T>(neighborsFunction));
		case OUT:
			// create <edge-targetVertex> pairs
			DataSet<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> edgesWithTargets = edges
					.join(this.vertices).where(1).equalTo(0)
					.with(new ProjectVertexIdJoin<K, VV, EV>(0));
			return edgesWithTargets.groupBy(0).reduceGroup(
					new ApplyNeighborGroupReduceFunction<K, VV, EV, T>(neighborsFunction));
		case ALL:
			// create <edge-sourceOrTargetVertex> pairs
			DataSet<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> edgesWithNeighbors = edges
					.flatMap(new EmitOneEdgeWithNeighborPerNode<K, VV, EV>())
					.join(this.vertices).where(1).equalTo(0)
					.with(new ProjectEdgeWithNeighbor<K, VV, EV>());

			return edgesWithNeighbors.groupBy(0).reduceGroup(
					new ApplyNeighborGroupReduceFunction<K, VV, EV, T>(neighborsFunction));
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

	private static final class ProjectVertexWithNeighborValueJoin<K, VV, EV>
			implements FlatJoinFunction<Edge<K, EV>, Vertex<K, VV>, Tuple2<K, VV>> {

		private int fieldPosition;

		public ProjectVertexWithNeighborValueJoin(int position) {
			this.fieldPosition = position;
		}

		@SuppressWarnings("unchecked")
		public void join(Edge<K, EV> edge, Vertex<K, VV> otherVertex, 
				Collector<Tuple2<K, VV>> out) {
			out.collect(new Tuple2<K, VV>((K) edge.getField(fieldPosition), otherVertex.getValue()));
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
			out.collect(new Tuple3<K, Edge<K, EV>, Vertex<K, VV>>((K) edge.getField(fieldPosition), edge, otherVertex));
		}
	}

	private static final class ProjectNeighborValue<K, VV, EV> implements FlatJoinFunction<
		Tuple3<K, K, Edge<K, EV>>, Vertex<K, VV>, Tuple2<K, VV>> {

		public void join(Tuple3<K, K, Edge<K, EV>> keysWithEdge, Vertex<K, VV> neighbor,
				Collector<Tuple2<K, VV>> out) {

			out.collect(new Tuple2<K, VV>(keysWithEdge.f0, neighbor.getValue()));
		}
	}

	private static final class ProjectEdgeWithNeighbor<K, VV, EV> implements FlatJoinFunction<
		Tuple3<K, K, Edge<K, EV>>, Vertex<K, VV>, Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> {

		public void join(Tuple3<K, K, Edge<K, EV>> keysWithEdge, Vertex<K, VV> neighbor,
						Collector<Tuple3<K, Edge<K, EV>, Vertex<K, VV>>> out) {
			out.collect(new Tuple3<K, Edge<K, EV>, Vertex<K, VV>>(keysWithEdge.f0, keysWithEdge.f2, neighbor));
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
			function.iterateNeighbors(vertex.iterator().next(),	neighbors, out);
		}

		@Override
		public TypeInformation<T> getProducedType() {
			return TypeExtractor.createTypeInfo(NeighborsFunctionWithVertexValue.class,	function.getClass(), 3, null, null);
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
					return new Tuple2<Edge<K, EV>, Vertex<K, VV>>(next.f1, next.f2);
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

			function.iterateNeighbors(vertex.iterator().next(), neighborsIterable, out);
		}

		@Override
		public TypeInformation<T> getProducedType() {
			return TypeExtractor.createTypeInfo(NeighborsFunctionWithVertexValue.class,	function.getClass(), 3, null, null);
		}
	}

	/**
	 * Compute an aggregate over the neighbor values of each
	 * vertex.
	 *
	 * @param reduceNeighborsFunction the function to apply to the neighborhood
	 * @param direction the edge direction (in-, out-, all-)
	 * @return a Dataset containing one value per vertex (vertex id, aggregate vertex value)
	 * @throws IllegalArgumentException
	 */
	public DataSet<Tuple2<K, VV>> reduceOnNeighbors(ReduceNeighborsFunction<VV> reduceNeighborsFunction,
									EdgeDirection direction) throws IllegalArgumentException {
		switch (direction) {
			case IN:
				// create <vertex-source value> pairs
				final DataSet<Tuple2<K, VV>> verticesWithSourceNeighborValues = edges
						.join(this.vertices).where(0).equalTo(0)
						.with(new ProjectVertexWithNeighborValueJoin<K, VV, EV>(1));
				return verticesWithSourceNeighborValues.groupBy(0).reduce(new ApplyNeighborReduceFunction<K, VV>(
						reduceNeighborsFunction));
			case OUT:
				// create <vertex-target value> pairs
				DataSet<Tuple2<K, VV>> verticesWithTargetNeighborValues = edges
						.join(this.vertices).where(1).equalTo(0)
						.with(new ProjectVertexWithNeighborValueJoin<K, VV, EV>(0));
				return verticesWithTargetNeighborValues.groupBy(0).reduce(new ApplyNeighborReduceFunction<K, VV>(
						reduceNeighborsFunction));
			case ALL:
				// create <vertex-neighbor value> pairs
				DataSet<Tuple2<K, VV>> verticesWithNeighborValues = edges
						.flatMap(new EmitOneEdgeWithNeighborPerNode<K, VV, EV>())
						.join(this.vertices).where(1).equalTo(0)
						.with(new ProjectNeighborValue<K, VV, EV>());

				return verticesWithNeighborValues.groupBy(0).reduce(new ApplyNeighborReduceFunction<K, VV>(
						reduceNeighborsFunction));
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
			first.setField(function.reduceNeighbors(first.f1, second.f1), 1);
			return first;
		}
	}

	/**
	 * Compute an aggregate over the edge values of each vertex.
	 *
	 * @param reduceEdgesFunction
	 *            the function to apply to the neighborhood
	 * @param direction
	 *            the edge direction (in-, out-, all-)
	 * @return a Dataset containing one value per vertex(vertex key, aggegate edge value)
	 * @throws IllegalArgumentException
	 */
	public DataSet<Tuple2<K, EV>> reduceOnEdges(ReduceEdgesFunction<EV> reduceEdgesFunction,
								EdgeDirection direction) throws IllegalArgumentException {

		switch (direction) {
			case IN:
				return edges.map(new ProjectVertexWithEdgeValueMap<K, EV>(1))
						.withForwardedFields("f1->f0")
						.groupBy(0).reduce(new ApplyReduceFunction<K, EV>(reduceEdgesFunction));
			case OUT:
				return edges.map(new ProjectVertexWithEdgeValueMap<K, EV>(0))
						.withForwardedFields("f0->f0")
						.groupBy(0).reduce(new ApplyReduceFunction<K, EV>(reduceEdgesFunction));
			case ALL:
				return edges.flatMap(new EmitOneVertexWithEdgeValuePerNode<K, VV, EV>())
						.withForwardedFields("f2->f1")
						.groupBy(0).reduce(new ApplyReduceFunction<K, EV>(reduceEdgesFunction));
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
			first.setField(function.reduceEdges(first.f1, second.f1), 1);
			return first;
		}
	}
}
