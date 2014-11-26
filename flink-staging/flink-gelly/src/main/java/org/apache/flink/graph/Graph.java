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
 * distributed under the License is distributed on an "AS IS" BASIS,K extends Serializablr & Comparable, 
    	VV implements Serializable, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package flink.graphs;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.FunctionAnnotation.ConstantFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ConstantFieldsFirst;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.spargel.java.MessagingFunction;
import org.apache.flink.spargel.java.VertexCentricIteration;
import org.apache.flink.spargel.java.VertexUpdateFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.types.NullValue;

@SuppressWarnings("serial")
public class Graph<K extends Comparable<K> & Serializable, VV extends Serializable,
	EV extends Serializable> implements Serializable {

    private final ExecutionEnvironment context;

	private final DataSet<Vertex<K, VV>> vertices;

	private final DataSet<Edge<K, EV>> edges;

	private boolean isUndirected;

	private static TypeInformation<?> keyType;
	private static TypeInformation<?> vertexValueType;
	private static TypeInformation<?> edgeValueType;


	public Graph(DataSet<Vertex<K, VV>> vertices, DataSet<Edge<K, EV>> edges, ExecutionEnvironment context) {

		/** a graph is directed by default */
		this(vertices, edges, context, false);
	}

	public Graph(DataSet<Vertex<K, VV>> vertices, DataSet<Edge<K, EV>> edges, ExecutionEnvironment context,
			boolean undirected) {
		this.vertices = vertices;
		this.edges = edges;
        this.context = context;
		this.isUndirected = undirected;
		
		Graph.keyType = ((TupleTypeInfo<?>) vertices.getType()).getTypeAt(0);
		Graph.vertexValueType = ((TupleTypeInfo<?>) vertices.getType()).getTypeAt(1);
		Graph.edgeValueType = ((TupleTypeInfo<?>) edges.getType()).getTypeAt(2);
	}

	/**
	 * Function that checks whether a graph's ids are valid
	 * @return
 	 */
	public DataSet<Boolean> validate(GraphValidator<K, VV, EV> validator) {

		return validator.validate(this);
	}

	public DataSet<Vertex<K, VV>> getVertices() {
		return vertices;
	}

	public DataSet<Edge<K, EV>> getEdges() {
		return edges;
	}
    
    /**
     * Apply a function to the attribute of each vertex in the graph.
     * @param mapper
     * @return
     */
    public <NV extends Serializable> DataSet<Vertex<K, NV>> mapVertices(final MapFunction<VV, NV> mapper) {
        return vertices.map(new ApplyMapperToVertexWithType<K, VV, NV>(mapper));
    }
    
    private static final class ApplyMapperToVertexWithType<K extends Comparable<K> & Serializable, 
    	VV extends Serializable, NV extends Serializable> implements MapFunction
		<Vertex<K, VV>, Vertex<K, NV>>, ResultTypeQueryable<Vertex<K, NV>> {
	
		private MapFunction<VV, NV> innerMapper;
		
		public ApplyMapperToVertexWithType(MapFunction<VV, NV> theMapper) {
			this.innerMapper = theMapper;
		}
		
		public Vertex<K, NV> map(Vertex<K, VV> value) throws Exception {
			return new Vertex<K, NV>(value.f0, innerMapper.map(value.f1));
		}
	
		@Override
		public TypeInformation<Vertex<K, NV>> getProducedType() {
			@SuppressWarnings("unchecked")
			TypeInformation<NV> newVertexValueType = TypeExtractor.getMapReturnTypes(innerMapper, 
					(TypeInformation<VV>)vertexValueType);
			
			return new TupleTypeInfo<Vertex<K, NV>>(keyType, newVertexValueType);
		}
    }
    
    /**
     * Apply a function to the attribute of each edge in the graph.
     * @param mapper
     * @return 
     */
    public <NV extends Serializable> DataSet<Edge<K, NV>> mapEdges(final MapFunction<EV, NV> mapper) {
        return edges.map(new ApplyMapperToEdgeWithType<K, EV, NV>(mapper));
    }
    
    private static final class ApplyMapperToEdgeWithType<K extends Comparable<K> & Serializable, 
		EV extends Serializable, NV extends Serializable> implements MapFunction
		<Edge<K, EV>, Edge<K, NV>>, ResultTypeQueryable<Edge<K, NV>> {
	
		private MapFunction<EV, NV> innerMapper;
		
		public ApplyMapperToEdgeWithType(MapFunction<EV, NV> theMapper) {
			this.innerMapper = theMapper;
		}
		
		public Edge<K, NV> map(Edge<K, EV> value) throws Exception {
			return new Edge<K, NV>(value.f0, value.f1, innerMapper.map(value.f2));
		}
	
		@Override
		public TypeInformation<Edge<K, NV>> getProducedType() {
			@SuppressWarnings("unchecked")
			TypeInformation<NV> newEdgeValueType = TypeExtractor.getMapReturnTypes(innerMapper, 
					(TypeInformation<EV>)edgeValueType);
			
			return new TupleTypeInfo<Edge<K, NV>>(keyType, keyType, newEdgeValueType);
		}
    }

    /**
     * Apply value-based filtering functions to the graph 
     * and return a sub-graph that satisfies the predicates
     * for both vertex values and edge values.
     * @param vertexFilter
     * @param edgeFilter
     * @return
     */
    public Graph<K, VV, EV> subgraph(FilterFunction<VV> vertexFilter, FilterFunction<EV> edgeFilter) {

        DataSet<Vertex<K, VV>> filteredVertices = this.vertices.filter(
        		new ApplyVertexFilter<K, VV>(vertexFilter));

        DataSet<Edge<K, EV>> remainingEdges = this.edges.join(filteredVertices)
        		.where(0).equalTo(0)
        		.with(new ProjectEdge<K, VV, EV>())
        		.join(filteredVertices).where(1).equalTo(0)
        		.with(new ProjectEdge<K, VV, EV>());

        DataSet<Edge<K, EV>> filteredEdges = remainingEdges.filter(
        		new ApplyEdgeFilter<K, EV>(edgeFilter));

        return new Graph<K, VV, EV>(filteredVertices, filteredEdges, this.context);
    }

	/**
	 * Apply value-based filtering functions to the graph
	 * and return a sub-graph that satisfies the predicates
	 * only for the vertices.
	 * @param vertexFilter
	 * @return
	 */
	public Graph<K, VV, EV> filterOnVertices(FilterFunction<VV> vertexFilter) {

		DataSet<Vertex<K, VV>> filteredVertices = this.vertices.filter(
				new ApplyVertexFilter<K, VV>(vertexFilter));

		DataSet<Edge<K, EV>> remainingEdges = this.edges.join(filteredVertices)
				.where(0).equalTo(0)
				.with(new ProjectEdge<K, VV, EV>())
				.join(filteredVertices).where(1).equalTo(0)
				.with(new ProjectEdge<K, VV, EV>());

		return new Graph<K, VV, EV>(filteredVertices, remainingEdges, this.context);
	}

	/**
	 * Apply value-based filtering functions to the graph
	 * and return a sub-graph that satisfies the predicates
	 * only for the edges.
	 * @param edgeFilter
	 * @return
	 */
	public Graph<K, VV, EV> filterOnEdges(FilterFunction<EV> edgeFilter) {
		DataSet<Edge<K, EV>> filteredEdges = this.edges.filter(
				new ApplyEdgeFilter<K, EV>(edgeFilter));

		return new Graph<K, VV, EV>(this.vertices, filteredEdges, this.context);
	}
    
    @ConstantFieldsFirst("0->0;1->1;2->2")
    private static final class ProjectEdge<K extends Comparable<K> & Serializable, 
	VV extends Serializable, EV extends Serializable> implements FlatJoinFunction<Edge<K,EV>, Vertex<K,VV>, 
		Edge<K,EV>> {
		public void join(Edge<K, EV> first,
				Vertex<K, VV> second, Collector<Edge<K, EV>> out) {
			out.collect(first);
		}
    }
    
    private static final class ApplyVertexFilter<K extends Comparable<K> & Serializable, 
    	VV extends Serializable> implements FilterFunction<Vertex<K, VV>> {

    	private FilterFunction<VV> innerFilter;
    	
    	public ApplyVertexFilter(FilterFunction<VV> theFilter) {
    		this.innerFilter = theFilter;
    	}

		public boolean filter(Vertex<K, VV> value) throws Exception {
			return innerFilter.filter(value.f1);
		}
    	
    }

    private static final class ApplyEdgeFilter<K extends Comparable<K> & Serializable, 
		EV extends Serializable> implements FilterFunction<Edge<K, EV>> {

    	private FilterFunction<EV> innerFilter;
    	
    	public ApplyEdgeFilter(FilterFunction<EV> theFilter) {
    		this.innerFilter = theFilter;
    	}    	
        public boolean filter(Edge<K, EV> value) throws Exception {
            return innerFilter.filter(value.f2);
        }
    }

    /**
     * Return the out-degree of all vertices in the graph
     * @return A DataSet of Tuple2<vertexId, outDegree>
     */
	public DataSet<Tuple2<K, Long>> outDegrees() {

		return vertices.coGroup(edges).where(0).equalTo(0)
				.with(new CountNeighborsCoGroup<K, VV, EV>());
	}

	private static final class CountNeighborsCoGroup<K extends Comparable<K> & Serializable, 
		VV extends Serializable, EV extends Serializable> implements CoGroupFunction<Vertex<K, VV>, 
		Edge<K, EV>, Tuple2<K, Long>> {
		@SuppressWarnings("unused")
		public void coGroup(Iterable<Vertex<K, VV>> vertex,
				Iterable<Edge<K, EV>> outEdges, Collector<Tuple2<K, Long>> out) {
			long count = 0;
			for (Edge<K, EV> edge : outEdges) {
				count++;
			}
			out.collect(new Tuple2<K, Long>(vertex.iterator().next().f0, count));
		}
	}
	
	/**
	 * Return the in-degree of all vertices in the graph
	 * @return A DataSet of Tuple2<vertexId, inDegree>
	 */
	public DataSet<Tuple2<K, Long>> inDegrees() {

		return vertices.coGroup(edges).where(0).equalTo(1)
				.with(new CountNeighborsCoGroup<K, VV, EV>());
	}

	/**
	 * Return the degree of all vertices in the graph
	 * @return A DataSet of Tuple2<vertexId, degree>
	 */
	public DataSet<Tuple2<K, Long>> getDegrees() {
		return outDegrees().union(inDegrees()).groupBy(0).sum(1);
	}

	/**
	 * Convert the directed graph into an undirected graph
	 * by adding all inverse-direction edges.
	 *
	 */
	public Graph<K, VV, EV> getUndirected() throws UnsupportedOperationException {
		if (this.isUndirected) {
			throw new UnsupportedOperationException("The graph is already undirected.");
		}
		else {
			DataSet<Edge<K, EV>> undirectedEdges =
					edges.union(edges.map(new ReverseEdgesMap<K, EV>()));
			return new Graph<K, VV, EV>(vertices, undirectedEdges, this.context, true);
			}
	}
	
	/**
	 * Utility function that allows each vertex of the graph
	 * to access its out-neighbors
	 * @param edgesFunction the function to apply to the neighborhood
	 * @return
	 */
	public <T> DataSet<T> foreachEdge(OutEdgesFunction<K, VV, EV, T> edgesFunction) {
		return vertices.coGroup(edges).where(0).equalTo(0).with(
				new ApplyCoGroupFunction<K, VV, EV, T>(edgesFunction));
	}

	/**
	 * 
	 * @param <K>
	 * @param <VV>
	 * @param <EV>
	 * @param <T>
	 */
	private static final class ApplyCoGroupFunction<K extends Comparable<K> & Serializable, 
		VV extends Serializable, EV extends Serializable, T> 
		implements CoGroupFunction<Tuple2<K, VV>, Tuple3<K, K, EV>, T>,
		ResultTypeQueryable<T> {
		
		private OutEdgesFunction<K, VV, EV, T> function;
		
		public ApplyCoGroupFunction (OutEdgesFunction<K, VV, EV, T> fun) {
			this.function = fun;
		}
		public void coGroup(Iterable<Tuple2<K, VV>> vertex,
				Iterable<Tuple3<K, K, EV>> outEdges, Collector<T> out) throws Exception {
			out.collect(function.iterateOutEdges(vertex.iterator().next(), outEdges));
		}
		@Override
		public TypeInformation<T> getProducedType() {
			// TODO Auto-generated method stub
			return null;
		}		
	}

	@ConstantFields("0->1;1->0;2->2")
	private static final class ReverseEdgesMap<K extends Comparable<K> & Serializable, 
		EV extends Serializable> implements MapFunction<Edge<K, EV>,
		Edge<K, EV>> {

		public Edge<K, EV> map(Edge<K, EV> value) {
			return new Edge<K, EV>(value.f1, value.f0, value.f2);
		}
	}

	/**
	 * Reverse the direction of the edges in the graph
	 * @return a new graph with all edges reversed
	 * @throws UnsupportedOperationException
	 */
	public Graph<K, VV, EV> reverse() throws UnsupportedOperationException {
		if (this.isUndirected) {
			throw new UnsupportedOperationException("The graph is already undirected.");
		}
		else {
			DataSet<Edge<K, EV>> undirectedEdges = edges.map(new ReverseEdgesMap<K, EV>());
			return new Graph<K, VV, EV>(vertices, (DataSet<Edge<K, EV>>) undirectedEdges, this.context, true);
		}
	}

	/**
	 * Creates a graph from a dataset of vertices and a dataset of edges
	 * @param vertices
	 * @param edges
	 * @return
	 */
	public static <K extends Comparable<K> & Serializable, VV extends Serializable,
		EV extends Serializable> Graph<K, VV, EV>
		create(DataSet<Vertex<K, VV>> vertices, DataSet<Edge<K, EV>> edges, 
				ExecutionEnvironment context) {
		return new Graph<K, VV, EV>(vertices, edges, context);
	}
	
	/**
	 * Creates a graph from a DataSet of edges.
	 * Vertices are created automatically and their values are set to NullValue.
	 * @param edges
	 * @return
	 */
	public static <K extends Comparable<K> & Serializable, EV extends Serializable> 
		Graph<K, NullValue, EV> create(DataSet<Edge<K, EV>> edges, ExecutionEnvironment context) {
		DataSet<Vertex<K, NullValue>> vertices = 
				edges.flatMap(new EmitSrcAndTarget<K, EV>()).distinct(); 
		return new Graph<K, NullValue, EV>(vertices, edges, context);
	}
	
	/**
	 * Creates a graph from a DataSet of edges.
	 * Vertices are created automatically and their values are set
	 * by applying the provided map function to the vertex ids.
	 * @param edges the input edges
	 * @param mapper the map function to set the initial vertex value
	 * @return
	 */
	public static <K extends Comparable<K> & Serializable, VV extends Serializable,	EV extends Serializable> 
		Graph<K, VV, EV> create(DataSet<Edge<K, EV>> edges, final MapFunction<K, VV> mapper, 
				ExecutionEnvironment context) {
		DataSet<Vertex<K, VV>> vertices = 
				edges.flatMap(new EmitSrcAndTargetAsTuple1<K, EV>())
				.distinct().map(new ApplyMapperToVertexValuesWithType<K, VV>(mapper));
		return new Graph<K, VV, EV>(vertices, edges, context);
	}
	
	private static final class ApplyMapperToVertexValuesWithType<K extends Comparable<K> & Serializable, 
		VV extends Serializable> implements MapFunction
		<Tuple1<K>, Vertex<K, VV>>, ResultTypeQueryable<Vertex<K, VV>> {

		private MapFunction<K, VV> innerMapper;
		
		public ApplyMapperToVertexValuesWithType(MapFunction<K, VV> theMapper) {
			this.innerMapper = theMapper;
		}
		
		public Vertex<K, VV> map(Tuple1<K> value) throws Exception {
			return new Vertex<K, VV>(value.f0, innerMapper.map(value.f0));
		}
	
		@Override
		public TypeInformation<Vertex<K, VV>> getProducedType() {
			@SuppressWarnings("unchecked")
			TypeInformation<VV> newVertexValueType = TypeExtractor.getMapReturnTypes(innerMapper, 
					(TypeInformation<K>)keyType);
			
			return new TupleTypeInfo<Vertex<K, VV>>(keyType, newVertexValueType);
		}
	}
	
	private static final class EmitSrcAndTarget<K extends Comparable<K> & Serializable, EV extends Serializable>
		implements FlatMapFunction<Edge<K, EV>, Vertex<K, NullValue>> {
		public void flatMap(Edge<K, EV> edge,
				Collector<Vertex<K, NullValue>> out) {

				out.collect(new Vertex<K, NullValue>(edge.f0, NullValue.getInstance()));
				out.collect(new Vertex<K, NullValue>(edge.f1, NullValue.getInstance()));
		}	
	}

	private static final class EmitSrcAndTargetAsTuple1<K extends Comparable<K> & Serializable, 
		EV extends Serializable> implements FlatMapFunction<Edge<K, EV>, Tuple1<K>> {
		public void flatMap(Edge<K, EV> edge, Collector<Tuple1<K>> out) {

			out.collect(new Tuple1<K>(edge.f0));
			out.collect(new Tuple1<K>(edge.f1));
		}	
	}

	/**
	 * Read and create the graph Tuple2 dataset from a csv file
	 * @param env
	 * @param filePath
	 * @param delimiter
	 * @param Tuple2IdClass
	 * @param Tuple2ValueClass
	 * @return
	 */
	public static <K extends Comparable<K> & Serializable, VV extends Serializable>
		DataSet<Tuple2<K, VV>> readTuple2CsvFile(ExecutionEnvironment env, String filePath,
			char delimiter, Class<K> Tuple2IdClass, Class<VV> Tuple2ValueClass) {

		CsvReader reader = new CsvReader(filePath, env);
		DataSet<Tuple2<K, VV>> vertices = reader.fieldDelimiter(delimiter).types(Tuple2IdClass, Tuple2ValueClass)
		.map(new MapFunction<Tuple2<K, VV>, Tuple2<K, VV>>() {

			public Tuple2<K, VV> map(Tuple2<K, VV> value) throws Exception {
				return (Tuple2<K, VV>)value;
			}
		});
		return vertices;
	}

    /**
     * @return Singleton DataSet containing the vertex count
     */
	public DataSet<Integer> numberOfVertices () {
        return GraphUtils.count(vertices, context);
    }

    /**
     *
     * @return Singleton DataSet containing the edge count
     */
	public DataSet<Integer> numberOfEdges () {
        return GraphUtils.count(edges, context);
    }

    /**
     *
     * @return The IDs of the vertices as DataSet
     */
    public DataSet<K> getVertexIds () {
        return vertices.map(new ExtractVertexIDMapper<K, VV>());
    }
    
    private static final class ExtractVertexIDMapper<K extends Comparable<K> & Serializable, 
    	VV extends Serializable> implements MapFunction<Vertex<K, VV>, K> {
            @Override
            public K map(Vertex<K, VV> vertex) {
                return vertex.f0;
            }
    }

    public DataSet<Tuple2<K, K>> getEdgeIds () {
        return edges.map(new ExtractEdgeIDsMapper<K, EV>());
    }
    
    private static final class ExtractEdgeIDsMapper<K extends Comparable<K> & Serializable, 
    	EV extends Serializable> implements MapFunction<Edge<K, EV>, Tuple2<K, K>> {
            @Override
            public Tuple2<K, K> map(Edge<K, EV> edge) throws Exception {
                return new Tuple2<K,K>(edge.f0, edge.f1);
            }
    }

    /**
     * Checks the weak connectivity of a graph.
     * @param maxIterations the maximum number of iterations for the inner delta iteration
     * @return true if the graph is weakly connected.
     */
	public DataSet<Boolean> isWeaklyConnected (int maxIterations) {
		Graph<K, VV, EV> graph;
		
		if (!(this.isUndirected)) {
			// first, convert to an undirected graph
			graph = this.getUndirected();
		}
		else {
			graph = this;
		}

        DataSet<K> vertexIds = graph.getVertexIds();
        DataSet<Tuple2<K,K>> verticesWithInitialIds = vertexIds
                .map(new DuplicateVertexIDMapper<K>());

        DataSet<Tuple2<K,K>> edgeIds = graph.getEdgeIds();

        DeltaIteration<Tuple2<K,K>, Tuple2<K,K>> iteration = verticesWithInitialIds
                .iterateDelta(verticesWithInitialIds, maxIterations, 0);

        DataSet<Tuple2<K, K>> changes = iteration.getWorkset()
                .join(edgeIds).where(0).equalTo(0)
                .with(new FindNeighborsJoin<K>())
                .groupBy(0)
                .aggregate(Aggregations.MIN, 1)
                .join(iteration.getSolutionSet()).where(0).equalTo(0)
                .with(new VertexWithNewComponentJoin<K>());

        DataSet<Tuple2<K, K>> components = iteration.closeWith(changes, changes);
        DataSet<Boolean> result = GraphUtils.count(components.groupBy(1).reduceGroup(
        		new EmitFirstReducer<K>()), context).map(new CheckIfOneComponentMapper());	
        return result;
    }
	
	private static final class DuplicateVertexIDMapper<K> implements MapFunction<K, Tuple2<K, K>> {
            @Override
            public Tuple2<K, K> map(K k) {
                return new Tuple2<K, K>(k, k);
            }
	}
	
	private static final class FindNeighborsJoin<K> implements JoinFunction<Tuple2<K, K>, Tuple2<K, K>, 
		Tuple2<K, K>> {
        @Override
        public Tuple2<K, K> join(Tuple2<K, K> vertexWithComponent, Tuple2<K, K> edge) {
            return new Tuple2<K,K>(edge.f1, vertexWithComponent.f1);
        }
	}

	private static final class VertexWithNewComponentJoin<K extends Comparable<K>> 
		implements FlatJoinFunction<Tuple2<K, K>, Tuple2<K, K>, Tuple2<K, K>> {
        @Override
        public void join(Tuple2<K, K> candidate, Tuple2<K, K> old, Collector<Tuple2<K, K>> out) {
            if (candidate.f1.compareTo(old.f1) < 0) {
                out.collect(candidate);
            }
        }
	}
	
	private static final class EmitFirstReducer<K> implements 
		GroupReduceFunction<Tuple2<K, K>, Tuple2<K, K>> {
		public void reduce(Iterable<Tuple2<K, K>> values, Collector<Tuple2<K, K>> out) {
			out.collect(values.iterator().next());			
		}
	}
	
	private static final class CheckIfOneComponentMapper implements MapFunction<Integer, Boolean> {
        @Override
        public Boolean map(Integer n) {
        	return (n == 1);
        }
	}
	
    public Graph<K, VV, EV> fromCollection (Collection<Vertex<K,VV>> vertices, Collection<Edge<K,EV>> edges) {

		DataSet<Vertex<K, VV>> v = context.fromCollection(vertices);
		DataSet<Edge<K, EV>> e = context.fromCollection(edges);

		return new Graph<K, VV, EV>(v, e, context);
    }

    /**
     * Adds the input vertex and edges to the graph.
     * If the vertex already exists in the graph, it will not be added again,
     * but the given edges will. 
     * @param vertex
     * @param edges
     * @return
     */
    @SuppressWarnings("unchecked")
	public Graph<K, VV, EV> addVertex (final Vertex<K,VV> vertex, List<Edge<K, EV>> edges) {
    	DataSet<Vertex<K, VV>> newVertex = this.context.fromElements(vertex);

    	// Take care of empty edge set
    	if (edges.isEmpty()) {
    		return Graph.create(getVertices().union(newVertex).distinct(), getEdges(), context);
    	}

    	// Add the vertex and its edges
    	DataSet<Vertex<K, VV>> newVertices = getVertices().union(newVertex).distinct();
    	DataSet<Edge<K, EV>> newEdges = getEdges().union(context.fromCollection(edges));

    	return Graph.create(newVertices, newEdges, context);
    }

    /** 
     * Adds the given edge to the graph.
     * If the source and target vertices do not exist in the graph,
     * they will also be added.
     * @param source
     * @param target
     * @param edgeValue
     * @return
     */
    public Graph<K, VV, EV> addEdge (Vertex<K,VV> source, Vertex<K,VV> target, EV edgeValue) {
    	Graph<K,VV,EV> partialGraph = this.fromCollection(Arrays.asList(source, target),
				Arrays.asList(new Edge<K, EV>(source.f0, target.f0, edgeValue)));
        return this.union(partialGraph);
    }

    /**
     * Removes the given vertex and its edges from the graph.
     * @param vertex
     * @return
     */
    public Graph<K, VV, EV> removeVertex (Vertex<K,VV> vertex) {
		DataSet<Vertex<K, VV>> newVertices = getVertices().filter(
				new RemoveVertexFilter<K, VV>(vertex));
		DataSet<Edge<K, EV>> newEdges = getEdges().filter(
				new VertexRemovalEdgeFilter<K, VV, EV>(vertex));
        return new Graph<K, VV, EV>(newVertices, newEdges, this.context);
    }
    
    private static final class RemoveVertexFilter<K extends Comparable<K> & Serializable, 
		VV extends Serializable> implements FilterFunction<Vertex<K, VV>> {

    	private Vertex<K, VV> vertexToRemove;

        public RemoveVertexFilter(Vertex<K, VV> vertex) {
        	vertexToRemove = vertex;
		}

        @Override
        public boolean filter(Vertex<K, VV> vertex) throws Exception {
        	return !vertex.f0.equals(vertexToRemove.f0);
        }
    }
    
    private static final class VertexRemovalEdgeFilter<K extends Comparable<K> & Serializable, 
    	VV extends Serializable, EV extends Serializable> implements FilterFunction<Edge<K, EV>> {

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
     * @param edge
     * @return
     */
    public Graph<K, VV, EV> removeEdge (Edge<K, EV> edge) {
		DataSet<Edge<K, EV>> newEdges = getEdges().filter(
				new EdgeRemovalEdgeFilter<K, EV>(edge));
        return new Graph<K, VV, EV>(this.getVertices(), newEdges, this.context);
    }
    
    private static final class EdgeRemovalEdgeFilter<K extends Comparable<K> & Serializable, 
		EV extends Serializable> implements FilterFunction<Edge<K, EV>> {
    	private Edge<K, EV> edgeToRemove;

        public EdgeRemovalEdgeFilter(Edge<K, EV> edge) {
			edgeToRemove = edge;
		}

        @Override
        public boolean filter(Edge<K, EV> edge) {
    		return (!(edge.f0.equals(edgeToRemove.f0) 
    				&& edge.f1.equals(edgeToRemove.f1)));
        }
    }

    /**
     * Performs union on the vertices and edges sets of the input graphs
     * removing duplicate vertices but maintaining duplicate edges.
     * @param graph
     * @return
     */
    public Graph<K, VV, EV> union (Graph<K, VV, EV> graph) {
        DataSet<Vertex<K,VV>> unionedVertices = graph.getVertices().union(this.getVertices()).distinct();
        DataSet<Edge<K,EV>> unionedEdges = graph.getEdges().union(this.getEdges());
        return new Graph<K,VV,EV>(unionedVertices, unionedEdges, this.context);
    }

    /**
     * Runs a Vertex-Centric iteration on the graph.
     * @param vertexUpdateFunction
     * @param messagingFunction
     * @param maximumNumberOfIterations
     * @return
     */
    public <M>Graph<K, VV, EV> runVertexCentricIteration(VertexUpdateFunction<K, VV, M> vertexUpdateFunction,
    		MessagingFunction<K, VV, M, EV> messagingFunction, int maximumNumberOfIterations) {
    	DataSet<Tuple2<K, VV>> newVertices = vertices.map(new VertexToTuple2Map<K, VV>()).runOperation(
				VertexCentricIteration.withValuedEdges(edges.map(new EdgeToTuple3Map<K, EV>()),
						vertexUpdateFunction, messagingFunction, maximumNumberOfIterations));
		return new Graph<K, VV, EV>(newVertices.map(new Tuple2ToVertexMap<K, VV>()), edges, context);
    }

	/**
     	 * Creates a graph from the given vertex and edge collections
     	 * @param env
     	 * @param v the collection of vertices
         * @param e the collection of edges
         * @return a new graph formed from the set of edges and vertices
         */
	 public static <K extends Comparable<K> & Serializable, VV extends Serializable,
			EV extends Serializable> Graph<K, VV, EV> fromCollection(ExecutionEnvironment env, 
					Collection<Vertex<K, VV>> v, Collection<Edge<K, EV>> e) throws Exception {
		DataSet<Vertex<K, VV>> vertices = env.fromCollection(v);
		DataSet<Edge<K, EV>> edges = env.fromCollection(e);

		return Graph.create(vertices, edges, env);
	}

	/**
	 * Vertices may not have a value attached or may receive a value as a result of running the algorithm.
	 * @param env
	 * @param e the collection of edges
	 * @return a new graph formed from the edges, with no value for the vertices
	 */
	public static <K extends Comparable<K> & Serializable, VV extends Serializable,
			EV extends Serializable> Graph<K, NullValue, EV> fromCollection(ExecutionEnvironment env, 
					Collection<Edge<K, EV>> e) {

		DataSet<Edge<K, EV>> edges = env.fromCollection(e);

		return Graph.create(edges, env);
	}

	/**
	 * Vertices may have an initial value defined by a function.
	 * @param env
	 * @param e the collection of edges
	 * @return a new graph formed from the edges, with a custom value for the vertices,
	 * determined by the mapping function
	 */
	public static <K extends Comparable<K> & Serializable, VV extends Serializable,
			EV extends Serializable> Graph<K, VV, EV> fromCollection(ExecutionEnvironment env,
																	 Collection<Edge<K, EV>> e,
																	 final MapFunction<K, VV> mapper) {
		DataSet<Edge<K, EV>> edges = env.fromCollection(e);
		return Graph.create(edges, mapper, env);
	}

	public Graph<K,VV,EV> run (GraphAlgorithm algorithm) {
		return algorithm.run(this);
	}

}
